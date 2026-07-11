package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 区块申请客户端（主客户端侧，Phase 2；规范"特殊的区块加载"）。
 * <p>
 * 集成服务端每次要加载/生成一个区块，必须先向物理服务端申请占用
 * （CHUNK_CLAIM_REQUEST，服务端原子校验目标 + 四邻缓冲层）：
 * <ul>
 *   <li><b>授予 + 有服务端数据</b> → {@link #fetchChunkData} 拉取区块 NBT
 *       （规范"若该区块在服务端有数据……则由服务端发送给主客户端加载"）；</li>
 *   <li><b>授予 + 无数据</b> → 本地按种子生成（规范"若无数据，则由主客户端
 *       根据地图种子进行计算"）；</li>
 *   <li><b>被拒</b>（被其他组占用）→ 该区块保持未加载，按配置间隔自动重试
 *       （Phase 3 在此拒绝信号上接预连接/合并流程）。</li>
 * </ul>
 * 去重：同一区块的并发申请共享一个 future（ChunkMap 对每个区块只会发起一次
 * 加载，但防御重复无害）。重试由 {@link ThreadPools#scheduler()} 驱动，
 * 绝不阻塞集成服务端主线程。
 * <p>
 * {@link #shutdown} 时所有未决 future 以异常完成 —— 挂在上面的区块加载链
 * 随集成服务端一起停摆，不会泄漏。
 */
public final class ChunkClaimClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/chunk-claim");

    /** 区块数据拉取失败的重试上限（超过后继续按申请重试间隔慢速重试）。 */
    private static final int FETCH_FAST_RETRIES = 3;

    /** 区块 NBT 解压上限（与服务端侧一致；防解压炸弹）。 */
    private static final long MAX_CHUNK_NBT_BYTES = 64L * 1024 * 1024;

    /** 申请结果：hasServerData 决定"拉取数据"还是"种子生成"。 */
    public record ClaimOutcome(boolean hasServerData) {
    }

    private final ControlConnection conn;
    private final UUID groupId;
    private final int retrySeconds;

    /** 未决申请：key → 共享 future（授予后移除；被拒时保留并由定时重试驱动）。 */
    private final Map<ChunkKey, CompletableFuture<ClaimOutcome>> pending = new ConcurrentHashMap<>();

    /** 已授予且尚未释放的区块（上行服务据此过滤"释放后仍触发的存盘上行"）。 */
    private final Set<ChunkKey> claimed = ConcurrentHashMap.newKeySet();

    private volatile boolean shutdown;

    public ChunkClaimClient(ControlConnection conn, UUID groupId, int retrySeconds) {
        this.conn = conn;
        this.groupId = groupId;
        this.retrySeconds = Math.max(1, retrySeconds);
    }

    // ------------------------------------------------------------ 区块申请

    /**
     * 申请区块占用；被拒时自动按间隔重试，future 直到授予（或 shutdown）才完成。
     * 任意线程可调（非阻塞）；future 的完成线程是 Netty 事件循环或 scheduler，
     * 消费方不得在回调里做重活/阻塞。
     */
    public CompletableFuture<ClaimOutcome> claimWithRetry(ChunkKey key) {
        return pending.computeIfAbsent(key, k -> {
            CompletableFuture<ClaimOutcome> future = new CompletableFuture<>();
            attemptClaim(k, future);
            return future;
        });
    }

    /** 是否持有该区块的占用。 */
    public boolean isClaimed(ChunkKey key) {
        return claimed.contains(key);
    }

    /** 已授予区块的快照（预同步发送端遍历用；防御性拷贝）。 */
    public Set<ChunkKey> claimedSnapshot() {
        return Set.copyOf(claimed);
    }

    /**
     * 本地摘除占用记录（Phase 3 分离/合并：区块占用已在<b>服务端侧</b>迁移给
     * 其他组，本组不再拥有 —— 只清本地集合，不发 CHUNK_RELEASE：发了也会因
     * 占用者不符被拒）。摘除后该区块的迟到存盘上行被 {@link ChunkUploadService}
     * 过滤，不产生"上行被拒"日志噪音。
     */
    public void forgetLocal(ChunkKey key) {
        claimed.remove(key);
    }

    /** 整体摘除全部本地占用记录（合并让出方在关停集成服务端前调用）。 */
    public void forgetAllLocal() {
        claimed.clear();
    }

    /** 已占用区块数（诊断用）。 */
    public int claimedCount() {
        return claimed.size();
    }

    /** 发起一次申请；被拒/失败则安排下一轮重试。 */
    private void attemptClaim(ChunkKey key, CompletableFuture<ClaimOutcome> future) {
        if (shutdown) {
            future.completeExceptionally(new IllegalStateException("组运行时已关停"));
            pending.remove(key, future);
            return;
        }
        JsonObject req = new JsonObject();
        req.addProperty("dimension", key.dimension());
        req.addProperty("x", key.x());
        req.addProperty("z", key.z());
        req.addProperty("groupId", groupId.toString());
        conn.request(ControlMessage.of(MessageType.CHUNK_CLAIM_REQUEST, req))
                .whenComplete((resp, err) -> {
                    if (shutdown) {
                        future.completeExceptionally(new IllegalStateException("组运行时已关停"));
                        pending.remove(key, future);
                        return;
                    }
                    if (err != null || resp == null || resp.type() != MessageType.CHUNK_CLAIM_RESPONSE) {
                        // 网络失败/ERROR 应答：按被拒同样节奏重试（控制连接由会话层负责恢复）
                        LOGGER.debug("区块申请通信失败，{} 秒后重试: {} ({})",
                                retrySeconds, key.asString(), err != null ? err.toString() : "非预期应答");
                        scheduleRetry(key, future);
                        return;
                    }
                    if (JsonUtil.getBoolean(resp.json(), "granted", false)) {
                        claimed.add(key);
                        pending.remove(key, future);
                        future.complete(new ClaimOutcome(
                                JsonUtil.getBoolean(resp.json(), "hasServerData", false)));
                        return;
                    }
                    // 被拒：区块保持未加载（规范：其他组占用中），按间隔重试。
                    // blockingGroup 是规范"预连接"的入口 —— 转发给合并触发桥
                    // （MergeClient 挂接后决定是否发起 MERGE_REQUEST），并继续重试
                    // 作兜底：合并期间/失败后区块仍按原节奏申请。
                    String blockingGroupRaw = JsonUtil.getString(resp.json(), "blockingGroup", "");
                    LOGGER.info("区块申请被拒（{} 秒后重试）: {} 阻塞组={} 阻塞区块={}",
                            retrySeconds, key.asString(), blockingGroupRaw,
                            JsonUtil.getString(resp.json(), "blockingChunk", "?"));
                    try {
                        MergeTriggers.onClaimBlocked(UUID.fromString(blockingGroupRaw), key);
                    } catch (IllegalArgumentException ignored) {
                        // 阻塞组字段非法（入站不可信）：只重试，不触发合并
                    }
                    scheduleRetry(key, future);
                });
    }

    /** 安排下一轮申请重试。 */
    private void scheduleRetry(ChunkKey key, CompletableFuture<ClaimOutcome> future) {
        ThreadPools.scheduler().schedule(() -> attemptClaim(key, future),
                retrySeconds, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------ 数据拉取

    /**
     * 拉取区块的服务端数据（CHUNK_DATA_REQUEST → CHUNK_DATA）。
     * <p>
     * <b>预同步优先（Phase 3）</b>：合并接管方 B 的暂存库（{@link PresyncStore}）
     * 里若有该区块（A 经 P2P 直发的镜像，与服务端存档同源），直接消费返回，
     * 省一次服务端往返与下行带宽（规范"继承"）。
     * <p>
     * 返回 {@code Optional.empty()} 表示服务端确认"无此区块数据"（探测误报），
     * 调用方应回退种子生成。通信失败先快速重试 {@value #FETCH_FAST_RETRIES} 次，
     * 之后转慢速（申请重试间隔）——<b>绝不</b>降级为本地生成：服务端明确说有数据，
     * 擅自重新生成会在下次上行时覆盖服务端存档，属于数据丢失事故。
     */
    public CompletableFuture<Optional<CompoundTag>> fetchChunkData(ChunkKey key) {
        CompoundTag staged = PresyncStore.take(key);
        if (staged != null) {
            return CompletableFuture.completedFuture(Optional.of(staged));
        }
        CompletableFuture<Optional<CompoundTag>> future = new CompletableFuture<>();
        attemptFetch(key, future, 0);
        return future;
    }

    /** 发起一次数据拉取；失败按次数决定快/慢重试。 */
    private void attemptFetch(ChunkKey key, CompletableFuture<Optional<CompoundTag>> future,
                              int attempt) {
        if (shutdown) {
            future.completeExceptionally(new IllegalStateException("组运行时已关停"));
            return;
        }
        JsonObject req = new JsonObject();
        req.addProperty("dimension", key.dimension());
        req.addProperty("x", key.x());
        req.addProperty("z", key.z());
        conn.request(ControlMessage.of(MessageType.CHUNK_DATA_REQUEST, req))
                .whenCompleteAsync((resp, err) -> {
                    if (shutdown) {
                        future.completeExceptionally(new IllegalStateException("组运行时已关停"));
                        return;
                    }
                    if (resp != null && resp.type() == MessageType.CHUNK_DATA) {
                        try {
                            if (!JsonUtil.getBoolean(resp.json(), "exists", false)) {
                                future.complete(Optional.empty()); // 服务端确认无数据 → 种子生成
                                return;
                            }
                            CompoundTag tag = NbtIo.readCompressed(
                                    new ByteArrayInputStream(resp.binary()),
                                    NbtAccounter.create(MAX_CHUNK_NBT_BYTES));
                            future.complete(Optional.of(tag));
                            return;
                        } catch (Exception e) {
                            LOGGER.warn("区块数据解析失败，重试: {}", key.asString(), e);
                        }
                    } else {
                        LOGGER.debug("区块数据拉取失败，重试: {} ({})", key.asString(),
                                err != null ? err.toString() : "非预期应答");
                    }
                    long delay = attempt < FETCH_FAST_RETRIES ? 2 : retrySeconds;
                    ThreadPools.scheduler().schedule(
                            () -> attemptFetch(key, future, attempt + 1), delay, TimeUnit.SECONDS);
                }, ThreadPools.io()); // NBT 解压是重活，别占 Netty 事件循环
    }

    // ------------------------------------------------------------ 释放

    /**
     * 释放区块占用并携带最终区块数据（CHUNK_RELEASE，binary=压缩 NBT；可为 null）。
     * 先摘 claimed 集：其后集成服务端的存盘上行会被 {@link ChunkUploadService}
     * 过滤掉，不会出现"释放后上行被服务端拒绝"的日志噪音。
     */
    public void releaseWithData(ChunkKey key, byte[] gzipNbt) {
        claimed.remove(key);
        JsonObject req = new JsonObject();
        req.addProperty("dimension", key.dimension());
        req.addProperty("x", key.x());
        req.addProperty("z", key.z());
        req.addProperty("groupId", groupId.toString());
        conn.request(ControlMessage.of(MessageType.CHUNK_RELEASE, req, gzipNbt))
                .whenComplete((resp, err) -> {
                    if (err != null) {
                        // 释放失败兜底：断连时服务端会 releaseAll，占用不会永久泄漏
                        LOGGER.warn("区块释放请求失败（服务端断连清理兜底）: {}", key.asString());
                    }
                });
    }

    /** 关停：未决申请全部异常完成（区块加载链随集成服务端停摆，不泄漏）。 */
    public void shutdown() {
        shutdown = true;
        for (Map.Entry<ChunkKey, CompletableFuture<ClaimOutcome>> entry : pending.entrySet()) {
            entry.getValue().completeExceptionally(new IllegalStateException("组运行时已关停"));
        }
        pending.clear();
        claimed.clear();
    }
}
