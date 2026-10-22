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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

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

    /**
     * 最近一次被拒的只读诊断（登录看门狗/回归验收用；不参与 claim 状态机）。
     *
     * @param blockingGroup      拒绝方组 ID（服务端下发；空串表示字段缺失/非法）
     * @param blockingChunk      拒绝方占用的区块（服务端下发）
     * @param attempts           该 key 连续被拒次数（授予后清零）
     * @param firstDeniedAtMillis 本轮首次被拒的墙钟时间
     * @param lastDeniedAtMillis  最近一次被拒的墙钟时间
     */
    public record ClaimDiagnostics(String blockingGroup, String blockingChunk,
                                   int attempts, long firstDeniedAtMillis,
                                   long lastDeniedAtMillis) {
    }

    private final ControlConnection conn;
    private final UUID groupId;
    private final int retrySeconds;
    /** 申请/释放失败后的异步重试入口；测试通过包可见构造器注入手动调度器。 */
    private final Consumer<Runnable> retryScheduler;

    /** 线性化退出阶段与 release 注册，避免 freeze 和新释放请求之间出现竞态。 */
    private final Object lifecycleLock = new Object();

    /** 未决申请：每个 key 只有一个操作对象；它同时约束共享 future、在途请求和重试代次。 */
    private final Map<ChunkKey, ClaimOperation> pending = new ConcurrentHashMap<>();

    /** 已授权但仍在拉取 NBT 的加载也属于本代运行时，迁移时必须一起终结。 */
    private final Map<CompletableFuture<Optional<CompoundTag>>, ChunkKey> pendingFetches =
            new ConcurrentHashMap<>();

    /** 分离准备的旧玩家区域：仅阻止新增加载，已有区块仍保有写权直到正常卸载 ACK。 */
    private Predicate<ChunkKey> splitDrainRegion;

    /** 分离撤销后仍在确认未知授权/释放的 key，ACK 前不能让新 holder 复用同一份占用。 */
    private final Set<ChunkKey> retiringLoads = new java.util.HashSet<>();

    /** 字段全部由 lifecycleLock 保护，不能用 future 是否完成代替网络在途状态。 */
    private static final class ClaimOperation {
        private final CompletableFuture<ClaimOutcome> future = new CompletableFuture<>();
        private long generation;
        private boolean inFlight;
        private boolean retryWaiting;
        private boolean wakeRequested;
        private UUID blockingGroup;
        /** future 已对 ChunkMap 取消，但网络结果未明；必须追到明确拒绝或授予后可靠释放。 */
        private boolean draining;
        /** 请求发出但尚未得到明确拒绝；包括网络超时后的重试等待，不能遗忘服务端可能的授予。 */
        private boolean allocationUncertain;
    }

    /** 已在锁内保留的一次唤醒；锁外发请求，generation 使重复事件与旧定时器无效。 */
    private record ClaimAttempt(ChunkKey key, ClaimOperation operation, long generation) {
    }

    /** 已授予且尚未释放的区块（上行服务据此过滤"释放后仍触发的存盘上行"）。 */
    private final Set<ChunkKey> claimed = ConcurrentHashMap.newKeySet();

    /**
     * 最近一次被拒的诊断（只读，不改变状态机）。授予/摘除/关停时清除，
     * 防止过期阻塞组被登录看门狗误报。
     */
    private final ConcurrentHashMap<ChunkKey, ClaimDiagnostics> denials =
            new ConcurrentHashMap<>();

    /**
     * 当前 claim 对应的服务端数据基线：true 表示物理服已经有可保留的权威区块数据，
     * false 表示本次从种子生成且尚未得到任何一次耐久写入确认。记录只与仍在本地持有的
     * claim 同寿命；释放或迁移后立即删除，迟到的实时上传 ACK 只能 replace，不能复活状态。
     */
    private final ConcurrentHashMap<ChunkKey, Boolean> serverDataKnown = new ConcurrentHashMap<>();

    /** 已登记但尚未得到正 CHUNK_RELEASE_ACK 的释放操作。 */
    private final Map<ChunkKey, ReleaseOperation> releases = new ConcurrentHashMap<>();

    private static final class ReleaseOperation {
        /**
         * 一次逻辑释放的稳定身份。网络超时只会重放该身份，服务端据此返回第一次
         * 已耐久完成的结果，不能把一次重试误判成“owner 已不存在”的新释放。
         */
        private final UUID releaseId;
        private final byte[] gzipNbt;
        private final CompletableFuture<Void> future;
        private final java.util.concurrent.atomic.AtomicInteger attempts =
                new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger confirmationAttempts =
                new java.util.concurrent.atomic.AtomicInteger();

        private ReleaseOperation(UUID releaseId, byte[] gzipNbt,
                                 CompletableFuture<Void> future) {
            this.releaseId = releaseId;
            this.gzipNbt = gzipNbt;
            this.future = future;
        }

        private UUID releaseId() {
            return releaseId;
        }

        private byte[] gzipNbt() {
            return gzipNbt;
        }

        private CompletableFuture<Void> future() {
            return future;
        }

        private int beginAttempt() {
            return attempts.incrementAndGet();
        }

        private int attemptCount() {
            return attempts.get();
        }

        private int beginConfirmationAttempt() {
            return confirmationAttempts.incrementAndGet();
        }
    }

    private volatile boolean shutdown;

    /** 整组所有权已经由物理服提交转交；本实例永久失去写权限，不能再次启用。 */
    private volatile boolean transferred;

    /**
     * 只表示已提交合并或已确认分离准备后的旧加载取消，不能使用 IOException 或空 NBT
     * 表示：原版会把它们解释为可生成区块。ChunkMapMixin 将此信号转为 UNLOADED_CHUNK。
     */
    public static final class ChunkLoadRetiredException extends IllegalStateException {
        private ChunkLoadRetiredException() {
            super("组区块所有权已提交转交，旧运行时停止加载");
        }

        private ChunkLoadRetiredException(String reason) {
            super(reason);
        }
    }

    /** 仅展开 future 的包装异常，真实磁盘错误仍保留原版处理。 */
    public static boolean isTransferredFailure(Throwable error) {
        while ((error instanceof java.util.concurrent.CompletionException
                || error instanceof java.util.concurrent.ExecutionException)
                && error.getCause() != null && error.getCause() != error) {
            error = error.getCause();
        }
        return error instanceof ChunkLoadRetiredException;
    }

    /** EXIT_BEGIN 前准备：拒绝新 claim，但允许已经登记的普通 release 继续完成。 */
    private volatile boolean exitPreparing;

    /** EXIT_BEGIN 后最终冻结：不再新增 claim 或启动普通 release。 */
    private volatile boolean exitFrozen;

    public ChunkClaimClient(ControlConnection conn, UUID groupId, int retrySeconds) {
        this(conn, groupId, retrySeconds,
                task -> ThreadPools.scheduler().schedule(
                        task, Math.max(1, retrySeconds), TimeUnit.SECONDS));
    }

    /**
     * 包可见测试构造器。生产路径仍统一使用 {@link ThreadPools#scheduler()}；注入点只替换
     * “何时再次尝试”这一层，不改变请求线程和状态机语义。
     */
    ChunkClaimClient(ControlConnection conn, UUID groupId, int retrySeconds,
                     Consumer<Runnable> retryScheduler) {
        this.conn = conn;
        this.groupId = groupId;
        this.retrySeconds = Math.max(1, retrySeconds);
        this.retryScheduler = Objects.requireNonNull(retryScheduler, "retryScheduler");
    }

    // ------------------------------------------------------------ 区块申请

    /**
     * 申请区块占用；被拒时自动按间隔重试，future 直到授予（或 shutdown）才完成。
     * 任意线程可调（非阻塞）；future 的完成线程是 Netty 事件循环或 scheduler，
     * 消费方不得在回调里做重活/阻塞。
     */
    public CompletableFuture<ClaimOutcome> claimWithRetry(ChunkKey key) {
        ClaimOperation operation;
        boolean start = false;
        synchronized (lifecycleLock) {
            if (transferred) {
                return CompletableFuture.failedFuture(new ChunkLoadRetiredException());
            }
            if (shutdown || exitPreparing || exitFrozen) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("主客户端正在准备或提交退出事务"));
            }
            if (inSplitDrain(key) || retiringLoads.contains(key)) {
                return CompletableFuture.failedFuture(splitLoadCancelled());
            }
            operation = pending.get(key);
            if (operation == null) {
                operation = new ClaimOperation();
                pending.put(key, operation);
                start = true;
            }
        }
        if (start) {
            attemptClaim(key, operation, 0);
        }
        return operation.future;
    }

    /**
     * 权威服务端通知归属变化后提前重试被指定组阻塞的申请；null 用于合并提交等已确认的
     * 全局变化，唤醒本客户端全部未决申请。通知绝不授予占用，仍须重新经过服务端 claim。
     * <p>
     * 等待中的旧 timer 用代次失效；同 key 已在途时只记一个后续唤醒位，不能并发发出
     * 第二次 claim。若在途应答已授予，后续唤醒自动丢弃，避免已完成的申请被旧事件复活。
     */
    public void wakeBlockedClaims(UUID blockingGroup) {
        List<ClaimAttempt> ready = new ArrayList<>();
        synchronized (lifecycleLock) {
            if (shutdown || exitPreparing || exitFrozen) {
                return;
            }
            pending.forEach((key, operation) -> {
                if (operation.future.isDone() || (blockingGroup != null
                        && !blockingGroup.equals(operation.blockingGroup))) {
                    return;
                }
                if (operation.inFlight) {
                    operation.wakeRequested = true;
                } else if (operation.retryWaiting) {
                    operation.retryWaiting = false;
                    ready.add(new ClaimAttempt(key, operation, ++operation.generation));
                }
            });
        }
        for (ClaimAttempt attempt : ready) {
            attemptClaim(attempt.key(), attempt.operation(), attempt.generation());
        }
    }

    /** 是否持有该区块的占用。 */
    public boolean isClaimed(ChunkKey key) {
        return claimed.contains(key);
    }

    /**
     * 最近一次被拒的只读诊断；null = 该 key 尚未被拒（或已授予/已摘除）。
     * 任意线程可调，返回不可变快照。
     */
    public ClaimDiagnostics diagnostics(ChunkKey key) {
        return denials.get(key);
    }

    /** 已授予区块的快照（预同步发送端遍历用；防御性拷贝）。 */
    public Set<ChunkKey> claimedSnapshot() {
        return Set.copyOf(claimed);
    }

    /** 分离事务规划需要覆盖正在生成的外圈，不能只枚举已经授予的区块。 */
    public Set<ChunkKey> trackedKeysSnapshot() {
        synchronized (lifecycleLock) {
            Set<ChunkKey> keys = new java.util.HashSet<>(claimed);
            keys.addAll(pending.keySet());
            keys.addAll(pendingFetches.values());
            keys.addAll(releases.keySet());
            keys.addAll(retiringLoads);
            return Set.copyOf(keys);
        }
    }

    /**
     * 两阶段分离准备：关闭离组玩家区域的新加载，保留已加载区块的合法写权与正常卸载链。
     * 在途申请不能直接丢弃：服务端可能已授予但 ACK 仍在路上，需保留 draining 操作并在
     * 获得明确结果后释放，避免服务端留下没有本地 holder 的孤儿占用。
     */
    public void beginSplitDrain(Predicate<ChunkKey> region) {
        Objects.requireNonNull(region, "region");
        List<ClaimOperation> cancelled = new ArrayList<>();
        Map<CompletableFuture<Optional<CompoundTag>>, ChunkKey> fetches = new java.util.HashMap<>();
        synchronized (lifecycleLock) {
            if (shutdown || exitPreparing || exitFrozen) {
                throw new IllegalStateException("组运行时正在退出，不能准备分离");
            }
            if (splitDrainRegion != null) {
                throw new IllegalStateException("已有分离区域正在排空");
            }
            splitDrainRegion = region;
            pending.forEach((key, operation) -> {
                if (!region.test(key)) return;
                operation.draining = true;
                cancelled.add(operation);
                denials.remove(key);
                if (!operation.inFlight && !operation.allocationUncertain) {
                    // 明确被拒且只剩 timer 的申请没有在途授权，可以直接结束；旧 timer 的
                    // 身份校验会失败。此前超时结果不明的 draining 重试不能走这个分支。
                    pending.remove(key, operation);
                } else {
                    retiringLoads.add(key);
                }
            });
            pendingFetches.forEach((future, key) -> {
                if (region.test(key) && !future.isDone()) {
                    fetches.put(future, key);
                    pendingFetches.remove(future);
                    retiringLoads.add(key);
                }
            });
        }
        cancelled.forEach(operation -> operation.future.completeExceptionally(splitLoadCancelled()));
        fetches.forEach((future, key) -> {
            future.completeExceptionally(splitLoadCancelled());
            // 拉取尚未完成，原版还未读入/修改区块，权威数据仍在物理服，可以无数据释放。
            // 已生成/已加载的区块不在这份集合内，仍由正常预卸载上传最终状态。
            releaseWhenConfirmed(key, null);
        });
    }

    /** 只有该区域的已授予占用、未知结果申请和数据拉取全部结束才允许提交分离。 */
    public boolean isSplitDrained() {
        synchronized (lifecycleLock) {
            if (splitDrainRegion == null) return true;
            return claimed.stream().noneMatch(splitDrainRegion)
                    && pending.keySet().stream().noneMatch(splitDrainRegion)
                    && pendingFetches.values().stream().noneMatch(splitDrainRegion)
                    && releases.keySet().stream().noneMatch(splitDrainRegion)
                    && retiringLoads.stream().noneMatch(splitDrainRegion);
        }
    }

    /**
     * 取消/结束分离围栏；已取消的 holder 不会复活，调用方必须先等它们卸载再允许玩家恢复。
     * 尚不确定网络结果的 draining 申请继续追踪并释放，不能因取消事务而丢失所有权账目。
     */
    public void cancelSplitDrain() {
        synchronized (lifecycleLock) {
            splitDrainRegion = null;
        }
    }

    /** 调用方持有 lifecycleLock。 */
    private boolean inSplitDrain(ChunkKey key) {
        return splitDrainRegion != null && splitDrainRegion.test(key);
    }

    private static ChunkLoadRetiredException splitLoadCancelled() {
        return new ChunkLoadRetiredException("分离准备已撤销旧玩家区域加载，等待原版卸载");
    }

    /**
     * 本地摘除占用记录（Phase 3 分离/合并：区块占用已在<b>服务端侧</b>迁移给
     * 其他组，本组不再拥有 —— 只清本地集合，不发 CHUNK_RELEASE：发了也会因
     * 占用者不符被拒）。摘除后该区块的迟到存盘上行被 {@link ChunkUploadService}
     * 过滤，不产生"上行被拒"日志噪音。
     */
    public void forgetLocal(ChunkKey key) {
        ReleaseOperation release;
        synchronized (lifecycleLock) {
            claimed.remove(key);
            retiringLoads.remove(key);
            serverDataKnown.remove(key);
            denials.remove(key);
            release = releases.remove(key);
        }
        if (release != null) {
            release.future().completeExceptionally(
                    new IllegalStateException("区块占用已由其他组接管"));
        }
    }

    /**
     * 权威合并已提交后的整组退休（调用前必须已经确认所有权转交）。与 forgetLocal 不同，
     * 这是不可恢复的代次终点：禁止新申请、终结未决加载/拉取/释放，并使所有旧重试失效。
     * 不能等 SERVER_STOPPED 再关停，因为原版 Saving worlds 本身正在等待这些加载结束。
     * 本方法不发 CHUNK_RELEASE；所有权已经属于新主，旧主无权释放或重新上传。
     */
    public void forgetAllLocal() {
        List<ClaimOperation> pendingClaims;
        List<CompletableFuture<Optional<CompoundTag>>> fetches;
        Map<ChunkKey, ReleaseOperation> activeReleases;
        synchronized (lifecycleLock) {
            transferred = true;
            shutdown = true;
            exitPreparing = true;
            exitFrozen = true;
            pendingClaims = new ArrayList<>(pending.values());
            pending.clear();
            fetches = new ArrayList<>(pendingFetches.keySet());
            pendingFetches.clear();
            claimed.clear();
            retiringLoads.clear();
            serverDataKnown.clear();
            denials.clear();
            activeReleases = new java.util.HashMap<>(releases);
            releases.clear();
        }
        pendingClaims.forEach(operation ->
                operation.future.completeExceptionally(new ChunkLoadRetiredException()));
        fetches.forEach(future -> future.completeExceptionally(new ChunkLoadRetiredException()));
        activeReleases.values().forEach(operation -> operation.future().completeExceptionally(
                new IllegalStateException("区块占用已由其他组接管")));
    }

    /** 已占用区块数（诊断用）。 */
    public int claimedCount() {
        return claimed.size();
    }

    /**
     * 首次从种子生成的区块在卸载前必须至少写回一次；否则物理服没有任何可供下一组加载的
     * 数据。已经释放或迁移的 key 没有本地基线，因而返回 false。
     */
    public boolean requiresInitialWrite(ChunkKey key) {
        return Boolean.FALSE.equals(serverDataKnown.get(key));
    }

    /**
     * 标记物理服已经耐久保存该区块。使用 replace 而不是 put，确保释放后的迟到 ACK 不会
     * 重新创建基线，也不会让一个已经不归本组所有的区块重新进入本地状态机。
     */
    public void markServerDataKnown(ChunkKey key) {
        serverDataKnown.replace(key, true);
    }

    /** 发起一次申请；操作身份、代次与在途位必须同时匹配才能发送。 */
    private void attemptClaim(ChunkKey key, ClaimOperation operation, long expectedGeneration) {
        JsonObject req = new JsonObject();
        req.addProperty("dimension", key.dimension());
        req.addProperty("x", key.x());
        req.addProperty("z", key.z());
        req.addProperty("groupId", groupId.toString());
        CompletableFuture<ControlMessage> response;
        long generation;
        synchronized (lifecycleLock) {
            if (!activeClaim(key, operation) || operation.inFlight
                    || operation.generation != expectedGeneration) {
                return;
            }
            operation.inFlight = true;
            operation.allocationUncertain = true;
            operation.retryWaiting = false;
            operation.wakeRequested = false;
            generation = ++operation.generation;
            try {
                // request 只登记非阻塞网络写；与退出冻结在同一临界区线性化发送起点。
                response = Objects.requireNonNull(conn.request(
                        ControlMessage.of(MessageType.CHUNK_CLAIM_REQUEST, req)), "请求 future");
            } catch (RuntimeException failure) {
                response = CompletableFuture.failedFuture(failure);
            }
        }
        response.whenComplete((reply, failure) -> finishClaim(
                key, operation, generation, reply, failure));
    }

    /** 调用方持有 lifecycleLock；身份判断使退出/替换后的迟到回调彻底失效。 */
    private boolean activeClaim(ChunkKey key, ClaimOperation operation) {
        return !shutdown && !exitPreparing && !exitFrozen
                && (!operation.future.isDone() || operation.draining)
                && pending.get(key) == operation;
    }

    private void finishClaim(ChunkKey key, ClaimOperation operation, long generation,
                             ControlMessage reply, Throwable failure) {
        ClaimOutcome outcome = null;
        UUID blocked = null;
        boolean wakeImmediately = false;
        boolean drainGranted = false;
        boolean drainDenied = false;
        long retryGeneration = generation;
        synchronized (lifecycleLock) {
            if (!activeClaim(key, operation) || !operation.inFlight
                    || operation.generation != generation) {
                return;
            }
            operation.inFlight = false;
            boolean validReply = failure == null && reply != null
                    && reply.type() == MessageType.CHUNK_CLAIM_RESPONSE;
            if (validReply && JsonUtil.getBoolean(reply.json(), "granted", false)) {
                boolean hasServerData = JsonUtil.getBoolean(reply.json(), "hasServerData", false);
                serverDataKnown.put(key, hasServerData);
                claimed.add(key);
                denials.remove(key);
                pending.remove(key, operation);
                if (operation.draining || inSplitDrain(key)) {
                    drainGranted = true;
                    retiringLoads.add(key);
                }
                else outcome = new ClaimOutcome(hasServerData);
            } else {
                if (validReply) operation.allocationUncertain = false;
                if (validReply && operation.draining) {
                    // 服务端明确拒绝，证明本次未知授权已经收束；分离区域不再触发合并。
                    pending.remove(key, operation);
                    denials.remove(key);
                    retiringLoads.remove(key);
                    drainDenied = true;
                } else if (validReply) {
                    String blockingGroupRaw = JsonUtil.getString(reply.json(), "blockingGroup", "");
                    String blockingChunkRaw = JsonUtil.getString(reply.json(), "blockingChunk", "?");
                    recordDenial(key, blockingGroupRaw, blockingChunkRaw);
                    try {
                        blocked = UUID.fromString(blockingGroupRaw);
                    } catch (IllegalArgumentException ignored) {
                        // 不可信的阻塞组字段不进入定向唤醒或合并触发链。
                    }
                    operation.blockingGroup = blocked;
                    LOGGER.info("区块申请被拒（{} 秒后重试）: {} 阻塞组={} 阻塞区块={}",
                            retrySeconds, key.asString(), blockingGroupRaw, blockingChunkRaw);
                } else {
                    LOGGER.debug("区块申请通信失败，{} 秒后重试: {} ({})", retrySeconds,
                            key.asString(), failure != null ? failure.toString() : "非预期应答");
                }
                wakeImmediately = operation.wakeRequested;
                operation.wakeRequested = false;
                operation.retryWaiting = !wakeImmediately;
                if (wakeImmediately) {
                    retryGeneration = ++operation.generation;
                }
            }
        }
        if (drainGranted) {
            operation.future.completeExceptionally(splitLoadCancelled());
            releaseWhenConfirmed(key, null);
            return;
        }
        if (drainDenied) return;
        if (outcome != null) {
            synchronized (lifecycleLock) {
                // grant 的本地登记与 future 完成之间可能恰好收到 COMMIT；此时不得把
                // “允许开始生成”的旧结果交给 ChunkMap，即使该 operation 已从 pending 摘除。
                if (transferred) operation.future.completeExceptionally(new ChunkLoadRetiredException());
                else if (inSplitDrain(key)) {
                    retiringLoads.add(key);
                    operation.future.completeExceptionally(splitLoadCancelled());
                    // grant 已登记但还未交付原版，尚无任何生成/修改，可以安全撤销该占用。
                    releaseWhenConfirmed(key, null);
                }
                else operation.future.complete(outcome);
            }
            return;
        }
        if (blocked != null) {
            boolean notifyMerge;
            synchronized (lifecycleLock) {
                notifyMerge = !operation.draining && !inSplitDrain(key) && !shutdown;
            }
            if (notifyMerge) MergeTriggers.onClaimBlocked(blocked, key);
        }
        if (wakeImmediately) {
            attemptClaim(key, operation, retryGeneration);
        } else {
            scheduleRetry(key, operation, retryGeneration);
        }
    }

    /** 定时兜底保留原配置间隔；事件唤醒后该闭包持有的旧代次只会空操作。 */
    private void scheduleRetry(ChunkKey key, ClaimOperation operation, long generation) {
        try {
            retryScheduler.accept(() -> attemptClaim(key, operation, generation));
        } catch (RuntimeException schedulingFailure) {
            boolean failedCurrent = false;
            synchronized (lifecycleLock) {
                // 提交旧 timer 的同时可能已经被事件唤醒；只有仍等待这一代 timer 的
                // 操作可以失败，绝不能让旧调度异常删除新的在途请求或覆盖它的成功结果。
                if (operation.generation == generation && operation.retryWaiting
                        && !operation.inFlight && pending.remove(key, operation)) {
                    denials.remove(key);
                    failedCurrent = true;
                }
            }
            if (failedCurrent) {
                operation.future.completeExceptionally(schedulingFailure);
            }
        }
    }

    /** 记录一次拒绝；同一 key 连续被拒累加次数并保留首次拒绝时间。 */
    private void recordDenial(ChunkKey key, String blockingGroup, String blockingChunk) {
        long now = System.currentTimeMillis();
        denials.compute(key, (ignored, previous) -> previous == null
                ? new ClaimDiagnostics(blockingGroup, blockingChunk, 1, now, now)
                : new ClaimDiagnostics(blockingGroup, blockingChunk,
                        previous.attempts() + 1, previous.firstDeniedAtMillis(), now));
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
        CompletableFuture<Optional<CompoundTag>> future = new CompletableFuture<>();
        synchronized (lifecycleLock) {
            if (transferred) {
                return CompletableFuture.failedFuture(new ChunkLoadRetiredException());
            }
            if (shutdown) {
                return CompletableFuture.failedFuture(new IllegalStateException("组运行时已关停"));
            }
            if (inSplitDrain(key) || retiringLoads.contains(key)) {
                // 可能在 claim 完成和 thenCompose 发起拉取之间进入分离准备。
                if (claimed.contains(key)) {
                    retiringLoads.add(key);
                    releaseWhenConfirmed(key, null);
                }
                return CompletableFuture.failedFuture(splitLoadCancelled());
            }
            CompoundTag staged = PresyncStore.take(key);
            if (staged != null) {
                return CompletableFuture.completedFuture(Optional.of(staged));
            }
            pendingFetches.put(future, key);
        }
        future.whenComplete((result, error) -> pendingFetches.remove(future));
        attemptFetch(key, future, 0);
        return future;
    }

    /** 发起一次数据拉取；失败按次数决定快/慢重试。 */
    private void attemptFetch(ChunkKey key, CompletableFuture<Optional<CompoundTag>> future,
                              int attempt) {
        JsonObject req = new JsonObject();
        req.addProperty("dimension", key.dimension());
        req.addProperty("x", key.x());
        req.addProperty("z", key.z());
        CompletableFuture<ControlMessage> response;
        synchronized (lifecycleLock) {
            if (future.isDone()) return;
            if (!pendingFetches.containsKey(future)) {
                future.completeExceptionally(splitLoadCancelled());
                return;
            }
            if (shutdown) {
                future.completeExceptionally(transferred ? new ChunkLoadRetiredException()
                        : new IllegalStateException("组运行时已关停"));
                return;
            }
            try {
                // 与退休共享临界区，保证退休返回后不会再开始新的 NBT 请求。
                response = Objects.requireNonNull(conn.request(
                        ControlMessage.of(MessageType.CHUNK_DATA_REQUEST, req)), "请求 future");
            } catch (RuntimeException failure) {
                response = CompletableFuture.failedFuture(failure);
            }
        }
        response.whenCompleteAsync((resp, err) -> {
                    if (future.isDone()) return;
                    if (shutdown) {
                        future.completeExceptionally(transferred ? new ChunkLoadRetiredException()
                                : new IllegalStateException("组运行时已关停"));
                        return;
                    }
                    if (resp != null && resp.type() == MessageType.CHUNK_DATA) {
                        try {
                            if (!JsonUtil.getBoolean(resp.json(), "exists", false)) {
                                completeFetch(future, Optional.empty()); // 服务端确认无数据 → 种子生成
                                return;
                            }
                            CompoundTag tag = NbtIo.readCompressed(
                                    new ByteArrayInputStream(resp.binary()),
                                    NbtAccounter.create(MAX_CHUNK_NBT_BYTES));
                            completeFetch(future, Optional.of(tag));
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

    /** 解压可与 COMMIT 并行；交付结果时再验证代次，不能以迟到 NBT 恢复旧加载。 */
    private void completeFetch(CompletableFuture<Optional<CompoundTag>> future,
                               Optional<CompoundTag> result) {
        synchronized (lifecycleLock) {
            if (transferred) future.completeExceptionally(new ChunkLoadRetiredException());
            else if (shutdown) future.completeExceptionally(new IllegalStateException("组运行时已关停"));
            else if (!pendingFetches.containsKey(future)) future.completeExceptionally(splitLoadCancelled());
            else future.complete(result);
        }
    }

    // ------------------------------------------------------------ 释放

    /**
     * 可靠释放区块占用。请求失败、应答类型错误、{@code released=false}，以及携带最终
     * 数据时 {@code finalDataWritten=false}，都保留本地 claim 并通过异步 scheduler 无限重试。
     * 只有物理服务端给出完整正 ACK 后，才原子清理 claim 与数据基线并完成返回 future。
     */
    public CompletableFuture<Void> releaseWhenConfirmed(ChunkKey key, byte[] gzipNbt) {
        ReleaseOperation operation;
        synchronized (lifecycleLock) {
            if (shutdown) {
                return CompletableFuture.failedFuture(new IllegalStateException("组运行时已关停"));
            }
            if (exitFrozen) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("退出清单已冻结，禁止启动新的区块释放"));
            }
            if (!claimed.contains(key)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("区块不属于当前组，不能释放: " + key.asString()));
            }
            operation = releases.get(key);
            if (operation == null) {
                byte[] payload = gzipNbt == null ? null : gzipNbt.clone();
                operation = new ReleaseOperation(
                        UUID.randomUUID(), payload, new CompletableFuture<>());
                releases.put(key, operation);
            } else {
                // 同一 key 的预卸载请求只允许一个在途事务；后续调用共享同一 future，
                // 防止重复 CHUNK_RELEASE 造成服务端竞争和本地卸载顺序失真。
                return operation.future();
            }
        }
        attemptRelease(key, operation);
        return operation.future();
    }

    /** 旧调用点兼容层：保留返回 future，调用方可等待；实现本身不再 fire-and-forget。 */
    public CompletableFuture<Void> releaseWithData(ChunkKey key, byte[] gzipNbt) {
        return releaseWhenConfirmed(key, gzipNbt);
    }

    /** 发起或继续一次释放尝试；已开始的操作在 exitFrozen 后仍可继续重试。 */
    private void attemptRelease(ChunkKey key, ReleaseOperation operation) {
        synchronized (lifecycleLock) {
            if (releases.get(key) != operation) {
                return;
            }
            if (shutdown) {
                failRelease(key, operation, new IllegalStateException("组运行时已关停"));
                return;
            }
        }
        operation.beginAttempt();
        JsonObject req = new JsonObject();
        req.addProperty("dimension", key.dimension());
        req.addProperty("x", key.x());
        req.addProperty("z", key.z());
        req.addProperty("groupId", groupId.toString());
        req.addProperty("releaseId", operation.releaseId().toString());
        CompletableFuture<ControlMessage> responseFuture;
        try {
            synchronized (lifecycleLock) {
                // 压缩/构造请求期间可能已提交迁移；在真实发送点再次线性化检查，
                // 避免退休后继续向物理服释放已经属于新主的 key。
                if (shutdown || releases.get(key) != operation) return;
                responseFuture = conn.request(ControlMessage.of(
                        MessageType.CHUNK_RELEASE, req, operation.gzipNbt()));
            }
            if (responseFuture == null) {
                throw new IllegalStateException("控制连接返回 null future");
            }
        } catch (Throwable error) {
            scheduleReleaseRetry(key, operation, error);
            return;
        }
        responseFuture.whenComplete((resp, err) -> {
            if (err == null && isPositiveReleaseAck(resp, operation.gzipNbt())) {
                completeRelease(key, operation);
            } else {
                Throwable cause = err != null ? err
                        : new IllegalStateException("区块释放未获完整正 ACK");
                scheduleReleaseRetry(key, operation, cause);
            }
        });
    }

    private boolean isPositiveReleaseAck(ControlMessage response, byte[] gzipNbt) {
        if (response == null || response.type() != MessageType.CHUNK_RELEASE_ACK
                || !JsonUtil.getBoolean(response.json(), "released", false)) {
            return false;
        }
        return gzipNbt == null || gzipNbt.length == 0
                || JsonUtil.getBoolean(response.json(), "finalDataWritten", false);
    }

    private void scheduleReleaseRetry(ChunkKey key, ReleaseOperation operation, Throwable cause) {
        synchronized (lifecycleLock) {
            if (releases.get(key) != operation || shutdown) {
                return;
            }
        }
        int attempt = operation.attemptCount();
        // 断网时每个区块都会无限轮询。只记录首次和每 30 次里程碑，既能确认重试
        // 仍在推进，又不会让 latest.log 被每秒重复消息淹没。
        if (attempt == 1 || attempt % 30 == 0) {
            LOGGER.warn("区块释放第 {} 次未确认，将继续重试: {} ({})",
                    attempt, key.asString(), cause.toString());
        }
        retryScheduler.accept(() -> attemptRelease(key, operation));
    }

    private void completeRelease(ChunkKey key, ReleaseOperation operation) {
        boolean completed;
        synchronized (lifecycleLock) {
            completed = releases.remove(key, operation);
            if (completed) {
                // 先在同一临界区摘除 claim 与基线，再完成 future；任何后续上传都会看到
                // “已释放”并被过滤，迟到的 markServerDataKnown 也只能 replace 现有项。
                claimed.remove(key);
                retiringLoads.remove(key);
                serverDataKnown.remove(key);
            }
        }
        if (completed) {
            int attempts = operation.attemptCount();
            if (attempts > 1) {
                LOGGER.info("区块释放在第 {} 次尝试获得确认: {}",
                        attempts, key.asString());
            } else {
                LOGGER.debug("区块释放已确认: {}", key.asString());
            }
            // 正释放 ACK 已足以开放原版卸载；必须先完成屏障，再启动纯垃圾回收。
            // 即使连接/调度器正好关闭，回执确认异常也绝不能把已成功的卸载重新卡住。
            operation.future().complete(null);
            attemptReceiptConfirmation(key, operation);
        }
    }

    /**
     * 后台确认服务端成功回执已送达。确认 ACK 丢失时可安全无限重放：服务端即使已经
     * 删除回执也会返回 accepted=true；本流程不重新加入 releases，因而不阻塞卸载。
     */
    private void attemptReceiptConfirmation(
            ChunkKey key, ReleaseOperation operation) {
        if (shutdown) {
            return;
        }
        int attempt = operation.beginConfirmationAttempt();
        JsonObject req = new JsonObject();
        req.addProperty("releaseId", operation.releaseId().toString());
        req.addProperty("groupId", groupId.toString());
        CompletableFuture<ControlMessage> responseFuture;
        try {
            responseFuture = conn.request(ControlMessage.of(
                    MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM, req));
            if (responseFuture == null) {
                throw new IllegalStateException("控制连接返回 null future");
            }
        } catch (Throwable failure) {
            scheduleReceiptConfirmationRetry(
                    key, operation, attempt, failure);
            return;
        }
        responseFuture.whenComplete((response, failure) -> {
            boolean accepted = failure == null
                    && response != null
                    && response.type()
                    == MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM
                    && JsonUtil.getBoolean(
                    response.json(), "accepted", false);
            if (accepted) {
                if (attempt > 1) {
                    LOGGER.debug("区块释放回执在第 {} 次确认后已回收: {}",
                            attempt, key.asString());
                }
                return;
            }
            scheduleReceiptConfirmationRetry(
                    key, operation, attempt,
                    failure != null ? failure
                            : new IllegalStateException("释放回执确认未获正 ACK"));
        });
    }

    private void scheduleReceiptConfirmationRetry(
            ChunkKey key, ReleaseOperation operation,
            int attempt, Throwable cause) {
        if (shutdown) {
            return;
        }
        if (attempt == 1 || attempt % 30 == 0) {
            LOGGER.debug("区块释放回执第 {} 次确认失败，后台继续重试: {} ({})",
                    attempt, key.asString(), cause.toString());
        }
        try {
            retryScheduler.accept(() ->
                    attemptReceiptConfirmation(key, operation));
        } catch (RuntimeException schedulingFailure) {
            // 回执最多暂时留到周期快照/整组清理；绝不能影响已经开放的本地卸载。
            LOGGER.debug("区块释放回执确认调度器已停止: {} ({})",
                    key.asString(), schedulingFailure.toString());
        }
    }

    private void failRelease(ChunkKey key, ReleaseOperation operation, Throwable cause) {
        if (releases.remove(key, operation)) {
            operation.future().completeExceptionally(cause);
        }
    }

    /**
     * 退出准备阶段：停止新的 claim 并终止未决申请。已经登记的普通 release 不受影响，
     * 这样预卸载屏障可以在网络恢复后继续完成；调用方应等待它们清空后再 freeze。
     */
    public void beginExitPreparation() {
        Map<ChunkKey, ClaimOperation> pendingClaims;
        synchronized (lifecycleLock) {
            if (shutdown || exitPreparing) {
                return;
            }
            exitPreparing = true;
            pendingClaims = new java.util.HashMap<>(pending);
            pending.clear();
            denials.clear();
        }
        pendingClaims.values().forEach(operation -> operation.future.completeExceptionally(
                new IllegalStateException("主客户端正在准备退出事务")));
    }

    /**
     * 冻结当前占用并返回快照。所有未决申请异常结束；此后普通释放同样被抑制，
     * 最终由 EXIT_COMMIT 在物理服整组释放。
     */
    public Set<ChunkKey> freezeForExit() {
        Map<ChunkKey, ClaimOperation> pendingClaims;
        Set<ChunkKey> snapshot;
        synchronized (lifecycleLock) {
            exitPreparing = true;
            exitFrozen = true;
            pendingClaims = new java.util.HashMap<>(pending);
            pending.clear();
            denials.clear();
            snapshot = Set.copyOf(claimed);
        }
        pendingClaims.values().forEach(operation -> operation.future.completeExceptionally(
                new IllegalStateException("主客户端正在提交退出事务")));
        return snapshot;
    }

    /** 关停：未决申请全部异常完成（区块加载链随集成服务端停摆，不泄漏）。 */
    public void shutdown() {
        Map<ChunkKey, ClaimOperation> pendingClaims;
        Map<ChunkKey, ReleaseOperation> activeReleases;
        List<CompletableFuture<Optional<CompoundTag>>> fetches;
        synchronized (lifecycleLock) {
            shutdown = true;
            exitPreparing = true;
            exitFrozen = true;
            pendingClaims = new java.util.HashMap<>(pending);
            pending.clear();
            fetches = new ArrayList<>(pendingFetches.keySet());
            pendingFetches.clear();
            activeReleases = new java.util.HashMap<>(releases);
            releases.clear();
            claimed.clear();
            retiringLoads.clear();
            serverDataKnown.clear();
            denials.clear();
        }
        pendingClaims.values().forEach(operation -> operation.future.completeExceptionally(
                new IllegalStateException("组运行时已关停")));
        activeReleases.values().forEach(operation -> operation.future().completeExceptionally(
                new IllegalStateException("组运行时已关停")));
        fetches.forEach(future -> future.completeExceptionally(
                new IllegalStateException("组运行时已关停")));
    }
}
