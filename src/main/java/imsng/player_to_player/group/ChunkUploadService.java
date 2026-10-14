package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.ThreadPools;
import imsng.player_to_player.util.Sha256;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 区块上行服务（主客户端侧，Phase 2；规范"服务端需要实时计算区块的更改更新
 * 本地区块文件"——数据源就是主客户端的实时上行）。
 * <p>
 * 数据来源：{@code mixin.ChunkStorageMixin} 在集成服务端<b>每次把区块写盘</b>
 * （自动存盘 / 卸载存盘 / 停服 saveAll）时捕获 (ChunkKey, CompoundTag)，调用
 * {@link #enqueue}。选择"存盘时上行"而非"每 tick 上行"：MC 自身的 unsaved 脏标记
 * 已经天然去抖，上行频率与原版写盘频率一致，带宽可控且不额外拖慢 tick。
 *
 * <h2>队列设计</h2>
 * <ul>
 *   <li><b>按区块合并</b>（LinkedHashMap，同键覆盖）：存盘风暴里同一区块的多次
 *       写只留最新一份 —— 上行永远是幂等的"最终态覆盖"，丢中间版本无损；</li>
 *   <li><b>有界</b>（{@value #MAX_PENDING_CHUNKS} 个区块）：超限丢最旧（该区块
 *       数据仍在本地存档里，下次存盘会再次入队），绝不反压集成服务端主线程；</li>
 *   <li><b>单工作线程</b>（io 池）串行序列化 + 发送：NBT gzip 压缩是 CPU 活，
 *       enqueue（主线程调用）只做 map put。</li>
 * </ul>
 * 过滤：只上行仍在占用中的区块（{@link ChunkClaimClient#isClaimed}）——释放
 * （最终数据已随 CHUNK_RELEASE 上行）之后的迟到存盘直接丢弃。
 */
public final class ChunkUploadService {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/chunk-upload");

    /** 待上行队列的区块数上限（合并后；视距内区块量级为数百）。 */
    private static final int MAX_PENDING_CHUNKS = 8192;

    /** 实时单帧大小上限；超限快照保留到正常退出时经分片事务提交。 */
    private static final int MAX_COMPRESSED_BYTES = Protocol.MAX_FRAME_BYTES - 64 * 1024;
    /** 实时批次的记录预算；留出批次头和控制协议 JSON。 */
    private static final int BATCH_RECORD_BYTES = 4 * 1024 * 1024;
    private static final int MAX_BATCH_DELTAS = 256;

    /**
     * 单区块滞留窗口（毫秒）：上行队列里恰好只有 1 个待发区块时，先短等这一小段再决定
     * 发单区块帧还是批次帧。
     * <p>
     * 为什么需要：原版存盘是<b>逐块入队</b>（存盘/卸载循环一次一个），工作线程在第一个
     * 区块入队时就被唤醒，于是"一次卸载 100 个区块"会退化成 100 个单区块帧 ——
     * 服务端每个帧都要 append 一次世界 WAL 并 {@code force(true)} 一次，
     * 这就是"逐区块一次 fsync"的直接来源。短等一小段即可把这些同批区块合成一个
     * {@code CHUNK_DATA_BATCH}：服务端一个 WAL 记录 + <b>一次</b> force 覆盖全部区块。
     * <p>
     * 代价：孤立区块的上行 ACK 晚 {@value} 毫秒左右（预卸载屏障随之晚完成同样时长）。
     * 每次工作会话只滞留一次，不会持续拖延；退出流程有自己的整批打包，不受影响。
     */
    private static final long SINGLE_LINGER_MILLIS = 30;

    private final ControlConnection conn;
    private final UUID groupId;
    private final Executor executor;
    private final Consumer<Runnable> retryScheduler;
    private final Predicate<ChunkKey> claimedPredicate;
    /** 实时写入正 ACK 后更新 ChunkClaimClient 的服务端数据基线。 */
    private final Consumer<ChunkKey> serverDataKnownCallback;
    /** 本次客户端会话的批次事务 ID；sequence 单调递增，断线重放仍幂等。 */
    private final UUID runtimeBatchId = UUID.randomUUID();

    /** 待上行：key → 最新 NBT（LinkedHashMap 保序 + 同键覆盖；以自身为锁）。 */
    private final LinkedHashMap<ChunkKey, CompoundTag> queue = new LinkedHashMap<>();

    /**
     * 服务端尚未物理确认的每区块最新快照。
     * <p>
     * 这里不是历史快照缓存：当前版本一旦收到 {@code written=true} 就必须删除；若旧版本
     * 在途期间同键产生了新版本，则依靠对象身份比较只确认旧版本，绝不能误删新版本。
     */
    private final Map<ChunkKey, CompoundTag> unacknowledgedSnapshots =
            new java.util.HashMap<>();

    /**
     * 服务端最近一次正确认的压缩内容摘要。停服强制保存可能再次产生同一份 NBT，
     * 发送前用摘要去重，避免把已经耐久化的数据重新放入退出事务。
     */
    private final Map<ChunkKey, String> durableHashes = new java.util.HashMap<>();
    /** 本地快照版本，仅用于批内去重与诊断，不作为服务端安全边界。 */
    private final Map<ChunkKey, Long> snapshotVersions = new java.util.HashMap<>();
    private long nextSnapshotVersion;
    private int nextBatchSequence;
    /**
     * 尚未获得 durable ACK 的唯一批次。ACK 丢失后必须重放这里保存的原始字节；
     * 期间产生的新快照继续留在 queue，待本批确认后使用下一 sequence 发送。
     */
    private InFlightBatch inFlightBatch;
    /** 与 inFlightBatch 对应的本地等待；forget 可取消它以立即重组剩余区块。 */
    private CompletableFuture<ControlMessage> inFlightBatchRequest;

    /** 工作线程是否在跑（enqueue 只在队列从空变非空时投递一次任务）。 */
    private boolean workerRunning;

    /**
     * 退出冻结后停止普通实时请求，但 {@link #enqueue} 仍继续记录停服存盘产生的最终脏快照，
     * 由退出事务统一打包。该字段只在 {@link #queue} 锁内读写。
     */
    private boolean exitFrozen;

    private volatile boolean shutdown;

    public ChunkUploadService(ControlConnection conn, UUID groupId) {
        this(conn, groupId, ThreadPools.io(),
                task -> ThreadPools.scheduler().schedule(task, 2, TimeUnit.SECONDS),
                key -> {
                    ChunkClaimClient claims = GroupRuntime.claims();
                    return claims != null && claims.isClaimed(key);
                }, ignored -> {
                });
    }

    /**
     * 运行时显式注入服务端数据基线回调，避免上传服务依赖不可测试的静态全局查找。
     */
    public ChunkUploadService(ControlConnection conn, UUID groupId,
                              Consumer<ChunkKey> serverDataKnownCallback) {
        this(conn, groupId, ThreadPools.io(),
                task -> ThreadPools.scheduler().schedule(task, 2, TimeUnit.SECONDS),
                key -> {
                    ChunkClaimClient claims = GroupRuntime.claims();
                    return claims != null && claims.isClaimed(key);
                }, serverDataKnownCallback);
    }

    ChunkUploadService(ControlConnection conn, UUID groupId, Executor executor,
                       Consumer<Runnable> retryScheduler,
                       Predicate<ChunkKey> claimedPredicate) {
        this(conn, groupId, executor, retryScheduler, claimedPredicate, ignored -> {
        });
    }

    ChunkUploadService(ControlConnection conn, UUID groupId, Executor executor,
                       Consumer<Runnable> retryScheduler,
                       Predicate<ChunkKey> claimedPredicate,
                       Consumer<ChunkKey> serverDataKnownCallback) {
        this.conn = conn;
        this.groupId = groupId;
        this.executor = executor;
        this.retryScheduler = Objects.requireNonNull(retryScheduler, "retryScheduler");
        this.claimedPredicate = Objects.requireNonNull(claimedPredicate, "claimedPredicate");
        this.serverDataKnownCallback = Objects.requireNonNull(
                serverDataKnownCallback, "serverDataKnownCallback");
    }

    /**
     * 区块 NBT 入队上行（集成服务端主线程调用；只做 map 操作，微秒级）。
     * tag 是 ChunkSerializer.write 刚生成的新对象，此后无人再改，跨线程只读安全。
     */
    public void enqueue(ChunkKey key, CompoundTag tag) {
        if (shutdown) {
            return;
        }
        // 预同步分接（Phase 3）：合并/分离的发送端在此拿到快照后的全部增量
        // （无消费者时是一次空列表遍历，可忽略）
        PresyncTaps.offer(key, tag);
        synchronized (queue) {
            // 与 shutdown() 竞争时以锁内状态为准，避免关停清空后又留下孤立快照。
            if (shutdown) {
                return;
            }
            CompoundTag snapshot = tag.copy();
            queue.put(key, snapshot);
            unacknowledgedSnapshots.put(key, snapshot);
            snapshotVersions.put(key, ++nextSnapshotVersion);
            if (queue.size() > MAX_PENDING_CHUNKS) {
                // 丢最旧：该区块数据仍在本地存档，下次存盘会重新入队
                Map.Entry<ChunkKey, CompoundTag> oldest = queue.entrySet().iterator().next();
                LOGGER.warn("上行队列超限，暂弃最旧区块（本地存档仍有，后续存盘会补传）: {}",
                        oldest.getKey().asString());
                queue.remove(oldest.getKey());
            }
            if (!workerRunning && !exitFrozen) {
                workerRunning = true;
                executor.execute(this::drainLoop);
            }
        }
    }

    /**
     * 工作线程：尽可能把同一轮排队的多个区块合并为一个批次；只有单区块时保留
     * CHUNK_DATA_UPLOAD 兼容路径。批次 ACK 在服务端 WAL force 后返回。
     */
    private void drainLoop() {
        boolean lingerUsed = false;
        while (true) {
            if (!lingerUsed) {
                lingerUsed = true;
                // 只在"队列里恰好只有 1 个待发区块"时滞留：已有多个时立刻成批发送，
                // 退出/风暴场景下不引入任何额外延迟。
                if (hasSinglePending()) {
                    sleepQuietly(SINGLE_LINGER_MILLIS);
                }
            }
            List<Candidate> candidates;
            try {
                candidates = collectCandidates();
            } catch (RuntimeException e) {
                scheduleRetry(null, e);
                return;
            }
            if (candidates.isEmpty()) {
                synchronized (queue) {
                    if (queue.isEmpty() || shutdown || exitFrozen) {
                        workerRunning = false;
                        return;
                    }
                }
                continue;
            }
            try {
                if (candidates.size() == 1) {
                    sendSingle(candidates.get(0));
                } else {
                    sendBatches(candidates);
                }
            } catch (Exception e) {
                scheduleRetry(candidates.get(0).key(), e);
                return;
            }
        }
    }

    /** 队列里是否恰好只有 1 个待发区块（滞留判据；不带任何副作用）。 */
    private boolean hasSinglePending() {
        synchronized (queue) {
            return !shutdown && !exitFrozen && queue.size() == 1;
        }
    }

    /** 可中断安全的短睡眠：被打断时立即返回（放弃本次滞留，不丢任务）。 */
    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record Candidate(ChunkKey key, CompoundTag tag, byte[] gzip, long version) {
    }

    private record InFlightBatch(ChunkDeltaBatch batch, byte[] encoded,
                                 List<Candidate> candidates) {
        private InFlightBatch {
            encoded = encoded.clone();
            candidates = List.copyOf(candidates);
        }
    }

    /** 在锁外压缩，锁内只做快照身份检查和队列摘取。 */
    private List<Candidate> collectCandidates() {
        List<Map.Entry<ChunkKey, CompoundTag>> snapshot = new ArrayList<>();
        synchronized (queue) {
            if (shutdown || exitFrozen || queue.isEmpty()) {
                return List.of();
            }
            for (Map.Entry<ChunkKey, CompoundTag> entry : queue.entrySet()) {
                snapshot.add(Map.entry(entry.getKey(), entry.getValue()));
                if (snapshot.size() >= MAX_BATCH_DELTAS) {
                    break;
                }
            }
        }
        List<Candidate> result = new ArrayList<>(snapshot.size());
        for (Map.Entry<ChunkKey, CompoundTag> entry : snapshot) {
            ChunkKey key = entry.getKey();
            CompoundTag tag = entry.getValue();
            try {
                if (!claimedPredicate.test(key)) {
                    removeStateIfCurrent(key, tag);
                    continue;
                }
                byte[] gzip = compress(tag);
                if (gzip == null || gzip.length > MAX_COMPRESSED_BYTES) {
                    LOGGER.warn("区块 NBT 压缩后超过实时帧长上限，留待退出分片提交: {}",
                            key.asString());
                    removeIfCurrent(key, tag);
                    continue;
                }
                if (removeIfAlreadyDurable(key, tag, gzip)) {
                    continue;
                }
                synchronized (queue) {
                    if (shutdown || exitFrozen || queue.get(key) != tag) {
                        continue;
                    }
                    result.add(new Candidate(key, tag, gzip,
                            snapshotVersions.getOrDefault(key, 0L)));
                }
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "区块快照准备失败: " + key.asString(), e);
            }
        }
        return result;
    }

    private void scheduleRetry(ChunkKey key, Exception error) {
        if (key != null) {
            LOGGER.warn("区块上行失败，保留最新状态等待重试: {}",
                    key.asString(), error);
        } else {
            LOGGER.warn("区块上行批次准备失败，保留队列等待重试", error);
        }
        synchronized (queue) {
            workerRunning = false;
        }
        retryScheduler.accept(() -> {
            synchronized (queue) {
                if (!workerRunning && !queue.isEmpty()
                        && !shutdown && !exitFrozen) {
                    workerRunning = true;
                    executor.execute(this::drainLoop);
                }
            }
        });
    }

    private void sendSingle(Candidate candidate) throws Exception {
        JsonObject json = new JsonObject();
        json.addProperty("dimension", candidate.key().dimension());
        json.addProperty("x", candidate.key().x());
        json.addProperty("z", candidate.key().z());
        json.addProperty("groupId", groupId.toString());
        // 同一未确认快照重试时 uploadId 保持稳定；同内容的后续新版本仍有不同 ID，
        // 从而同时满足 ACK 丢失幂等和 A→B→A 的事件顺序。
        UUID uploadId = UUID.nameUUIDFromBytes(
                (runtimeBatchId + ":single:" + candidate.version())
                        .getBytes(StandardCharsets.UTF_8));
        json.addProperty("uploadId", uploadId.toString());
        java.util.concurrent.CompletableFuture<ControlMessage> responseFuture;
        synchronized (queue) {
            if (shutdown || exitFrozen || queue.get(candidate.key()) != candidate.tag()) {
                return;
            }
            responseFuture = conn.request(ControlMessage.of(
                    MessageType.CHUNK_DATA_UPLOAD, json, candidate.gzip()));
        }
        ControlMessage response = responseFuture
                .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (response.type() != MessageType.CHUNK_DATA_UPLOAD
                || !imsng.player_to_player.util.JsonUtil.getBoolean(
                response.json(), "written", false)) {
            throw new IllegalStateException("区块上行未获写入确认");
        }
        markDurable(candidate);
    }

    private void sendBatches(List<Candidate> candidates) throws Exception {
        InFlightBatch replay;
        synchronized (queue) {
            replay = inFlightBatch;
        }
        if (replay != null) {
            sendBatch(replay);
            return;
        }

        List<ChunkDelta> deltas = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            deltas.add(ChunkDelta.of(candidate.key(), candidate.version(), candidate.gzip()));
        }
        List<ChunkDeltaBatch> batches;
        try {
            batches = ChunkDeltaBatch.split(runtimeBatchId, 0, deltas,
                    BATCH_RECORD_BYTES);
        } catch (IllegalArgumentException tooLarge) {
            // 单个极大区块已经由 collectCandidates 过滤；理论上只可能是整数溢出，
            // 退回逐项路径让现有分片退出流程接管。
            for (Candidate candidate : candidates) {
                sendSingle(candidate);
            }
            return;
        }
        if (batches.size() < candidates.size()) {
            LOGGER.debug("区块上行批次合并: {} 个区块 → {} 个批次"
                            + "（服务端每批一次 WAL append + 一次 force 覆盖批内全部区块）",
                    candidates.size(), batches.size());
        }
        for (ChunkDeltaBatch batch : batches) {
            int sequence;
            synchronized (queue) {
                sequence = nextBatchSequence;
            }
            ChunkDeltaBatch outbound = new ChunkDeltaBatch(runtimeBatchId, 0,
                    sequence, batch.deltas());
            byte[] encoded = outbound.encode();
            List<Candidate> batchCandidates = new ArrayList<>(outbound.deltas().size());
            for (ChunkDelta delta : outbound.deltas()) {
                Candidate candidate = findCandidate(candidates, delta.key());
                if (candidate == null) {
                    return;
                }
                batchCandidates.add(candidate);
            }
            InFlightBatch pending = new InFlightBatch(
                    outbound, encoded, batchCandidates);
            synchronized (queue) {
                if (shutdown || exitFrozen) {
                    return;
                }
                boolean allCurrent = true;
                for (Candidate candidate : batchCandidates) {
                    if (queue.get(candidate.key()) != candidate.tag()) {
                        allCurrent = false;
                        break;
                    }
                }
                if (!allCurrent) {
                    return; // 下一轮会以最新版本重新合并
                }
                inFlightBatch = pending;
            }
            sendBatch(pending);
        }
    }

    /** 发送或逐字节重放一个已冻结的在途批次。 */
    private void sendBatch(InFlightBatch pending) throws Exception {
        JsonObject json = new JsonObject();
        json.addProperty("batchId", pending.batch().batchId().toString());
        json.addProperty("sequence", pending.batch().sequence());
        json.addProperty("deltaCount", pending.batch().deltas().size());
        json.addProperty("sha256", Sha256.hex(pending.encoded()));
        CompletableFuture<ControlMessage> responseFuture;
        synchronized (queue) {
            if (shutdown || exitFrozen || inFlightBatch != pending) {
                return;
            }
            responseFuture = conn.request(ControlMessage.of(
                    MessageType.CHUNK_DATA_BATCH, json, pending.encoded()));
            inFlightBatchRequest = responseFuture;
        }
        ControlMessage response;
        try {
            response = responseFuture
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (CancellationException cancelled) {
            synchronized (queue) {
                if (inFlightBatch != pending) {
                    // forget 已经推进 sequence 并移除被释放区块；直接回到 drainLoop
                    // 重组剩余队列，不能把本地取消误当成网络失败进入退避。
                    return;
                }
            }
            throw cancelled;
        } finally {
            synchronized (queue) {
                if (inFlightBatchRequest == responseFuture) {
                    inFlightBatchRequest = null;
                }
            }
        }
        if (response.type() != MessageType.CHUNK_DATA_BATCH
                || !imsng.player_to_player.util.JsonUtil.getBoolean(
                response.json(), "written", false)) {
            throw new IllegalStateException("区块增量批次未获 durable ACK");
        }
        synchronized (queue) {
            if (inFlightBatch != pending) {
                return;
            }
            nextBatchSequence = pending.batch().sequence() + 1;
            inFlightBatch = null;
        }
        for (Candidate candidate : pending.candidates()) {
            markDurable(candidate);
        }
    }

    private static Candidate findCandidate(List<Candidate> candidates, ChunkKey key) {
        for (Candidate candidate : candidates) {
            if (candidate.key().equals(key)) {
                return candidate;
            }
        }
        return null;
    }

    private void markDurable(Candidate candidate) {
        String digest = Sha256.hex(candidate.gzip());
        synchronized (queue) {
            // 先记录 durable 基线，再按对象身份清理，避免旧 ACK 覆盖新版本状态。
            durableHashes.put(candidate.key(), digest);
        }
        if (claimedPredicate.test(candidate.key())) {
            try {
                serverDataKnownCallback.accept(candidate.key());
            } catch (RuntimeException callbackError) {
                LOGGER.warn("区块上行已写入但更新服务端数据基线失败: {}",
                        candidate.key().asString(), callbackError);
            }
        }
        removeStateIfCurrent(candidate.key(), candidate.tag());
        LOGGER.debug("区块上行: {} ({} 字节压缩)",
                candidate.key().asString(), candidate.gzip().length);
    }

    /**
     * 删除一个确切快照对应的全部实时状态。queue 与未确认快照必须在同一临界区内分别按
     * 对象身份删除，从而同时防止两类 ABA 竞态：旧请求的迟到 ACK 不能确认新版本，旧 claim
     * 的清理线程也不能删除同坐标重新申领后产生的新版本。
     */
    private void removeStateIfCurrent(ChunkKey key, CompoundTag tag) {
        synchronized (queue) {
            if (queue.get(key) == tag) {
                queue.remove(key);
            }
            if (unacknowledgedSnapshots.get(key) == tag) {
                unacknowledgedSnapshots.remove(key);
                snapshotVersions.remove(key);
            }
        }
    }

    private void removeIfCurrent(ChunkKey key, CompoundTag tag) {
        synchronized (queue) {
            if (queue.get(key) == tag) {
                queue.remove(key);
            }
        }
    }

    /**
     * 删除已经被服务端确认的同内容快照。
     *
     * @return true 表示当前快照已是 durable 基线，不应再发网络请求
     */
    private boolean removeIfAlreadyDurable(ChunkKey key, CompoundTag tag,
                                           byte[] gzip) {
        String digest = Sha256.hex(gzip);
        synchronized (queue) {
            if (!digest.equals(durableHashes.get(key))) {
                return false;
            }
            if (queue.get(key) == tag) {
                queue.remove(key);
            }
            if (unacknowledgedSnapshots.get(key) == tag) {
                unacknowledgedSnapshots.remove(key);
                snapshotVersions.remove(key);
            }
            return true;
        }
    }

    /**
     * 退出冻结后压缩并清理停服阶段捕获的重复快照。普通实时 worker 已停止，
     * 因此退出 bundle 构建前必须显式调用本方法完成最后一次去重。
     */
    public void compactForExit() {
        Map<ChunkKey, CompoundTag> candidates;
        synchronized (queue) {
            candidates = new java.util.HashMap<>(unacknowledgedSnapshots);
        }
        for (Map.Entry<ChunkKey, CompoundTag> entry : candidates.entrySet()) {
            byte[] gzip = compress(entry.getValue());
            if (gzip == null) {
                continue;
            }
            removeIfAlreadyDurable(entry.getKey(), entry.getValue(), gzip);
        }
    }

    /** 当前是否存在尚未获得物理服务端写入确认的版本。 */
    public boolean hasUnacknowledged(ChunkKey key) {
        synchronized (queue) {
            return unacknowledgedSnapshots.containsKey(key);
        }
    }

    /**
     * 取得最新未确认快照的防御性副本；当前版本已经确认或从未捕获过时返回 {@code null}。
     */
    public CompoundTag latestUnacknowledged(ChunkKey key) {
        synchronized (queue) {
            CompoundTag tag = unacknowledgedSnapshots.get(key);
            return tag != null ? tag.copy() : null;
        }
    }

    /**
     * 兼容现有调用点的旧名称；语义现已收窄为“最新未确认快照”，不再返回历史已确认版本。
     */
    public CompoundTag latestSnapshot(ChunkKey key) {
        return latestUnacknowledged(key);
    }

    /**
     * 原子遗忘一个区块的实时待发项与未确认快照。
     * <p>
     * 已安排的重试任务只会重新检查当前 queue，因此本方法返回后它不能复活这个 key；若请求
     * 已经在途，其迟到 ACK 也会因对象已不再是当前项而成为无害操作。
     */
    public void forget(ChunkKey key) {
        CompletableFuture<ControlMessage> requestToCancel = null;
        synchronized (queue) {
            if (inFlightBatch != null && inFlightBatch.candidates().stream()
                    .anyMatch(candidate -> candidate.key().equals(key))) {
                // 服务端可能已经 force 了旧批次，只是 ACK 丢失。载荷发生变化后
                // 绝不能复用原 sequence；留下空洞比把新内容绑定到旧幂等键安全。
                nextBatchSequence = Math.max(nextBatchSequence,
                        Math.addExact(inFlightBatch.batch().sequence(), 1));
                inFlightBatch = null;
                requestToCancel = inFlightBatchRequest;
                inFlightBatchRequest = null;
            }
            queue.remove(key);
            unacknowledgedSnapshots.remove(key);
            durableHashes.remove(key);
            snapshotVersions.remove(key);
        }
        if (requestToCancel != null) {
            requestToCancel.cancel(false);
        }
    }

    /**
     * 冻结普通实时上传，供全局退出事务固定最终状态。
     * <p>
     * 已经调用 {@link ControlConnection#request(ControlMessage)} 的请求允许自然完成；之后的
     * enqueue 仍覆盖最新未确认状态，但不会启动 worker，最终由退出 bundle 读取。
     */
    public void freezeForExit() {
        synchronized (queue) {
            exitFrozen = true;
        }
    }

    /**
     * 同步压缩一份区块 NBT（供 CHUNK_RELEASE 最终数据复用；io/事件线程调用）。
     *
     * @return gzip 字节；失败返回 null
     */
    public static byte[] compress(CompoundTag tag) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(64 * 1024);
            NbtIo.writeCompressed(tag, bos);
            return bos.toByteArray();
        } catch (Exception e) {
            LOGGER.warn("区块 NBT 压缩失败", e);
            return null;
        }
    }

    /** 关停：丢弃残余队列（停服路径的最终数据由 CHUNK_UNLOAD 释放流程负责）。 */
    public void shutdown() {
        shutdown = true;
        synchronized (queue) {
            inFlightBatch = null;
            queue.clear();
            unacknowledgedSnapshots.clear();
            durableHashes.clear();
            snapshotVersions.clear();
        }
    }
}
