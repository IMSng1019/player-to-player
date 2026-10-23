package imsng.player_to_player.group;

import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.mixin.ChunkMapSplitAccess;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ChunkHolder;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.storage.ChunkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 预同步发送端（Phase 3，合并让出方 A 侧；规范"预同步"）。
 * <p>
 * 在一条 {@link ReliableChannel}（P2P 直连或中转，上层透明）上执行完整的
 * 快照 + 增量追赶 + 原子切换序列（协议见 {@link PresyncProtocol}）：
 * <ol>
 *   <li><b>先挂增量分接</b>（{@link PresyncTaps}）再拍快照 —— 快照期间发生的
 *       写盘不会漏；增量按区块合并（同键覆盖，幂等最终态）；</li>
 *   <li><b>快照</b>：遍历本组占用区块，在<b>服务器主线程</b>按个数与时间预算
 *       分批序列化（NBT 生成后仅只读使用），gzip 与发送在后台工作线程。
 *       一批消费完再申请下一批，避免每块都独立排队或全量 NBT 同时驻留内存；
 *       单块序列化不可中断，时间预算是软上限。玩家数据一并快照；</li>
 *   <li><b>追赶</b>：SNAPSHOT_DONE / ACK_SNAPSHOT 握手后循环发送积累的增量，
 *       直至增量队列吃空（B 已接近同步）；</li>
 *   <li><b>切换尾部</b>：{@link GroupRuntime#setTickFrozen 冻结} A 的后续世界演算
 *       → 发送已有存盘增量 → 重新采集完整区块和玩家最终态 → 发送尾部
 *       → TAIL_DONE / ACK_TAIL 握手。<b>冻结窗口只覆盖这一段</b>（采集 + 尾部
 *       传输 + 一次 ACK 往返），实际时长由阶段日志测量；300ms 是目标而非保证。</li>
 * </ol>
 * 任何失败即抛出（调用方回报 MERGE_PROGRESS failed 并解冻），A 继续运行 ——
 * 规范"若 B 追赶失败或断开，A 继续运行，服务端取消合并"。
 * <p>
 * 线程模型：{@link #run} 整体在 io 线程（阻塞流读写允许）；序列化经
 * {@link GroupControlTasks} 回主线程；增量分接的消费在主线程（只做 map put）。
 * 区块等待中的轮询不是完整 tick 边界，不能据此宣称迁移了任意模组的在途调用栈。
 */
public final class PresyncSender {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/presync-send");

    /** 等待对端 ACK 的上限（毫秒）：超时按失败处理（B 死亡/网络断）。 */
    private static final long ACK_TIMEOUT_MILLIS = 60_000;

    /** 追赶阶段的最大轮数：防对端消费极慢时无限追（超过即失败，A 继续运行）。 */
    private static final int MAX_CATCHUP_ROUNDS = 50;

    /** 单次主线程快照任务的双重上限，避免一批大 NBT 长时间占住游戏 tick。 */
    private static final int SNAPSHOT_BATCH_CHUNKS = 32;
    private static final long SNAPSHOT_BATCH_NANOS = TimeUnit.MILLISECONDS.toNanos(2);

    private final ReliableChannel channel;
    private final SnapshotSource source;
    private final String label;
    /** 进度回调（阶段名；MergeClient 转成 MERGE_PROGRESS 上报服务端）。 */
    private final Consumer<String> progress;

    /** 增量队列：分接器捕获的快照后写盘（主线程写，io 线程锁内摘取）。 */
    private final LinkedHashMap<ChunkKey, CompoundTag> deltas = new LinkedHashMap<>();

    /** 挂在分接器上的消费者（结束时摘除）。 */
    private final BiConsumer<ChunkKey, CompoundTag> tap = (key, tag) -> {
        synchronized (deltas) {
            deltas.put(key, tag);
        }
    };

    public PresyncSender(ReliableChannel channel, MinecraftServer server,
                         ChunkClaimClient claims, String label, Consumer<String> progress) {
        this(channel, new MinecraftSnapshotSource(server, claims), label, progress);
    }

    /** 世界读取和传输分离，使同一真实协议流程可以验证悬置区块与尾部覆盖顺序。 */
    PresyncSender(ReliableChannel channel, SnapshotSource source,
                  String label, Consumer<String> progress) {
        this.channel = channel;
        this.source = source;
        this.label = label;
        this.progress = progress;
    }

    /**
     * 执行完整预同步（io 线程，阻塞至切换完成或失败抛出）。
     * 成功返回后 A 的世界仍处于<b>冻结</b>状态且 B 已确认尾部 —— 调用方应立即
     * 回报 switched，随后走"让出"流程（关停集成服务端、以副客户端重连 B）。
     */
    public void run() throws Exception {
        var timing = new imsng.player_to_player.util.WorkflowTrace("presync", label);
        DataOutputStream out = new DataOutputStream(channel.outputStream());
        DataInputStream in = new DataInputStream(channel.inputStream());
        PresyncTaps.add(tap);
        boolean frozen = false;
        try {
            progress.accept("presync_started");

            // ---- 1. 快照：占用区块 + 玩家数据 ----
            Set<ChunkKey> snapshot = source.claimedChunks();
            LOGGER.info("预同步 {} 快照开始: {} 个区块", label, snapshot.size());
            SnapshotTiming snapshotTiming = new SnapshotTiming();
            SnapshotBatchCollector<ChunkKey, CompoundTag> collector = new SnapshotBatchCollector<>(
                    snapshot.iterator(), source.executor(), this::captureChunkOnMainThread,
                    SNAPSHOT_BATCH_CHUNKS, SNAPSHOT_BATCH_NANOS, System::nanoTime);
            boolean snapshotFinished = snapshot.isEmpty();
            while (!snapshotFinished) {
                var pending = collector.next();
                SnapshotBatchCollector.Batch<ChunkKey, CompoundTag> batch;
                try {
                    batch = pending.get(ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                } catch (Exception error) {
                    // 主线程没能及时采集时终止预同步，不能把整批超时误报为正常空区块。
                    pending.cancel(false);
                    throw error;
                }
                snapshotFinished = batch.finished();
                snapshotTiming.batches++;
                snapshotTiming.queueNanos += batch.queueNanos();
                snapshotTiming.captureNanos += batch.captureNanos();
                for (Map.Entry<ChunkKey, CompoundTag> entry : batch.values()) {
                    compressAndWrite(out, PresyncProtocol.TYPE_CHUNK,
                            entry.getKey().asString(), entry.getValue(), snapshotTiming);
                }
                // 延续现有未加载区块的服务端拉取路径；分项日志显式记下未实发数量，
                // 不能把“占用区块数”误认为“完整世界状态已成功迁移的数量”。
            }
            sendPlayerData(out, snapshotTiming);
            PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_SNAPSHOT_DONE);
            timing.mark("snapshot_sent");
            LOGGER.info("预同步 {} 快照发送分项: batches={} chunks={} skipped={} players={} gzip_bytes={}"
                            + " main_queue_ms={} main_capture_ms={} compress_ms={} channel_write_ms={}",
                    label, snapshotTiming.batches, snapshotTiming.chunks,
                    snapshot.size() - snapshotTiming.chunks, snapshotTiming.players,
                    snapshotTiming.compressedBytes, snapshotTiming.queueNanos / 1_000_000,
                    snapshotTiming.captureNanos / 1_000_000,
                    snapshotTiming.compressNanos / 1_000_000, snapshotTiming.writeNanos / 1_000_000);
            long snapshotAckStart = System.nanoTime();
            awaitAck(in, PresyncProtocol.TYPE_ACK_SNAPSHOT);
            progress.accept("snapshot_done");
            timing.mark("snapshot_ack");
            LOGGER.info("预同步 {} 快照完成: 实发 {} 个区块，ack_wait_ms={}，进入增量追赶",
                    label, snapshotTiming.chunks, (System.nanoTime() - snapshotAckStart) / 1_000_000);

            // ---- 2. 增量追赶：循环吃空快照期间与追赶期间的写盘增量 ----
            int rounds = 0;
            while (true) {
                Map<ChunkKey, CompoundTag> batch = drainDeltas();
                if (batch.isEmpty()) {
                    break; // 已追平（无新增量），可以进入冻结切换
                }
                if (++rounds > MAX_CATCHUP_ROUNDS) {
                    throw new IOException("增量追赶超过 " + MAX_CATCHUP_ROUNDS + " 轮仍未收敛");
                }
                for (Map.Entry<ChunkKey, CompoundTag> e : batch.entrySet()) {
                    compressAndWrite(out, PresyncProtocol.TYPE_CHUNK,
                            e.getKey().asString(), e.getValue(), null);
                }
            }
            progress.accept("caught_up");
            timing.mark("catchup_sent");

            // ---- 3. 切换尾部：冻结 → 历史增量 → 当前完整区块最终态 ----
            long freezeStart = System.nanoTime();
            timing.mark("freeze_begin");
            frozen = true;
            // 冻结是实例校验后的 volatile 控制标志，可直接设置；不把置位排进可超时
            // 队列，避免“等待已取消，迟到任务却又把世界冻结”的竞态。世界读取仍走主线程。
            source.setFrozen(true);
            // 不能调用 saveAllChunks(true)：它等待 holder.chunkToSave，其中含等待
            // 他组归属的加载链；只有 COMMIT 才能解开该链，COMMIT 又在尾部之后。
            // 先发送旧存盘事件，再重采集内存最终态，避免旧 NBT 覆盖未保存修改。
            Map<ChunkKey, CompoundTag> tail = drainDeltas();
            for (Map.Entry<ChunkKey, CompoundTag> e : tail.entrySet()) {
                compressAndWrite(out, PresyncProtocol.TYPE_CHUNK,
                        e.getKey().asString(), e.getValue(), null);
            }
            SnapshotTiming tailTiming = new SnapshotTiming();
            sendFinalChunks(out, tailTiming);
            timing.mark("final_chunks_captured");
            sendPlayerData(out, null); // 玩家最终态（位置/背包）也在尾部重发一次
            PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_TAIL_DONE);
            awaitAck(in, PresyncProtocol.TYPE_ACK_TAIL);
            timing.finish("tail_ack");
            LOGGER.info("预同步 {} 切换完成: 历史增量 {} / 最终完整区块 {}，冻结窗口 {} ms",
                    label, tail.size(), tailTiming.chunks, (System.nanoTime() - freezeStart) / 1_000_000);
            // 成功路径不解冻：A 即将关停集成服务端，冻结状态随 detach 一起清除
        } catch (Exception e) {
            timing.finish("failed");
            if (frozen) {
                source.setFrozen(false); // 失败：A 解冻继续运行
            }
            throw e;
        } finally {
            PresyncTaps.remove(tap);
        }
    }

    // ------------------------------------------------------------ 内部

    /** 只在批次主线程任务中读取区块；NBT 交给后台工作线程压缩和发送。 */
    private CompoundTag captureChunkOnMainThread(ChunkKey key) {
        if (!channel.isOpen()) {
            throw new IllegalStateException("预同步通道已关闭，取消迟到的采集任务");
        }
        // 已加载区块的序列化失败必须传播；只有确实未加载时才能返回 null。
        return source.captureChunk(key);
    }

    /** 冻结后重新取占用集合，覆盖初始快照期间新加载的区块；仍按批次限制内存。 */
    private void sendFinalChunks(DataOutputStream out, SnapshotTiming timing) throws Exception {
        var collector = new SnapshotBatchCollector<>(source.claimedChunks().iterator(),
                source.executor(), this::captureChunkOnMainThread,
                SNAPSHOT_BATCH_CHUNKS, SNAPSHOT_BATCH_NANOS, System::nanoTime);
        boolean finished = false;
        while (!finished) {
            var pending = collector.next();
            SnapshotBatchCollector.Batch<ChunkKey, CompoundTag> batch;
            try {
                batch = pending.get(ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (Exception error) {
                pending.cancel(false);
                throw error;
            }
            finished = batch.finished();
            for (var entry : batch.values()) {
                compressAndWrite(out, PresyncProtocol.TYPE_CHUNK,
                        entry.getKey().asString(), entry.getValue(), timing);
            }
        }
    }

    /**
     * 玩家对象只能在主线程读取；压缩与网络写入回到当前后台线程，避免原实现把 gzip
     * 一并放在主线程，在多人或玩家 NBT 较大时增加游戏暂停。
     */
    private void sendPlayerData(DataOutputStream out, SnapshotTiming timing) throws Exception {
        long submitted = System.nanoTime();
        CompletableFuture<PlayerSnapshot> future = new CompletableFuture<>();
        source.executor().execute(() -> {
            if (future.isCancelled()) {
                return;
            }
            long started = System.nanoTime();
            try {
                if (!channel.isOpen()) throw new IOException("预同步通道已关闭");
                Map<String, CompoundTag> result = source.capturePlayers();
                future.complete(new PlayerSnapshot(result, started - submitted, System.nanoTime() - started));
            } catch (Exception error) {
                future.completeExceptionally(error);
            }
        });
        PlayerSnapshot players;
        try {
            players = future.get(ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Exception error) {
            future.cancel(false);
            throw error;
        }
        if (timing != null) {
            timing.queueNanos += players.queueNanos();
            timing.captureNanos += players.captureNanos();
        }
        for (Map.Entry<String, CompoundTag> entry : players.tags().entrySet()) {
            compressAndWrite(out, PresyncProtocol.TYPE_PLAYER, entry.getKey(), entry.getValue(), timing);
        }
    }

    /** 压缩与通道背压独立计时；write 时间只代表本端写流，不等同于对端已落地。 */
    private void compressAndWrite(DataOutputStream out, byte type, String key, CompoundTag tag,
                                  SnapshotTiming timing) throws IOException {
        long started = System.nanoTime();
        byte[] gzip = ChunkUploadService.compress(tag);
        if (timing != null) {
            timing.compressNanos += System.nanoTime() - started;
        }
        if (gzip == null) {
            throw new IOException("预同步 NBT 压缩失败: " + key);
        }
        started = System.nanoTime();
        PresyncProtocol.writeRecord(out, type, key, gzip);
        if (timing != null) {
            timing.writeNanos += System.nanoTime() - started;
            timing.compressedBytes += gzip.length;
            if (type == PresyncProtocol.TYPE_CHUNK) {
                timing.chunks++;
            } else {
                timing.players++;
            }
        }
    }

    private record PlayerSnapshot(Map<String, CompoundTag> tags, long queueNanos, long captureNanos) {
    }

    /** 全部由单条预同步后台线程汇总，不需额外原子字段，也不产出逐区块噪声日志。 */
    private static final class SnapshotTiming {
        private int batches;
        private int chunks;
        private int players;
        private long queueNanos;
        private long captureNanos;
        private long compressNanos;
        private long writeNanos;
        private long compressedBytes;
    }

    /** 摘取当前全部增量（锁内换出，锁外使用）。 */
    private Map<ChunkKey, CompoundTag> drainDeltas() {
        synchronized (deltas) {
            if (deltas.isEmpty()) {
                return Map.of();
            }
            Map<ChunkKey, CompoundTag> batch = new LinkedHashMap<>(deltas);
            deltas.clear();
            // 卸载后也必须发送最终存盘态：接收端可能已有该块的旧快照，单独按当前
            // claims 过滤增量会让旧快照残留。是否允许加载仍由接收端的 claim 门控决定。
            return batch;
        }
    }

    /** 阻塞等待对端的指定 ACK 标记（其他类型即协议错误）。 */
    private void awaitAck(DataInputStream in, byte expected) throws IOException {
        // 总超时覆盖实际阻塞读取，不依赖对端主动关闭。
        var timeout = imsng.player_to_player.util.ThreadPools.scheduler().schedule(
                channel::abort, ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        try {
            byte type = in.readByte();
            if (type != expected)
                throw new IOException("预同步协议错误: 期望 ACK " + expected + " 收到 " + type);
        } finally {
            timeout.cancel(false);
        }
    }

    /** 仅 executor/claimedChunks 与冻结标志可跨线程，捕获方法只在所属主线程调用。 */
    interface SnapshotSource {
        Executor executor();
        Set<ChunkKey> claimedChunks();
        CompoundTag captureChunk(ChunkKey key);
        Map<String, CompoundTag> capturePlayers();
        void setFrozen(boolean frozen);
    }

    private record MinecraftSnapshotSource(MinecraftServer server, ChunkClaimClient claims)
            implements SnapshotSource {
        public Executor executor() { return GroupControlTasks.executor(server); }
        public Set<ChunkKey> claimedChunks() { return claims.claimedSnapshot(); }

        private void requireCurrent() {
            if (!server.isSameThread() || GroupRuntime.server() != server
                    || GroupRuntime.claims() != claims) {
                throw new IllegalStateException("预同步所属组实例已失效或线程不正确");
            }
        }

        public CompoundTag captureChunk(ChunkKey key) {
            requireCurrent();
            if (!claims.isClaimed(key)) return null;
            for (ServerLevel level : server.getAllLevels()) {
                if (!level.dimension().location().toString().equals(key.dimension())) continue;
                LevelChunk chunk = level.getChunkSource().getChunkNow(key.x(), key.z());
                if (chunk == null) {
                    // 可见表之外仍可能有等待释放 ACK 的完整区块，不能遗漏其最后修改。
                    var access = (ChunkMapSplitAccess) level.getChunkSource().chunkMap;
                    long pos = ChunkPos.asLong(key.x(), key.z());
                    ChunkHolder holder = access.player_to_player$updatingChunks().get(pos);
                    if (holder == null) holder = access.player_to_player$pendingUnloads().get(pos);
                    if (holder != null) {
                        var full = holder.getFutureIfPresentUnchecked(
                                net.minecraft.world.level.chunk.ChunkStatus.FULL).getNow(null);
                        var ready = full == null ? null : full.left().orElse(null);
                        if (ready instanceof LevelChunk complete) chunk = complete;
                        else if (ready instanceof net.minecraft.world.level.chunk.ImposterProtoChunk imposter)
                            chunk = imposter.getWrapped();
                    }
                }
                // 不读取正在 worldgen 线程修改的 ProtoChunk，也不请求加载任何新块。
                return chunk == null ? null : ChunkSerializer.write(level, chunk);
            }
            throw new IllegalStateException("预同步区块维度不存在: " + key.dimension());
        }

        public Map<String, CompoundTag> capturePlayers() {
            requireCurrent();
            Map<String, CompoundTag> players = new LinkedHashMap<>();
            for (var player : server.getPlayerList().getPlayers()) {
                players.put(player.getUUID().toString(), player.saveWithoutId(new CompoundTag()));
            }
            return players;
        }

        public void setFrozen(boolean frozen) {
            // 失败处理可能来自后台；旧事务不得修改下一实例的冻结状态。
            if (GroupRuntime.server() == server && GroupRuntime.claims() == claims) {
                GroupRuntime.setTickFrozen(frozen);
            } else if (frozen) {
                throw new IllegalStateException("组实例已更换，不能冻结");
            }
        }
    }
}
