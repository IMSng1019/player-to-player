package imsng.player_to_player.group;

import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
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
 *   <li><b>快照</b>：遍历本组占用区块，逐个在<b>服务器主线程</b>序列化
 *       （NBT 一经生成即不可变），gzip 与发送在本 io 线程 —— 主线程每次只被
 *       占用单区块序列化的微秒~毫秒级时间，游戏不卡顿；玩家数据一并快照；</li>
 *   <li><b>追赶</b>：SNAPSHOT_DONE / ACK_SNAPSHOT 握手后循环发送积累的增量，
 *       直至增量队列吃空（B 已接近同步）；</li>
 *   <li><b>原子切换</b>：{@link GroupRuntime#setTickFrozen 冻结} A 的世界演算
 *       → 主线程 saveAllChunks（所有脏区块经写盘咽喉产生最终增量）→ 发送尾部
 *       → TAIL_DONE / ACK_TAIL 握手。<b>冻结窗口只覆盖这一段</b>（存盘 + 尾部
 *       传输 + 一次 ACK 往返），正常在数百毫秒内（规范目标 300ms 量级）。</li>
 * </ol>
 * 任何失败即抛出（调用方回报 MERGE_PROGRESS failed 并解冻），A 继续运行 ——
 * 规范"若 B 追赶失败或断开，A 继续运行，服务端取消合并"。
 * <p>
 * 线程模型：{@link #run} 整体在 io 线程（阻塞流读写允许）；序列化经
 * {@code server.execute} 回主线程；增量分接的消费在主线程（只做 map put）。
 */
public final class PresyncSender {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/presync-send");

    /** 等待对端 ACK 的上限（毫秒）：超时按失败处理（B 死亡/网络断）。 */
    private static final long ACK_TIMEOUT_MILLIS = 60_000;

    /** 追赶阶段的最大轮数：防对端消费极慢时无限追（超过即失败，A 继续运行）。 */
    private static final int MAX_CATCHUP_ROUNDS = 50;

    private final ReliableChannel channel;
    private final MinecraftServer server;
    private final ChunkClaimClient claims;
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
        this.channel = channel;
        this.server = server;
        this.claims = claims;
        this.label = label;
        this.progress = progress;
    }

    /**
     * 执行完整预同步（io 线程，阻塞至切换完成或失败抛出）。
     * 成功返回后 A 的世界仍处于<b>冻结</b>状态且 B 已确认尾部 —— 调用方应立即
     * 回报 switched，随后走"让出"流程（关停集成服务端、以副客户端重连 B）。
     */
    public void run() throws Exception {
        DataOutputStream out = new DataOutputStream(channel.outputStream());
        DataInputStream in = new DataInputStream(channel.inputStream());
        PresyncTaps.add(tap);
        boolean frozen = false;
        try {
            progress.accept("presync_started");

            // ---- 1. 快照：占用区块 + 玩家数据 ----
            Set<ChunkKey> snapshot = claims.claimedSnapshot();
            LOGGER.info("预同步 {} 快照开始: {} 个区块", label, snapshot.size());
            int sent = 0;
            for (ChunkKey key : snapshot) {
                byte[] gzip = serializeChunkOnMainThread(key);
                if (gzip != null) {
                    PresyncProtocol.writeRecord(out, PresyncProtocol.TYPE_CHUNK,
                            key.asString(), gzip);
                    sent++;
                }
                // 未加载/序列化失败的区块跳过：其最新态已在服务端存档
                //（存盘上行），B 申请时走服务端拉取路径，无数据损失
            }
            sendPlayerData(out);
            PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_SNAPSHOT_DONE);
            awaitAck(in, PresyncProtocol.TYPE_ACK_SNAPSHOT);
            progress.accept("snapshot_done");
            LOGGER.info("预同步 {} 快照完成: 实发 {} 个区块，进入增量追赶", label, sent);

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
                    byte[] gzip = ChunkUploadService.compress(e.getValue());
                    if (gzip != null) {
                        PresyncProtocol.writeRecord(out, PresyncProtocol.TYPE_CHUNK,
                                e.getKey().asString(), gzip);
                    }
                }
            }
            progress.accept("caught_up");

            // ---- 3. 原子切换：冻结 → 全量存盘（脏区块产生最终增量）→ 尾部 ----
            long freezeStart = System.currentTimeMillis();
            GroupRuntime.setTickFrozen(true);
            frozen = true;
            // saveAllChunks 在主线程执行：所有 unsaved 区块经写盘咽喉 → 分接器
            //（suppressLogs=true, flush=true, forced=true —— 与停服 saveAll 同参）
            runOnMainThread(() -> server.saveAllChunks(true, true, true));
            Map<ChunkKey, CompoundTag> tail = drainDeltas();
            for (Map.Entry<ChunkKey, CompoundTag> e : tail.entrySet()) {
                byte[] gzip = ChunkUploadService.compress(e.getValue());
                if (gzip != null) {
                    PresyncProtocol.writeRecord(out, PresyncProtocol.TYPE_CHUNK,
                            e.getKey().asString(), gzip);
                }
            }
            sendPlayerData(out); // 玩家最终态（位置/背包）也在尾部重发一次
            PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_TAIL_DONE);
            awaitAck(in, PresyncProtocol.TYPE_ACK_TAIL);
            LOGGER.info("预同步 {} 切换完成: 尾部 {} 个区块，冻结窗口 {} ms",
                    label, tail.size(), System.currentTimeMillis() - freezeStart);
            // 成功路径不解冻：A 即将关停集成服务端，冻结状态随 detach 一起清除
        } catch (Exception e) {
            if (frozen) {
                GroupRuntime.setTickFrozen(false); // 失败：A 解冻继续运行
            }
            throw e;
        } finally {
            PresyncTaps.remove(tap);
        }
    }

    // ------------------------------------------------------------ 内部

    /** 主线程序列化单个区块并在本线程压缩；未加载/失败返回 null。 */
    private byte[] serializeChunkOnMainThread(ChunkKey key) {
        CompletableFuture<CompoundTag> future = new CompletableFuture<>();
        server.execute(() -> {
            try {
                ServerLevel level = resolveLevel(key.dimension());
                LevelChunk chunk = level != null
                        ? level.getChunkSource().getChunkNow(key.x(), key.z())
                        : null;
                future.complete(chunk != null ? ChunkSerializer.write(level, chunk) : null);
            } catch (Exception e) {
                LOGGER.warn("预同步序列化失败（跳过，服务端存档兜底）: {}", key.asString(), e);
                future.complete(null);
            }
        });
        try {
            CompoundTag tag = future.get(ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return tag != null ? ChunkUploadService.compress(tag) : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** 发送全部在线玩家的 NBT（B 写入其存档 playerdata，玩家背包/位置连续）。 */
    private void sendPlayerData(DataOutputStream out) throws Exception {
        CompletableFuture<Map<String, byte[]>> future = new CompletableFuture<>();
        server.execute(() -> {
            Map<String, byte[]> result = new LinkedHashMap<>();
            for (var player : server.getPlayerList().getPlayers()) {
                try {
                    CompoundTag tag = player.saveWithoutId(new CompoundTag());
                    byte[] gzip = ChunkUploadService.compress(tag);
                    if (gzip != null) {
                        result.put(player.getUUID().toString(), gzip);
                    }
                } catch (Exception e) {
                    LOGGER.warn("预同步玩家数据序列化失败: {}", player.getUUID(), e);
                }
            }
            future.complete(result);
        });
        for (Map.Entry<String, byte[]> e
                : future.get(ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).entrySet()) {
            PresyncProtocol.writeRecord(out, PresyncProtocol.TYPE_PLAYER, e.getKey(), e.getValue());
        }
    }

    /** 摘取当前全部增量（锁内换出，锁外使用）。 */
    private Map<ChunkKey, CompoundTag> drainDeltas() {
        synchronized (deltas) {
            if (deltas.isEmpty()) {
                return Map.of();
            }
            Map<ChunkKey, CompoundTag> batch = new LinkedHashMap<>(deltas);
            deltas.clear();
            return batch;
        }
    }

    /** 阻塞等待对端的指定 ACK 标记（其他类型即协议错误）。 */
    private void awaitAck(DataInputStream in, byte expected) throws IOException {
        // ReliableChannel 输入流阻塞语义：底层死亡抛 IOException，无需自旋
        byte type = in.readByte();
        if (type != expected) {
            throw new IOException("预同步协议错误: 期望 ACK " + expected + " 收到 " + type);
        }
    }

    /** 在主线程执行任务并等待完成。 */
    private void runOnMainThread(Runnable task) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        server.execute(() -> {
            try {
                task.run();
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        future.get(ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** 按维度标识解析 ServerLevel。 */
    private ServerLevel resolveLevel(String dimension) {
        for (ServerLevel level : server.getAllLevels()) {
            if (level.dimension().location().toString().equals(dimension)) {
                return level;
            }
        }
        return null;
    }
}
