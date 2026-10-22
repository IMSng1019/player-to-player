package imsng.player_to_player.group;

import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * 预同步接收端（Phase 3，合并接管方 B 侧；协议见 {@link PresyncProtocol}）。
 * <p>
 * 当前实现只暂存区块 NBT 和玩家文件，不运行影子服务端，也不重放每 tick 事件。
 * 实体分区、POI、世界 SavedData 与任意模组内存状态尚未在此协议中完整覆盖，
 * 因此不能宣称与运行中的影子世界等价；完整热迁移必须另行实现这些状态的屏障。
 * <ul>
 *   <li>区块记录 → 解压校验 → {@link PresyncStore}（B 集成服务端的加载门控
 *       申请授予后直接消费，省服务端往返）；</li>
 *   <li>玩家记录 → 写入 B 本地存档的 {@code playerdata/<uuid>.dat}（原子写；
 *       原主的玩家以副客户端重连 B 时，B 的集成服务端从这里恢复其状态）；</li>
 *   <li>SNAPSHOT_DONE / TAIL_DONE → 回 ACK（发送方据此推进阶段）。</li>
 * </ul>
 * 收到 TAIL_DONE 并回 ACK 后返回 —— 此刻 A 已冻结、最终态已全部在 B 手中，
 * B（或其 MergeClient）回报服务端 caught_up/switched 由发送方与协调器编排。
 * <p>
 * 线程模型：{@link #run} 整体在 io 线程（阻塞流读取允许）。
 */
public final class PresyncReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/presync-recv");

    /** 单条记录 NBT 解压上限（与发送侧/服务端一致；防解压炸弹）。 */
    private static final long MAX_NBT_BYTES = 64L * 1024 * 1024;

    private final ReliableChannel channel;
    /** B 本地存档的 playerdata 目录（null = 不落玩家数据，仅暂存区块）。 */
    private final Path playerDataDir;
    private final String label;
    /** 进度回调（阶段名；MergeClient 转成 MERGE_PROGRESS 上报服务端）。 */
    private final Consumer<String> progress;
    private volatile boolean cancelled;

    /** 与入库互斥，返回后旧接收线程不能再次写暂存库或玩家文件。 */
    public synchronized void cancel() { cancelled = true; }

    private void requireActive() throws IOException {
        if (cancelled) throw new IOException("预同步事务已取消");
    }

    public PresyncReceiver(ReliableChannel channel, Path playerDataDir, String label,
                           Consumer<String> progress) {
        this.channel = channel;
        this.playerDataDir = playerDataDir;
        this.label = label;
        this.progress = progress;
    }

    /**
     * 执行接收循环（io 线程，阻塞至 TAIL_DONE 或通道死亡抛出）。
 * 成功返回仅表示本协议携带的记录已经通过校验并暂存，不代表物理服耐久提交。
     */
    public void run() throws Exception {
        DataInputStream in = new DataInputStream(channel.inputStream());
        DataOutputStream out = new DataOutputStream(channel.outputStream());
        int chunks = 0;
        int players = 0;
        long compressedBytes = 0;
        long readNanos = 0;
        long chunkStageNanos = 0;
        long playerStoreNanos = 0;
        progress.accept("presync_started");
        while (true) {
            requireActive();
            long readStarted = System.nanoTime();
            byte type;
            try {
                type = in.readByte();
            } catch (EOFException e) {
                throw new IOException("预同步流提前结束（发送方断开）", e);
            }
            switch (type) {
                case PresyncProtocol.TYPE_CHUNK -> {
                    String rawKey = in.readUTF();
                    byte[] gzip = PresyncProtocol.readPayload(in);
                    readNanos += System.nanoTime() - readStarted;
                    compressedBytes += gzip.length;
                    long stageStarted = System.nanoTime();
                    stageChunk(rawKey, gzip);
                    chunkStageNanos += System.nanoTime() - stageStarted;
                    chunks++;
                }
                case PresyncProtocol.TYPE_PLAYER -> {
                    String rawUuid = in.readUTF();
                    byte[] gzip = PresyncProtocol.readPayload(in);
                    readNanos += System.nanoTime() - readStarted;
                    compressedBytes += gzip.length;
                    long storeStarted = System.nanoTime();
                    writePlayerData(rawUuid, gzip);
                    playerStoreNanos += System.nanoTime() - storeStarted;
                    players++;
                }
                case PresyncProtocol.TYPE_SNAPSHOT_DONE -> {
                    readNanos += System.nanoTime() - readStarted;
                    PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_ACK_SNAPSHOT);
                    progress.accept("snapshot_done");
                    // read_wait 含发送方采集/压缩空隙与网络等待，不能当作纯链路传输耗时。
                    // 区块计时包含解压、合法性校验与暂存；玩家计时另外包含原子文件写入。
                    LOGGER.info("预同步 {} 快照已入库: {} 区块 / {} 玩家 gzip_bytes={}"
                                    + " read_wait_ms={} chunk_decode_stage_ms={} player_decode_store_ms={}，等待增量",
                            label, chunks, players, compressedBytes, readNanos / 1_000_000,
                            chunkStageNanos / 1_000_000, playerStoreNanos / 1_000_000);
                }
                case PresyncProtocol.TYPE_TAIL_DONE -> {
                    readNanos += System.nanoTime() - readStarted;
                    PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_ACK_TAIL);
                    LOGGER.info("预同步 {} 完成: 共 {} 区块 / {} 玩家（含尾部最终态）gzip_bytes={}"
                                    + " read_wait_ms={} chunk_decode_stage_ms={} player_decode_store_ms={}",
                            label, chunks, players, compressedBytes, readNanos / 1_000_000,
                            chunkStageNanos / 1_000_000, playerStoreNanos / 1_000_000);
                    return;
                }
                default -> throw new IOException("预同步协议错误: 未知记录类型 " + type);
            }
        }
    }

    // ------------------------------------------------------------ 入库

    /** 必需记录失败即终止会话；没有权威耐久基线证明时不能跳过后仍 ACK。 */
    synchronized void stageChunk(String rawKey, byte[] gzip) throws IOException {
        requireActive();
        try {
            ChunkKey key = ChunkKey.parse(rawKey);
            CompoundTag tag = NbtIo.readCompressed(new ByteArrayInputStream(gzip),
                    NbtAccounter.create(MAX_NBT_BYTES));
            // 坐标一致性：NBT 里的 xPos/zPos 必须与声称的键一致（防错位；对端不可信）
            if (!tag.contains("xPos", 3) || !tag.contains("zPos", 3)
                    || tag.getInt("xPos") != key.x() || tag.getInt("zPos") != key.z())
                throw new IOException("预同步区块坐标缺失或不一致: " + rawKey);
            PresyncStore.put(key, tag);
        } catch (Exception e) {
            throw new IOException("预同步区块校验或暂存失败: " + rawKey, e);
        }
    }

    /** 玩家最终态写入失败禁止确认尾部，保留 A 的原组继续运行。 */
    private synchronized void writePlayerData(String rawUuid, byte[] gzip) throws IOException {
        requireActive();
        if (playerDataDir == null) {
            throw new IOException("预同步接收端没有玩家数据目录");
        }
        try {
            UUID uuid = UUID.fromString(rawUuid); // 同时充当文件名合法性校验（防路径穿越）
            CompoundTag tag = NbtIo.readCompressed(new ByteArrayInputStream(gzip),
                    NbtAccounter.create(MAX_NBT_BYTES));
            Files.createDirectories(playerDataDir);
            Path tmp = playerDataDir.resolve(uuid + ".dat.p2p-tmp");
            NbtIo.writeCompressed(tag, tmp);
            Files.move(tmp, playerDataDir.resolve(uuid + ".dat"),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            LOGGER.info("预同步玩家数据已写入本地存档: {}", uuid);
        } catch (Exception e) {
            throw new IOException("预同步玩家数据写入失败: " + rawUuid, e);
        }
    }
}
