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
 * <b>与规范"影子实例"的工程取舍</b>：规范设想 B 提前启动一个不可见的完整 MC
 * 服务端实例重放增量。客户端 JVM 同时只能有一个集成服务端（原版 Minecraft
 * 单例结构），真起第二个实例需要类加载器隔离，脆弱且吃内存。本实现用
 * <b>数据等价</b>方案达到同一目标：区块级全量覆盖幂等，"重放到影子世界"与
 * "暂存最新 NBT、接管后按需装载"最终态一致 ——
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

    public PresyncReceiver(ReliableChannel channel, Path playerDataDir, String label,
                           Consumer<String> progress) {
        this.channel = channel;
        this.playerDataDir = playerDataDir;
        this.label = label;
        this.progress = progress;
    }

    /**
     * 执行接收循环（io 线程，阻塞至 TAIL_DONE 或通道死亡抛出）。
     * 成功返回 = 最终态已全部入库；异常 = 预同步失败（调用方清空暂存并回报 failed）。
     */
    public void run() throws Exception {
        DataInputStream in = new DataInputStream(channel.inputStream());
        DataOutputStream out = new DataOutputStream(channel.outputStream());
        int chunks = 0;
        int players = 0;
        progress.accept("presync_started");
        while (true) {
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
                    stageChunk(rawKey, gzip);
                    chunks++;
                }
                case PresyncProtocol.TYPE_PLAYER -> {
                    String rawUuid = in.readUTF();
                    byte[] gzip = PresyncProtocol.readPayload(in);
                    writePlayerData(rawUuid, gzip);
                    players++;
                }
                case PresyncProtocol.TYPE_SNAPSHOT_DONE -> {
                    PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_ACK_SNAPSHOT);
                    progress.accept("snapshot_done");
                    LOGGER.info("预同步 {} 快照已入库: {} 区块 / {} 玩家，等待增量",
                            label, chunks, players);
                }
                case PresyncProtocol.TYPE_TAIL_DONE -> {
                    PresyncProtocol.writeMarker(out, PresyncProtocol.TYPE_ACK_TAIL);
                    LOGGER.info("预同步 {} 完成: 共 {} 区块 / {} 玩家（含尾部最终态）",
                            label, chunks, players);
                    return;
                }
                default -> throw new IOException("预同步协议错误: 未知记录类型 " + type);
            }
        }
    }

    // ------------------------------------------------------------ 入库

    /** 区块记录入暂存库（解压 + 键一致性校验；坏数据跳过不中断 —— 服务端存档兜底）。 */
    private void stageChunk(String rawKey, byte[] gzip) {
        try {
            ChunkKey key = ChunkKey.parse(rawKey);
            CompoundTag tag = NbtIo.readCompressed(new ByteArrayInputStream(gzip),
                    NbtAccounter.create(MAX_NBT_BYTES));
            // 坐标一致性：NBT 里的 xPos/zPos 必须与声称的键一致（防错位；对端不可信）
            if (tag.getInt("xPos") != key.x() || tag.getInt("zPos") != key.z()) {
                LOGGER.warn("预同步区块坐标不符，丢弃: 声称 {} 实际 {},{}",
                        rawKey, tag.getInt("xPos"), tag.getInt("zPos"));
                return;
            }
            PresyncStore.put(key, tag);
        } catch (Exception e) {
            LOGGER.warn("预同步区块解析失败，丢弃（服务端存档兜底）: {}", rawKey, e);
        }
    }

    /** 玩家数据写入 B 本地存档 playerdata（原子写；坏数据跳过）。 */
    private void writePlayerData(String rawUuid, byte[] gzip) {
        if (playerDataDir == null) {
            return;
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
            LOGGER.warn("预同步玩家数据写入失败，跳过: {}", rawUuid, e);
        }
    }
}
