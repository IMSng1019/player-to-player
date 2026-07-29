package imsng.player_to_player.group;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.Tag;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * 把物理服务器权威玩家状态安装到主客户端本地存档。
 * <p>
 * 除覆盖 {@code playerdata/<uuid>.dat} 外，还会把同一权威状态写入
 * {@code level.dat} 与 {@code level.dat_old} 的 {@code Data.Player}。
 * Minecraft 1.20.4 对单人世界所有者会优先读取这个内嵌标签；只要它非 null，
 * 就不会回退读取正确的 UUID 玩家文件。因此三个位置必须保持同一权威状态，
 * 不能通过删除内嵌标签来猜测原版加载器一定会返回 null。
 */
public final class PlayerStateFiles {

    private PlayerStateFiles() {
    }

    /**
     * 原子安装权威玩家状态。所有输入文件先完成解析和临时写入，之后才替换目标文件。
     */
    public static void installAuthoritative(Path saveDir, UUID playerId,
                                            CompoundTag playerState) throws IOException {
        if (saveDir == null || playerId == null || playerState == null) {
            throw new IOException("安装玩家状态所需参数不完整");
        }

        Path levelFile = saveDir.resolve("level.dat");
        Path levelOldFile = saveDir.resolve("level.dat_old");
        CompoundTag level = readAndInstallEmbeddedPlayer(levelFile, playerState);
        CompoundTag levelOld = Files.isRegularFile(levelOldFile)
                ? readAndInstallEmbeddedPlayer(levelOldFile, playerState)
                : null;

        Path playerDir = saveDir.resolve("playerdata");
        Path playerFile = playerDir.resolve(playerId + ".dat");
        Path playerTmp = playerDir.resolve(playerId + ".dat.p2p-tmp");
        Path levelTmp = saveDir.resolve("level.dat.p2p-tmp");
        Path levelOldTmp = saveDir.resolve("level.dat_old.p2p-tmp");

        Files.createDirectories(playerDir);
        try {
            deleteTemps(playerTmp, levelTmp, levelOldTmp);
            // 先把全部内容写入同目录临时文件；解析/编码失败时现有存档保持不变。
            NbtIo.writeCompressed(playerState, playerTmp);
            NbtIo.writeCompressed(level, levelTmp);
            if (levelOld != null) {
                NbtIo.writeCompressed(levelOld, levelOldTmp);
            }

            replace(playerTmp, playerFile);
            replace(levelTmp, levelFile);
            if (levelOld != null) {
                replace(levelOldTmp, levelOldFile);
            }
        } finally {
            deleteTemps(playerTmp, levelTmp, levelOldTmp);
        }
    }

    /**
     * 退出事务构建最终玩家数据时，优先采用集成服停服后原版刚写出的玩家文件。
     * 文件不存在或校验失败才使用较早的内存快照；两者都不可用时拒绝构建 bundle。
     */
    public static byte[] readFinalPlayerData(Path playerDataDir, UUID playerId,
                                             byte[] fallback) throws IOException {
        if (playerDataDir == null || playerId == null) {
            throw new IOException("读取最终玩家状态所需参数不完整");
        }
        Path file = playerDataDir.resolve(playerId + ".dat");
        if (Files.isRegularFile(file)) {
            try {
                byte[] bytes = Files.readAllBytes(file);
                PlayerStateNbt.decodeValidated(bytes);
                return bytes;
            } catch (Exception e) {
                if (fallback == null) {
                    throw new IOException("最终玩家文件校验失败: " + file, e);
                }
            }
        }
        if (fallback != null) {
            try {
                PlayerStateNbt.decodeValidated(fallback);
                return fallback.clone();
            } catch (Exception e) {
                throw new IOException("最终玩家状态与内存兜底均不可用: " + playerId, e);
            }
        }
        throw new IOException("缺少最终玩家状态: " + playerId);
    }

    /**
     * 读取一个 level 文件，并把单人世界所有者状态替换为权威玩家 NBT。
     * 使用 {@link CompoundTag#copy()}，避免两个 level 根与调用方共享可变标签实例。
     */
    private static CompoundTag readAndInstallEmbeddedPlayer(Path file,
                                                             CompoundTag playerState)
            throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new IOException("本地存档缺少 " + file.getFileName());
        }
        CompoundTag root;
        try {
            root = NbtIo.readCompressed(file, NbtAccounter.unlimitedHeap());
        } catch (IOException | RuntimeException e) {
            throw new IOException("无法读取本地 " + file.getFileName(), e);
        }
        if (!root.contains("Data", Tag.TAG_COMPOUND)) {
            throw new IOException(file.getFileName() + " 缺少 Data 复合标签");
        }
        root.getCompound("Data").put("Player", playerState.copy());
        return root;
    }

    private static void replace(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void deleteTemps(Path... paths) throws IOException {
        IOException failure = null;
        for (Path path : paths) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
