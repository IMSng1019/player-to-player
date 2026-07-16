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
 * 除覆盖 {@code playerdata/<uuid>.dat} 外，还会删除 {@code level.dat} 与
 * {@code level.dat_old} 中的 {@code Data.Player}。原版集成服务器对单人世界
 * 所有者会优先读取该内嵌标签；若保留空或陈旧内容，正确的 UUID 玩家文件不会被读取。
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
        CompoundTag level = readAndStripEmbeddedPlayer(levelFile);
        CompoundTag levelOld = Files.isRegularFile(levelOldFile)
                ? readAndStripEmbeddedPlayer(levelOldFile)
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

    private static CompoundTag readAndStripEmbeddedPlayer(Path file) throws IOException {
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
        root.getCompound("Data").remove("Player");
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
