package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.group.PlayerStateNbt;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.Sha256;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.UUID;

/**
 * 最终退出载荷的耐久暂存区。
 * <p>
 * pending 阶段只写本目录，绝不触碰正式世界；committed 后由
 * {@link #apply(ExitManifest)} 幂等写入全部正式玩家/区块数据并 flush。
 */
public final class ExitPayloadStore
        implements ExitTransactionHandlers.PayloadStore {

    @FunctionalInterface
    public interface PlayerWriter {
        boolean write(UUID playerId, byte[] gzipNbt);
    }

    public interface ChunkAccess {
        boolean validate(UUID groupId, ChunkKey key, byte[] gzipNbt,
                         boolean preserveExisting);

        boolean write(UUID groupId, ChunkKey key, byte[] gzipNbt,
                      boolean preserveExisting);

        void flush();
    }

    private static final byte[] PRESERVE_MARKER = "preserve\n".getBytes(
            StandardCharsets.US_ASCII);

    private final Path root;
    private final PlayerWriter playerWriter;
    private final ChunkAccess chunks;

    public ExitPayloadStore(Path root, PlayerWriter playerWriter,
                            ChunkAccess chunks) {
        this.root = root;
        this.playerWriter = playerWriter;
        this.chunks = chunks;
    }

    @Override
    public boolean stagePlayer(ExitManifest manifest, UUID playerId,
                               byte[] gzipNbt) {
        if (manifest == null || playerId == null || gzipNbt == null
                || !manifest.playerIds().contains(playerId)) {
            return false;
        }
        try {
            PlayerStateNbt.decodeValidated(gzipNbt);
            writeAtomic(playerPath(manifest.shutdownId(), playerId), gzipNbt);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean stageChunk(ExitManifest manifest, ChunkKey key,
                              byte[] gzipNbt, boolean preserveExisting) {
        if (manifest == null || key == null || !manifest.chunks().contains(key)
                || chunks == null
                || !chunks.validate(manifest.groupId(), key, gzipNbt,
                preserveExisting)) {
            return false;
        }
        Path data = chunkDataPath(manifest.shutdownId(), key);
        Path marker = chunkPreservePath(manifest.shutdownId(), key);
        try {
            if (preserveExisting) {
                if (Files.isRegularFile(data)) {
                    return false;
                }
                writeAtomic(marker, PRESERVE_MARKER);
            } else {
                if (gzipNbt == null || Files.isRegularFile(marker)) {
                    return false;
                }
                writeAtomic(data, gzipNbt);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void apply(ExitManifest manifest) {
        if (manifest == null || playerWriter == null || chunks == null) {
            throw new IllegalStateException("退出暂存应用器未完整配置");
        }
        try {
            for (UUID playerId : manifest.playerIds().stream().sorted().toList()) {
                byte[] data = Files.readAllBytes(
                        playerPath(manifest.shutdownId(), playerId));
                PlayerStateNbt.decodeValidated(data);
                if (!playerWriter.write(playerId, data)) {
                    throw new IOException("正式玩家数据写入失败: " + playerId);
                }
            }
            for (ChunkKey key : manifest.chunks().stream()
                    .sorted(Comparator.comparing(ChunkKey::asString)).toList()) {
                Path dataPath = chunkDataPath(manifest.shutdownId(), key);
                Path markerPath = chunkPreservePath(manifest.shutdownId(), key);
                boolean hasData = Files.isRegularFile(dataPath);
                boolean preserve = Files.isRegularFile(markerPath);
                if (hasData == preserve) {
                    throw new IOException("退出区块暂存类型缺失或冲突: "
                            + key.asString());
                }
                byte[] data = hasData ? Files.readAllBytes(dataPath) : null;
                if (!chunks.write(manifest.groupId(), key, data, preserve)) {
                    throw new IOException("正式区块数据写入失败: " + key.asString());
                }
            }
            chunks.flush();
        } catch (Exception e) {
            throw new IllegalStateException("应用退出暂存数据失败: "
                    + manifest.shutdownId(), e);
        }
    }

    @Override
    public void discard(ExitManifest manifest) {
        if (manifest == null) {
            return;
        }
        Path directory = transactionDir(manifest.shutdownId());
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (Exception e) {
            throw new IllegalStateException("删除退出暂存目录失败: " + directory, e);
        }
    }

    private Path playerPath(UUID shutdownId, UUID playerId) {
        return transactionDir(shutdownId).resolve("players")
                .resolve(playerId + ".nbt.gz");
    }

    private Path chunkDataPath(UUID shutdownId, ChunkKey key) {
        return transactionDir(shutdownId).resolve("chunks")
                .resolve(chunkFileName(key) + ".nbt.gz");
    }

    private Path chunkPreservePath(UUID shutdownId, ChunkKey key) {
        return transactionDir(shutdownId).resolve("preserve")
                .resolve(chunkFileName(key) + ".marker");
    }

    private Path transactionDir(UUID shutdownId) {
        return root.resolve(shutdownId.toString());
    }

    private static String chunkFileName(ChunkKey key) {
        return Sha256.hex(key.asString().getBytes(StandardCharsets.UTF_8));
    }

    private static void writeAtomic(Path target, byte[] data) throws IOException {
        Files.createDirectories(target.toAbsolutePath().getParent());
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        Files.deleteIfExists(temporary);
        Files.write(temporary, data);
        force(temporary);
        try {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
        force(target);
    }

    private static void force(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(
                file, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }
}
