package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.group.PlayerStateNbt;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

/**
 * 最终退出载荷的可重建暂存区。
 * <p>
 * pending 阶段只写本目录且不承诺跨崩溃保留，绝不触碰正式世界；committed 后由
 * {@link #apply(ExitManifest, ExitTransactionLedger.ChunkDisposition)} 按耐久计划
 * 幂等写入正式玩家/区块数据并 flush。
 */
public final class ExitPayloadStore
        implements ExitTransactionHandlers.PayloadStore {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/exit-payloads");

    @FunctionalInterface
    public interface PlayerWriter {
        boolean write(UUID playerId, byte[] gzipNbt);
    }

    public interface ChunkAccess {
        boolean validate(UUID groupId, ChunkKey key, byte[] gzipNbt,
                         boolean preserveExisting);

        boolean write(UUID groupId, ChunkKey key, byte[] gzipNbt,
                      boolean preserveExisting);

        /** WAL 已耐久后只排入 MCA 物化，不得再次追加 WAL 或同步 flush。 */
        default void materialize(UUID groupId, ChunkKey key, byte[] gzipNbt) {
            if (!write(groupId, key, gzipNbt, false)) {
                throw new IllegalStateException("区块异步物化排队失败: " + key.asString());
            }
        }

        /** 整个 WAL 批次只排一个后台任务，避免无界 IO 池为每个区块创建线程。 */
        default void materializeBatch(UUID groupId, List<ChunkDelta> deltas) {
            for (ChunkDelta delta : deltas) {
                materialize(groupId, delta.key(), delta.payload());
            }
        }

        void flush();
    }

    private final Path root;
    private final PlayerWriter playerWriter;
    private final ChunkAccess chunks;
    /** 非空时退出区块以全局 WAL 为权威耐久源，MCA 只做非阻塞物化。 */
    private final DurableChunkJournal durableJournal;

    public ExitPayloadStore(Path root, PlayerWriter playerWriter,
                            ChunkAccess chunks) {
        this(root, playerWriter, chunks, null);
    }

    public ExitPayloadStore(Path root, PlayerWriter playerWriter,
                            ChunkAccess chunks,
                            DurableChunkJournal durableJournal) {
        this.root = root;
        this.playerWriter = playerWriter;
        this.chunks = chunks;
        this.durableJournal = durableJournal;
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
            writeRebuildable(playerPath(manifest.shutdownId(), playerId), gzipNbt);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean stageChunk(ExitManifest manifest, ChunkKey key,
                              byte[] gzipNbt, boolean preserveExisting) {
        // v5 的干净区块由 EXIT_COMMIT 中未声明隐式表达，不再允许新建 marker。
        // preserve 目录只保留给升级前已经 committed 的旧事务恢复使用。
        if (preserveExisting || manifest == null || key == null
                || !manifest.chunks().contains(key)
                || chunks == null
                || !chunks.validate(manifest.groupId(), key, gzipNbt,
                false)) {
            return false;
        }
        Path data = chunkDataPath(manifest.shutdownId(), key);
        Path marker = chunkPreservePath(manifest.shutdownId(), key);
        try {
            if (gzipNbt == null || Files.isRegularFile(marker)) {
                return false;
            }
            writeRebuildable(data, gzipNbt);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * v5 批次暂存：先逐项做轻量 NBT/ownership 校验，再把整个批次写成一个文件。
     * pending 数据可由客户端重放，因此阶段 ACK 不做 force；最终 COMMIT 会把全部
     * 玩家与区块尾部合成一条 WAL 记录并只 force 一次。
     */
    @Override
    public boolean stageChunkBatch(ExitManifest manifest, ChunkDeltaBatch batch) {
        if (manifest == null || batch == null || batch.deltas().isEmpty()) {
            return false;
        }
        Set<ChunkKey> keys = new java.util.HashSet<>();
        try {
            for (ChunkDelta delta : batch.deltas()) {
                if (!keys.add(delta.key()) || !manifest.chunks().contains(delta.key())
                        || !chunks.validate(manifest.groupId(), delta.key(),
                        delta.payload(), false)) {
                    return false;
                }
            }
            // pending 数据先留在事务专属目录，不能在 COMMIT 前发布到全局 WAL 索引；
            // 否则强杀后放弃事务时，部分尾部数据会覆盖“最后实时确认态”。
            long started = System.nanoTime();
            byte[] encoded = batch.encode();
            writeRebuildable(batchPath(manifest.shutdownId(), batch), encoded);
            LOGGER.info("退出区块批次已暂存: shutdownId={} sequence={} chunks={} "
                            + "bytes={} exit.stage_ms={}",
                    manifest.shutdownId(), batch.sequence(), batch.deltas().size(),
                    encoded.length, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - started));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void apply(ExitManifest manifest,
                      ExitTransactionLedger.ChunkDisposition disposition) {
        if (manifest == null || disposition == null
                || playerWriter == null || chunks == null) {
            throw new IllegalStateException("退出暂存应用器未完整配置");
        }
        try {
            if (durableJournal != null) {
                DurableChunkJournal.ExitCommitEntry durable = durableJournal
                        .exitCommit(manifest.shutdownId()).orElse(null);
                if (durable != null) {
                    ExitCommitPayload payload = durable.payload();
                    if (!payload.manifest().equals(manifest)
                            || !payload.writtenChunks().equals(
                            disposition.writtenChunks())) {
                        throw new IOException("退出复合 WAL 与事务计划不一致");
                    }
                    applyDurable(payload);
                    return;
                }
            }
            for (UUID playerId : manifest.playerIds().stream().sorted().toList()) {
                byte[] data = Files.readAllBytes(
                        playerPath(manifest.shutdownId(), playerId));
                PlayerStateNbt.decodeValidated(data);
                if (!playerWriter.write(playerId, data)) {
                    throw new IOException("正式玩家数据写入失败: " + playerId);
                }
            }
            if (disposition.explicit()) {
                applyExplicitChunks(manifest, disposition.writtenChunks());
            } else {
                applyLegacyChunks(manifest);
            }
            // WAL 模式下区块数据已经 durable；这里的 chunks.write 只排入 MCA 物化队列，
            // 不能再为退出 ACK 阻塞等待 1654 个 region 写入。无 WAL 的兼容路径仍 flush。
            if (durableJournal == null) {
                chunks.flush();
            }
        } catch (Exception e) {
            throw new IllegalStateException("应用退出暂存数据失败: "
                    + manifest.shutdownId(), e);
        }
    }

    /**
     * 从可重建暂存区组装最终复合提交。该方法只读取与校验，不发布全局可见状态；
     * 调用方随后把返回对象作为一个带终结标记的逻辑 WAL 事务一次 force，才可以
     * 回复 committed ACK。
     */
    @Override
    public ExitCommitPayload prepareCommit(
            ExitManifest manifest,
            ExitTransactionLedger.ChunkDisposition disposition) {
        if (manifest == null || disposition == null || !disposition.explicit()) {
            throw new IllegalStateException("新退出复合提交必须提供明确 written 计划");
        }
        try {
            Map<UUID, byte[]> players = new HashMap<>();
            for (UUID playerId : manifest.playerIds().stream().sorted().toList()) {
                byte[] data = Files.readAllBytes(
                        playerPath(manifest.shutdownId(), playerId));
                PlayerStateNbt.decodeValidated(data);
                players.put(playerId, data);
            }

            List<ChunkDeltaBatch> batches = readBatches(manifest.shutdownId());
            Map<ChunkKey, ChunkDelta> latestBatched = flattenBatchDeltas(batches);
            List<ChunkDelta> deltas = new ArrayList<>();
            for (ChunkKey key : disposition.writtenChunks().stream()
                    .sorted(Comparator.comparing(ChunkKey::asString)).toList()) {
                ChunkDelta delta = latestBatched.get(key);
                if (delta == null) {
                    Path dataPath = chunkDataPath(manifest.shutdownId(), key);
                    if (!Files.isRegularFile(dataPath)) {
                        throw new IOException(
                                "退出 written 区块暂存缺失: " + key.asString());
                    }
                    delta = ChunkDelta.of(key, 0, Files.readAllBytes(dataPath));
                }
                deltas.add(delta);
            }
            if (!disposition.writtenChunks().containsAll(latestBatched.keySet())) {
                throw new IOException("退出批次区块集合与 committed 计划不一致");
            }
            return new ExitCommitPayload(manifest, players, deltas);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "组装退出复合提交失败: " + manifest.shutdownId(), e);
        }
    }

    /** 正式文件/MCA 只是 WAL 的异步物化视图，失败可从同一复合记录幂等重试。 */
    private void applyDurable(ExitCommitPayload payload) throws IOException {
        for (UUID playerId : payload.manifest().playerIds().stream().sorted().toList()) {
            byte[] data = payload.playerData().get(playerId);
            PlayerStateNbt.decodeValidated(data);
            if (!playerWriter.write(playerId, data)) {
                throw new IOException("正式玩家数据写入失败: " + playerId);
            }
        }
        chunks.materializeBatch(
                payload.manifest().groupId(), payload.chunkDeltas());
    }

    /**
     * 新事务只读取 committed 时声明的 written 文件。manifest 中其余干净区块完全不访问
     * region 和暂存目录，因此退出耗时与脏区块量相关，而不再与 ownership 总量相关。
     */
    private void applyExplicitChunks(ExitManifest manifest, Set<ChunkKey> writtenChunks)
            throws IOException {
        if (!manifest.chunks().containsAll(writtenChunks)) {
            throw new IOException("退出 written 计划超出 ownership 清单");
        }
        List<ChunkDeltaBatch> batches = readBatches(manifest.shutdownId());
        Map<ChunkKey, byte[]> batched = flattenBatches(batches);
        if (durableJournal != null) {
            applyBatchesToJournal(manifest, writtenChunks, batches, batched);
            return;
        }
        for (ChunkKey key : writtenChunks.stream()
                .sorted(Comparator.comparing(ChunkKey::asString)).toList()) {
            Path dataPath = chunkDataPath(manifest.shutdownId(), key);
            if (Files.isRegularFile(chunkPreservePath(manifest.shutdownId(), key))) {
                throw new IOException("退出 written 区块暂存缺失或冲突: "
                        + key.asString());
            }
            byte[] data;
            if (batched.containsKey(key)) {
                data = batched.get(key);
            } else if (Files.isRegularFile(dataPath)) {
                data = Files.readAllBytes(dataPath);
            } else {
                throw new IOException("退出 written 区块暂存缺失: " + key.asString());
            }
            if (!chunks.write(manifest.groupId(), key, data, false)) {
                throw new IOException("正式区块数据写入失败: " + key.asString());
            }
        }
    }

    /**
     * committed 后把事务批次发布到全局 WAL。每个原始批次一次 force；小退出走旧单区块
     * 消息时，把至多两个文件合成一个合成批次。完成后仅排队 MCA 物化，不等待 flush。
     */
    private void applyBatchesToJournal(ExitManifest manifest,
                                       Set<ChunkKey> writtenChunks,
                                       List<ChunkDeltaBatch> batches,
                                       Map<ChunkKey, byte[]> batched)
            throws IOException {
        List<ChunkDeltaBatch> durableBatches = new ArrayList<>(batches);
        List<ChunkDelta> legacyDeltas = new ArrayList<>();
        for (ChunkKey key : writtenChunks) {
            if (batched.containsKey(key)) {
                continue;
            }
            Path dataPath = chunkDataPath(manifest.shutdownId(), key);
            if (!Files.isRegularFile(dataPath)) {
                throw new IOException("退出 written 区块暂存缺失: " + key.asString());
            }
            legacyDeltas.add(ChunkDelta.of(key, 0, Files.readAllBytes(dataPath)));
        }
        if (!legacyDeltas.isEmpty()) {
            UUID legacyBatchId = UUID.nameUUIDFromBytes(
                    (manifest.shutdownId() + ":legacy-exit")
                            .getBytes(StandardCharsets.UTF_8));
            durableBatches.add(new ChunkDeltaBatch(
                    legacyBatchId, 0, 0, legacyDeltas));
        }
        Set<ChunkKey> stagedKeys = new java.util.HashSet<>();
        for (ChunkDeltaBatch batch : durableBatches) {
            for (ChunkDelta delta : batch.deltas()) {
                if (!stagedKeys.add(delta.key())) {
                    throw new IOException("退出批次含重复区块: " + delta.key().asString());
                }
            }
        }
        if (!stagedKeys.equals(writtenChunks)) {
            throw new IOException("退出批次区块集合与 committed 计划不一致");
        }
        for (ChunkDeltaBatch batch : durableBatches) {
            long started = System.nanoTime();
            DurableChunkJournal.AppendResult result =
                    durableJournal.append(manifest.groupId(), batch);
            LOGGER.info("退出区块批次已发布 WAL: shutdownId={} sequence={} chunks={} "
                            + "bytes={} exit.wal_force_ms={} duplicate={}",
                    manifest.shutdownId(), batch.sequence(), batch.deltas().size(),
                    result.bytes(), java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - started), result.duplicate());
            chunks.materializeBatch(manifest.groupId(), batch.deltas());
        }
    }

    /** 解码事务目录内所有 v5 批次，并按区块版本保留最终态。 */
    private List<ChunkDeltaBatch> readBatches(UUID shutdownId)
            throws IOException {
        Path directory = transactionDir(shutdownId).resolve("chunk-batches");
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        List<ChunkDeltaBatch> batches = new ArrayList<>();
        try (var paths = Files.list(directory)) {
            for (Path path : paths.filter(Files::isRegularFile)
                    .filter(value -> value.getFileName().toString().endsWith(".bin"))
                    .toList()) {
                batches.add(ChunkDeltaBatch.decode(Files.readAllBytes(path)));
            }
        }
        batches.sort(Comparator.comparingInt(ChunkDeltaBatch::sequence));
        return List.copyOf(batches);
    }

    private static Map<ChunkKey, byte[]> flattenBatches(
            List<ChunkDeltaBatch> batches) {
        Map<ChunkKey, ChunkDelta> latest = flattenBatchDeltas(batches);
        Map<ChunkKey, byte[]> result = new HashMap<>();
        latest.forEach((key, delta) -> result.put(key, delta.payload()));
        return result;
    }

    private static Map<ChunkKey, ChunkDelta> flattenBatchDeltas(
            List<ChunkDeltaBatch> batches) {
        Map<ChunkKey, ChunkDelta> latest = new HashMap<>();
        for (ChunkDeltaBatch batch : batches) {
            for (ChunkDelta delta : batch.deltas()) {
                ChunkDelta previous = latest.get(delta.key());
                if (previous == null || delta.version() >= previous.version()) {
                    latest.put(delta.key(), delta);
                }
            }
        }
        return Map.copyOf(latest);
    }

    /** 兼容升级前 committed 事务：旧格式必须继续按完整 data/marker 二选一扫描。 */
    private void applyLegacyChunks(ExitManifest manifest) throws IOException {
        for (ChunkKey key : manifest.chunks().stream()
                .sorted(Comparator.comparing(ChunkKey::asString)).toList()) {
            Path dataPath = chunkDataPath(manifest.shutdownId(), key);
            Path markerPath = chunkPreservePath(manifest.shutdownId(), key);
            boolean hasData = Files.isRegularFile(dataPath);
            boolean preserve = Files.isRegularFile(markerPath);
            if (hasData == preserve) {
                throw new IOException("旧退出区块暂存类型缺失或冲突: "
                        + key.asString());
            }
            byte[] data = hasData ? Files.readAllBytes(dataPath) : null;
            if (!chunks.write(manifest.groupId(), key, data, preserve)) {
                throw new IOException("旧退出区块数据应用失败: " + key.asString());
            }
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

    private Path batchPath(UUID shutdownId, ChunkDeltaBatch batch) {
        String identity = batch.batchId() + ":" + batch.sequence();
        return transactionDir(shutdownId).resolve("chunk-batches")
                .resolve(Sha256.hex(identity.getBytes(StandardCharsets.UTF_8)) + ".bin");
    }

    private Path transactionDir(UUID shutdownId) {
        return root.resolve(shutdownId.toString());
    }

    private static String chunkFileName(ChunkKey key) {
        return Sha256.hex(key.asString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 写入可由客户端完整重放的 pending 文件。这里故意不 force：服务端若在最终
     * COMMIT 前崩溃，received 集合也会丢失，客户端会用同一 shutdownId 重新发送。
     */
    private static void writeRebuildable(Path target, byte[] data) throws IOException {
        Files.createDirectories(target.toAbsolutePath().getParent());
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        Files.deleteIfExists(temporary);
        Files.write(temporary, data);
        try {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
