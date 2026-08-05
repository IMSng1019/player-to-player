package imsng.player_to_player.server;

import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.group.PlayerStateNbt;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 对应 26.8.5 日志中 1654 个区块、约 7.2 MB 的退出规模回归。 */
class DurableExitCheckpointScaleTest {

    @TempDir
    Path temp;

    @Test
    void sixteenHundredChunksCommitWithOneForceUnderTwoSeconds() throws Exception {
        UUID owner = UUID.randomUUID();
        UUID shutdownId = UUID.randomUUID();
        List<ChunkDelta> deltas = new ArrayList<>();
        for (int i = 0; i < 1_654; i++) {
            byte[] payload = new byte[4_300];
            java.util.Arrays.fill(payload, (byte) (i * 31));
            deltas.add(ChunkDelta.of(new ChunkKey(
                    i % 2 == 0 ? "minecraft:overworld" : "minecraft:the_nether",
                    i % 97, i / 97), i + 1L, payload));
        }
        AtomicInteger forces = new AtomicInteger();
        DurableChunkJournal journal = new DurableChunkJournal(temp,
                (elapsed, epoch, count) -> forces.incrementAndGet());
        ExitManifest manifest = new ExitManifest(
                shutdownId, owner, owner, Set.of(owner),
                deltas.stream().map(ChunkDelta::key)
                        .collect(java.util.stream.Collectors.toUnmodifiableSet()));
        List<ChunkDeltaBatch> batches = ChunkDeltaBatch.split(
                shutdownId, 0, deltas, 4 * 1024 * 1024);
        ExitPayloadStore payloads = new ExitPayloadStore(
                temp.resolve("exit-payloads"),
                (playerId, data) -> true,
                new ExitPayloadStore.ChunkAccess() {
                    @Override
                    public boolean validate(UUID groupId, ChunkKey key,
                                            byte[] gzipNbt,
                                            boolean preserveExisting) {
                        return !preserveExisting && gzipNbt != null;
                    }

                    @Override
                    public boolean write(UUID groupId, ChunkKey key,
                                         byte[] gzipNbt,
                                         boolean preserveExisting) {
                        return true;
                    }

                    @Override
                    public void flush() {
                    }
                }, journal);

        long started = System.nanoTime();
        assertTrue(payloads.stagePlayer(manifest, owner, validPlayer()));
        for (ChunkDeltaBatch batch : batches) {
            assertTrue(payloads.stageChunkBatch(manifest, batch));
        }
        ExitCommitPayload payload = payloads.prepareCommit(
                manifest, new ExitTransactionLedger.ChunkDisposition(
                        manifest.chunks(), true));
        journal.appendExitCommit(payload);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - started);

        assertEquals(2, batches.size());
        assertEquals(1, forces.get());
        assertTrue(countPhysicalRecords(temp.resolve("chunks.wal")) >= 3,
                "7.2 MiB 回归必须实际跨越至少两个数据片并包含终结 marker");
        assertEquals(1_654, journal.latestCount());
        assertTrue(journal.exitCommit(shutdownId).isPresent());
        assertTrue(elapsedMillis < 2_000,
                "7.2 MB 退出复合 WAL 落盘耗时异常: " + elapsedMillis + " ms");
        System.out.println("durable_exit_scale_ms=" + elapsedMillis);
        journal.close();
    }

    private static int countPhysicalRecords(Path wal) throws Exception {
        byte[] bytes = Files.readAllBytes(wal);
        int offset = 0;
        int records = 0;
        while (offset < bytes.length) {
            int length = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
            offset = Math.addExact(offset, Integer.BYTES + length);
            records++;
        }
        assertEquals(bytes.length, offset);
        return records;
    }

    private static byte[] validPlayer() throws Exception {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(1.0));
        pos.add(DoubleTag.valueOf(64.0));
        pos.add(DoubleTag.valueOf(2.0));
        tag.put("Pos", pos);
        return PlayerStateNbt.encode(tag);
    }
}
