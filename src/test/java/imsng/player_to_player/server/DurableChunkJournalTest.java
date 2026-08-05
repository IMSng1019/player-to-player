package imsng.player_to_player.server;

import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableChunkJournalTest {

    private static final UUID OWNER =
            UUID.fromString("8fd8f9de-6b13-4a85-a0b8-3e3f1e1cbf42");
    private static final UUID BATCH =
            UUID.fromString("c3ee07b7-df9d-42df-ae28-0ff1c2a64412");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", 2, -4);

    @TempDir
    Path temp;

    @Test
    void forceOnceAndRecoverLatestIndexAfterRestart() {
        AtomicInteger forces = new AtomicInteger();
        DurableChunkJournal journal = new DurableChunkJournal(temp,
                (elapsed, epoch, count) -> forces.incrementAndGet());
        ChunkDeltaBatch batch = new ChunkDeltaBatch(BATCH, 0, 0,
                List.of(ChunkDelta.of(KEY, 9, new byte[]{4, 5, 6})));

        DurableChunkJournal.AppendResult first = journal.append(OWNER, batch);
        DurableChunkJournal.AppendResult duplicate = journal.append(OWNER, batch);

        assertEquals(1, forces.get());
        assertEquals(first.durableEpoch(), duplicate.durableEpoch());
        assertTrue(duplicate.duplicate());
        journal.close();

        DurableChunkJournal recovered = new DurableChunkJournal(temp);
        DurableChunkJournal.Entry entry = recovered.latest(KEY).orElseThrow();
        assertEquals(9, entry.version());
        assertArrayEquals(new byte[]{4, 5, 6}, entry.payload());
        assertEquals(1, recovered.batchCount());
        recovered.close();
    }

    @Test
    void exitCommitUsesOneForceAndRecoversPayloadAndLatestIndex() {
        UUID shutdownId = UUID.fromString(
                "f26b60a7-7d75-450c-a3d9-0fd3d1226e47");
        ExitManifest manifest = new ExitManifest(
                shutdownId, OWNER, OWNER, Set.of(OWNER), Set.of(KEY));
        ExitCommitPayload payload = new ExitCommitPayload(
                manifest,
                Map.of(OWNER, new byte[]{7, 8, 9}),
                List.of(ChunkDelta.of(KEY, 10, new byte[]{4, 5, 6})));
        AtomicInteger forces = new AtomicInteger();
        DurableChunkJournal journal = new DurableChunkJournal(temp,
                (elapsed, epoch, count) -> forces.incrementAndGet());

        DurableChunkJournal.AppendResult first =
                journal.appendExitCommit(payload);
        DurableChunkJournal.AppendResult duplicate =
                journal.appendExitCommit(payload);

        assertEquals(1, forces.get());
        assertEquals(first.durableEpoch(), duplicate.durableEpoch());
        assertTrue(duplicate.duplicate());
        assertArrayEquals(new byte[]{4, 5, 6},
                journal.latest(KEY).orElseThrow().payload());
        journal.close();

        DurableChunkJournal recovered = new DurableChunkJournal(temp);
        ExitCommitPayload recoveredPayload = recovered.exitCommit(shutdownId)
                .orElseThrow().payload();
        assertEquals(manifest, recoveredPayload.manifest());
        assertArrayEquals(new byte[]{7, 8, 9},
                recoveredPayload.playerData().get(OWNER));
        assertArrayEquals(new byte[]{4, 5, 6},
                recoveredPayload.chunkDeltas().get(0).payload());
        assertEquals(10, recovered.latest(KEY).orElseThrow().version());
        recovered.close();
    }

    @Test
    void exitCommitSpansMultiplePhysicalRecordsWithOneForceAndRecovers()
            throws Exception {
        var constructor = assertDoesNotThrow(() ->
                DurableChunkJournal.class.getDeclaredConstructor(
                        Path.class, DurableChunkJournal.ForceObserver.class,
                        int.class));
        constructor.setAccessible(true);
        AtomicInteger forces = new AtomicInteger();
        DurableChunkJournal journal = constructor.newInstance(
                temp,
                (DurableChunkJournal.ForceObserver)
                        (elapsed, epoch, count) -> forces.incrementAndGet(),
                96);
        UUID shutdownId = UUID.fromString(
                "23ad9aae-26ae-4fca-a520-3d61ca33d4a0");
        ExitManifest manifest = new ExitManifest(
                shutdownId, OWNER, OWNER, Set.of(OWNER), Set.of());
        byte[] playerData = new byte[512];
        java.util.Arrays.fill(playerData, (byte) 0x5a);
        ExitCommitPayload payload = new ExitCommitPayload(
                manifest, Map.of(OWNER, playerData), List.of());

        DurableChunkJournal.AppendResult first =
                journal.appendExitCommit(payload);
        int recordsAfterFirst = countPhysicalRecords(temp.resolve("chunks.wal"));
        DurableChunkJournal.AppendResult duplicate =
                journal.appendExitCommit(payload);

        assertEquals(1, forces.get());
        assertTrue(recordsAfterFirst > 1,
                "逻辑退出事务必须允许由多条有界物理记录组成");
        assertEquals(recordsAfterFirst,
                countPhysicalRecords(temp.resolve("chunks.wal")));
        assertEquals(first.durableEpoch(), duplicate.durableEpoch());
        assertTrue(duplicate.duplicate());
        journal.close();

        DurableChunkJournal recovered = new DurableChunkJournal(temp);
        assertArrayEquals(playerData, recovered.exitCommit(shutdownId)
                .orElseThrow().payload().playerData().get(OWNER));
        recovered.close();
    }

    @Test
    void completeExitPartsWithoutMarkerAreDiscardedAsUnacknowledgedTail()
            throws Exception {
        DurableChunkJournal journal = new DurableChunkJournal(
                temp, (elapsed, epoch, count) -> {
                }, 96);
        UUID shutdownId = UUID.fromString(
                "6e541921-38b7-44c0-98ff-233903bef4c2");
        ExitManifest manifest = new ExitManifest(
                shutdownId, OWNER, OWNER, Set.of(OWNER), Set.of());
        byte[] playerData = new byte[512];
        java.util.Arrays.fill(playerData, (byte) 0x36);
        journal.appendExitCommit(new ExitCommitPayload(
                manifest, Map.of(OWNER, playerData), List.of()));
        journal.close();

        Path wal = temp.resolve("chunks.wal");
        byte[] complete = Files.readAllBytes(wal);
        int markerOffset = lastPhysicalRecordOffset(complete);
        Files.write(wal, java.util.Arrays.copyOf(complete, markerOffset));
        assertTrue(Files.size(wal) > 0, "测试必须保留至少一个完整数据片");

        DurableChunkJournal recovered = new DurableChunkJournal(temp);

        assertTrue(recovered.exitCommit(shutdownId).isEmpty());
        assertEquals(0L, Files.size(wal),
                "没有终结 marker 的完整 part 尾部从未获 ACK，应在恢复时回收");
        recovered.close();
    }

    @Test
    void orphanExitPartsAreTruncatedAfterDurablePrefixAndCanBeReplayed()
            throws Exception {
        DurableChunkJournal journal = new DurableChunkJournal(
                temp, (elapsed, epoch, count) -> {
                }, 96);
        ChunkDeltaBatch durablePrefix = new ChunkDeltaBatch(BATCH, 0, 0,
                List.of(ChunkDelta.of(KEY, 1, new byte[]{1, 2, 3})));
        journal.append(OWNER, durablePrefix);

        UUID shutdownId = UUID.fromString(
                "ed8f3214-e159-48bb-a899-34d9bd066233");
        ExitManifest manifest = new ExitManifest(
                shutdownId, OWNER, OWNER, Set.of(OWNER), Set.of());
        byte[] playerData = new byte[512];
        java.util.Arrays.fill(playerData, (byte) 0x44);
        ExitCommitPayload exitPayload = new ExitCommitPayload(
                manifest, Map.of(OWNER, playerData), List.of());
        journal.appendExitCommit(exitPayload);
        journal.close();

        Path wal = temp.resolve("chunks.wal");
        byte[] complete = Files.readAllBytes(wal);
        int durablePrefixBytes = firstPhysicalRecordEnd(complete);
        int markerOffset = lastPhysicalRecordOffset(complete);
        Files.write(wal, java.util.Arrays.copyOf(complete, markerOffset));
        assertTrue(Files.size(wal) > durablePrefixBytes,
                "测试必须在 durable 前缀后保留完整孤儿 part");

        DurableChunkJournal recovered = new DurableChunkJournal(
                temp, (elapsed, epoch, count) -> {
                }, 96);
        assertArrayEquals(new byte[]{1, 2, 3},
                recovered.latest(KEY).orElseThrow().payload());
        assertTrue(recovered.exitCommit(shutdownId).isEmpty());
        assertEquals(durablePrefixBytes, Files.size(wal),
                "恢复只能回收未确认尾部，不能截断已经 ACK 的批次");

        recovered.appendExitCommit(exitPayload);
        recovered.close();

        DurableChunkJournal replayed = new DurableChunkJournal(temp);
        assertArrayEquals(new byte[]{1, 2, 3},
                replayed.latest(KEY).orElseThrow().payload());
        assertArrayEquals(playerData, replayed.exitCommit(shutdownId)
                .orElseThrow().payload().playerData().get(OWNER));
        replayed.close();
    }

    @Test
    void exitCommitRejectsShutdownIdReuseWithDifferentPayload() {
        UUID shutdownId = UUID.fromString(
                "f26b60a7-7d75-450c-a3d9-0fd3d1226e47");
        ExitManifest manifest = new ExitManifest(
                shutdownId, OWNER, OWNER, Set.of(OWNER), Set.of(KEY));
        DurableChunkJournal journal = new DurableChunkJournal(temp);
        journal.appendExitCommit(new ExitCommitPayload(
                manifest, Map.of(OWNER, new byte[]{1}),
                List.of(ChunkDelta.of(KEY, 1, new byte[]{2}))));

        assertThrows(IllegalArgumentException.class,
                () -> journal.appendExitCommit(new ExitCommitPayload(
                        manifest, Map.of(OWNER, new byte[]{9}),
                        List.of(ChunkDelta.of(KEY, 1, new byte[]{2})))));
        journal.close();
    }

    @Test
    void corruptTailIsTruncatedWithoutDroppingDurablePrefix() throws Exception {
        DurableChunkJournal journal = new DurableChunkJournal(temp);
        ChunkDeltaBatch batch = new ChunkDeltaBatch(BATCH, 0, 0,
                List.of(ChunkDelta.of(KEY, 1, new byte[]{1})));
        journal.append(OWNER, batch);
        journal.close();

        Path wal = temp.resolve("chunks.wal");
        Files.write(wal, new byte[]{0x55, 0x66, 0x77},
                java.nio.file.StandardOpenOption.APPEND);
        long damagedSize = Files.size(wal);
        DurableChunkJournal recovered = new DurableChunkJournal(temp);

        assertTrue(recovered.latest(KEY).isPresent());
        assertTrue(Files.size(wal) < damagedSize);
        recovered.close();
    }

    @Test
    void corruptCompleteRecordInTheMiddleFailsWithoutTruncatingAcknowledgedSuffix()
            throws Exception {
        DurableChunkJournal journal = new DurableChunkJournal(temp);
        ChunkDeltaBatch first = new ChunkDeltaBatch(BATCH, 0, 0,
                List.of(ChunkDelta.of(KEY, 1, new byte[]{1})));
        ChunkKey secondKey = new ChunkKey("minecraft:overworld", 3, -4);
        ChunkDeltaBatch second = new ChunkDeltaBatch(UUID.randomUUID(), 0, 1,
                List.of(ChunkDelta.of(secondKey, 1, new byte[]{2})));
        journal.append(OWNER, first);
        journal.append(OWNER, second);
        journal.close();

        Path wal = temp.resolve("chunks.wal");
        byte[] bytes = Files.readAllBytes(wal);
        bytes[8] ^= 0x01; // 第一条完整记录的格式版本字节，保留长度与后续记录。
        Files.write(wal, bytes);
        long sizeBeforeRecovery = Files.size(wal);

        assertThrows(IllegalStateException.class,
                () -> new DurableChunkJournal(temp));
        assertEquals(sizeBeforeRecovery, Files.size(wal),
                "完整中段损坏不能通过截断后缀静默丢弃已 ACK 记录");
    }

    @Test
    void recoveredPhysicalDuplicateStillAdvancesNextEpoch() throws Exception {
        DurableChunkJournal journal = new DurableChunkJournal(temp);
        ChunkDeltaBatch first = new ChunkDeltaBatch(BATCH, 0, 0,
                List.of(ChunkDelta.of(KEY, 1, new byte[]{1})));
        journal.append(OWNER, first);
        journal.close();

        Method encodeRecord = DurableChunkJournal.class.getDeclaredMethod(
                "encodeRecord", UUID.class, long.class, String.class, byte[].class);
        encodeRecord.setAccessible(true);
        byte[] encodedBatch = first.encode();
        byte[] duplicateRecord = (byte[]) encodeRecord.invoke(null,
                OWNER, 2L, Sha256.hex(encodedBatch), encodedBatch);
        Files.write(temp.resolve("chunks.wal"), duplicateRecord,
                StandardOpenOption.APPEND);

        DurableChunkJournal recovered = new DurableChunkJournal(temp);
        assertEquals(2L, recovered.latest(KEY).orElseThrow().durableEpoch());
        DurableChunkJournal.AppendResult replay = recovered.append(OWNER, first);
        assertTrue(replay.duplicate());
        assertEquals(2L, replay.durableEpoch());
        ChunkKey nextKey = new ChunkKey("minecraft:overworld", 4, -4);
        DurableChunkJournal.AppendResult appended = recovered.append(OWNER,
                new ChunkDeltaBatch(UUID.randomUUID(), 0, 2,
                        List.of(ChunkDelta.of(nextKey, 1, new byte[]{3}))));

        assertEquals(3L, appended.durableEpoch(),
                "恢复扫描必须越过日志里已经出现的最高 epoch");
        recovered.close();
    }

    @Test
    void failedAppendLeavesEpochHoleInsteadOfReusingUncertainNumber()
            throws Exception {
        DurableChunkJournal journal = new DurableChunkJournal(temp);
        Path wal = temp.resolve("chunks.wal");
        Files.createDirectory(wal);
        ChunkDeltaBatch failed = new ChunkDeltaBatch(BATCH, 0, 0,
                List.of(ChunkDelta.of(KEY, 1, new byte[]{1})));

        assertThrows(IllegalStateException.class,
                () -> journal.append(OWNER, failed));

        Files.delete(wal);
        ChunkKey nextKey = new ChunkKey("minecraft:overworld", 9, 9);
        DurableChunkJournal.AppendResult next = journal.append(OWNER,
                new ChunkDeltaBatch(UUID.randomUUID(), 0, 1,
                        List.of(ChunkDelta.of(nextKey, 2, new byte[]{2}))));

        assertEquals(2L, next.durableEpoch());
        journal.close();
    }

    private static int countPhysicalRecords(Path wal) throws Exception {
        byte[] bytes = Files.readAllBytes(wal);
        int offset = 0;
        int records = 0;
        while (offset < bytes.length) {
            int length = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
            assertTrue(length > 0);
            offset = Math.addExact(offset, Integer.BYTES + length);
            assertTrue(offset <= bytes.length);
            records++;
        }
        assertEquals(bytes.length, offset);
        return records;
    }

    private static int lastPhysicalRecordOffset(byte[] bytes) {
        int offset = 0;
        int last = -1;
        while (offset < bytes.length) {
            last = offset;
            int length = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
            offset = Math.addExact(offset, Integer.BYTES + length);
        }
        assertEquals(bytes.length, offset);
        assertTrue(last >= 0);
        return last;
    }

    private static int firstPhysicalRecordEnd(byte[] bytes) {
        int length = ByteBuffer.wrap(bytes, 0, Integer.BYTES).getInt();
        int end = Math.addExact(Integer.BYTES, length);
        assertTrue(end <= bytes.length);
        return end;
    }
}
