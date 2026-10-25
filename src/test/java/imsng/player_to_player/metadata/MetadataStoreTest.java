package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MetadataStoreTest {
    @TempDir Path directory;

    @Test
    void commitsTypedFieldsDeletionAndDefensiveCopies() throws Exception {
        CompoundTag initial = data(40);
        initial.putString("ModField", "kept");
        initial.put("Player", new CompoundTag());
        try (MetadataStore store = MetadataStore.open(directory, "world", initial)) {
            initial.putLong("DayTime", 999);
            assertEquals(40, store.snapshot().data().getLong("DayTime"));
            assertFalse(store.snapshot().data().contains("Player"));
            CompoundTag update = data(80);
            update.putBoolean("raining", true);
            MetadataPatch patch = store.commit("world", 0, store.snapshot().term(),
                    "op-1", update, Set.of("ModField"));
            update.putLong("DayTime", 999);
            patch.set().putLong("DayTime", 999);
            store.snapshot().data().putLong("DayTime", 999);
            assertEquals(80, store.snapshot().data().getLong("DayTime"));
            assertTrue(store.snapshot().data().getBoolean("raining"));
            assertFalse(store.snapshot().data().contains("ModField"));
            assertEquals(1, store.snapshot().version());
        }
    }

    @Test
    void rejectsConcurrentStaleWrongWorldAndOldTermUpdates() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(1))) {
            long term = store.snapshot().term();
            store.commit("world", 0, term, "first", data(2), Set.of());
            assertThrows(IllegalStateException.class,
                    () -> store.commit("world", 0, term, "stale", data(3), Set.of()));
            assertThrows(IllegalStateException.class,
                    () -> store.commit("world", 1, term - 1, "old", data(3), Set.of()));
            assertThrows(IllegalArgumentException.class,
                    () -> store.commit("other", 1, term, "other", data(3), Set.of()));
            assertEquals(2, store.snapshot().data().getLong("DayTime"));
        }
    }

    @Test
    void duplicateOperationSurvivesCheckpointRestartAndCannotChangePayload() throws Exception {
        long term;
        MetadataPatch first;
        try (MetadataStore store = MetadataStore.open(directory, "world", data(1))) {
            term = store.snapshot().term();
            first = store.commit("world", 0, term, "same", data(2), Set.of());
            store.commit("world", 1, term, "later", data(3), Set.of());
            assertEquals(first, store.lookupOperation("same").orElseThrow());
            assertTrue(store.lookupOperation("absent").isEmpty());
            assertEquals(first, store.commit("world", 0, term, "same", data(2), Set.of()));
            assertThrows(IllegalArgumentException.class,
                    () -> store.commit("world", 0, term, "same", data(4), Set.of()));
            store.checkpoint();
        }
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertTrue(recovered.snapshot().term() > term);
            assertEquals(3, recovered.snapshot().data().getLong("DayTime"));
            assertEquals(first, recovered.lookupOperation("same").orElseThrow());
            assertThrows(IllegalStateException.class,
                    () -> recovered.commit("world", 0, term, "same", data(2), Set.of()));
            assertEquals(first, recovered.commit("world", 0, recovered.snapshot().term(),
                    "same", data(2), Set.of()));
            assertEquals(2, recovered.snapshot().version());
        }
    }

    @Test
    void recoversDurableWalBeforeCloseAndTruncatesOnlyIncompleteTail() throws Exception {
        Path crashCopy = directory.resolve("crash");
        Files.createDirectories(crashCopy);
        try (MetadataStore store = MetadataStore.open(directory, "world", data(1))) {
            store.commit("world", 0, store.snapshot().term(), "one", data(2), Set.of());
            Files.copy(directory.resolve("metadata.checkpoint"), crashCopy.resolve("metadata.checkpoint"));
            Files.copy(directory.resolve("metadata.wal"), crashCopy.resolve("metadata.wal"));
        }
        Files.write(crashCopy.resolve("metadata.wal"), new byte[]{12, 34, 56}, StandardOpenOption.APPEND);
        try (MetadataStore recovered = MetadataStore.open(crashCopy, "world", data(999))) {
            assertEquals(1, recovered.snapshot().version());
            assertEquals(2, recovered.snapshot().data().getLong("DayTime"));
            recovered.commit("world", 1, recovered.snapshot().term(), "two", data(3), Set.of());
        }
        try (MetadataStore recovered = MetadataStore.open(crashCopy, "world", data(999))) {
            assertEquals(2, recovered.snapshot().version());
        }
    }

    @Test
    void refusesCompleteCorruptWalAndWrongWorldCheckpoint() throws Exception {
        Path crashCopy = directory.resolve("crash");
        Files.createDirectories(crashCopy);
        try (MetadataStore store = MetadataStore.open(directory, "world", data(1))) {
            store.commit("world", 0, store.snapshot().term(), "one", data(2), Set.of());
            Files.copy(directory.resolve("metadata.checkpoint"), crashCopy.resolve("metadata.checkpoint"));
            byte[] wal = Files.readAllBytes(directory.resolve("metadata.wal"));
            wal[wal.length - 1] ^= 1;
            Files.write(crashCopy.resolve("metadata.wal"), wal);
        }
        assertThrows(IOException.class, () -> MetadataStore.open(crashCopy, "world", data(1)));
        assertThrows(IOException.class, () -> MetadataStore.open(directory, "other", data(1)));
    }

    @Test
    void catchupIsOrderedAndSnapshotRequiredForMissingHistory() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(1))) {
            long term = store.snapshot().term();
            store.commit("world", 0, term, "one", data(2), Set.of());
            store.commit("world", 1, term, "two", data(3), Set.of());
            var patches = store.patchesAfter(0).orElseThrow();
            assertEquals(2, patches.size());
            assertEquals(1, patches.get(0).version());
            assertEquals(2, patches.get(1).version());
            assertTrue(store.patchesAfter(2).orElseThrow().isEmpty());
            assertTrue(store.patchesAfter(3).isEmpty());
            assertTrue(store.patchesAfter(-1).isEmpty());
        }
    }

    @Test
    void rejectsPlayerOverwriteAndAmbiguousSetRemove() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(1))) {
            CompoundTag player = new CompoundTag();
            player.put("Player", new CompoundTag());
            assertThrows(IllegalArgumentException.class,
                    () -> store.commit("world", 0, store.snapshot().term(), "player", player, Set.of()));
            assertThrows(IllegalArgumentException.class,
                    () -> store.commit("world", 0, store.snapshot().term(), "both", data(2), Set.of("DayTime")));
            assertEquals(0, store.snapshot().version());
        }
    }

    @Test
    void twoStoresCannotOwnSameDirectory() throws Exception {
        try (MetadataStore ignored = MetadataStore.open(directory, "world", data(1))) {
            assertThrows(IOException.class, () -> MetadataStore.open(directory, "world", data(1)));
        }
    }

    @Test
    void catchupWindowEvictionDoesNotForgetOperationReceipts() throws Exception {
        MetadataPatch first;
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            long term = store.snapshot().term();
            first = store.commit("world", 0, term, "first", data(1), Set.of());
            for (int version = 1; version <= 256; version++) {
                store.commit("world", version, term, "next-" + version, data(version + 1), Set.of());
            }
            assertTrue(store.patchesAfter(0).isEmpty());
            assertEquals(256, store.patchesAfter(1).orElseThrow().size());
            assertEquals(first, store.lookupOperation("first").orElseThrow());
            store.checkpoint();
        }
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(0))) {
            assertEquals(first, recovered.lookupOperation("first").orElseThrow());
            assertTrue(recovered.patchesAfter(256).isEmpty(), "任期切换需要新快照");
        }
    }

    @Test
    void naturalCheckpointsAdvanceVersionWithoutGrowingOperationWal() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            long initialWalBytes = Files.size(directory.resolve("metadata.wal"));
            MetadataPatch clock = store.checkpointChanges(data(20), Set.of());
            assertEquals(1, clock.version());
            assertEquals(20, store.snapshot().data().getLong("DayTime"));
            assertEquals(initialWalBytes, Files.size(directory.resolve("metadata.wal")));
            assertEquals(clock, store.patchesAfter(0).orElseThrow().get(0));
            assertTrue(store.lookupOperation(clock.operationId()).isEmpty());
        }
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertEquals(1, recovered.snapshot().version());
            assertEquals(20, recovered.snapshot().data().getLong("DayTime"));
        }
    }

    @Test
    void clockAndDurableOperationsInterleaveAcrossCrashRecovery() throws Exception {
        Path crashCopy = directory.resolve("crash");
        Files.createDirectories(crashCopy);
        MetadataPatch first;
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            store.checkpointChanges(data(20), Set.of());
            first = store.commit("world", 1, store.snapshot().term(), "command", data(30), Set.of());
            store.checkpointChanges(data(50), Set.of());
            store.commit("world", 3, store.snapshot().term(), "second", data(60), Set.of());
            Files.copy(directory.resolve("metadata.checkpoint"), crashCopy.resolve("metadata.checkpoint"));
            Files.copy(directory.resolve("metadata.wal"), crashCopy.resolve("metadata.wal"));
        }
        try (MetadataStore recovered = MetadataStore.open(crashCopy, "world", data(999))) {
            assertEquals(4, recovered.snapshot().version());
            assertEquals(60, recovered.snapshot().data().getLong("DayTime"));
            assertEquals(first, recovered.lookupOperation("command").orElseThrow());
        }
    }

    @Test
    void checkpointProtectsDurableReceiptsFromTruncation() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            store.commit("world", 0, store.snapshot().term(), "command", data(10), Set.of());
            store.checkpointChanges(data(20), Set.of());
        }
        Files.write(directory.resolve("metadata.wal"), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        assertThrows(IOException.class, () -> MetadataStore.open(directory, "world", data(999)));
    }

    @Test
    void topologyTermBarrierIsDurableAndRetainsOperationReceipts() throws Exception {
        long barrier;
        MetadataPatch first;
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            long previous = store.snapshot().term();
            first = store.commit("world", 0, previous, "command", data(10), Set.of());
            MetadataSnapshot snapshot = store.advanceTerm();
            barrier = snapshot.term();
            assertEquals(previous + 1, barrier);
            assertEquals(1, snapshot.version());
            assertTrue(store.patchesAfter(0).isEmpty());
            assertThrows(IllegalStateException.class,
                    () -> store.commit("world", 1, previous, "old-owner", data(20), Set.of()));
            assertEquals(first, store.lookupOperation("command").orElseThrow());
        }
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertEquals(barrier + 1, recovered.snapshot().term());
            assertEquals(first, recovered.lookupOperation("command").orElseThrow());
        }
    }

    @Test
    void failedCheckpointPoisonsWriterUntilRecovery() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            Files.createDirectory(directory.resolve("metadata.checkpoint.tmp"));
            assertThrows(IOException.class, () -> store.checkpointChanges(data(20), Set.of()));
            assertThrows(IllegalStateException.class, store::snapshot);
            assertThrows(IllegalStateException.class,
                    () -> store.commit("world", 0, 1, "after-failure", data(30), Set.of()));
        }
        Files.delete(directory.resolve("metadata.checkpoint.tmp"));
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertEquals(0, recovered.snapshot().version());
            assertEquals(0, recovered.snapshot().data().getLong("DayTime"));
        }
    }

    @Test
    void oldCheckpointCannotLoseAcknowledgedChangesAfterNaturalVersionGap() throws Exception {
        byte[] oldCheckpoint;
        long durableTerm;
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            oldCheckpoint = Files.readAllBytes(directory.resolve("metadata.checkpoint"));
            CompoundTag clock = data(20); clock.putString("OnlyInBaseline", "preserved");
            store.checkpointChanges(clock, Set.of());
            durableTerm = store.advanceTerm().term();
            store.checkpointChanges(data(40), Set.of());
            CompoundTag change = new CompoundTag(); change.putInt("GameType", 1);
            store.commit("world", 2, durableTerm, "acknowledged", change, Set.of());
        }
        // 模拟断电后目录项回到较老的原子替换结果，WAL 中已 force 的确认操作仍完整。
        Files.write(directory.resolve("metadata.checkpoint"), oldCheckpoint);
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertEquals(3, recovered.snapshot().version());
            assertEquals(40, recovered.snapshot().data().getLong("DayTime"));
            assertEquals("preserved", recovered.snapshot().data().getString("OnlyInBaseline"));
            assertEquals(1, recovered.snapshot().data().getInt("GameType"));
            assertTrue(recovered.snapshot().term() > durableTerm);
            assertEquals(3, recovered.lookupOperation("acknowledged").orElseThrow().version());
        }
    }

    @Test
    void missingCheckpointRecoversFromWalInsteadOfImportingDiskAgain() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            store.checkpointChanges(data(20), Set.of());
            store.commit("world", 1, store.snapshot().term(), "acknowledged", data(30), Set.of());
        }
        Files.delete(directory.resolve("metadata.checkpoint"));
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertEquals(2, recovered.snapshot().version());
            assertEquals(30, recovered.snapshot().data().getLong("DayTime"));
            assertTrue(recovered.lookupOperation("acknowledged").isPresent());
        }
    }

    @Test
    void completeAnchorSurvivesIncompletePatchWithoutPublishingItsReceipt() throws Exception {
        byte[] oldCheckpoint;
        byte[] wal;
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            oldCheckpoint = Files.readAllBytes(directory.resolve("metadata.checkpoint"));
            store.checkpointChanges(data(20), Set.of());
            store.commit("world", 1, store.snapshot().term(), "incomplete", data(30), Set.of());
            wal = Files.readAllBytes(directory.resolve("metadata.wal"));
        }
        Files.write(directory.resolve("metadata.checkpoint"), oldCheckpoint);
        Files.write(directory.resolve("metadata.wal"), Arrays.copyOf(wal, wal.length - 3));
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertEquals(1, recovered.snapshot().version());
            assertEquals(20, recovered.snapshot().data().getLong("DayTime"));
            assertTrue(recovered.lookupOperation("incomplete").isEmpty());
            recovered.commit("world", 1, recovered.snapshot().term(), "after-tail", data(40), Set.of());
        }
    }

    @Test
    void corruptAnchorIsNotSkippedEvenWhenCheckpointContainsNewerState() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            store.checkpointChanges(data(20), Set.of());
            store.commit("world", 1, store.snapshot().term(), "acknowledged", data(30), Set.of());
        }
        byte[] wal = Files.readAllBytes(directory.resolve("metadata.wal"));
        int firstLength = ByteBuffer.wrap(wal).getInt(4);
        wal[8 + firstLength - 1] ^= 1;
        Files.write(directory.resolve("metadata.wal"), wal);
        assertThrows(IOException.class, () -> MetadataStore.open(directory, "world", data(999)));
    }

    @Test
    void termBarrierSurvivesCheckpointDirectoryEntryRollbackWithoutLaterCommit() throws Exception {
        byte[] oldCheckpoint;
        long term;
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            oldCheckpoint = Files.readAllBytes(directory.resolve("metadata.checkpoint"));
            term = store.advanceTerm().term();
        }
        Files.write(directory.resolve("metadata.checkpoint"), oldCheckpoint);
        try (MetadataStore recovered = MetadataStore.open(directory, "world", data(999))) {
            assertTrue(recovered.snapshot().term() > term);
        }
    }

    private static CompoundTag data(long time) {
        CompoundTag tag = new CompoundTag();
        tag.putLong("DayTime", time);
        return tag;
    }
}
