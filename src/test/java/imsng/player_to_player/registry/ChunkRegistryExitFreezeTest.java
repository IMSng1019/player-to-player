package imsng.player_to_player.registry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkRegistryExitFreezeTest {

    private static final UUID GROUP =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey FIRST =
            new ChunkKey("minecraft:overworld", 10, 10);
    private static final ChunkKey SECOND =
            new ChunkKey("minecraft:overworld", 20, 20);
    private static final ChunkKey STALE =
            new ChunkKey("minecraft:overworld", 30, 30);
    private static final UUID OTHER_GROUP =
            UUID.fromString("30c88a44-b992-4a3a-9f5b-6b64f176f501");
    private static final UUID SECONDARY =
            UUID.fromString("3f67ff74-a3c3-43b0-a743-702dc7eb0bf2");

    @TempDir
    Path temporary;

    @Test
    void frozenGroupCannotClaimOrReleaseUntilExitCompletes() {
        MemoryStore store = new MemoryStore();
        ChunkRegistry registry = new ChunkRegistry(store, null);
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());

        Set<ChunkKey> frozen = registry.freezeForExit(GROUP);

        assertEquals(Set.of(FIRST), frozen);
        assertTrue(registry.isExitFrozen(GROUP));
        assertFalse(registry.release(FIRST, GROUP));
        assertFalse(registry.tryClaim(SECOND, GROUP).granted());
        assertEquals(0, registry.migrateAll(GROUP, OTHER_GROUP));
        assertEquals(0, registry.migrate(Set.of(FIRST), GROUP, OTHER_GROUP));
        assertEquals(GROUP, registry.ownerOf(FIRST));

        assertEquals(1, registry.completeExitDurably(GROUP, Set.of(FIRST)));
        assertFalse(registry.isExitFrozen(GROUP));
        assertEquals(0, registry.size());
    }

    @Test
    void pendingRecoveryRebuildsManifestClaimsAndRemovesStaleGroupClaims() {
        MemoryStore store = new MemoryStore();
        store.data.put(STALE, GROUP);
        ChunkRegistry registry = new ChunkRegistry(store, null);
        registry.load();

        registry.restoreExitProtection(GROUP, Set.of(FIRST));

        assertTrue(registry.isExitFrozen(GROUP));
        assertEquals(GROUP, registry.ownerOf(FIRST));
        assertEquals(null, registry.ownerOf(STALE));
        // EXIT_BEGIN 的 ACK 只依赖先行 WAL；全量后端快照由周期 checkpoint 收敛。
        assertEquals(Map.of(STALE, GROUP), store.data);
        registry.persist();
        assertEquals(Map.of(FIRST, GROUP), store.data);
    }

    @Test
    void orphanRecoveryRestoresRequestedClaimsAfterDisconnectCleanup() {
        MemoryStore store = new MemoryStore();
        ChunkRegistry registry = new ChunkRegistry(store, null);
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        assertTrue(registry.tryClaim(SECOND, GROUP).granted());
        assertEquals(2, registry.releaseAllDurably(GROUP));

        Set<ChunkKey> recovered = registry.freezeOrRecoverForExit(
                GROUP, Set.of(FIRST, SECOND), true);

        assertEquals(Set.of(FIRST, SECOND), recovered);
        assertEquals(null, registry.ownerOf(FIRST));
        assertEquals(null, registry.ownerOf(SECOND));

        registry.restoreExitProtection(GROUP, recovered);

        assertEquals(GROUP, registry.ownerOf(FIRST));
        assertEquals(GROUP, registry.ownerOf(SECOND));
        assertTrue(registry.isExitFrozen(GROUP));
    }

    @Test
    void orphanRecoveryAcceptsDurableEmptyOwnershipProof() {
        Path journalFile = temporary.resolve("empty-ownership.jsonl");
        ChunkRegistry first = new ChunkRegistry(
                new MemoryStore(), null, new RegistryMutationJournal(journalFile));

        assertEquals(0, first.releaseAllDurably(GROUP, Set.of(GROUP, SECONDARY)));

        ChunkRegistry recovered = new ChunkRegistry(
                new MemoryStore(), null, new RegistryMutationJournal(journalFile));
        recovered.load();
        assertEquals(Set.of(), recovered.freezeOrRecoverForExit(
                GROUP, Set.of(), Set.of(GROUP, SECONDARY), true));
        recovered.restoreExitProtection(GROUP, Set.of());
        assertTrue(recovered.isExitFrozen(GROUP));
    }

    @Test
    void orphanRecoveryRejectsClaimsWithoutDurableDisconnectProof() {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);

        assertThrows(IllegalStateException.class, () ->
                registry.freezeOrRecoverForExit(
                        GROUP, Set.of(FIRST), true));

        assertFalse(registry.isExitFrozen(GROUP));
    }

    @Test
    void orphanRecoveryAcceptsExactPersistedSoloClaimsAfterServerRestart() {
        MemoryStore store = new MemoryStore();
        store.data.put(FIRST, GROUP);
        ChunkRegistry registry = new ChunkRegistry(store, null);
        registry.load();

        Set<ChunkKey> recovered = registry.freezeOrRecoverForExit(
                GROUP, Set.of(FIRST), Set.of(GROUP), true);

        assertEquals(Set.of(FIRST), recovered);
        assertTrue(registry.isExitFrozen(GROUP));
        assertEquals(GROUP, registry.ownerOf(FIRST));
    }

    @Test
    void orphanRecoveryRejectsManifestDifferentFromDisconnectProof() {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        assertEquals(1, registry.releaseAllDurably(GROUP));

        assertThrows(IllegalStateException.class, () ->
                registry.freezeOrRecoverForExit(
                        GROUP, Set.of(FIRST, SECOND), true));

        assertFalse(registry.isExitFrozen(GROUP));
    }

    @Test
    void orphanRecoveryRejectsPlayerMembersDifferentFromDisconnectProof() {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        assertEquals(1, registry.releaseAllDurably(
                GROUP, Set.of(GROUP, SECONDARY)));

        assertThrows(IllegalStateException.class, () ->
                registry.freezeOrRecoverForExit(
                        GROUP, Set.of(FIRST), Set.of(GROUP), true));

        assertFalse(registry.isExitFrozen(GROUP));
    }

    @Test
    void disconnectRecoverySurvivesCheckpointAndRegistryRecreation() {
        MemoryStore store = new MemoryStore();
        Path journalFile = temporary.resolve("mutations.jsonl");
        ChunkRegistry first = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(first.tryClaim(FIRST, GROUP).granted());
        assertEquals(1, first.releaseAllDurably(
                GROUP, Set.of(GROUP, SECONDARY)));

        ChunkRegistry recovered = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        recovered.load();

        assertEquals(Set.of(FIRST), recovered.freezeOrRecoverForExit(
                GROUP, Set.of(FIRST), Set.of(GROUP, SECONDARY), true));
    }

    @Test
    void normalRejoinConsumptionSurvivesRestartAndRejectsOldExitBundle() {
        MemoryStore store = new MemoryStore();
        Path journalFile = temporary.resolve("mutations.jsonl");
        ChunkRegistry first = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(first.tryClaim(FIRST, GROUP).granted());
        assertEquals(1, first.releaseAllDurably(GROUP));
        assertTrue(first.discardDisconnectRecoveryDurably(GROUP));

        ChunkRegistry recovered = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        recovered.load();

        assertThrows(IllegalStateException.class, () ->
                recovered.freezeOrRecoverForExit(
                        GROUP, Set.of(FIRST), true));
    }

    @Test
    void orphanRecoveryRejectsDurableFourNeighborConflict() {
        ChunkKey neighbor = new ChunkKey(
                FIRST.dimension(), FIRST.x() + 1, FIRST.z());
        MemoryStore store = new MemoryStore();
        store.data.put(FIRST, GROUP);
        store.data.put(neighbor, OTHER_GROUP);
        ChunkRegistry registry = new ChunkRegistry(store, null);
        registry.load();
        assertEquals(1, registry.releaseAllDurably(GROUP));

        assertThrows(IllegalStateException.class, () ->
                registry.freezeOrRecoverForExit(
                        GROUP, Set.of(FIRST), true));

        assertFalse(registry.isExitFrozen(GROUP));
    }

    @Test
    void orphanRecoveryRejectsRequestedFourNeighborConflictWhenClaimsWereReleased() {
        ChunkKey neighbor = new ChunkKey(
                FIRST.dimension(), FIRST.x() + 1, FIRST.z());
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        assertEquals(1, registry.releaseAllDurably(GROUP));
        assertTrue(registry.tryClaim(neighbor, OTHER_GROUP).granted());

        assertThrows(IllegalStateException.class, () ->
                registry.freezeOrRecoverForExit(
                        GROUP, Set.of(FIRST), true));

        assertEquals(OTHER_GROUP, registry.ownerOf(neighbor));
        assertFalse(registry.isExitFrozen(GROUP));
    }

    @Test
    void orphanRecoveryNeverStealsClaimFromAnotherGroup() {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        assertTrue(registry.tryClaim(SECOND, GROUP).granted());
        assertEquals(1, registry.releaseAllDurably(GROUP));
        assertTrue(registry.tryClaim(SECOND, OTHER_GROUP).granted());

        assertThrows(IllegalStateException.class, () -> registry.freezeOrRecoverForExit(
                GROUP, Set.of(SECOND), true));

        assertEquals(OTHER_GROUP, registry.ownerOf(SECOND));
        assertFalse(registry.isExitFrozen(GROUP));
    }

    @Test
    void failedRegistryWalCleanupKeepsClaimsFrozenForRetry() throws Exception {
        MemoryStore store = new MemoryStore();
        Path journalFile = temporary.resolve("cleanup-failure.jsonl");
        ChunkRegistry registry = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        registry.freezeForExit(GROUP);
        Files.delete(journalFile);
        Files.createDirectory(journalFile);

        assertThrows(IllegalStateException.class,
                () -> registry.completeExitDurably(GROUP, Set.of(FIRST)));

        assertTrue(registry.isExitFrozen(GROUP));
        assertEquals(GROUP, registry.ownerOf(FIRST));
    }

    @Test
    void abnormalDisconnectReleasesWholeFrozenGroupDurably() {
        MemoryStore store = new MemoryStore();
        ChunkRegistry registry = new ChunkRegistry(store, null);
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        assertTrue(registry.tryClaim(SECOND, GROUP).granted());
        registry.freezeForExit(GROUP);

        assertEquals(2, registry.releaseAllDurably(GROUP));

        assertFalse(registry.isExitFrozen(GROUP));
        assertEquals(0, registry.size());
        assertTrue(store.data.isEmpty());
    }

    @Test
    void failedDisconnectWalAppendKeepsClaimsAndFreeze() throws Exception {
        MemoryStore store = new MemoryStore();
        Path journalFile = temporary.resolve("disconnect-failure.jsonl");
        ChunkRegistry registry = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        registry.freezeForExit(GROUP);
        Files.delete(journalFile);
        Files.createDirectory(journalFile);

        assertThrows(IllegalStateException.class,
                () -> registry.releaseAllDurably(GROUP));

        assertTrue(registry.isExitFrozen(GROUP));
        assertEquals(GROUP, registry.ownerOf(FIRST));
    }

    @Test
    void stalePeriodicSnapshotCannotOverwriteNewerExitProtection() throws Exception {
        BlockingStore store = new BlockingStore();
        store.data.put(STALE, GROUP);
        ChunkRegistry registry = new ChunkRegistry(store, null);
        registry.load();
        Thread oldPersist = new Thread(registry::persist);
        oldPersist.start();
        assertTrue(store.firstSaveEntered.await(5, TimeUnit.SECONDS));

        Thread recovery = new Thread(
                () -> registry.restoreExitProtection(GROUP, Set.of(FIRST)));
        recovery.start();
        assertFalse(store.secondSaveEntered.await(300, TimeUnit.MILLISECONDS),
                "前一个注册表快照未写完时，不得并发进入后端保存新快照");
        store.allowFirstSave.countDown();
        oldPersist.join(5_000);
        recovery.join(5_000);

        assertFalse(oldPersist.isAlive());
        assertFalse(recovery.isAlive());
        assertEquals(GROUP, registry.ownerOf(FIRST));
        assertEquals(null, registry.ownerOf(STALE));
        registry.persist();
        assertEquals(Map.of(FIRST, GROUP), store.data);
    }

    private static final class MemoryStore implements RegistryStore {
        private Map<ChunkKey, UUID> data = new HashMap<>();
        private boolean failNextSave;

        @Override
        public Map<ChunkKey, UUID> load() {
            return new HashMap<>(data);
        }

        @Override
        public void save(Map<ChunkKey, UUID> snapshot) {
            if (failNextSave) {
                failNextSave = false;
                throw new IllegalStateException("disk unavailable");
            }
            data = new HashMap<>(snapshot);
        }

        @Override
        public String describe() {
            return "memory";
        }
    }

    private static final class BlockingStore implements RegistryStore {
        private Map<ChunkKey, UUID> data = new HashMap<>();
        private final CountDownLatch firstSaveEntered = new CountDownLatch(1);
        private final CountDownLatch secondSaveEntered = new CountDownLatch(1);
        private final CountDownLatch allowFirstSave = new CountDownLatch(1);
        private int saves;

        @Override
        public Map<ChunkKey, UUID> load() {
            return new HashMap<>(data);
        }

        @Override
        public void save(Map<ChunkKey, UUID> snapshot) {
            int current;
            synchronized (this) {
                current = ++saves;
            }
            if (current == 1) {
                firstSaveEntered.countDown();
                try {
                    assertTrue(allowFirstSave.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            } else if (current == 2) {
                secondSaveEntered.countDown();
            }
            data = new HashMap<>(snapshot);
        }

        @Override
        public String describe() {
            return "blocking-memory";
        }
    }
}
