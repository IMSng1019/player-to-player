package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExitTransactionLedgerTest {

    private static final UUID SHUTDOWN_ID =
            UUID.fromString("c768829c-9160-43c7-a4fd-ecc2290936f4");
    private static final UUID CLIENT_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey CHUNK =
            new ChunkKey("minecraft:overworld", -7, 0);
    private static final ChunkKey CLEAN =
            new ChunkKey("minecraft:overworld", -6, 0);

    @Test
    void pendingTransactionOnlyRequiresDeclaredWrittenChunksBeforeCommit() {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ExitManifest manifest = manifest(
                SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK, CLEAN));

        ExitTransactionLedger.BeginResult begin = ledger.begin(CLIENT_ID, manifest);

        assertTrue(begin.accepted());
        assertFalse(begin.committed());
        assertTrue(ledger.isProtected(CLIENT_ID));
        assertFalse(ledger.commit(CLIENT_ID, SHUTDOWN_ID, Set.of(CHUNK)));

        assertTrue(ledger.markPlayerReceived(CLIENT_ID, SHUTDOWN_ID, CLIENT_ID));
        assertTrue(ledger.markPlayerReceived(CLIENT_ID, SHUTDOWN_ID, CLIENT_ID));
        assertFalse(ledger.commit(CLIENT_ID, SHUTDOWN_ID, Set.of(CHUNK)));

        assertTrue(ledger.markChunkReceived(CLIENT_ID, SHUTDOWN_ID, CHUNK));
        assertTrue(ledger.markChunkReceived(CLIENT_ID, SHUTDOWN_ID, CHUNK));
        assertTrue(ledger.commit(CLIENT_ID, SHUTDOWN_ID, Set.of(CHUNK)));
        assertTrue(ledger.isProtected(CLIENT_ID));
        assertTrue(persistence.records.get(SHUTDOWN_ID).committed());
        assertEquals(Set.of(CHUNK),
                persistence.records.get(SHUTDOWN_ID).writtenChunks());
        assertTrue(persistence.records.get(SHUTDOWN_ID).writtenChunksDeclared());

        assertTrue(ledger.markDataApplied(CLIENT_ID, SHUTDOWN_ID));
        assertTrue(persistence.records.get(SHUTDOWN_ID).dataApplied());
        assertTrue(ledger.markCleanupComplete(CLIENT_ID, SHUTDOWN_ID));
        assertFalse(ledger.isProtected(CLIENT_ID));
        assertTrue(persistence.records.get(SHUTDOWN_ID).cleanupComplete());
    }

    @Test
    void commitRejectsMissingUndeclaredAndOutOfManifestChunks() {
        ExitTransactionLedger ledger = new ExitTransactionLedger(
                new InMemoryPersistence());
        ExitManifest manifest = manifest(
                SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK, CLEAN));
        assertTrue(ledger.begin(CLIENT_ID, manifest).accepted());
        assertTrue(ledger.markPlayerReceived(CLIENT_ID, SHUTDOWN_ID, CLIENT_ID));
        assertTrue(ledger.markChunkReceived(CLIENT_ID, SHUTDOWN_ID, CHUNK));

        assertFalse(ledger.commit(
                CLIENT_ID, SHUTDOWN_ID, Set.of(CHUNK, CLEAN)));
        assertFalse(ledger.commit(CLIENT_ID, SHUTDOWN_ID, Set.of()));
        assertFalse(ledger.commit(CLIENT_ID, SHUTDOWN_ID, Set.of(
                new ChunkKey("minecraft:the_end", 1, 1))));
    }

    @Test
    void duplicateBeginIsIdempotentButConflictingMetadataIsRejected() {
        ExitTransactionLedger ledger = new ExitTransactionLedger(new InMemoryPersistence());
        ExitManifest manifest = manifest(SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK));

        assertTrue(ledger.begin(CLIENT_ID, manifest).accepted());
        assertTrue(ledger.begin(CLIENT_ID, manifest).accepted());

        ExitManifest conflicting = manifest(SHUTDOWN_ID, CLIENT_ID,
                Set.of(new ChunkKey("minecraft:overworld", 5, 6)));
        assertFalse(ledger.begin(CLIENT_ID, conflicting).accepted());
        assertFalse(ledger.begin(UUID.randomUUID(), manifest).accepted());
    }

    @Test
    void oneClientCannotCreateSecondPendingTransactionAndCanAbandonTheFirst() {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ExitManifest first = manifest(SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK));
        ExitManifest second = manifest(UUID.randomUUID(), CLIENT_ID, Set.of(CHUNK));

        assertTrue(ledger.begin(CLIENT_ID, first).accepted());
        assertFalse(ledger.begin(CLIENT_ID, second).accepted());
        assertEquals(Set.of(first), ledger.pendingManifestsForClient(CLIENT_ID));

        assertTrue(ledger.abandon(CLIENT_ID, SHUTDOWN_ID));
        assertTrue(ledger.isCommitted(CLIENT_ID, SHUTDOWN_ID));
        assertTrue(ledger.isAbandoned(CLIENT_ID, SHUTDOWN_ID));
        assertTrue(ledger.isProtected(CLIENT_ID));
        assertTrue(ledger.markCleanupComplete(CLIENT_ID, SHUTDOWN_ID));
        assertFalse(ledger.isProtected(CLIENT_ID));
        assertTrue(ledger.begin(CLIENT_ID, second).accepted());
    }

    @Test
    void pendingAndCommittedRecordsRecoverAcrossLedgerRestart() {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitManifest manifest = manifest(SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK));
        ExitTransactionLedger first = new ExitTransactionLedger(persistence);
        first.begin(CLIENT_ID, manifest);

        ExitTransactionLedger recoveredPending = new ExitTransactionLedger(persistence);
        assertTrue(recoveredPending.isProtected(CLIENT_ID));
        assertFalse(recoveredPending.begin(CLIENT_ID, manifest).committed());
        assertEquals(Set.of(manifest), recoveredPending.pendingManifests());

        recoveredPending.markPlayerReceived(CLIENT_ID, SHUTDOWN_ID, CLIENT_ID);
        recoveredPending.markChunkReceived(CLIENT_ID, SHUTDOWN_ID, CHUNK);
        assertTrue(recoveredPending.commit(
                CLIENT_ID, SHUTDOWN_ID, Set.of(CHUNK)));

        ExitTransactionLedger recoveredCommitted = new ExitTransactionLedger(persistence);
        assertTrue(recoveredCommitted.isProtected(CLIENT_ID));
        assertEquals(Set.of(manifest),
                recoveredCommitted.committedManifestsNeedingCleanup());
        assertTrue(recoveredCommitted.markDataApplied(CLIENT_ID, SHUTDOWN_ID));

        ExitTransactionLedger recoveredApplied = new ExitTransactionLedger(persistence);
        assertTrue(recoveredApplied.isDataApplied(CLIENT_ID, SHUTDOWN_ID));
        assertEquals(Set.of(manifest),
                recoveredApplied.committedManifestsNeedingCleanup());
        assertTrue(recoveredApplied.markCleanupComplete(CLIENT_ID, SHUTDOWN_ID));

        ExitTransactionLedger recoveredClean = new ExitTransactionLedger(persistence);
        ExitTransactionLedger.BeginResult begin =
                recoveredClean.begin(CLIENT_ID, manifest);
        assertTrue(begin.accepted());
        assertTrue(begin.committed());
        assertFalse(recoveredClean.isProtected(CLIENT_ID));
        assertTrue(recoveredClean.committedManifestsNeedingCleanup().isEmpty());
    }

    @Test
    void writeThatBecomesVisibleBeforeThrowKeepsProtectionAndIsRetried() {
        UncertainPersistence persistence = new UncertainPersistence(1);
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ExitManifest manifest = manifest(SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK));

        assertThrows(IllegalStateException.class,
                () -> ledger.begin(CLIENT_ID, manifest));
        assertTrue(ledger.isProtected(CLIENT_ID));

        ExitTransactionLedger.BeginResult retried = ledger.begin(CLIENT_ID, manifest);

        assertTrue(retried.accepted());
        assertFalse(retried.committed());
        assertEquals(2, persistence.saves);
    }

    @Test
    void committedWriteVisibleBeforeThrowPublishesCommittedState() {
        UncertainPersistence persistence = new UncertainPersistence(2);
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ExitManifest manifest = manifest(SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK));
        assertTrue(ledger.begin(CLIENT_ID, manifest).accepted());
        assertTrue(ledger.markPlayerReceived(CLIENT_ID, SHUTDOWN_ID, CLIENT_ID));
        assertTrue(ledger.markChunkReceived(CLIENT_ID, SHUTDOWN_ID, CHUNK));

        assertThrows(IllegalStateException.class,
                () -> ledger.commit(CLIENT_ID, SHUTDOWN_ID, Set.of(CHUNK)));

        assertTrue(ledger.isCommitted(CLIENT_ID, SHUTDOWN_ID));
        assertTrue(ledger.begin(CLIENT_ID, manifest).committed());
        assertEquals(2, persistence.saves);
    }

    @Test
    void abandonedWriteVisibleBeforeThrowPublishesAbandonedState() {
        UncertainPersistence persistence = new UncertainPersistence(2);
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ExitManifest manifest = manifest(SHUTDOWN_ID, CLIENT_ID, Set.of(CHUNK));
        assertTrue(ledger.begin(CLIENT_ID, manifest).accepted());

        assertThrows(IllegalStateException.class,
                () -> ledger.abandon(CLIENT_ID, SHUTDOWN_ID));

        assertTrue(ledger.isCommitted(CLIENT_ID, SHUTDOWN_ID));
        assertTrue(ledger.isAbandoned(CLIENT_ID, SHUTDOWN_ID));
        assertEquals(2, persistence.saves);
    }

    @Test
    void cleanupWriteVisibleBeforeThrowPublishesCleanupState() {
        UncertainPersistence persistence = new UncertainPersistence(4);
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ExitManifest manifest = manifest(SHUTDOWN_ID, CLIENT_ID, Set.of());
        assertTrue(ledger.begin(CLIENT_ID, manifest).accepted());
        assertTrue(ledger.markPlayerReceived(CLIENT_ID, SHUTDOWN_ID, CLIENT_ID));
        assertTrue(ledger.commit(CLIENT_ID, SHUTDOWN_ID, Set.of()));
        assertTrue(ledger.markDataApplied(CLIENT_ID, SHUTDOWN_ID));

        assertThrows(IllegalStateException.class,
                () -> ledger.markCleanupComplete(CLIENT_ID, SHUTDOWN_ID));

        assertTrue(ledger.isCleanupComplete(CLIENT_ID, SHUTDOWN_ID));
        assertFalse(ledger.isProtected(CLIENT_ID));
        assertEquals(4, persistence.saves);
    }

    private static ExitManifest manifest(UUID shutdownId, UUID clientId,
                                         Set<ChunkKey> chunks) {
        return new ExitManifest(shutdownId, clientId, clientId,
                Set.of(clientId), chunks);
    }

    private static final class InMemoryPersistence
            implements ExitTransactionPersistence {
        private final Map<UUID, ExitTransactionRecord> records = new HashMap<>();

        @Override
        public Map<UUID, ExitTransactionRecord> load() {
            return new HashMap<>(records);
        }

        @Override
        public void save(ExitTransactionRecord record) {
            records.put(record.manifest().shutdownId(), record);
        }
    }

    private static final class UncertainPersistence
            implements ExitTransactionPersistence {
        private final Map<UUID, ExitTransactionRecord> records = new HashMap<>();
        private final int failingSave;
        private int saves;

        private UncertainPersistence(int failingSave) {
            this.failingSave = failingSave;
        }

        @Override
        public Map<UUID, ExitTransactionRecord> load() {
            return new HashMap<>(records);
        }

        @Override
        public void save(ExitTransactionRecord record) {
            records.put(record.manifest().shutdownId(), record);
            if (++saves == failingSave) {
                throw new IllegalStateException("force failed after rename");
            }
        }
    }
}
