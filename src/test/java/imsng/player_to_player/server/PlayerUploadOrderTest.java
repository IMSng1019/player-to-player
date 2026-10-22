package imsng.player_to_player.server;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PlayerUploadOrderTest {
    private final PlayerUploadOrder uploads = new PlayerUploadOrder();
    private final Object connection = new Object();
    private final UUID player = UUID.randomUUID();

    @Test
    void queuedOldFrameCannotOverwriteConfirmedFinalSnapshot() {
        AtomicReference<String> file = new AtomicReference<>("initial");
        assertTrue(uploads.write(connection, player, 9, "new", () -> true,
                () -> { file.set("final"); return true; }));
        assertTrue(uploads.write(connection, player, 8, "old", () -> true,
                () -> { file.set("stale"); return true; }));
        assertEquals("final", file.get());
    }

    @Test
    void retryIsIdempotentAndSameSequenceDifferentContentIsRejected() {
        AtomicInteger writes = new AtomicInteger();
        assertTrue(uploads.write(connection, player, 4, "payload", () -> true,
                () -> { writes.incrementAndGet(); return true; }));
        assertTrue(uploads.write(connection, player, 4, "payload", () -> true,
                () -> { writes.incrementAndGet(); return true; }));
        assertFalse(uploads.write(connection, player, 4, "different", () -> true,
                () -> { writes.incrementAndGet(); return true; }));
        assertEquals(1, writes.get());
    }

    @Test
    void failedDiskWriteDoesNotAcknowledgeOrConsumeItsRetry() {
        assertFalse(uploads.write(connection, player, 5, "data", () -> true, () -> false));
        AtomicBoolean retried = new AtomicBoolean();
        assertTrue(uploads.write(connection, player, 5, "data", () -> true,
                () -> { retried.set(true); return true; }));
        assertTrue(retried.get());
    }

    @Test
    void authorityIsRecheckedEvenForAlreadyAcknowledgedSequence() {
        assertTrue(uploads.write(connection, player, 5, "data", () -> true, () -> true));
        AtomicBoolean wrote = new AtomicBoolean();
        assertFalse(uploads.write(connection, player, 5, "data", () -> false,
                () -> { wrote.set(true); return true; }));
        assertFalse(wrote.get());
    }

    @Test
    void newControlConnectionGetsIndependentSequenceSpace() {
        assertTrue(uploads.write(connection, player, 999, "data", () -> true, () -> true));
        AtomicBoolean wrote = new AtomicBoolean();
        assertTrue(uploads.write(new Object(), player, 1, "fresh", () -> true,
                () -> { wrote.set(true); return true; }));
        assertTrue(wrote.get());
    }

    @Test
    void formerPrimaryCannotUploadSelfAfterBecomingSecondary() {
        UUID previous = UUID.randomUUID(), next = UUID.randomUUID();
        GroupTable groups = new GroupTable();
        groups.createGroup(previous);
        groups.createGroup(next);
        assertTrue(PlayerUploadOrder.isAuthorized(groups, previous, previous));
        groups.addSecondary(next, previous);
        assertFalse(PlayerUploadOrder.isAuthorized(groups, previous, previous));
        assertTrue(PlayerUploadOrder.isAuthorized(groups, next, previous));
    }
}
