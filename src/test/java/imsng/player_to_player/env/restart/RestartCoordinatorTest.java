package imsng.player_to_player.env.restart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RestartCoordinatorTest {
    @TempDir Path root;

    @Test void cancellationAndSwitchingInvalidateDelayedCallbacks() {
        var coordinator = new RestartCoordinator(null);
        var first = coordinator.begin("first:25579");
        assertTrue(coordinator.reserve(first, false));
        assertTrue(coordinator.cancel(first));
        assertFalse(coordinator.commit(first));
        var second = coordinator.begin("second:25565");
        assertFalse(coordinator.reserve(first, true));
        assertTrue(coordinator.reserve(second, false));
        assertFalse(coordinator.fail(first));
        assertTrue(coordinator.commit(second));
    }

    @Test void duplicateCallbacksCannotReserveOrCommitTwice() {
        var coordinator = new RestartCoordinator(null);
        var intent = coordinator.begin("host:25565");
        assertTrue(coordinator.reserve(intent, false));
        assertFalse(coordinator.reserve(intent, true));
        assertTrue(coordinator.commit(intent));
        assertFalse(coordinator.commit(intent));
        assertFalse(coordinator.cancel(intent));
        assertNull(coordinator.begin("other:25565"));
        assertTrue(coordinator.committed());
    }

    @Test void failedHandshakeKeepsClientAndRequiresManualRetry() {
        var coordinator = new RestartCoordinator(null);
        var intent = coordinator.begin("host:25565");
        assertTrue(coordinator.reserve(intent, false));
        assertTrue(coordinator.fail(intent));
        assertFalse(coordinator.committed());
        assertFalse(coordinator.reserve(intent, false));
        assertTrue(coordinator.reserve(intent, true));
    }

    @Test void restartedFlowCannotAutomaticallyRestartEvenForDifferentEnvironmentHash() throws Exception {
        var store = new RestartTaskStore(root);
        var first = new RestartCoordinator(null);
        var intent = first.begin("[2001:db8::1]:25579");
        var task = store.create(intent.chainId(), intent.address(), "a".repeat(64), root.resolve("游戏"));
        var resumed = new RestartCoordinator(task);
        var reconnect = resumed.begin(task.address());
        assertEquals(intent.chainId(), reconnect.chainId());
        assertFalse(resumed.reserve(reconnect, false));
        assertTrue(resumed.reserve(reconnect, true));
        resumed.fail(reconnect);
        var other = resumed.begin("another:25565");
        assertNotEquals(intent.chainId(), other.chainId());
        assertTrue(resumed.reserve(other, false));
    }
}
