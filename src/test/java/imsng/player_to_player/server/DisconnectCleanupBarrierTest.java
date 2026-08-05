package imsng.player_to_player.server;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisconnectCleanupBarrierTest {

    private static final UUID CLIENT =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @Test
    void awaitReturnsBeforeCleanupIsPublished() {
        DisconnectCleanupBarrier barrier = new DisconnectCleanupBarrier();

        barrier.await(CLIENT);

        assertFalse(barrier.hasPending(CLIENT));
    }

    @Test
    void awaitBlocksDuringCleanupAndReturnsAfterCompletion() throws Exception {
        DisconnectCleanupBarrier barrier = new DisconnectCleanupBarrier();
        CompletableFuture<Void> cleanup = new CompletableFuture<>();
        barrier.publish(CLIENT, cleanup);
        CompletableFuture<Void> waiter = CompletableFuture.runAsync(
                () -> barrier.await(CLIENT));

        assertThrows(TimeoutException.class,
                () -> waiter.get(100, TimeUnit.MILLISECONDS));
        cleanup.complete(null);

        waiter.get(5, TimeUnit.SECONDS);
        assertFalse(barrier.hasPending(CLIENT));
    }

    @Test
    void overlappingCleanupCannotHideAnEarlierRelease() throws Exception {
        DisconnectCleanupBarrier barrier = new DisconnectCleanupBarrier();
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        barrier.publish(CLIENT, first);
        barrier.publish(CLIENT, second);
        CompletableFuture<Void> waiter = CompletableFuture.runAsync(
                () -> barrier.await(CLIENT));

        second.complete(null);
        TimeUnit.MILLISECONDS.sleep(100);
        assertFalse(waiter.isDone(), "后发布的清理完成不能掩盖仍在执行的旧释放");
        first.complete(null);

        waiter.get(5, TimeUnit.SECONDS);
        assertTrue(waiter.isDone());
        assertFalse(barrier.hasPending(CLIENT));
    }
}
