package imsng.player_to_player.server;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkWriteback;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkWriteCoordinatorTest {

    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", 3, -4);
    private static final UUID OWNER =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @Test
    void finalWriteAndOwnershipReleaseBlockLateRealtimeWriteAsOneCriticalSection()
            throws Exception {
        ChunkWriteCoordinator coordinator = new ChunkWriteCoordinator();
        AtomicReference<UUID> owner = new AtomicReference<>(OWNER);
        AtomicBoolean lateWriteOverwroteFinal = new AtomicBoolean();
        CountDownLatch finalWriteEntered = new CountDownLatch(1);
        CountDownLatch lateWriteAttempting = new CountDownLatch(1);

        CompletableFuture<ChunkWriteback.ReleaseResult> release =
                CompletableFuture.supplyAsync(() -> coordinator.finalizeAndRelease(
                        KEY,
                        true,
                        () -> {
                            finalWriteEntered.countDown();
                            await(lateWriteAttempting);
                            return true;
                        },
                        () -> owner.compareAndSet(OWNER, null)));
        assertTrue(finalWriteEntered.await(5, TimeUnit.SECONDS));

        CompletableFuture<Void> lateWrite = CompletableFuture.runAsync(() -> {
            lateWriteAttempting.countDown();
            coordinator.withLock(KEY, () -> {
                if (OWNER.equals(owner.get())) {
                    lateWriteOverwroteFinal.set(true);
                }
                return null;
            });
        });

        ChunkWriteback.ReleaseResult result = release.get(5, TimeUnit.SECONDS);
        lateWrite.get(5, TimeUnit.SECONDS);

        assertTrue(result.finalDataWritten());
        assertTrue(result.released());
        assertFalse(lateWriteOverwroteFinal.get());
    }

    @Test
    void failedFinalWriteNeverRunsOwnershipRelease() {
        ChunkWriteCoordinator coordinator = new ChunkWriteCoordinator();
        AtomicBoolean releaseCalled = new AtomicBoolean();

        ChunkWriteback.ReleaseResult result = coordinator.finalizeAndRelease(
                KEY, true, () -> false, () -> {
                    releaseCalled.set(true);
                    return true;
                });

        assertFalse(result.finalDataWritten());
        assertFalse(result.released());
        assertFalse(releaseCalled.get());
    }

    @Test
    void duplicateReleaseReplaysReceiptWithoutWritingFinalDataAgain() {
        ChunkWriteCoordinator coordinator = new ChunkWriteCoordinator();
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger commits = new AtomicInteger();
        AtomicReference<ChunkWriteback.ReleaseResult> receipt =
                new AtomicReference<>();
        ChunkWriteback.ReleaseTransaction transaction =
                new ChunkWriteback.ReleaseTransaction() {
                    @Override
                    public ChunkWriteback.ReleaseResult replay() {
                        return receipt.get();
                    }

                    @Override
                    public ChunkWriteback.ReleaseResult commit(
                            boolean finalDataWritten) {
                        commits.incrementAndGet();
                        ChunkWriteback.ReleaseResult result =
                                new ChunkWriteback.ReleaseResult(
                                        finalDataWritten, true);
                        receipt.set(result);
                        return result;
                    }
                };

        ChunkWriteback.ReleaseResult first = coordinator.finalizeAndRelease(
                KEY, true, () -> {
                    writes.incrementAndGet();
                    return true;
                }, transaction);
        ChunkWriteback.ReleaseResult replayed = coordinator.finalizeAndRelease(
                KEY, true, () -> {
                    writes.incrementAndGet();
                    return true;
                }, transaction);

        assertTrue(first.released());
        assertEquals(first, replayed);
        assertEquals(1, writes.get());
        assertEquals(1, commits.get());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("等待并发测试条件超时");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("并发测试被中断", e);
        }
    }
}
