package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkPreUnloadServiceTest {

    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", -7, 0);

    @Test
    void duplicateBeginSharesOneBarrierAndOneRelease() {
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicInteger releaseCalls = new AtomicInteger();
        ChunkPreUnloadService service = service(
                false, false, true, releaseCalls, new AtomicReference<>(),
                release, new AtomicInteger());

        CompletableFuture<Void> first = service.begin(KEY, false, this::snapshotFuture);
        CompletableFuture<Void> second = service.begin(KEY, true, this::snapshotFuture);

        assertSame(first, second);
        assertEquals(1, releaseCalls.get());
        assertFalse(first.isDone());
    }

    @Test
    void dirtyUnacknowledgedOrMissingBaselineEachRequiresFinalData() {
        assertFinalDataDecision(true, false, false, true);
        assertFinalDataDecision(false, true, false, true);
        assertFinalDataDecision(false, false, true, true);
        assertFinalDataDecision(false, false, false, false);
    }

    @Test
    void barrierCompletesOnlyAfterReleaseAckThenForgetsUploadState() {
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicInteger forgotten = new AtomicInteger();
        ChunkPreUnloadService service = service(
                false, false, true, new AtomicInteger(), new AtomicReference<>(),
                release, forgotten);

        CompletableFuture<Void> barrier = service.begin(KEY, false, this::snapshotFuture);

        assertFalse(barrier.isDone());
        assertEquals(0, forgotten.get());
        release.complete(null);
        assertTrue(barrier.isDone());
        assertFalse(barrier.isCompletedExceptionally());
        assertEquals(1, forgotten.get());
    }

    @Test
    void exitPreparationWaitsForBarriersAndFreezeRejectsNewBarrierCreation() {
        List<CompletableFuture<Void>> releases = new ArrayList<>();
        AtomicInteger releaseCalls = new AtomicInteger();
        ChunkPreUnloadService service = new ChunkPreUnloadService(
                ignored -> true,
                ignored -> false,
                ignored -> false,
                (key, payload) -> {
                    releaseCalls.incrementAndGet();
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    releases.add(future);
                    return future;
                },
                ignored -> {
                },
                ignored -> new byte[]{1},
                Runnable::run);

        CompletableFuture<Void> first = service.begin(KEY, false, this::snapshotFuture);
        ChunkKey secondKey = new ChunkKey(KEY.dimension(), KEY.x() + 1, KEY.z());
        CompletableFuture<Void> second = service.begin(secondKey, false, this::snapshotFuture);
        service.beginExitPreparation();
        CompletableFuture<Void> quiescent = service.awaitQuiescent();

        assertFalse(quiescent.isDone());
        releases.get(0).complete(null);
        assertTrue(first.isDone());
        assertFalse(second.isDone());
        assertFalse(quiescent.isDone());

        releases.get(1).complete(null);
        assertTrue(second.isDone());
        assertTrue(quiescent.isDone());

        ChunkKey frozenKey = new ChunkKey(KEY.dimension(), KEY.x() + 2, KEY.z());
        CompletableFuture<Void> afterFreeze =
                service.begin(frozenKey, true, this::snapshotFuture);
        assertTrue(afterFreeze.isDone());
        assertFalse(afterFreeze.isCompletedExceptionally());
        assertEquals(2, releaseCalls.get(), "退出冻结后不得创建新的 release 屏障");
    }

    @Test
    void committedTransferReleasesSaveDependencyWithoutSendingLateSnapshot() {
        CompletableFuture<CompoundTag> snapshot = new CompletableFuture<>();
        AtomicInteger releaseCalls = new AtomicInteger();
        AtomicInteger forgotten = new AtomicInteger();
        ChunkPreUnloadService service = service(false, false, true,
                releaseCalls, new AtomicReference<>(), new CompletableFuture<>(), forgotten);
        CompletableFuture<Void> barrier = service.begin(KEY, true, () -> snapshot);
        assertFalse(barrier.isDone());

        service.retireAfterTransfer();

        assertTrue(barrier.isDone(), "COMMIT后旧所有者不应再阻塞原版保存依赖");
        assertFalse(barrier.isCompletedExceptionally());
        assertEquals(1, forgotten.get());
        snapshot.complete(snapshot());
        assertEquals(0, releaseCalls.get(), "COMMIT后的迟到快照无权再释放新主占用");
        assertTrue(service.begin(KEY, true, this::snapshotFuture).isDone());
        assertEquals(0, releaseCalls.get());
    }

    @Test
    void localSnapshotFailureKeepsBarrierAndRetriesWithFreshSnapshot() {
        AtomicInteger snapshots = new AtomicInteger();
        AtomicInteger releases = new AtomicInteger();
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkPreUnloadService service = new ChunkPreUnloadService(
                ignored -> true,
                ignored -> false,
                ignored -> true,
                (key, payload) -> {
                    releases.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                ignored -> {
                },
                ignored -> new byte[]{1},
                Runnable::run,
                retry::set);

        CompletableFuture<Void> barrier = service.begin(KEY, false, () -> {
            if (snapshots.incrementAndGet() == 1) {
                throw new IllegalStateException("snapshot failed");
            }
            return CompletableFuture.completedFuture(snapshot());
        });

        assertFalse(barrier.isDone(), "本地快照失败不能放行卸载");
        assertEquals(0, releases.get());
        retry.get().run();

        assertTrue(barrier.isDone());
        assertFalse(barrier.isCompletedExceptionally());
        assertEquals(2, snapshots.get());
        assertEquals(1, releases.get());
    }

    private void assertFinalDataDecision(boolean unsaved, boolean unacknowledged,
                                         boolean requiresInitialWrite,
                                         boolean expectPayload) {
        AtomicInteger snapshots = new AtomicInteger();
        AtomicReference<byte[]> payload = new AtomicReference<>();
        ChunkPreUnloadService service = service(
                unacknowledged, requiresInitialWrite, true,
                new AtomicInteger(), payload,
                CompletableFuture.completedFuture(null), new AtomicInteger());

        service.begin(KEY, unsaved, () -> {
            snapshots.incrementAndGet();
            return CompletableFuture.completedFuture(snapshot());
        }).join();

        assertEquals(expectPayload ? 1 : 0, snapshots.get());
        if (expectPayload) {
            assertEquals(1, payload.get().length);
        } else {
            assertNull(payload.get());
        }
    }

    private ChunkPreUnloadService service(
            boolean unacknowledged,
            boolean requiresInitialWrite,
            boolean claimed,
            AtomicInteger releaseCalls,
            AtomicReference<byte[]> payload,
            CompletableFuture<Void> release,
            AtomicInteger forgotten) {
        return new ChunkPreUnloadService(
                ignored -> claimed,
                ignored -> unacknowledged,
                ignored -> requiresInitialWrite,
                (key, bytes) -> {
                    releaseCalls.incrementAndGet();
                    payload.set(bytes);
                    return release;
                },
                ignored -> forgotten.incrementAndGet(),
                ignored -> new byte[]{1},
                Runnable::run);
    }

    private CompoundTag snapshot() {
        CompoundTag tag = new CompoundTag();
        tag.putInt("xPos", KEY.x());
        tag.putInt("zPos", KEY.z());
        return tag;
    }

    private CompletableFuture<CompoundTag> snapshotFuture() {
        return CompletableFuture.completedFuture(snapshot());
    }
}
