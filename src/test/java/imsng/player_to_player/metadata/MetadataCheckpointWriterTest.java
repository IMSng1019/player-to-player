package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class MetadataCheckpointWriterTest {
    @TempDir Path directory;

    @Test
    void readingCommittedSnapshotNeverWaitsForBackgroundWriterMonitor() throws Exception {
        try (MetadataStore store = MetadataStore.open(directory, "world", data(10))) {
            var executor = Executors.newFixedThreadPool(2);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            try {
                // 磁盘写入当前持有同一存储监视器；稳定复现慢盘时读快照被连带阻塞。
                var writer = executor.submit(() -> {
                    synchronized (store) {
                        entered.countDown();
                        release.await();
                    }
                    return null;
                });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var reader = executor.submit(store::snapshot);
                assertEquals(10, reader.get(1, TimeUnit.SECONDS).data().getLong("DayTime"));
                release.countDown();
                writer.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void slowWriterKeepsOneSampleAndPublishesExactlyOnceOnOwnerThread() throws Exception {
        var executor = new ManualExecutor();
        List<MetadataPatch> published = new ArrayList<>();
        Thread owner = Thread.currentThread();
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0));
             var writer = new MetadataCheckpointWriter(executor, patch -> {
                 assertSame(owner, Thread.currentThread());
                 published.add(patch);
             })) {
            assertTrue(writer.submit(() -> store.checkpointChanges(data(1), Set.of())));
            for (int i = 2; i <= 100; i++) {
                long time = i;
                assertFalse(writer.submit(() -> store.checkpointChanges(data(time), Set.of())));
            }
            assertEquals(1, executor.tasks.size());
            assertTrue(writer.isPending());
            writer.poll();
            assertEquals(0, store.snapshot().version());
            assertTrue(published.isEmpty());
            executor.runNext();
            assertEquals(1, store.snapshot().version());
            assertTrue(published.isEmpty(), "后台完成不能直接触碰世界镜像或广播");
            writer.poll();
            writer.poll();
            assertEquals(List.of(1L), published.stream().map(MetadataPatch::version).toList());
            assertFalse(writer.isPending());
            assertTrue(writer.submit(() -> store.checkpointChanges(data(101), Set.of())));
            executor.runNext();
            writer.drain();
            assertEquals(101, store.snapshot().data().getLong("DayTime"));
            assertEquals(2, published.size());
        }
    }

    @Test
    void discreteCommitAndTermBarrierFollowPendingCheckpointWithoutRollback() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        List<Long> published = new ArrayList<>();
        try {
            try (MetadataStore store = MetadataStore.open(directory, "world", data(0));
                 var writer = new MetadataCheckpointWriter(executor, patch -> published.add(patch.version()))) {
                writer.submit(() -> store.checkpointChanges(data(20), Set.of()));
                writer.drain(); // 离散命令先排空自然采样，才以最新版本提交。
                var base = store.snapshot();
                var command = store.commit("world", base.version(), base.term(), "time-command", data(200), Set.of());
                published.add(command.version());
                writer.submit(() -> store.checkpointChanges(data(220), Set.of()));
                writer.drain(); // 任期屏障也必须位于自然采样之后。
                long previousTerm = store.snapshot().term();
                store.advanceTerm();
                assertEquals(previousTerm + 1, store.snapshot().term());
                assertEquals(List.of(1L, 2L, 3L), published);
                assertEquals(220, store.snapshot().data().getLong("DayTime"));
            }
            try (MetadataStore recovered = MetadataStore.open(directory, "world", data(-1))) {
                assertEquals(220, recovered.snapshot().data().getLong("DayTime"));
                assertTrue(recovered.lookupOperation("time-command").isPresent());
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void writeFailurePoisonsFutureSamplesAndNeverPublishes() throws Exception {
        var executor = new ManualExecutor();
        List<MetadataPatch> published = new ArrayList<>();
        var writer = new MetadataCheckpointWriter(executor, published::add);
        writer.submit(() -> { throw new IOException("disk failed"); });
        executor.runNext();
        IOException failure = assertThrows(IOException.class, writer::poll);
        assertEquals("disk failed", failure.getMessage());
        assertThrows(IOException.class, () -> writer.submit(() -> null));
        assertThrows(IOException.class, writer::close);
        assertTrue(published.isEmpty());
        assertFalse(writer.isPending());
    }

    @Test
    void closeDrainsSlowWriteBeforePublishingAndRejectsLaterSamples() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        var closer = Executors.newSingleThreadExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var published = new AtomicBoolean();
        var writer = new MetadataCheckpointWriter(executor, patch -> published.set(true));
        try {
            writer.submit(() -> {
                entered.countDown();
                try { release.await(); }
                catch (InterruptedException interrupted) { throw new IOException(interrupted); }
                return new MetadataPatch("world", 0, 1, 1, "checkpoint", data(1), Set.of());
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var closed = closer.submit(() -> { writer.close(); return null; });
            assertFalse(published.get());
            assertFalse(closed.isDone());
            release.countDown();
            closed.get(5, TimeUnit.SECONDS);
            assertTrue(published.get());
            assertThrows(IOException.class, () -> writer.submit(() -> null));
        } finally {
            release.countDown();
            executor.shutdownNow();
            closer.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(closer.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void publicationFailureCannotReplayOrAcceptMoreWork() throws Exception {
        var executor = new ManualExecutor();
        var writer = new MetadataCheckpointWriter(executor, patch -> { throw new IOException("publish failed"); });
        writer.submit(() -> new MetadataPatch("world", 0, 1, 1, "checkpoint", data(1), Set.of()));
        executor.runNext();
        assertThrows(IOException.class, writer::poll);
        assertThrows(IOException.class, writer::poll);
        assertFalse(writer.isPending());
        assertThrows(IOException.class, () -> writer.submit(() -> null));
    }

    @Test
    void transferRemainsBlockedUntilOwnerFinishesPublishing() throws Exception {
        var executor = new ManualExecutor();
        MetadataCheckpointWriter[] holder = new MetadataCheckpointWriter[1];
        holder[0] = new MetadataCheckpointWriter(executor, patch ->
                assertTrue(holder[0].isPending(), "回调尚未结束时不能打开转移屏障"));
        try (var writer = holder[0]) {
            writer.submit(() -> new MetadataPatch("world", 0, 1, 1, "checkpoint", data(1), Set.of()));
            executor.runNext();
            writer.poll();
            assertFalse(writer.isPending());
        }
    }

    private static final class ManualExecutor implements Executor {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        @Override public void execute(Runnable task) { tasks.add(task); }
        void runNext() { tasks.remove().run(); }
    }

    private static CompoundTag data(long time) {
        CompoundTag data = new CompoundTag();
        data.putLong("DayTime", time);
        return data;
    }
}
