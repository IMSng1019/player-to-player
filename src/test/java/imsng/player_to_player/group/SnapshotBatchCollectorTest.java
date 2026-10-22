package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** 确认跨线程调度按批次发生，并在慢序列化、空区块和取消时保持有界。 */
class SnapshotBatchCollectorTest {
    @Test
    void capturesMultipleChunksInOneScheduledTaskWithoutExceedingCountLimit() {
        ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        List<Integer> captured = new ArrayList<>();
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1, 2, 3, 4, 5).iterator(), tasks::add,
                key -> { captured.add(key); return key; }, 3, 100, () -> 0L);

        var pending = collector.next();
        assertEquals(1, tasks.size());
        assertTrue(captured.isEmpty(), "未调度至主线程前不得读取世界");
        tasks.remove().run();

        assertEquals(List.of(1, 2, 3), captured);
        assertEquals(3, pending.join().values().size());
        assertFalse(pending.join().finished());
        var last = collector.next();
        tasks.remove().run();
        assertEquals(List.of(1, 2, 3, 4, 5), captured);
        assertTrue(last.join().finished());
        assertTrue(tasks.isEmpty());
    }

    @Test
    void stopsBeforeNextChunkWhenSerializationConsumesTimeBudget() {
        AtomicLong now = new AtomicLong();
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1, 2, 3).iterator(), Runnable::run,
                key -> { now.addAndGet(6); return key; }, 32, 10, now::get);

        var batch = collector.next().join();

        assertEquals(2, batch.visited());
        assertEquals(12, batch.captureNanos());
        assertFalse(batch.finished());
    }

    @Test
    void alwaysMakesProgressWhenOneChunkExceedsBudget() {
        AtomicLong now = new AtomicLong();
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1, 2).iterator(), Runnable::run,
                key -> { now.addAndGet(20); return key; }, 32, 10, now::get);

        assertEquals(1, collector.next().join().visited());
        assertTrue(collector.next().join().finished());
    }

    @Test
    void unloadedChunksStillConsumeBatchCountBudget() {
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1, 2, 3, 4).iterator(), Runnable::run,
                key -> null, 2, 100, () -> 0L);

        var batch = collector.next().join();

        assertEquals(2, batch.visited());
        assertTrue(batch.values().isEmpty());
        assertFalse(batch.finished());
    }

    @Test
    void cancelledQueuedBatchDoesNotReadWorld() {
        ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1, 2).iterator(), tasks::add,
                key -> { fail("已取消任务不得读取区块"); return key; }, 32, 100, () -> 0L);

        var pending = collector.next();
        pending.cancel(false);
        tasks.remove().run();

        assertTrue(pending.isCancelled());
    }

    @Test
    void reportsMainThreadQueueTimeSeparatelyFromCaptureTime() {
        AtomicLong now = new AtomicLong(100);
        ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1).iterator(), tasks::add,
                key -> { now.addAndGet(7); return key; }, 32, 100, now::get);
        var pending = collector.next();
        now.addAndGet(40);

        tasks.remove().run();

        assertEquals(40, pending.join().queueNanos());
        assertEquals(7, pending.join().captureNanos());
    }
}
