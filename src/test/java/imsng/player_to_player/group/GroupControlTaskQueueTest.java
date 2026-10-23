package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 模拟区块 managedBlock 只轮询专用队列，验证迁移采集无需等待下一个世界 tick。 */
class GroupControlTaskQueueTest {
    @Test
    void chunkWaitCanCaptureSnapshotWithoutRunningQueuedGameCommand() throws Exception {
        GroupControlTaskQueue control = new GroupControlTaskQueue(Thread.currentThread(), () -> true);
        ArrayDeque<Runnable> ordinaryServerTasks = new ArrayDeque<>();
        ordinaryServerTasks.add(() -> fail("区块等待内不能顺带执行游戏指令"));
        List<Integer> captured = new ArrayList<>();
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1, 2).iterator(), control,
                key -> { captured.add(key); return key; }, 32, 100, () -> 0L);
        AtomicReference<java.util.concurrent.CompletableFuture<SnapshotBatchCollector.Batch<Integer, Integer>>>
                batch = new AtomicReference<>();
        Thread sender = new Thread(() -> batch.set(collector.next()));
        sender.start();
        sender.join();

        assertTrue(captured.isEmpty(), "后台仅入队，不能读取世界");
        assertFalse(batch.get().isDone());
        assertTrue(control.poll(), "区块主线程的 managedBlock 必须能推进迁移快照");
        assertEquals(List.of(1, 2), captured);
        assertTrue(batch.get().join().finished());
        assertEquals(1, ordinaryServerTasks.size());
    }

    @Test
    void cancelledSnapshotNeverReadsWorldWhenWaitLoopLaterPolls() {
        GroupControlTaskQueue control = new GroupControlTaskQueue(Thread.currentThread(), () -> true);
        SnapshotBatchCollector<Integer, Integer> collector = new SnapshotBatchCollector<>(
                List.of(1).iterator(), control,
                key -> { fail("超时取消的快照不能读取旧世界"); return key; }, 32, 100, () -> 0L);
        var pending = collector.next();
        pending.cancel(false);

        assertTrue(control.poll());
        assertTrue(pending.isCancelled());
    }

    @Test
    void retiredWorldTasksCannotBeExecutedByTheNextWorld() {
        AtomicBoolean current = new AtomicBoolean(true);
        GroupControlTaskQueue oldWorld = new GroupControlTaskQueue(Thread.currentThread(), current::get);
        oldWorld.execute(() -> fail("已卸载实例的任务不能在新世界执行"));
        current.set(false);
        GroupControlTaskQueue newWorld = new GroupControlTaskQueue(Thread.currentThread(), () -> true);
        AtomicBoolean ranNew = new AtomicBoolean();
        newWorld.execute(() -> ranNew.set(true));

        assertFalse(oldWorld.poll());
        assertThrows(RejectedExecutionException.class, () -> oldWorld.execute(() -> {}));
        assertTrue(newWorld.poll());
        assertTrue(ranNew.get());
    }

    @Test
    void closedQueueRejectsNewWorkAndDropsPendingWorldReads() {
        GroupControlTaskQueue control = new GroupControlTaskQueue(Thread.currentThread(), () -> true);
        control.execute(() -> fail("关停后不能读取世界"));
        control.close();

        assertFalse(control.poll());
        assertThrows(RejectedExecutionException.class, () -> control.execute(() -> {}));
    }

    @Test
    void recursiveChunkWaitDoesNotReenterAnotherControlTask() {
        GroupControlTaskQueue control = new GroupControlTaskQueue(Thread.currentThread(), () -> true);
        List<String> events = new ArrayList<>();
        control.execute(() -> {
            events.add("capture-start");
            assertFalse(control.poll(), "采集中意外触发区块轮询时不能重入下一项控制任务");
            events.add("capture-end");
        });
        control.execute(() -> events.add("next-capture"));

        assertTrue(control.poll());
        assertEquals(List.of("capture-start", "capture-end"), events);
        assertTrue(control.poll());
        assertEquals(List.of("capture-start", "capture-end", "next-capture"), events);
    }

    @Test
    void pollingFromBackgroundThreadFailsBeforeReadingWorld() throws Exception {
        GroupControlTaskQueue control = new GroupControlTaskQueue(Thread.currentThread(), () -> true);
        control.execute(() -> fail("后台不能读取世界"));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try { control.poll(); } catch (Throwable error) { failure.set(error); }
        });
        worker.start();
        worker.join();

        assertInstanceOf(IllegalStateException.class, failure.get());
        control.close();
    }
}
