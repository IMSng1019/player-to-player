package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分离入口必须等到已有检查点正式发布，而不是要求网络帧恰好落在检查点之间。
 * 使用真实存储和检查点写入器，手动推进两个执行器以确定地制造“已写入、尚未发布”。
 */
class MetadataTransferTasksTest {
    @TempDir Path directory;

    @Test void prepareAndCommitRunAfterPendingCheckpointsPublishInRequestOrder() throws Exception {
        ManualExecutor owner = new ManualExecutor(), background = new ManualExecutor();
        List<String> events = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        AtomicBoolean sessionLockHeld = new AtomicBoolean();
        try (MetadataStore store = MetadataStore.open(directory, "world", data(0))) {
            AtomicReference<MetadataSnapshot> published = new AtomicReference<>(store.snapshot());
            try (MetadataCheckpointWriter writer = new MetadataCheckpointWriter(background, patch -> {
                assertFalse(sessionLockHeld.get(), "不能持会话锁等待或发布检查点");
                published.set(store.snapshot());
                events.add("publish:" + patch.version());
            })) {
                MetadataTransferTasks tasks = new MetadataTransferTasks(owner, () -> true, writer::drain);
                writer.submit(() -> store.checkpointChanges(data(20), Set.of()));
                background.runNext();
                assertEquals(1, store.snapshot().version());
                assertEquals(0, published.get().version());
                assertTrue(writer.isPending(), "写盘完成后仍须主线程发布，不能按磁盘版本直接放行");

                tasks.execute(action -> {
                    sessionLockHeld.set(true);
                    try { action.run(); return true; }
                    finally { sessionLockHeld.set(false); }
                }, () -> {
                    assertFalse(writer.isPending());
                    assertEquals(1, published.get().version());
                    events.add("prepare");
                }, failures::add);
                assertTrue(events.isEmpty(), "网络入站只排队，不在调用线程执行事务");
                owner.runNext();

                // 分离排空期间中心自然时间仍推进。提交也必须经过同一生产屏障，
                // 不能只修准备而让正常提交因下一次checkpoint被迫回滚。
                writer.submit(() -> store.checkpointChanges(data(40), Set.of()));
                background.runNext();
                tasks.execute(action -> { action.run(); return true; }, () -> {
                    assertFalse(writer.isPending());
                    assertEquals(2, published.get().version());
                    events.add("commit");
                }, failures::add);
                owner.runNext();
                assertEquals(List.of("publish:1", "prepare", "publish:2", "commit"), events);
                assertTrue(failures.isEmpty(), failures.toString());
            }
        }
    }

    @Test void queuedPhasesKeepTheirOrder() {
        ManualExecutor owner = new ManualExecutor();
        List<String> phases = new ArrayList<>();
        MetadataTransferTasks tasks = new MetadataTransferTasks(owner, () -> true, () -> { });
        for (String phase : List.of("prepare", "status", "cancel")) {
            tasks.execute(action -> { action.run(); return true; }, () -> phases.add(phase),
                    error -> fail(error));
        }
        assertTrue(phases.isEmpty());
        while (!owner.tasks.isEmpty()) owner.runNext();
        assertEquals(List.of("prepare", "status", "cancel"), phases);
    }

    @Test void sessionReplacedWhileQueuedCannotRunAfterPublication() {
        ManualExecutor owner = new ManualExecutor();
        AtomicBoolean currentSession = new AtomicBoolean(true);
        List<String> events = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        MetadataTransferTasks tasks = new MetadataTransferTasks(owner, () -> true,
                () -> events.add("published"));
        tasks.execute(action -> {
            if (!currentSession.get()) return false;
            action.run(); return true;
        }, () -> events.add("changed-groups"), failures::add);
        currentSession.set(false);
        owner.runNext();
        assertEquals(List.of("published"), events);
        assertEquals(1, failures.size());
    }

    @Test void stoppedOrReplacedServiceRejectsQueuedWorkWithoutTouchingStorage() {
        ManualExecutor owner = new ManualExecutor();
        AtomicBoolean active = new AtomicBoolean(true);
        List<String> events = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        MetadataTransferTasks tasks = new MetadataTransferTasks(owner, active::get,
                () -> events.add("drain"));
        tasks.execute(action -> { action.run(); return true; }, () -> events.add("split"), failures::add);
        active.set(false);
        owner.runNext();
        assertTrue(events.isEmpty());
        assertEquals(1, failures.size());
    }

    @Test void serviceInvalidatedByBarrierCannotExecuteTransaction() {
        ManualExecutor owner = new ManualExecutor();
        AtomicBoolean active = new AtomicBoolean(true), executed = new AtomicBoolean();
        List<Exception> failures = new ArrayList<>();
        MetadataTransferTasks tasks = new MetadataTransferTasks(owner, active::get, () -> active.set(false));
        tasks.execute(action -> { action.run(); return true; }, () -> executed.set(true), failures::add);
        owner.runNext();
        assertFalse(executed.get());
        assertEquals(1, failures.size());
    }

    @Test void checkpointFailureRejectsWithoutRunningSessionGuardOrTransaction() {
        ManualExecutor owner = new ManualExecutor();
        IOException diskFailure = new IOException("checkpoint fsync failed");
        List<Exception> failures = new ArrayList<>();
        MetadataTransferTasks tasks = new MetadataTransferTasks(owner, () -> true, () -> { throw diskFailure; });
        tasks.execute(action -> { fail("检查点失败不能进入会话锁"); return true; },
                () -> fail("检查点失败不能执行分离"), failures::add);
        owner.runNext();
        assertEquals(List.of(diskFailure), failures);
    }

    @Test void rejectedSchedulingProducesAnExplicitFailure() {
        RejectedExecutionException stopped = new RejectedExecutionException("server stopped");
        List<Exception> failures = new ArrayList<>();
        MetadataTransferTasks tasks = new MetadataTransferTasks(command -> { throw stopped; },
                () -> true, () -> fail("调度失败不得接触检查点"));
        tasks.execute(action -> { action.run(); return true; }, () -> fail("调度失败不得执行事务"), failures::add);
        assertEquals(List.of(stopped), failures);
    }

    private static CompoundTag data(long time) {
        CompoundTag result = new CompoundTag();
        result.putLong("DayTime", time);
        return result;
    }

    private static final class ManualExecutor implements Executor {
        final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        @Override public void execute(Runnable task) { tasks.addLast(task); }
        void runNext() { tasks.removeFirst().run(); }
    }
}
