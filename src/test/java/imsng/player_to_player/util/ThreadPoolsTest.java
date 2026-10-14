package imsng.player_to_player.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * io 池上界的回归测试（对应 2026-10-14 服务端 {@code OutOfMemoryError: unable to create
 * native thread} 缺陷，见 {@code docs/HANDOFF.md} §1 P0）。
 * <p>
 * 这两条断言在"无界 cached pool"的旧实现下必然失败：那时 2000 个逐区块任务会在提交线程里
 * 内联创建 2000 条线程（每条 ~1 MB 栈），这正是 native OOM 的成因。
 */
class ThreadPoolsTest {

    /** 与日志同量级的逐区块任务洪峰（实测一次会话 2500+ 个区块级帧）。 */
    private static final int BURST_TASKS = 2000;

    @Test
    void burstOfPerChunkTasksKeepsThreadCountBounded() throws Exception {
        CountDownLatch done = new CountDownLatch(BURST_TASKS);
        AtomicInteger peakThreads = new AtomicInteger();
        AtomicInteger peakQueue = new AtomicInteger();

        for (int i = 0; i < BURST_TASKS; i++) {
            ThreadPools.io().execute(() -> {
                try {
                    // 模拟真实任务：短暂阻塞（解压/校验/等待 fsync 的近似）
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                peakThreads.accumulateAndGet(ThreadPools.ioPoolThreadCount(), Math::max);
                peakQueue.accumulateAndGet(ThreadPools.ioQueueDepth(), Math::max);
                done.countDown();
            });
        }

        assertTrue(done.await(60, TimeUnit.SECONDS),
                "2000 个逐区块任务必须全部完成（有界池不得拒绝、不得丢任务）");
        assertTrue(peakThreads.get() <= ThreadPools.ioMaxThreads(),
                "io 线程数必须有上界：峰值 " + peakThreads.get()
                        + " 超过上限 " + ThreadPools.ioMaxThreads());
        assertTrue(peakQueue.get() > 0,
                "超出线程数的任务必须进入队列（而不是新建线程或抛拒绝）");
    }

    @Test
    void blockingThreadsDoNotConsumeIoPoolCapacity() throws Exception {
        int blockers = 8;
        CountDownLatch release = new CountDownLatch(1);
        List<Thread> blocking = new ArrayList<>();
        for (int i = 0; i < blockers; i++) {
            blocking.add(ThreadPools.startBlocking("test-block", () -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        // 长驻阻塞线程全部在跑的同时，io 池仍须能跑完短任务 ——
        // 否则"有界池 + 长驻任务"会把线程耗尽换成饥饿死锁
        int tasks = 300;
        CountDownLatch done = new CountDownLatch(tasks);
        for (int i = 0; i < tasks; i++) {
            ThreadPools.io().execute(done::countDown);
        }
        assertTrue(done.await(30, TimeUnit.SECONDS),
                "长驻阻塞任务不得占用 io 池容量");
        assertTrue(ThreadPools.blockingThreadCount() >= blockers,
                "长驻线程必须被登记（诊断可见），实际 " + ThreadPools.blockingThreadCount());
        assertTrue(ThreadPools.ioPoolThreadCount() <= ThreadPools.ioMaxThreads(),
                "长驻线程不得让 io 池超出上界");

        release.countDown();
        for (Thread thread : blocking) {
            thread.join(5_000);
        }
        assertEquals(0, ThreadPools.blockingThreadCount(),
                "长驻线程结束后必须从登记表移除，否则诊断计数会长期虚高");
    }

    @Test
    void ioPoolUpperBoundIsSane() {
        assertTrue(ThreadPools.ioMaxThreads() >= 8 && ThreadPools.ioMaxThreads() <= 32,
                "io 池上界需落在合理区间（容器 native 线程预算内），实际 "
                        + ThreadPools.ioMaxThreads());
    }
}
