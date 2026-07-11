package imsng.player_to_player.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 模组统一线程池管理。
 * <p>
 * 设计原则（见 DESIGN.md 第 9 节）：
 * <ul>
 *   <li>所有后台线程统一命名（p2p-io-*、p2p-sched-*、p2p-compute-*）便于排查卡顿/泄漏；</li>
 *   <li>全部为守护线程 —— MC 进程退出时不阻塞 JVM 关闭；</li>
 *   <li>阻塞 IO（文件扫描、哈希、磁盘写）走 {@link #io()}，
 *       CPU 密集任务（跑分、压缩）走 {@link #compute()}，
 *       定时任务（心跳、落盘）走 {@link #scheduler()}；</li>
 *   <li>严禁在 MC 主线程或 Netty 事件循环里做阻塞操作。</li>
 * </ul>
 */
public final class ThreadPools {

    /** IO 线程池：无界缓存池，IO 任务大多在等待磁盘/网络，线程可多。 */
    private static final ExecutorService IO =
            Executors.newCachedThreadPool(namedFactory("p2p-io"));

    /** 计算线程池：按核数固定（至少 1），避免 CPU 密集任务互相挤占。 */
    private static final ExecutorService COMPUTE =
            Executors.newFixedThreadPool(
                    Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
                    namedFactory("p2p-compute"));

    /** 调度线程池：2 线程足够承载心跳/落盘等轻量定时任务。 */
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(2, namedFactory("p2p-sched"));

    private ThreadPools() {
    }

    /** 创建"p2p-<名字>-<序号>"命名的守护线程工厂（Netty 事件循环等外部组件也用它）。 */
    public static ThreadFactory namedFactory(String name) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, name + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /** 阻塞 IO 线程池（文件扫描、哈希、磁盘读写、HTTP 请求）。 */
    public static ExecutorService io() {
        return IO;
    }

    /** CPU 密集线程池（算力跑分、序列化压缩）。 */
    public static ExecutorService compute() {
        return COMPUTE;
    }

    /** 定时调度线程池（心跳、周期落盘、超时检查）。注意任务体要短，重活丢给 io()/compute()。 */
    public static ScheduledExecutorService scheduler() {
        return SCHEDULER;
    }

    /** 关闭所有线程池（进程退出钩子里调用；守护线程本身不阻塞退出，此处属礼貌收尾）。 */
    public static void shutdownAll() {
        IO.shutdown();
        COMPUTE.shutdown();
        SCHEDULER.shutdown();
        try {
            //noinspection ResultOfMethodCallIgnored
            IO.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
