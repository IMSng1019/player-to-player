package imsng.player_to_player.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 模组统一线程池管理。
 * <p>
 * 设计原则（见 DESIGN.md 第 9 节）：
 * <ul>
 *   <li>所有后台线程统一命名（p2p-io-*、p2p-sched-*、p2p-compute-*、p2p-blocking-*）便于排查卡顿/泄漏；</li>
 *   <li>全部为守护线程 —— MC 进程退出时不阻塞 JVM 关闭；</li>
 *   <li>阻塞 IO（文件扫描、哈希、磁盘写）走 {@link #io()}，
 *       CPU 密集任务（跑分、压缩）走 {@link #compute()}，
 *       定时任务（心跳、落盘）走 {@link #scheduler()}；</li>
 *   <li><b>长驻阻塞任务</b>（阻塞 receive 的通道接收循环、TCP 隧道泵、无限重试的清理/退出流程）
 *       必须走 {@link #startBlocking}，<b>不得</b>占用 {@link #io()}；</li>
 *   <li>严禁在 MC 主线程或 Netty 事件循环里做阻塞操作。</li>
 * </ul>
 *
 * <h2>为什么 io() 必须是有界的（历史缺陷，勿改回 cached pool）</h2>
 * 2026-10-14 实测：服务端一次会话内 {@code newCachedThreadPool} 累计创建 921 条 {@code p2p-io-*} 线程，
 * 随后在 Netty 事件循环上抛 {@code OutOfMemoryError: unable to create native thread}，
 * 玩家控制连接被掐断、组被解散。成因是"逐区块帧 → 每帧一个 io 任务"的洪峰：
 * cached pool 在提交线程内联 {@code new Thread().start()}，无空闲线程就新建，
 * 而每条 io 线程还要 ~1 MB 栈 —— 线程数无上界就是 native OOM。
 * <p>
 * 现在改为<b>固定线程数 + 无界队列</b>：
 * <ul>
 *   <li>线程数有硬上界（{@link #IO_MAX_THREADS}）；</li>
 *   <li>不设拒绝策略：任务改为排队而不是丢失或抛异常 —— 上游有 pre-unload 屏障、
 *       释放重试等"必须最终送达"的语义，丢帧会变成无限重试风暴；</li>
 *   <li>队列里只放任务对象，远小于"每任务一条线程"的栈开销（同规模洪峰下
 *       约 1 MB/任务 → 数百字节/任务）。队列持续偏深时由看门狗打 WARN 暴露，
 *       不静默堆积。</li>
 * </ul>
 *
 * <h2>有界池下"池内任务等待池内任务"是唯一的死锁风险</h2>
 * 已核对并保持成立的等待闭包（改动 io 池容量或新增"任务内等待"前必须重新核对）：
 * <ul>
 *   <li>EXIT_BEGIN 任务 join 前一个 BEGIN 屏障 —— 前序任务 <b>FIFO 先出队先执行</b>，
 *       链头永不等待，故只要池内有 ≥1 条线程可用即可推进；</li>
 *   <li>claim/release/上行任务 join 自己客户端的 BEGIN 屏障 —— 屏障由 BEGIN 任务完成，
 *       而 BEGIN 任务只取会话条带锁（等待方在进入业务锁之前等待，见
 *       {@code ExitTransactionHandlers.writeRealtimeIfAllowed} 的调用位置约定）；</li>
 *   <li>磁盘等待（IOWorker、fsync）与网络等待都落在 <b>其他</b>执行器上，不消耗本池。</li>
 * </ul>
 * 因此容量按"下游串行点"而非"任务总数"选：fsync、批次 WAL 锁、区块分片锁都是串行的，
 * 线程再多也只排队。
 */
public final class ThreadPools {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/threads");

    /**
     * io 池线程数上界：IO 任务多在等待磁盘/网络，但下游有全局串行点（批次 WAL 锁 + fsync），
     * 超过一定数量只增线程不增吞吐。上限 32 亦是容器内 native 线程预算的保守值。
     */
    static final int IO_MAX_THREADS =
            Math.max(8, Math.min(32, Runtime.getRuntime().availableProcessors() * 2));

    /** 队列深度看门狗阈值：超过即 WARN（只观测不干预，供定位"谁在堆积"）。 */
    private static final int IO_QUEUE_WARN_DEPTH = 512;

    /** IO 线程池：固定线程数 + 无界队列（任务排队而非新建线程/拒绝）。 */
    private static final ThreadPoolExecutor IO = newIoPool();

    /** 计算线程池：按核数固定（至少 1），避免 CPU 密集任务互相挤占。 */
    private static final ExecutorService COMPUTE =
            Executors.newFixedThreadPool(
                    Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
                    namedFactory("p2p-compute"));

    /** 调度线程池：2 线程足够承载心跳/落盘等轻量定时任务。 */
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(2, namedFactory("p2p-sched"));

    /** 长驻阻塞线程登记表（诊断/测试用；线程退出时自动移除）。 */
    private static final Set<Thread> BLOCKING_THREADS = ConcurrentHashMap.newKeySet();

    /** 长驻阻塞线程命名序号（p2p-blocking-<名字>-<序号>）。 */
    private static final AtomicInteger BLOCKING_SEQ = new AtomicInteger();

    /**
     * EXIT_BEGIN 屏障任务专用执行器：与 io 池完全隔离，线程数上下界都受控。
     * <p>
     * 用途见 {@code ExitTransactionHandlers} 完整构造器的死锁说明：BEGIN 任务排在业务帧
     * 同一个池里时，等待该屏障的业务帧会把池内线程占满，BEGIN 任务永远排不上 → 永久死锁。
     * <p>
     * 为什么是"少数几条线程"而不是单线程、也不是"每帧一条专用线程"：
     * <ul>
     *   <li>单线程（曾短暂采用，复查定为 blocker）：BEGIN 任务体内有对断连清理等外部条件的
     *       无界等待，一条卡住的登记会冻结所有客户端的退出登记；</li>
     *   <li>每帧一条专用线程：EXIT_BEGIN 是网络帧触发的，等于把刚修掉的 native 线程耗尽
     *       风险重新引入。</li>
     * </ul>
     * 线程数固定为 {@value #EXIT_BARRIER_THREADS}，任务是 FIFO 排队（入队顺序与屏障发布顺序
     * 由 {@code ExitTransactionHandlers.beginSubmitLock} 保证一致）。
     */
    private static final int EXIT_BARRIER_THREADS = 4;

    private static final ExecutorService EXIT_BARRIER = new ThreadPoolExecutor(
            EXIT_BARRIER_THREADS, EXIT_BARRIER_THREADS, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), namedFactory("p2p-exit-begin"));

    /**
     * 主客户端接待入站 P2P 会话（accept + 读会话头）专用执行器：固定少数线程 + 队列。
     * <p>
     * 会话是<b>对端网络帧</b>驱动的，因此既不能用无界缓存池（对端可循环请求 → 线程无界），
     * 也不能放回共享 io 池（每条接待最长阻塞 15 秒，会占满池名额饿死区块上行）。
     * 排队超过容量的会话会依次处理，超时的会按 {@code HEADER_TIMEOUT_MILLIS} 正常关闭。
     */
    private static final int HOST_SESSION_THREADS = 4;

    private static final ExecutorService HOST_SESSION = new ThreadPoolExecutor(
            HOST_SESSION_THREADS, HOST_SESSION_THREADS, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), namedFactory("p2p-host-session"));


    static {
        // 队列深度看门狗：低频采样，偏深时告警（区分"线程不够"与"下游串行点堵住"）
        SCHEDULER.scheduleWithFixedDelay(() -> {
            int depth;
            try {
                depth = IO.getQueue().size();
            } catch (RuntimeException e) {
                return;
            }
            if (depth >= IO_QUEUE_WARN_DEPTH) {
                LOGGER.warn("io 池队列积压 {} 个任务（活跃线程 {} 条，上限 {}）——"
                                + "若持续增长，说明下游串行点（WAL fsync/区块分片锁）成为瓶颈，"
                                + "而不是线程不够",
                        depth, IO.getActiveCount(), IO_MAX_THREADS);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private ThreadPools() {
    }

    private static ThreadPoolExecutor newIoPool() {
        // core == max：线程数恒定有上界；keepAlive 0 且不允许核心线程超时 —— 线程常驻，
        // 避免洪峰后反复建销线程（建销本身也是 native 资源开销）。
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                IO_MAX_THREADS, IO_MAX_THREADS, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), namedFactory("p2p-io"));
        pool.allowCoreThreadTimeOut(false);
        return pool;
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

    /** 阻塞 IO 线程池（文件扫描、哈希、磁盘读写、HTTP 请求）。有界，长驻任务勿入。 */
    public static ExecutorService io() {
        return IO;
    }

    /**
     * 启动一条 <b>长驻阻塞</b> 专用守护线程。
     * <p>
     * 适用：阻塞 {@code receive} 的接收循环、TCP 隧道的两个方向泵、无限重试的断连清理/
     * 退出发布/退出事务流程 —— 这些任务的持续时间由对端决定（可能是整局游戏），
     * 放进有界 io 池会永久占住线程名额、饿死真正需要短任务的区块路径。
     * <p>
     * 线程名 {@code p2p-blocking-<name>-<序号>}，与 io 池在日志上一眼可分；
     * 线程退出（正常返回或抛错）时自动从登记表移除。
     *
     * @param name 任务类别（如 {@code p2p-receive}、{@code tunnel-pump}、{@code disconnect-cleanup}）
     * @param body 任务体；<b>必须</b>自行保证在关闭/中断时能退出（不得无限自旋）
     * @return 已启动的线程
     */
    public static Thread startBlocking(String name, Runnable body) {
        if (body == null) {
            throw new IllegalArgumentException("body 不得为 null");
        }
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } finally {
                BLOCKING_THREADS.remove(Thread.currentThread());
            }
        }, "p2p-blocking-" + name + "-" + BLOCKING_SEQ.incrementAndGet());
        thread.setDaemon(true);
        BLOCKING_THREADS.add(thread);
        thread.start();
        return thread;
    }

    /** CPU 密集线程池（算力跑分、序列化压缩）。 */
    public static ExecutorService compute() {
        return COMPUTE;
    }

    /**
     * EXIT_BEGIN 屏障任务专用执行器（与 io 池隔离）。
     * <b>不要</b>把普通业务帧丢进来：容量只为退出登记准备。
     */
    public static ExecutorService exitBarrierExecutor() {
        return EXIT_BARRIER;
    }

    /** 主客户端接待入站 P2P 会话专用执行器（accept 与读会话头，与 io 池隔离）。 */
    public static ExecutorService hostSessionExecutor() {
        return HOST_SESSION;
    }

    /** 定时调度线程池（心跳、周期落盘、超时检查）。注意任务体要短，重活丢给 io()/compute()。 */
    public static ScheduledExecutorService scheduler() {
        return SCHEDULER;
    }

    // ------------------------------------------------------------ 诊断/测试观测

    /** 当前存活的 io 池线程数（压测断言"线程数有上界"用）。 */
    public static int ioPoolThreadCount() {
        return IO.getPoolSize();
    }

    /** io 池线程数上界。 */
    public static int ioMaxThreads() {
        return IO_MAX_THREADS;
    }

    /** io 池当前排队任务数。 */
    public static int ioQueueDepth() {
        return IO.getQueue().size();
    }

    /** 当前存活的长驻阻塞线程数（诊断用）。 */
    public static int blockingThreadCount() {
        return BLOCKING_THREADS.size();
    }

    /** 关闭所有线程池（进程退出钩子里调用；守护线程本身不阻塞退出，此处属礼貌收尾）。 */
    public static void shutdownAll() {
        IO.shutdown();
        COMPUTE.shutdown();
        SCHEDULER.shutdown();
        EXIT_BARRIER.shutdown();
        HOST_SESSION.shutdown();
        try {
            //noinspection ResultOfMethodCallIgnored
            IO.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
