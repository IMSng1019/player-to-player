package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 登录期区块加载看门狗（Phase 2/P0，2026-10-18 登录锚点修复）。
 *
 * <p><b>为什么需要</b>：{@code ChunkMap.readChunk} 的加载门控在 claim 被其他组拒绝时
 * 会让 future 悬置，{@link ChunkClaimClient} 按间隔无限重试。异步区块加载等多久都
 * 没问题，但原版登录路径会在服务端主线程同步 {@code join()}（{@code ServerChunkCache.getChunk}
 * 与 {@code ChunkMap.isExistingChunkFull}），于是主线程被跨组 claim 无限期挂住：
 * 实测出现过 295 秒静默卡死、整台机器报 {@code Can't keep up}。
 *
 * <p><b>本类做什么</b>：登录窗口内、服务端主线程上发起的区块加载：
 * <ul>
 *   <li>5 秒未完成 → WARN（区块键、阻塞组、玩家/维度/锚点、调用栈），每 key 一次；</li>
 *   <li>60 秒未完成且登录窗口仍然有效 → 以 {@link LoginLoadTimeoutException}
 *       异常结束该 future。原版 {@code ChunkMap.isExistingChunkFull} 用
 *       {@code catch (Exception)} 收口，主线程得以继续。</li>
 * </ul>
 * <b>为什么超时异常不能用 {@link java.io.IOException}</b>：1.20.4
 * {@code ChunkMap.scheduleChunkLoad} 的 {@code exceptionallyAsync} 把 IOException 视为
 * “磁盘读失败”，记录 {@code Couldn't load chunk} 后返回一个<b>空 ProtoChunk</b>，
 * 后续生成管线会按种子生成并上行——这正是规范禁止的数据污染路径。改用非 IOException
 * 的 {@link LoginLoadTimeoutException} 后，原版走“标记失败并 rethrow”分支，
 * 不会制造空区块。
 *
 * <p>超时只结束本次加载链，{@link ChunkClaimClient} 的 claim 重试仍在后台继续；
 * 旧组释放后下一次加载即可成功。
 *
 * <p><b>登录窗口</b>：由 {@code ServerPlayerMixin} 在构造器 HEAD 开启、构造器 RETURN
 * 结束（仅覆盖 fudge 防御路径），再由 {@code PlayerListMixin} 在
 * {@code placeNewPlayer} HEAD 开启权威窗口，由 {@code GroupServerHooks} 的 JOIN 回调
 * 结束。窗口最长 {@value #LOGIN_WINDOW_MAX_MILLIS} ms 自动失效，防止异常路径永久开启。
 */
public final class LoginLoadGuard {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/login");

    /** 登录期区块加载多久未完成后打印 WARN。 */
    static final long DEFAULT_WARN_MILLIS = 5_000L;

    /** 登录期加载多久未完成且同一窗口仍有效时异常结束，保证主线程不再无限挂起。 */
    static final long DEFAULT_HARD_TIMEOUT_MILLIS = 60_000L;

    /** 登录窗口最长生命周期（覆盖构造器 → playerdata → addNewPlayer → JOIN）。 */
    static final long LOGIN_WINDOW_MAX_MILLIS = 120_000L;

    /** 登录日志里 firstClaims 的最大条数。 */
    static final int MAX_RECENT_CLAIMS = 20;

    /** 调用栈最多打印的帧数。 */
    private static final int MAX_STACK_FRAMES = 24;

    /**
     * 每个登录窗口最多捕获多少条调用栈。登录期可能发起数百次区块加载，
     * 每次 {@code Thread.getStackTrace()} 都是主线程上的分配开销；只需前几条
     * 用于定位调用来源，其余 WARN 省略栈即可。
     */
    private static final int MAX_STACK_CAPTURES = 8;

    /** 一次登录的上下文（构造器阶段只有 UUID/名字/初始维度与出生点锚点）。 */
    public record LoginContext(UUID playerId, String playerName, String dimension,
                               int anchorChunkX, int anchorChunkZ,
                               long startedAtMillis) {
    }

    /**
     * 登录期区块加载硬超时异常。
     *
     * <p><b>必须是 RuntimeException 且不能是 IOException</b>：1.20.4
     * {@code ChunkMap.scheduleChunkLoad} 的异常回退把 IOException 解释为“读盘失败”
     * 并生成空 ProtoChunk（随后按种子生成，可能上行覆盖其他组数据）；非 IOException
     * 的 CompletionException 会走“标记失败 + rethrow”，不会制造空区块。
     */
    public static final class LoginLoadTimeoutException extends RuntimeException {
        public LoginLoadTimeoutException(String message) {
            super(message);
        }
    }

    private static final Object CLAIM_LOCK = new Object();

    /** 当前登录上下文；null = 不在登录窗口。 */
    private static volatile LoginContext active;

    /** 本窗口内首批申请过的区块（保持插入顺序，最多 {@value #MAX_RECENT_CLAIMS} 个）。 */
    private static final Queue<ChunkKey> RECENT_CLAIMS = new ConcurrentLinkedQueue<>();
    private static final Set<ChunkKey> RECENT_CLAIM_SET = ConcurrentHashMap.newKeySet();

    /** 每个 key 只打印一次 5 秒 WARN，避免同一区块的多条加载链刷屏。 */
    private static final Set<ChunkKey> WARNED_KEYS = ConcurrentHashMap.newKeySet();

    /** 本窗口是否已经就"共享出生点区块被申请"告警过。 */
    private static final AtomicBoolean SPAWN_WARNED = new AtomicBoolean();

    /** 本窗口已捕获的调用栈数量（用于限制主线程栈捕获开销）。 */
    private static final AtomicInteger STACK_CAPTURES = new AtomicInteger();

    /** 生产路径用统一调度器；测试可注入。 */
    private static volatile ScheduledExecutorService scheduler = ThreadPools.scheduler();
    private static volatile long warnMillis = DEFAULT_WARN_MILLIS;
    private static volatile long hardTimeoutMillis = DEFAULT_HARD_TIMEOUT_MILLIS;

    private LoginLoadGuard() {
    }

    // ------------------------------------------------------------ 登录窗口

    /**
     * 开启登录窗口（{@code ServerPlayer} 构造器 HEAD；构造器早于 playerdata 读取）。
     *
     * @param anchorChunkX 构造器阶段的初始区块（共享出生点）——仅用于日志对照，
     *                     不代表玩家最终登录锚点
     */
    public static void beginLogin(UUID playerId, String playerName, String dimension,
                                  int anchorChunkX, int anchorChunkZ) {
        synchronized (CLAIM_LOCK) {
            RECENT_CLAIMS.clear();
            RECENT_CLAIM_SET.clear();
            WARNED_KEYS.clear();
        }
        SPAWN_WARNED.set(false);
        STACK_CAPTURES.set(0);
        active = new LoginContext(playerId, playerName, dimension,
                anchorChunkX, anchorChunkZ, System.currentTimeMillis());
        LOGGER.debug("登录窗口开启: 玩家={}({}) 维度={} 初始锚点区块=({},{})",
                playerName, playerId, dimension, anchorChunkX, anchorChunkZ);
    }

    /** 结束登录窗口（JOIN；只认当前玩家，重生/其他玩家的结束事件不会误清）。 */
    public static void endLogin(UUID playerId, String reason) {
        LoginContext context = active;
        if (context == null || playerId == null || !context.playerId().equals(playerId)) {
            return;
        }
        active = null;
        STACK_CAPTURES.set(0);
        synchronized (CLAIM_LOCK) {
            RECENT_CLAIMS.clear();
            RECENT_CLAIM_SET.clear();
            WARNED_KEYS.clear();
        }
        LOGGER.debug("登录窗口结束: 玩家={}({}) 原因={}",
                context.playerName(), context.playerId(), reason);
    }

    /** 集成服务端停止时清理，避免状态跨世界残留。 */
    public static void reset(String reason) {
        LoginContext context = active;
        active = null;
        SPAWN_WARNED.set(false);
        STACK_CAPTURES.set(0);
        synchronized (CLAIM_LOCK) {
            RECENT_CLAIMS.clear();
            RECENT_CLAIM_SET.clear();
            WARNED_KEYS.clear();
        }
        if (context != null) {
            LOGGER.debug("登录窗口因 {} 清理: 玩家={}({})",
                    reason, context.playerName(), context.playerId());
        }
    }

    /** 是否处于登录窗口（超过最长生命周期自动失效）。 */
    public static boolean isLoginPhase() {
        LoginContext context = active;
        if (context == null) {
            return false;
        }
        if (System.currentTimeMillis() - context.startedAtMillis() > LOGIN_WINDOW_MAX_MILLIS) {
            active = null;
            return false;
        }
        return true;
    }

    /** 当前登录上下文（JOIN 日志用；不做过期判定，JOIN 发生在窗口内）。 */
    public static LoginContext activeContext() {
        return active;
    }

    // ------------------------------------------------------------ 诊断记录

    /** 记录登录期发起的一次区块加载（firstClaims 诊断；最多保留前 20 个）。 */
    public static void recordClaim(ChunkKey key) {
        if (key == null || !isLoginPhase()) {
            return;
        }
        synchronized (CLAIM_LOCK) {
            if (RECENT_CLAIM_SET.size() >= MAX_RECENT_CLAIMS) {
                return;
            }
            if (RECENT_CLAIM_SET.add(key)) {
                RECENT_CLAIMS.add(key);
            }
        }
    }

    /** 登录期首批区块申请（防御性拷贝）。 */
    public static List<ChunkKey> recentClaimsSnapshot() {
        synchronized (CLAIM_LOCK) {
            return List.copyOf(RECENT_CLAIMS);
        }
    }

    /**
     * 登录期出现世界共享出生点区块的加载请求：一个窗口只告警一次，附调用栈。
     * 这是 P0-1（跳过 fudgeSpawnLocation）没有覆盖到的路径的回归证据。
     */
    public static void warnSpawnChunkClaim(ChunkKey key) {
        if (key == null || !isLoginPhase()) {
            return;
        }
        if (!SPAWN_WARNED.compareAndSet(false, true)) {
            return;
        }
        LoginContext context = active;
        LOGGER.warn("登录期仍在申请世界共享出生点区块（P0-1 未覆盖路径）: key={} {} 调用栈:{}",
                key.asString(), describe(context), captureStack());
    }

    // ------------------------------------------------------------ 看门狗

    /**
     * 武装登录期加载看门狗。
     *
     * @param key         区块键
     * @param future      组合后的加载 future（claim + 数据拉取/本地读）
     * @param diagnostics 告警/超时时追加的诊断（阻塞组等）；允许返回 null/抛错
     * @param hardTimeout true = 到点异常结束；false = 只告警（异步加载）
     * @return 原 future（超时通过 {@link CompletableFuture#completeExceptionally} 实现，
     *         调用方持有的引用必须看到同一失败）
     */
    public static <T> CompletableFuture<T> guard(ChunkKey key,
                                                 CompletableFuture<T> future,
                                                 Supplier<String> diagnostics,
                                                 boolean hardTimeout) {
        LoginContext context = active;
        if (key == null || future == null || context == null) {
            return future;
        }
        String stack = captureStackWithinBudget();
        String waitingThread = Thread.currentThread().getName();
        ScheduledExecutorService executor = scheduler;
        try {
            executor.schedule(
                    () -> warnIfStillPending(key, future, context, diagnostics, stack,
                            waitingThread),
                    warnMillis, TimeUnit.MILLISECONDS);
            if (hardTimeout) {
                executor.schedule(
                        () -> failIfStillPending(key, future, context, diagnostics, stack,
                                waitingThread),
                        hardTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (RejectedExecutionException rejected) {
            // 调度器关闭（进程/集成服退出）：退回原有行为，不因诊断设施崩溃而影响登录
            LOGGER.warn("登录期看门狗调度失败（调度器已关闭）: key={}", key.asString());
        }
        return future;
    }

    private static void warnIfStillPending(ChunkKey key, CompletableFuture<?> future,
                                           LoginContext context, Supplier<String> diagnostics,
                                           String stack, String waitingThread) {
        if (future.isDone() || !WARNED_KEYS.add(key)) {
            return;
        }
        LOGGER.warn("登录期同步区块加载已等待超过 {}ms 仍未完成: key={} {} 等待线程={} 调用栈:{}",
                warnMillis, key.asString(), describe(context), waitingThread, stack);
        LOGGER.warn("登录期区块加载诊断: key={} {}", key.asString(), diagnosticsText(diagnostics));
    }

    private static void failIfStillPending(ChunkKey key, CompletableFuture<?> future,
                                           LoginContext context, Supplier<String> diagnostics,
                                           String stack, String waitingThread) {
        // 只有"同一个登录窗口仍然有效"时才允许硬失败：
        // - JOIN 已结束窗口 → 服务端主线程不可能还阻塞在这次登录上，杀掉异步加载没有意义；
        // - 新登录/重生覆盖了上下文 → 旧窗口的定时器不得误杀新窗口的加载。
        if (future.isDone() || !isSameLoginActive(context)) {
            return;
        }
        String message = "登录期区块加载超时: key=" + key.asString()
                + " 等待=" + hardTimeoutMillis + "ms"
                + " " + describe(context)
                + " 等待线程=" + waitingThread
                + " " + diagnosticsText(diagnostics);
        LOGGER.warn("{}；异常结束本次区块加载（使用非 IOException，避免原版按读盘失败"
                + "生成空区块并转为种子生成）。调用栈:{}", message, stack);
        future.completeExceptionally(new LoginLoadTimeoutException(message));
    }

    /** 捕获的上下文是否仍是当前有效登录窗口（身份比较 + 过期判定）。 */
    private static boolean isSameLoginActive(LoginContext context) {
        return context != null
                && active == context
                && System.currentTimeMillis() - context.startedAtMillis()
                <= LOGIN_WINDOW_MAX_MILLIS;
    }

    private static String describe(LoginContext context) {
        if (context == null) {
            return "玩家=<无上下文>";
        }
        return "玩家=" + context.playerName() + "(" + context.playerId() + ")"
                + " 维度=" + context.dimension()
                + " 初始锚点区块=(" + context.anchorChunkX() + "," + context.anchorChunkZ() + ")";
    }

    private static String diagnosticsText(Supplier<String> diagnostics) {
        if (diagnostics == null) {
            return "诊断=无";
        }
        try {
            String text = diagnostics.get();
            return text == null || text.isEmpty() ? "诊断=无" : text;
        } catch (Throwable error) {
            return "诊断不可用: " + error;
        }
    }

    /** 在预算内捕获调用栈；超出预算时返回说明文本，避免主线程反复分配栈数组。 */
    private static String captureStackWithinBudget() {
        int index = STACK_CAPTURES.getAndIncrement();
        if (index >= MAX_STACK_CAPTURES) {
            return "\n\t（调用栈已省略：本登录窗口已捕获前 "
                    + MAX_STACK_CAPTURES + " 条）";
        }
        return captureStack();
    }

    /** 捕获当前（服务端主线程）调用栈，最多 {@value #MAX_STACK_FRAMES} 帧。 */
    private static String captureStack() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        StringBuilder builder = new StringBuilder();
        int appended = 0;
        for (StackTraceElement element : trace) {
            String className = element.getClassName();
            if (className.equals(LoginLoadGuard.class.getName())
                    || className.equals(Thread.class.getName())) {
                continue;
            }
            builder.append("\n\tat ").append(element);
            if (++appended >= MAX_STACK_FRAMES) {
                break;
            }
        }
        return builder.toString();
    }

    // ------------------------------------------------------------ 测试钩子（包可见）

    static void configureForTest(ScheduledExecutorService testScheduler,
                                 long testWarnMillis, long testTimeoutMillis) {
        scheduler = testScheduler;
        warnMillis = testWarnMillis;
        hardTimeoutMillis = testTimeoutMillis;
    }

    static void resetForTest() {
        active = null;
        SPAWN_WARNED.set(false);
        STACK_CAPTURES.set(0);
        synchronized (CLAIM_LOCK) {
            RECENT_CLAIMS.clear();
            RECENT_CLAIM_SET.clear();
            WARNED_KEYS.clear();
        }
        scheduler = ThreadPools.scheduler();
        warnMillis = DEFAULT_WARN_MILLIS;
        hardTimeoutMillis = DEFAULT_HARD_TIMEOUT_MILLIS;
    }
}
