package imsng.player_to_player.group;

import imsng.player_to_player.netproto.ControlConnection;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * 组运行时（Phase 2）：主客户端"集成服务端接管"的静态桥梁。
 * <p>
 * 主客户端在本机启动一个集成服务端来运算世界主线程（规范"主客户端……作为它这个
 * 组客户端的服务端"）。参与方分散在两个源集：
 * <ul>
 *   <li><b>src/client</b>（编排方）：{@code LocalWorldLauncher} 在启动本地世界<b>前</b>
 *       调用 {@link #arm} 预置组计划；</li>
 *   <li><b>src/main</b>（执行方）：{@code GroupServerHooks} 在 SERVER_STARTED 事件里
 *       {@link #tryAttach}（集成服务端类在 common 侧可见，事件处理器不必放 client
 *       源集）；Mixin（区块申请门控/上行捕获）经 {@link #isManagedLevel} 判定生效范围。</li>
 * </ul>
 * "armed → active → detached" 生命周期：arm 后第一个启动的 MC 服务器被接管
 * （客户端 JVM 同时只可能有一个集成服务端），SERVER_STOPPING 时 {@link #detach}。
 * <p>
 * 所有字段 volatile：写方是渲染线程/服务器主线程，读方是区块工作线程、Netty
 * 事件循环、io 池等任意线程。Mixin 走的判定方法刻意保持"无锁一次 volatile 读"。
 */
public final class GroupRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/group-runtime");

    /**
     * 组计划：arm 时由客户端编排方提供。
     *
     * @param groupId         本组 ID（Phase 1/2 约定 == 主客户端 clientId）
     * @param serverConn      与物理服务端的控制连接（区块申请/上行走它）
     * @param claimRetrySeconds 区块申请被拒后的重试间隔（秒）
     * @param onServerStarted 集成服务端接管完成后的回调（LAN 发布、组宿主启动；
     *                        在服务器主线程上调用，不得阻塞）
     */
    public record GroupPlan(UUID groupId, ControlConnection serverConn, int claimRetrySeconds,
                            Consumer<MinecraftServer> onServerStarted) {
    }

    /** 已预置、等待集成服务端启动的组计划；null = 未预置。 */
    private static volatile GroupPlan armedPlan;

    /** 被接管的集成服务端；null = 未接管。 */
    private static volatile MinecraftServer managedServer;

    /** 区块申请客户端（attach 时创建，detach 时关停）。 */
    private static volatile ChunkClaimClient claimClient;

    /** 区块上行服务（attach 时创建，detach 时关停）。 */
    private static volatile ChunkUploadService uploadService;

    /** 与物理服务端的控制连接（attach 时取自组计划；位置上报等直接发送用）。 */
    private static volatile ControlConnection serverConn;

    /** 本组 ID（attach 时取自组计划）。 */
    private static volatile UUID groupId;

    /**
     * 集成服务端<b>完全停止</b>（SERVER_STOPPED）后的回调（客户端会话层挂接，
     * 用于延迟拆除世界会话 —— 停服过程中的 saveAll 仍要经控制连接上行区块）。
     */
    private static volatile Runnable stopListener;

    /**
     * 一次性抑制旗标（Phase 3 合并）：让出方 A 提交后要关掉自己的集成服务端并以
     * 副客户端身份重连新主 —— 这次 SERVER_STOPPED 不代表"玩家离开世界"，
     * 会话层的停止回调应跳过拆除（与物理服务端的控制连接必须继续存活）。
     */
    private static volatile boolean suppressNextStopTeardown;

    private GroupRuntime() {
    }

    // ------------------------------------------------------------ 生命周期

    /** 预置组计划（LocalWorldLauncher 在打开本地世界前调用）。 */
    public static void arm(GroupPlan plan) {
        armedPlan = plan;
        LOGGER.info("组运行时已预置: groupId={}", plan.groupId());
    }

    /** 撤销预置（启动流程失败回退时调用）。 */
    public static void disarm() {
        armedPlan = null;
    }

    /**
     * 尝试接管刚启动的服务器（GroupServerHooks 的 SERVER_STARTED 回调，服务器主线程）。
     *
     * @return 是否接管（未预置 / 已接管别的实例时返回 false）
     */
    public static boolean tryAttach(MinecraftServer server) {
        GroupPlan plan = armedPlan;
        if (plan == null || managedServer != null) {
            return false;
        }
        armedPlan = null;
        managedServer = server;
        claimClient = new ChunkClaimClient(plan.serverConn(), plan.groupId(), plan.claimRetrySeconds());
        uploadService = new ChunkUploadService(plan.serverConn(), plan.groupId());
        serverConn = plan.serverConn();
        groupId = plan.groupId();
        LOGGER.info("集成服务端已被组运行时接管: groupId={}", plan.groupId());
        try {
            plan.onServerStarted().accept(server);
        } catch (Exception e) {
            LOGGER.error("组启动回调异常（接管保持有效）", e);
        }
        return true;
    }

    /** 解除接管（SERVER_STOPPED；幂等，只认当前接管的实例）。 */
    public static void detach(MinecraftServer server) {
        if (managedServer != server) {
            return;
        }
        ChunkClaimClient claims = claimClient;
        ChunkUploadService uploads = uploadService;
        claimClient = null;
        uploadService = null;
        serverConn = null;
        groupId = null;
        managedServer = null;
        tickFrozen = false; // 冻结旗标不得跨接管残留
        if (claims != null) {
            claims.shutdown();
        }
        if (uploads != null) {
            uploads.shutdown();
        }
        LOGGER.info("组运行时已解除对集成服务端的接管");
        if (suppressNextStopTeardown) {
            // Phase 3 合并让出方：本次停止属于"主→副降级切换"而非离开世界，
            // 会话层的拆除回调跳过一次（旗标一次性消费）
            suppressNextStopTeardown = false;
            LOGGER.info("主客户端让出切换中，跳过本次会话拆除回调");
            return;
        }
        Runnable listener = stopListener;
        if (listener != null) {
            try {
                listener.run();
            } catch (Exception e) {
                LOGGER.error("集成服务端停止回调异常", e);
            }
        }
    }

    /** 置位"下一次停止不拆会话"旗标（合并让出方在关闭集成服务端前调用）。 */
    public static void suppressNextStopTeardown() {
        suppressNextStopTeardown = true;
    }

    // ------------------------------------------------------------ tick 冻结

    /**
     * 集成服务端世界 tick 冻结旗标（Phase 3 合并"原子切换"）：让出方 A 在发送
     * 尾部增量前置位 —— {@code ServerLevelMixin} 读到后挂起世界演算（保留区块
     * 服务与实体装载，与服务端 suspendWorldTick 同款语义），保证尾部增量就是
     * 最终态。冻结窗口 = 尾部序列化 + 传输 + ACK 往返，正常在数百毫秒内。
     */
    private static volatile boolean tickFrozen;

    /** 冻结/解冻被接管集成服务端的世界 tick（任意线程；Mixin 每 tick 一次 volatile 读）。 */
    public static void setTickFrozen(boolean frozen) {
        if (tickFrozen == frozen) {
            return; // 幂等去噪：失败/中止路径可能重复解冻
        }
        tickFrozen = frozen;
        LOGGER.info("集成服务端世界 tick {}", frozen ? "已冻结（合并切换窗口）" : "已解冻");
    }

    /** 世界 tick 是否处于冻结状态。 */
    public static boolean isTickFrozen() {
        return tickFrozen;
    }

    /** 挂接集成服务端完全停止后的回调（客户端会话层调用，进程内挂一次即可）。 */
    public static void setStopListener(Runnable listener) {
        stopListener = listener;
    }

    // ------------------------------------------------------------ 判定与访问

    /** 是否已预置或已接管（MinecraftServerMixin 跳过出生点区块的判据）。 */
    public static boolean isArmedOrActive() {
        return armedPlan != null || managedServer != null;
    }

    /** 该服务器是否为被接管的集成服务端。 */
    public static boolean isManagedServer(MinecraftServer server) {
        MinecraftServer managed = managedServer;
        return managed != null && managed == server;
    }

    /** 该维度是否属于被接管的集成服务端（Mixin 热路径：一次 volatile 读 + 引用比较）。 */
    public static boolean isManagedLevel(ServerLevel level) {
        MinecraftServer managed = managedServer;
        return managed != null && level != null && level.getServer() == managed;
    }

    /** 被接管的集成服务端实例；未接管为 null（预同步/分离监视等 Phase 3 组件用）。 */
    public static MinecraftServer server() {
        return managedServer;
    }

    /** 区块申请客户端；未接管时为 null（调用方须判空）。 */
    public static ChunkClaimClient claims() {
        return claimClient;
    }

    /** 区块上行服务；未接管时为 null（调用方须判空）。 */
    public static ChunkUploadService uploads() {
        return uploadService;
    }

    /** 与物理服务端的控制连接；未接管时为 null（调用方须判空）。 */
    public static ControlConnection conn() {
        return serverConn;
    }

    /** 本组 ID；未接管时为 null。 */
    public static UUID activeGroupId() {
        return groupId;
    }
}
