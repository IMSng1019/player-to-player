package imsng.player_to_player.group;

import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.chunk.ChunkAccess;
import net.minecraft.world.level.chunk.storage.ChunkSerializer;
import net.minecraft.world.level.storage.LevelResource;
import net.minecraft.nbt.CompoundTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
     * @param localWorldIdentity 本地存档已对齐的世界身份哈希（空串 = 服务端 level.dat
     *                        无法给出身份）；非空即表示"本地存档与服务端骨架同源"，
     *                        区块加载才允许回退读本地磁盘（见 {@link #isLocalWorldIdentityVerified()}）
     * @param claimRetrySeconds 区块申请被拒后的重试间隔（秒）
     * @param onServerStarted 集成服务端接管完成后的回调（LAN 发布、组宿主启动；
     *                        在服务器主线程上调用，不得阻塞）
     */
    public record GroupPlan(UUID groupId, ControlConnection serverConn, String localWorldIdentity,
                            int claimRetrySeconds,
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

    /** 区块预卸载 ACK 屏障（attach 时创建，detach 时关停）。 */
    private static volatile ChunkPreUnloadService preUnloadService;

    /** 组内完整玩家 NBT 的实时确认上传器。 */
    private static volatile PlayerStateUploadService playerUploadService;

    /** 与物理服务端的控制连接（attach 时取自组计划；位置上报等直接发送用）。 */
    private static volatile ControlConnection serverConn;

    /** 本组 ID（attach 时取自组计划）。 */
    private static volatile UUID groupId;

    /**
     * 本地存档已对齐的世界身份哈希（arm 时由客户端编排方提供）；null = 未校验。
     * <p>
     * 只承载"是否已校验"这一事实，判定本身在 {@code LocalWorldLauncher}（client 源集）
     * 用 {@code world.WorldIdentity} 现算 —— mixin 在 main 源集，读不了会话层状态，
     * 故经本静态字段镜像。
     */
    private static volatile String localWorldIdentity;

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

    /** 耐久退出固定清单；非 null 表示普通 teardown 已被退出事务接管。 */
    private static volatile ExitManifest exitManifest;

    /** 正在等待预卸载 quiescent 的共享退出准备 future，防止重复点击创建并行冻结流程。 */
    private static volatile CompletableFuture<ExitManifest> exitPreparation;

    /** 退出前/停服期间捕获的最终状态（值始终为独立 NBT 副本）。 */
    private static final Map<UUID, CompoundTag> exitPlayers =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** 客户端会话层接收最终 bundle 的桥；只在客户端入口设置。 */
    private static volatile Consumer<ExitBundle> exitBundleListener;

    private static final Object EXIT_LOCK = new Object();

    private GroupRuntime() {
    }

    // ------------------------------------------------------------ 生命周期

    /** 预置组计划（LocalWorldLauncher 在打开本地世界前调用）。 */
    public static void arm(GroupPlan plan) {
        armedPlan = plan;
        // null 与空串一律按"未校验"处理：arm 是启动关键路径，不能因为缺字段就抛异常，
        // 更不能把 null 误当成"已校验"（见 isLocalWorldIdentityVerified 的严格判据）
        localWorldIdentity = plan.localWorldIdentity();
        String identity = plan.localWorldIdentity();
        LOGGER.info("组运行时已预置: groupId={} 本地存档世界身份={}",
                plan.groupId(), identity == null || identity.isEmpty()
                        ? "unknown（服务端骨架未提供身份）"
                        : imsng.player_to_player.world.WorldIdentity.shortId(identity));
    }

    /** 撤销预置（启动流程失败回退时调用）。 */
    public static void disarm() {
        armedPlan = null;
        localWorldIdentity = null;
        LocalPermissions.reset();
    }

    /**
     * 本地存档的世界身份是否已校验（或已据服务端骨架重建对齐）。
     * <p>
     * {@code ChunkMapMixin} 的区块加载门控用它决定"服务端说无数据"时能否回退
     * 到本地磁盘读：未校验时本地存档可能属于另一个世界，读到的是旧地形，
     * 而这些区块随后又会被上行覆盖服务端 MCA。
     *
     * @return true = 本地存档与服务端同源，允许本地磁盘回退
     */
    public static boolean isLocalWorldIdentityVerified() {
        String identity = localWorldIdentity;
        return identity != null && !identity.isEmpty();
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
        // 显式把实时 CHUNK_DATA_UPLOAD 正 ACK 接到本组 claim 客户端，建立“服务端已有
        // 数据”基线。不要在上传服务内部依赖静态全局查找，否则 ACK 与组生命周期竞态时
        // 无法测试也容易误更新已迁移区块。
        uploadService = new ChunkUploadService(
                plan.serverConn(), plan.groupId(), claimClient::markServerDataKnown);
        preUnloadService = new ChunkPreUnloadService(claimClient, uploadService);
        playerUploadService = new PlayerStateUploadService(
                plan.serverConn(), imsng.player_to_player.util.ThreadPools.io(),
                task -> imsng.player_to_player.util.ThreadPools.scheduler()
                        .schedule(task, 2, TimeUnit.SECONDS));
        serverConn = plan.serverConn();
        groupId = plan.groupId();
        // Phase 4：主客户端日志上行（规范"主客户端的日志需要发给服务端储存"）
        LogUploader.start(plan.serverConn(), NodeContext.get().playerName());
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
        ChunkPreUnloadService preUnloads = preUnloadService;
        PlayerStateUploadService playerUploads = playerUploadService;
        ExitManifest finishingExit = exitManifest;
        Map<UUID, CompoundTag> finalPlayers = new HashMap<>();
        Map<imsng.player_to_player.registry.ChunkKey, CompoundTag> finalChunks =
                new HashMap<>();
        Map<UUID, byte[]> playerFallback = new HashMap<>();
        if (finishingExit != null) {
            // SERVER_STOPPED 前的强制 save 可能再次捕获与实时 ACK 完全相同的 NBT。
            // 在构建最终 bundle 前做一次后台压缩摘要去重，避免把已 durable 的区块
            // 重新计入退出尾部。
            if (uploads != null) {
                uploads.compactForExit();
            }
            for (UUID playerId : finishingExit.playerIds()) {
                CompoundTag tag = exitPlayers.get(playerId);
                if (tag != null) {
                    finalPlayers.put(playerId, tag.copy());
                } else if (playerUploads != null) {
                    byte[] encoded = playerUploads.latestEncoded(playerId);
                    if (encoded != null) {
                        playerFallback.put(playerId, encoded);
                    }
                }
            }
            for (imsng.player_to_player.registry.ChunkKey key : finishingExit.chunks()) {
                CompoundTag tag = uploads != null
                        ? uploads.latestUnacknowledged(key) : null;
                if (tag != null) {
                    finalChunks.put(key, tag.copy());
                }
            }
        }
        claimClient = null;
        uploadService = null;
        preUnloadService = null;
        playerUploadService = null;
        serverConn = null;
        groupId = null;
        localWorldIdentity = null; // 身份结论不跨接管残留（下次 arm 重新携带）
        LocalPermissions.resetRuntime(); // 只结束当前实例的应用记录，会话名单供下一次角色切换复用
        managedServer = null;
        GroupControlTasks.discard(server); // 旧实例的迁移采集不得泄漏至下一次组世界。
        setTickFrozen(false); // 记录完整冻结时长（包含尾部后等待提交及原版停服）
        LoginLoadGuard.reset("集成服务端停止"); // 登录窗口/诊断不得跨世界残留
        LogUploader.stop(); // Phase 4：日志上行随接管终止
        if (preUnloads != null) {
            preUnloads.shutdown();
        }
        if (claims != null) {
            claims.shutdown();
        }
        if (uploads != null) {
            uploads.shutdown();
        }
        if (playerUploads != null) {
            playerUploads.shutdown();
        }
        LOGGER.info("组运行时已解除对集成服务端的接管");
        if (suppressNextStopTeardown) {
            // Phase 3 合并让出方：本次停止属于"主→副降级切换"而非离开世界，
            // 会话层的拆除回调跳过一次（旗标一次性消费）
            suppressNextStopTeardown = false;
            LOGGER.info("主客户端让出切换中，跳过本次会话拆除回调");
            return;
        }
        if (finishingExit != null) {
            Consumer<ExitBundle> listener = exitBundleListener;
            // 发布重试没有次数上界（直到服务端确认），属长驻阻塞任务：专用线程，
            // 不占 io 池 —— 退出期间 io 池还要跑区块批次上行
            imsng.player_to_player.util.ThreadPools.startBlocking("exit-publish", () -> {
                ExitBundlePublication.publishUntilAccepted(
                        () -> buildExitBundle(server, finishingExit,
                                playerFallback, finalPlayers, finalChunks),
                        bundle -> {
                            if (listener == null) {
                                throw new IllegalStateException("退出 bundle listener 未安装");
                            }
                            listener.accept(bundle);
                        },
                        e -> LOGGER.error(
                                "构建或发布主客户端最终退出数据包失败，1 秒后继续重试", e),
                        () -> sleepUninterruptibly(1, TimeUnit.SECONDS),
                        () -> {
                            synchronized (EXIT_LOCK) {
                                exitManifest = null;
                                exitPreparation = null;
                                exitPlayers.clear();
                            }
                        });
            });
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

    /**
     * 整组合并已由物理服提交后的旧写者退休。必须在等待成员排空/断开旧世界之前调用：
     * 区块所有权此刻已属于目标主客户端，旧申请重试、普通释放和实时上传均不得继续。
     * 保留 managedServer/claims 引用直到原版真正停止，使所有 Mixin 继续识别旧代次，
     * 拒绝新加载而非因“运行时为空”错误回落到原版无授权生成。
     */
    public static void retireAfterTransfer(UUID sourceGroup) {
        ChunkClaimClient claims = claimClient;
        ChunkUploadService uploads = uploadService;
        ChunkPreUnloadService preUnloads = preUnloadService;
        PlayerStateUploadService playerUploads = playerUploadService;
        if (managedServer == null || claims == null || !sourceGroup.equals(groupId)) {
            throw new IllegalStateException("合并提交与当前组运行时不匹配，拒绝退休其他实例");
        }
        suppressNextStopTeardown = true;
        if (uploads != null) uploads.shutdown();
        if (playerUploads != null) playerUploads.shutdown();
        // 先撤销本地占用，再放行预卸载屏障；迟到的 release 回调也会看到无权写入。
        claims.forgetAllLocal();
        if (preUnloads != null) preUnloads.retireAfterTransfer();
        LOGGER.info("合并提交后旧组运行时已退休: groupId={}，未决加载结束，等待原版停服", sourceGroup);
    }

    // ------------------------------------------------------------ tick 冻结

    /**
     * 集成服务端世界 tick 冻结旗标（Phase 3 合并"原子切换"）：让出方 A 在发送
     * 尾部增量前置位 —— {@code ServerLevelMixin} 读到后挂起世界演算（保留区块
     * 服务与实体装载，与服务端 suspendWorldTick 同款语义），保证尾部增量就是
     * 最终态。冻结窗口 = 尾部序列化 + 传输 + ACK 往返，正常在数百毫秒内。
     */
    private static volatile boolean tickFrozen;
    private static long freezeStartedNanos;

    /** 冻结/解冻被接管集成服务端的世界 tick（任意线程；Mixin 每 tick 一次 volatile 读）。 */
    public static synchronized void setTickFrozen(boolean frozen) {
        if (tickFrozen == frozen) {
            return; // 幂等去噪：失败/中止路径可能重复解冻
        }
        tickFrozen = frozen;
        if (frozen) freezeStartedNanos = System.nanoTime();
        else if (freezeStartedNanos != 0) {
            LOGGER.info("世界冻结结束: freeze.total_ms={}",
                    (System.nanoTime() - freezeStartedNanos) / 1_000_000);
            freezeStartedNanos = 0;
        }
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

    public static void setExitBundleListener(Consumer<ExitBundle> listener) {
        exitBundleListener = listener;
    }

    /**
     * 在原版断开本地玩家之前固定本地清单并捕获玩家状态。返回 future 的完成线程是
     * 集成服务端线程，调用方只能在后台等待。
     */
    public static CompletableFuture<ExitManifest> beginExitDrain(UUID shutdownId) {
        MinecraftServer server = managedServer;
        ChunkClaimClient claims = claimClient;
        ChunkUploadService uploads = uploadService;
        ChunkPreUnloadService preUnloads = preUnloadService;
        UUID activeGroup = groupId;
        UUID clientId = NodeContext.get().clientId();
        if (server == null || claims == null || uploads == null || preUnloads == null
                || activeGroup == null || clientId == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("当前没有完整接管的组世界"));
        }
        CompletableFuture<ExitManifest> future;
        synchronized (EXIT_LOCK) {
            if (exitManifest != null) {
                return CompletableFuture.completedFuture(exitManifest);
            }
            if (exitPreparation != null) {
                return exitPreparation;
            }
            future = new CompletableFuture<>();
            exitPreparation = future;
        }

        // 先停止新 claim；预卸载仍可在 preparation 阶段登记，并被 awaitQuiescent 原子纳入。
        claims.beginExitPreparation();
        preUnloads.beginExitPreparation();
        preUnloads.awaitQuiescent().whenComplete((ignored, preparationError) -> {
            if (preparationError != null) {
                failExitPreparation(future, preparationError);
                return;
            }
            try {
                server.execute(() -> {
                    try {
                        if (managedServer != server || claimClient != claims
                                || uploadService != uploads || preUnloadService != preUnloads) {
                            throw new IllegalStateException("退出准备期间组运行时已经切换");
                        }
                        preUnloads.freezeForExit();
                        Set<imsng.player_to_player.registry.ChunkKey> chunks =
                                claims.freezeForExit();
                        uploads.freezeForExit();
                        Set<UUID> players = new HashSet<>();
                        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
                            players.add(player.getUUID());
                            exitPlayers.put(player.getUUID(),
                                    player.saveWithoutId(new CompoundTag()).copy());
                        }
                        players.add(clientId);
                        ExitManifest candidate = new ExitManifest(
                                shutdownId, clientId, activeGroup, players, chunks);
                        ExitManifest resolved;
                        synchronized (EXIT_LOCK) {
                            if (exitManifest == null) {
                                exitManifest = candidate;
                            }
                            resolved = exitManifest;
                            if (exitPreparation == future) {
                                exitPreparation = null;
                            }
                        }
                        future.complete(resolved);
                    } catch (Throwable error) {
                        failExitPreparation(future, error);
                    }
                });
            } catch (RuntimeException schedulingError) {
                failExitPreparation(future, schedulingError);
            }
        });
        return future;
    }

    private static void failExitPreparation(
            CompletableFuture<ExitManifest> future, Throwable error) {
        synchronized (EXIT_LOCK) {
            if (exitPreparation == future) {
                exitPreparation = null;
            }
        }
        future.completeExceptionally(error);
    }

    /** EXIT_BEGIN 应答下发的服务端权威清单替换本地候选清单。 */
    public static void adoptExitManifest(ExitManifest canonical) {
        synchronized (EXIT_LOCK) {
            ExitManifest local = exitManifest;
            if (local == null || canonical == null
                    || !local.shutdownId().equals(canonical.shutdownId())
                    || !local.clientId().equals(canonical.clientId())
                    || !local.groupId().equals(canonical.groupId())) {
                throw new IllegalArgumentException("服务端退出清单与本地退出会话不匹配");
            }
            exitManifest = canonical;
        }
    }

    public static boolean isExitDraining() {
        return exitManifest != null;
    }

    /** CHUNK_UNLOAD 与停服扫描在服务器线程记录最终区块快照。 */
    public static void recordExitChunk(imsng.player_to_player.registry.ChunkKey key,
                                       CompoundTag tag) {
        ExitManifest manifest = exitManifest;
        ChunkUploadService uploads = uploadService;
        if (manifest != null && uploads != null && key != null && tag != null
                && manifest.chunks().contains(key)) {
            // 退出冻结后的 enqueue 只覆盖 latestUnacknowledged，不会再启动普通实时请求。
            uploads.enqueue(key, tag);
        }
    }

    /** SERVER_STOPPING：只补抓玩家；区块由停服 save 的 ChunkStorageMixin 增量捕获。 */
    public static void captureExitStopping(MinecraftServer server) {
        ExitManifest manifest = exitManifest;
        if (manifest == null || managedServer != server) {
            return;
        }
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            if (manifest.playerIds().contains(player.getUUID())) {
                exitPlayers.put(player.getUUID(),
                        player.saveWithoutId(new CompoundTag()).copy());
            }
        }
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

    /** 区块预卸载服务；未接管时为 null（Mixin/防御性卸载钩子使用）。 */
    public static ChunkPreUnloadService preUnloads() {
        return preUnloadService;
    }

    /**
     * 为 ChunkMap.scheduleUnload 建立预卸载依赖。
     * <p>
     * holder 的原始 chunkToSave 可能由世界生成线程完成，因此先等待它，再把真正的状态检查
     * 和 ChunkSerializer.write 调度回集成服务端主线程。返回 future 只有物理服正 ACK 后才完成。
     */
    public static CompletableFuture<Void> beginChunkPreUnload(
            ServerLevel level, CompletableFuture<ChunkAccess> originalChunkToSave) {
        ChunkPreUnloadService service = preUnloadService;
        MinecraftServer server = managedServer;
        if (service == null || server == null || level == null
                || level.getServer() != server || originalChunkToSave == null) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> dependency = new CompletableFuture<>();
        originalChunkToSave.whenComplete((chunk, error) -> {
            if (error != null) {
                dependency.completeExceptionally(error);
                return;
            }
            if (chunk == null) {
                dependency.complete(null);
                return;
            }
            Runnable install = () -> {
                if (managedServer != server || preUnloadService != service
                        || !isManagedLevel(level)) {
                    dependency.complete(null);
                    return;
                }
                imsng.player_to_player.registry.ChunkKey key =
                        new imsng.player_to_player.registry.ChunkKey(
                                level.dimension().location().toString(),
                                chunk.getPos().x, chunk.getPos().z);
                service.begin(key, chunk.isUnsaved(),
                        () -> captureChunkSnapshot(level, chunk))
                        .whenComplete((ignored, releaseError) -> {
                            if (releaseError != null) {
                                dependency.completeExceptionally(releaseError);
                            } else {
                                dependency.complete(null);
                            }
                        });
            };
            try {
                if (server.isSameThread()) {
                    install.run();
                } else {
                    // 此 dependency 会被停服 saveAll 的区块 managedBlock 等待，
                    // 因此不能投进该等待不消费的普通 MinecraftServer 队列。
                    GroupControlTasks.executor(server).execute(install);
                }
            } catch (RuntimeException schedulingError) {
                dependency.completeExceptionally(schedulingError);
            }
        });
        return dependency;
    }

    /**
     * 每次预卸载重试都重新回服务器主线程生成最终 NBT，绝不从 scheduler/io 线程直接读取
     * ChunkAccess。返回的 CompoundTag 是新对象，随后可安全交给后台 gzip。
     */
    static CompletableFuture<CompoundTag> captureChunkSnapshot(
            ServerLevel level, ChunkAccess chunk) {
        MinecraftServer server = managedServer;
        if (server == null || level == null || chunk == null
                || level.getServer() != server) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("集成服务端已解除接管，无法捕获区块快照"));
        }
        CompletableFuture<CompoundTag> future = new CompletableFuture<>();
        Runnable capture = () -> {
            try {
                if (!isManagedLevel(level)) {
                    throw new IllegalStateException("区块所属世界已不再受组运行时接管");
                }
                future.complete(ChunkSerializer.write(level, chunk));
            } catch (Throwable error) {
                future.completeExceptionally(error);
            }
        };
        try {
            if (server.isSameThread()) {
                capture.run();
            } else {
                GroupControlTasks.executor(server).execute(capture);
            }
        } catch (RuntimeException schedulingError) {
            future.completeExceptionally(schedulingError);
        }
        return future;
    }

    public static PlayerStateUploadService playerUploads() {
        return playerUploadService;
    }

    /** 与物理服务端的控制连接；未接管时为 null（调用方须判空）。 */
    public static ControlConnection conn() {
        return serverConn;
    }

    /** 本组 ID；未接管时为 null。 */
    public static UUID activeGroupId() {
        return groupId;
    }

    private static ExitBundle buildExitBundle(
            MinecraftServer server, ExitManifest manifest,
            Map<UUID, byte[]> playerFallback,
            Map<UUID, CompoundTag> finalPlayers,
            Map<imsng.player_to_player.registry.ChunkKey, CompoundTag> finalChunks) {
        try {
            Map<UUID, byte[]> fallbackPlayers = new HashMap<>(playerFallback);
            for (Map.Entry<UUID, CompoundTag> entry : finalPlayers.entrySet()) {
                fallbackPlayers.put(
                        entry.getKey(), PlayerStateNbt.encode(entry.getValue()));
            }
            // 原版已在 SERVER_STOPPED 前完成玩家保存。最终文件必须优先于
            // EXIT_BEGIN 时的内存快照，后者只用于文件缺失/不可读的兜底。
            Map<UUID, byte[]> players = new HashMap<>();
            java.nio.file.Path playerDataDir = server.getWorldPath(
                    LevelResource.PLAYER_DATA_DIR);
            for (UUID playerId : manifest.playerIds()) {
                players.put(playerId, PlayerStateFiles.readFinalPlayerData(
                        playerDataDir, playerId, fallbackPlayers.get(playerId)));
            }
            Map<imsng.player_to_player.registry.ChunkKey, byte[]> chunks =
                    new HashMap<>();
            for (Map.Entry<imsng.player_to_player.registry.ChunkKey, CompoundTag> entry
                    : finalChunks.entrySet()) {
                byte[] encoded = ChunkUploadService.compress(entry.getValue());
                if (encoded == null) {
                    throw new IllegalStateException("最终区块压缩失败: "
                            + entry.getKey().asString());
                }
                chunks.put(entry.getKey(), encoded);
            }
            return new ExitBundle(manifest, players, chunks);
        } catch (Exception e) {
            throw new IllegalStateException("构建最终退出 bundle 失败", e);
        }
    }

    private static void sleepUninterruptibly(long duration, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(duration);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(remaining);
            } catch (InterruptedException ignored) {
                // 用户要求最终 bundle 构建不可取消。
            }
        }
    }
}
