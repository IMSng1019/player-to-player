package imsng.player_to_player.group;

import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.core.registries.Registries;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.world.level.chunk.LevelChunk;
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

    /** 组内完整玩家 NBT 的实时确认上传器。 */
    private static volatile PlayerStateUploadService playerUploadService;

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

    /** 耐久退出固定清单；非 null 表示普通 teardown 已被退出事务接管。 */
    private static volatile ExitManifest exitManifest;

    /** 退出前/停服期间捕获的最终状态（值始终为独立 NBT 副本）。 */
    private static final Map<UUID, CompoundTag> exitPlayers =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final Map<imsng.player_to_player.registry.ChunkKey, CompoundTag> exitChunks =
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
        PlayerStateUploadService playerUploads = playerUploadService;
        ExitManifest finishingExit = exitManifest;
        Map<UUID, CompoundTag> finalPlayers = new HashMap<>();
        Map<imsng.player_to_player.registry.ChunkKey, CompoundTag> finalChunks =
                new HashMap<>();
        Map<UUID, byte[]> playerFallback = new HashMap<>();
        if (finishingExit != null) {
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
                CompoundTag tag = exitChunks.get(key);
                if (tag == null && uploads != null) {
                    tag = uploads.latestSnapshot(key);
                }
                if (tag != null) {
                    finalChunks.put(key, tag.copy());
                }
            }
        }
        claimClient = null;
        uploadService = null;
        playerUploadService = null;
        serverConn = null;
        groupId = null;
        managedServer = null;
        tickFrozen = false; // 冻结旗标不得跨接管残留
        LogUploader.stop(); // Phase 4：日志上行随接管终止
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
            exitManifest = null;
            exitPlayers.clear();
            exitChunks.clear();
            Consumer<ExitBundle> listener = exitBundleListener;
            imsng.player_to_player.util.ThreadPools.io().execute(() -> {
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
                    for (UUID playerId : finishingExit.playerIds()) {
                        players.put(playerId, PlayerStateFiles.readFinalPlayerData(
                                playerDataDir, playerId, fallbackPlayers.get(playerId)));
                    }
                    Map<imsng.player_to_player.registry.ChunkKey, byte[]> chunks =
                            new HashMap<>();
                    for (Map.Entry<imsng.player_to_player.registry.ChunkKey, CompoundTag> entry
                            : finalChunks.entrySet()) {
                        byte[] encoded = ChunkUploadService.compress(entry.getValue());
                        if (encoded != null) {
                            chunks.put(entry.getKey(), encoded);
                        }
                    }
                    Set<imsng.player_to_player.registry.ChunkKey> preserve =
                            new HashSet<>(finishingExit.chunks());
                    preserve.removeAll(chunks.keySet());
                    ExitBundle bundle = new ExitBundle(
                            finishingExit, players, chunks, preserve);
                    if (listener != null) {
                        listener.accept(bundle);
                    }
                } catch (Exception e) {
                    // 不回退普通关闭：bundle 构建失败时客户端保持退出屏障，便于保留现场。
                    LOGGER.error("构建主客户端最终退出数据包失败，关闭继续被阻止", e);
                }
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

    public static void setExitBundleListener(Consumer<ExitBundle> listener) {
        exitBundleListener = listener;
    }

    /**
     * 在原版断开本地玩家之前固定本地清单并捕获玩家状态。返回 future 的完成线程是
     * 集成服务端线程，调用方只能在后台等待。
     */
    public static CompletableFuture<ExitManifest> beginExitDrain(UUID shutdownId) {
        CompletableFuture<ExitManifest> future = new CompletableFuture<>();
        MinecraftServer server = managedServer;
        ChunkClaimClient claims = claimClient;
        UUID activeGroup = groupId;
        UUID clientId = NodeContext.get().clientId();
        if (server == null || claims == null || activeGroup == null || clientId == null) {
            future.completeExceptionally(new IllegalStateException("当前没有被接管的组世界"));
            return future;
        }
        synchronized (EXIT_LOCK) {
            if (exitManifest != null) {
                future.complete(exitManifest);
                return future;
            }
        }
        server.execute(() -> {
            try {
                Set<imsng.player_to_player.registry.ChunkKey> chunks = claims.freezeForExit();
                Set<UUID> players = new HashSet<>();
                for (ServerPlayer player : server.getPlayerList().getPlayers()) {
                    players.add(player.getUUID());
                    exitPlayers.put(player.getUUID(),
                            player.saveWithoutId(new CompoundTag()).copy());
                }
                players.add(clientId);
                ExitManifest manifest = new ExitManifest(
                        shutdownId, clientId, activeGroup, players, chunks);
                synchronized (EXIT_LOCK) {
                    if (exitManifest == null) {
                        exitManifest = manifest;
                    }
                    future.complete(exitManifest);
                }
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
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
        if (manifest != null && key != null && tag != null && manifest.chunks().contains(key)) {
            exitChunks.put(key, tag.copy());
        }
    }

    /** SERVER_STOPPING：在世界仍可读时补抓玩家和所有清单区块。 */
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
        for (imsng.player_to_player.registry.ChunkKey key : manifest.chunks()) {
            try {
                ResourceLocation location = ResourceLocation.tryParse(key.dimension());
                if (location == null) {
                    continue;
                }
                ServerLevel level = server.getLevel(
                        ResourceKey.create(Registries.DIMENSION, location));
                if (level == null) {
                    continue;
                }
                LevelChunk chunk = level.getChunkSource().getChunkNow(key.x(), key.z());
                if (chunk != null) {
                    recordExitChunk(key, ChunkSerializer.write(level, chunk));
                }
            } catch (Exception e) {
                LOGGER.warn("停服前捕获退出区块失败，稍后使用最近上行快照: {}",
                        key.asString(), e);
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
}
