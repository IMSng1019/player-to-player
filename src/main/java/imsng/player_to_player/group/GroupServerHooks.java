package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerChunkEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.chunk.storage.ChunkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * 组客户端的集成服务端事件钩子（Phase 2，src/main —— 集成服务端类在 common 侧
 * 可见，事件处理器不必放 client 源集；{@code Player_to_player.onInitialize} 注册）。
 * <p>
 * 所有回调先做一次廉价判定（{@link GroupRuntime} 的 volatile 读），未接管场景
 * （专用服务端、单人游戏、未走 P2P 流程的集成服务端）零行为差异：
 * <ul>
 *   <li><b>SERVER_STARTED</b>：客户端节点且组计划已预置 → {@link GroupRuntime#tryAttach}
 *       接管刚启动的集成服务端；</li>
 *   <li><b>SERVER_STOPPING</b>：解除接管（申请客户端/上行服务关停）；</li>
 *   <li><b>CHUNK_UNLOAD</b>：正常路径在 ChunkMap.scheduleUnload 的 save dependency
 *       中已经取得服务端释放 ACK，因此事件只做防御性校验；若仍持有 claim，说明屏障未
 *       覆盖到该卸载，立即使用同一可靠状态机补交并记录错误，不再 fire-and-forget；</li>
 *   <li><b>END_SERVER_TICK</b>：每 {@value #POS_REPORT_INTERVAL_TICKS} tick 把组内
 *       全部玩家（主 + 副）的位置经 PLAYER_POS_UPDATE 上报玩家表（规范"服务端
 *       保存所有玩家的坐标信息"）。</li>
 * </ul>
 */
public final class GroupServerHooks {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/group-hooks");

    /** 位置上报间隔（tick）：1 秒一次，注册表侧玩家表对时效要求不高。 */
    private static final int POS_REPORT_INTERVAL_TICKS = 20;

    /** END_SERVER_TICK 计数器（仅服务器主线程读写）。 */
    private static int tickCounter;

    private GroupServerHooks() {
    }

    /** 注册全部钩子（模组初始化时调用一次；回调自身按接管状态门控）。 */
    public static void register() {
        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            // 只有客户端节点可能预置组计划；tryAttach 对未预置场景是廉价 no-op
            if (NodeContext.get().isClient()) {
                GroupRuntime.tryAttach(server);
            }
        });

        // 解除接管挂在 SERVER_STOPPED（而非 STOPPING）：stopServer 过程中的
        // saveAllChunks 仍会经上行捕获 Mixin 入队发送 —— 过早解除会让停服前的
        // 最终区块状态丢失上行；完全停止后再解除并通知会话层做延迟拆除
        ServerLifecycleEvents.SERVER_STOPPED.register(server -> {
            SplitMonitor.reset(); // 分离计时状态不得跨世界残留
            PearlHandoff.reset(); // 珍珠探测/交接在途状态同理（Phase 4）
            PortalPreloader.reset(); // 传送门预检限速表同理（Phase 4）
            GroupRuntime.detach(server);
        });

        ServerLifecycleEvents.SERVER_STOPPING.register(GroupRuntime::captureExitStopping);

        // 玩家加入被接管的组世界（房主本地进入 / 副客户端经隧道进入）：① 按服务端下发的
        // ops 名单落权（名单常在角色指派阶段就到了，JOIN 是补落点）；② 补投切换窗口
        // （合并让出/退出事务）内暂存的指令回执 —— 那时集成服已不可用，回执投不出去；
        // ③ 记录真实登录锚点（P0-4）：此刻 playerdata 已读取，但玩家尚未登记进PlayerList，
        //    坐标就是权威位置，用于回归验收"不再从共享出生点开始申请"。
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            if (GroupRuntime.isManagedServer(server)) {
                // JOIN 尚未把玩家放入 PlayerList；直接给事件对象落权，并登记下一 tick 核验。
                LocalPermissions.applyJoiningPlayer(server, handler.player);
                CommandRelayClient.flushPendingFeedback(handler.player);
                logLoginAnchor(handler.player);
            }
        });

        // 登录在 JOIN 前失败/断线：结束窗口，避免看门狗把后续正常加载当成登录同步等待
        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            if (GroupRuntime.isManagedServer(server) && handler.player != null) {
                // Fabric DISCONNECT持有尚可序列化的玩家实体；先保存离组最终态，再清实例权限。
                SplitMonitor.onPlayerDisconnect(server, handler.player);
                LocalPermissions.onPlayerDisconnect(handler.player);
                LoginLoadGuard.endLogin(handler.player.getUUID(), "DISCONNECT");
            }
        });

        ServerChunkEvents.CHUNK_UNLOAD.register((level, chunk) -> {
            if (!GroupRuntime.isManagedLevel(level)) {
                return;
            }
            ChunkClaimClient claims = GroupRuntime.claims();
            if (claims == null) {
                return;
            }
            ChunkKey key = new ChunkKey(level.dimension().location().toString(),
                    chunk.getPos().x, chunk.getPos().z);
            if (!claims.isClaimed(key)) {
                return; // 正常路径：预卸载屏障已收到正 ACK 并清理本地 claim
            }
            if (GroupRuntime.isExitDraining()) {
                try {
                    GroupRuntime.recordExitChunk(key, ChunkSerializer.write(level, chunk));
                } catch (Exception error) {
                    LOGGER.error("退出冻结后的卸载区块快照捕获失败: {}", key.asString(), error);
                }
                return;
            }
            ChunkPreUnloadService preUnloads = GroupRuntime.preUnloads();
            if (preUnloads == null) {
                LOGGER.error("区块已触发本地卸载但预卸载服务不存在，保留服务端 claim: {}",
                        key.asString());
                return;
            }
            LOGGER.error("预卸载屏障未覆盖到 CHUNK_UNLOAD，启动防御性可靠提交: {}",
                    key.asString());
            preUnloads.begin(key, chunk.isUnsaved(),
                    () -> GroupRuntime.captureChunkSnapshot(level, chunk));
        });

        ServerTickEvents.END_SERVER_TICK.register(server -> {
            LocalPermissions.onServerTick(server);
            reportPositions(server);
        });

        LOGGER.info("组客户端事件钩子已注册");
    }

    /**
     * JOIN 时记录真实登录锚点与首批区块申请（P0-4 验收证据）。
     * <p>
     * 此时 {@code placeNewPlayer} 已执行完 {@code load(playerdata)}，
     * 玩家坐标是权威位置；若日志里的 chunk 仍是世界共享出生点，
     * 说明登录锚点修复没有生效。{@code firstClaims} 由 {@link LoginLoadGuard}
     * 在构造器 → JOIN 窗口内收集（最多 20 个）。
     */
    private static void logLoginAnchor(ServerPlayer player) {
        LoginLoadGuard.LoginContext context = LoginLoadGuard.activeContext();
        List<ChunkKey> firstClaims = LoginLoadGuard.recentClaimsSnapshot();
        LOGGER.info("[player_to_player/login] 玩家加入: name={} uuid={} group={} "
                        + "dimension={} chunk=({},{}) pos=({},{},{}) firstClaims={}",
                player.getGameProfile().getName(), player.getUUID(),
                GroupRuntime.activeGroupId(),
                player.serverLevel().dimension().location(),
                player.chunkPosition().x, player.chunkPosition().z,
                player.getX(), player.getY(), player.getZ(), firstClaims);
        if (context == null) {
            LOGGER.warn("[player_to_player/login] JOIN 时登录窗口已不存在"
                    + "（playerdata 读取前没有构造 ServerPlayer？），firstClaims 可能不完整");
        }
        // P0-4：申请了玩家渲染矩形之外的区块 = 疑似启动期/共享出生点加载重新出现。
        // firstClaims 保留的是窗口内最早的 20 个（不是最后 20 个），因此出生点
        // 依赖圈若再次被申请，一定会出现在这里。
        String dimension = player.serverLevel().dimension().location().toString();
        int serverViewDistance = player.server.getPlayerList().getViewDistance();
        int requestedViewDistance = player.requestedViewDistance();
        // 玩家实际渲染半径 = min(客户端请求, 服务端允许)；请求值缺失（0）时退回服务端值
        int viewDistance = requestedViewDistance > 0
                ? Math.min(requestedViewDistance, serverViewDistance)
                : serverViewDistance;
        int chunkX = player.chunkPosition().x;
        int chunkZ = player.chunkPosition().z;
        List<ChunkKey> outside = new java.util.ArrayList<>();
        for (ChunkKey key : firstClaims) {
            if (LoginAnchorPolicy.isOutsideRenderArea(key.dimension(), dimension,
                    key.x(), key.z(), chunkX, chunkZ, viewDistance)) {
                outside.add(key);
            }
        }
        if (!outside.isEmpty()) {
            LOGGER.warn("[player_to_player/login] 登录期出现玩家视野外的区块申请"
                            + "（疑似启动期/共享出生点加载，检查 P0-1 是否被绕过）: "
                            + "玩家={} chunk=({},{}) 视距={} 视野外={}",
                    player.getGameProfile().getName(), chunkX, chunkZ,
                    viewDistance, outside);
        }
        LoginLoadGuard.endLogin(player.getUUID(), "JOIN");
    }

    /** 周期位置上报 + 分离检测（服务器主线程；纯 JSON 组装 + 异步发送，微秒级）。 */
    private static void reportPositions(MinecraftServer server) {
        if (!GroupRuntime.isManagedServer(server)) {
            return;
        }
        if (++tickCounter % POS_REPORT_INTERVAL_TICKS != 0) {
            return;
        }
        // 分离检测（Phase 3）：每秒一轮的渲染区域交集判定（矩形运算，微秒级）
        SplitMonitor.tick(server);
        ControlConnection conn = GroupRuntime.conn();
        if (conn == null || !conn.isOpen()) {
            return;
        }
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            PlayerStateUploadService playerUploads = GroupRuntime.playerUploads();
            if (playerUploads != null && !GroupRuntime.isExitDraining()) {
                UUID canonical = LocalPermissions.canonicalMemberId(
                        player.getUUID(), player.getGameProfile().getName());
                if (canonical != null) {
                    playerUploads.offer(canonical, player.saveWithoutId(new CompoundTag()));
                }
            }
            JsonObject json = new JsonObject();
            json.addProperty("uuid", player.getUUID().toString());
            // 玩家名一并上报：服务端据此把"上报身份"与本组成员 HELLO 声明的名字对齐
            //（集成服务端的玩家 UUID 在离线/开发环境可能与档案 UUID 不同，名字是稳健判据）
            json.addProperty("name", player.getGameProfile().getName());
            json.addProperty("dimension",
                    player.serverLevel().dimension().location().toString());
            json.addProperty("x", player.getX());
            json.addProperty("y", player.getY());
            json.addProperty("z", player.getZ());
            conn.send(ControlMessage.of(MessageType.PLAYER_POS_UPDATE, json));
        }
    }
}
