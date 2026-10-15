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

        // 玩家加入被接管的组世界（房主本地进入 / 副客户端经隧道进入）：按服务端下发的
        // ops 名单落权 —— 名单可能在玩家进服之前就到了（角色指派阶段），JOIN 是补落点
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            if (GroupRuntime.isManagedServer(server)) {
                LocalPermissions.applyScheduled(server);
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

        ServerTickEvents.END_SERVER_TICK.register(GroupServerHooks::reportPositions);

        LOGGER.info("组客户端事件钩子已注册");
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
                playerUploads.offer(player.getUUID(),
                        player.saveWithoutId(new CompoundTag()));
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
