package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.ThreadPools;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerChunkEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
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
 *   <li><b>CHUNK_UNLOAD</b>：规范"玩家卸载区块，服务器将该区块占用解除，并将最后的
 *       区块数据由主客户端发给服务端"——在事件线程（服务器主线程）序列化区块，
 *       压缩与发送转 io 池，随 CHUNK_RELEASE 上行最终数据。<b>先行序列化</b>而非
 *       依赖卸载存盘的上行捕获：Fabric 事件与 save 的先后顺序是实现细节，
 *       自带数据可保证"释放帧里就是最终态"，与顺序无关；</li>
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
                return; // 门控窗口期内加载的区块（理论不存在），防御
            }
            // 序列化必须在服务器主线程（事件线程即是）：区块此刻仍完整可读；
            // NBT 一经生成即为不可变快照，压缩 + 发送转 io 池
            try {
                CompoundTag tag = ChunkSerializer.write(level, chunk);
                ThreadPools.io().execute(() ->
                        claims.releaseWithData(key, ChunkUploadService.compress(tag)));
            } catch (Exception e) {
                // 序列化失败：仍要释放占用（不带数据），否则区块被本组永久锁死
                LOGGER.error("卸载区块序列化失败，改为无数据释放: {}", key.asString(), e);
                claims.releaseWithData(key, null);
            }
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
            JsonObject json = new JsonObject();
            json.addProperty("uuid", player.getUUID().toString());
            json.addProperty("dimension",
                    player.serverLevel().dimension().location().toString());
            json.addProperty("x", player.getX());
            json.addProperty("y", player.getY());
            json.addProperty("z", player.getZ());
            conn.send(ControlMessage.of(MessageType.PLAYER_POS_UPDATE, json));
        }
    }
}
