package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 分离监视器（Phase 3，主客户端集成服务端侧；规范"单端分离"）。
 * <p>
 * 规范判据：副客户端的渲染区块与同组其他客户端的"渲染区块的周围 4 个区块的
 * 周围 4 个区块"无交集且持续 {@value #SPLIT_HOLD_MILLIS} ms。四邻域膨胀两次 =
 * 曼哈顿距离 ≤ 2 的膨胀，因此"无交集"等价于两渲染矩形的<b>曼哈顿间隙 &gt; 2</b>
 * （间隙 = X 轴区间距 + Z 轴区间距），矩形运算 O(1)，每秒一轮全组两两比较开销可忽略。
 * <p>
 * 达标后的分离动作（io 线程）：
 * <ol>
 *   <li>上传离组玩家的最终数据（PLAYER_DATA_UPLOAD —— 其新集成服务端经
 *       PLAYER_DATA_REQUEST 取回，位置/背包连续）；</li>
 *   <li>SPLIT_REQUEST（携带离组者渲染区内本组占用的区块）→ 服务端组表分离 +
 *       注册表定向迁移，并向离组客户端推送 ROLE_ASSIGN(primary)；</li>
 *   <li>granted → 本地 {@link ChunkClaimClient#forgetLocal 摘除}已迁移区块 +
 *       {@link MergeTriggers#suppressFor 抑制}合并触发（防分离↔合并震荡）。</li>
 * </ol>
 * <b>Phase 3 简化（对规范"这 10s 期间副客户端进行预同步"）</b>：离组者的区块
 * 数据不走 P2P 预同步而由服务端供给 —— 主客户端的实时存盘上行保证服务端
 * 存档始终新鲜，离组者申请区块时从服务端拉取即可，数据一致性等价；代价是
 * 离组者要经历一次本地世界加载（数秒），无缝接管留待影子实例成熟后收口。
 * <p>
 * 线程模型：{@link #tick} 在集成服务端主线程（GroupServerHooks 每秒驱动一次），
 * 只做矩形运算与计时；序列化在主线程（微秒级），上传/请求转 io 池。
 */
public final class SplitMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/split");

    /** 无交集须持续的时长（规范：10s）。 */
    private static final long SPLIT_HOLD_MILLIS = 10_000;

    /** 膨胀边距（"周围4格的周围4格" = 曼哈顿距离 2）。 */
    private static final int MARGIN_CHUNKS = 2;

    /** 分离执行后的合并触发抑制窗口（毫秒）。 */
    private static final long MERGE_SUPPRESS_MILLIS = 30_000;

    /** 候选计时：玩家 UUID → 首次检测到无交集的时刻（仅主线程读写）。 */
    private static final Map<UUID, Long> candidates = new HashMap<>();

    /** 分离请求已在途的玩家（防重复发送；主线程读写）。 */
    private static final Map<UUID, Boolean> inFlight = new HashMap<>();

    private SplitMonitor() {
    }

    /** 世界会话拆除时清空计时状态（幂等）。 */
    public static void reset() {
        candidates.clear();
        inFlight.clear();
    }

    /**
     * 每秒一轮的分离检测（集成服务端主线程，GroupServerHooks 驱动；
     * 调用方已确认 server 是被接管的集成服务端）。
     */
    public static void tick(MinecraftServer server) {
        ChunkClaimClient claims = GroupRuntime.claims();
        ControlConnection conn = GroupRuntime.conn();
        UUID groupId = GroupRuntime.activeGroupId();
        if (claims == null || conn == null || !conn.isOpen() || groupId == null) {
            return;
        }
        List<ServerPlayer> players = server.getPlayerList().getPlayers();
        if (players.size() < 2) {
            candidates.clear(); // 只剩主客户端本人，无从分离
            return;
        }
        UUID hostId = NodeContext.get().clientId(); // 主客户端玩家（不参与离组）
        int viewDistance = server.getPlayerList().getViewDistance();
        long now = System.currentTimeMillis();

        for (ServerPlayer player : players) {
            UUID id = player.getUUID();
            if (id.equals(hostId) || Boolean.TRUE.equals(inFlight.get(id))) {
                continue;
            }
            if (isIsolated(player, players, viewDistance)) {
                Long since = candidates.putIfAbsent(id, now);
                if (since != null && now - since >= SPLIT_HOLD_MILLIS) {
                    candidates.remove(id);
                    inFlight.put(id, true);
                    initiateSplit(server, conn, claims, groupId, player, viewDistance);
                }
            } else {
                candidates.remove(id); // 交集恢复：计时归零（规范要求"持续"）
            }
        }
        // 已下线玩家的残留计时清理
        candidates.keySet().removeIf(id -> players.stream().noneMatch(p -> p.getUUID().equals(id)));
    }

    // ------------------------------------------------------------ 判定

    /** 该玩家的渲染矩形与组内其他所有玩家的渲染矩形（膨胀 2）是否全部无交集。 */
    private static boolean isIsolated(ServerPlayer player, List<ServerPlayer> all, int viewDistance) {
        for (ServerPlayer other : all) {
            if (other == player) {
                continue;
            }
            // 跨维度不算交集（规范语义：渲染区块是维度内概念）
            if (other.serverLevel() != player.serverLevel()) {
                continue;
            }
            if (manhattanGap(player, other, viewDistance) <= MARGIN_CHUNKS) {
                return false;
            }
        }
        return true;
    }

    /** 两玩家渲染矩形（边长 2*vd+1）的曼哈顿间隙（区块数；重叠为 0）。 */
    private static int manhattanGap(ServerPlayer a, ServerPlayer b, int viewDistance) {
        int size = viewDistance; // 半边长
        int ax = a.chunkPosition().x;
        int az = a.chunkPosition().z;
        int bx = b.chunkPosition().x;
        int bz = b.chunkPosition().z;
        int gapX = Math.max(0, Math.abs(ax - bx) - 2 * size);
        int gapZ = Math.max(0, Math.abs(az - bz) - 2 * size);
        return gapX + gapZ;
    }

    // ------------------------------------------------------------ 执行

    /** 触发一次单端分离（主线程：序列化玩家 NBT + 收集区块；网络转 io）。 */
    private static void initiateSplit(MinecraftServer server, ControlConnection conn,
                                      ChunkClaimClient claims, UUID groupId,
                                      ServerPlayer player, int viewDistance) {
        UUID departing = player.getUUID();
        LOGGER.info("单端分离触发: 玩家 {} 渲染区域与组 {} 其他成员无交集已持续 {}s",
                departing, groupId, SPLIT_HOLD_MILLIS / 1000);

        // 主线程序列化玩家最终数据（NBT 生成后不可变，跨线程只读安全）
        CompoundTag playerTag = player.saveWithoutId(new CompoundTag());

        // 收集离组者渲染区内本组占用的区块（迁移清单）
        String dimension = player.serverLevel().dimension().location().toString();
        int cx = player.chunkPosition().x;
        int cz = player.chunkPosition().z;
        List<ChunkKey> migrating = new ArrayList<>();
        for (int x = cx - viewDistance; x <= cx + viewDistance; x++) {
            for (int z = cz - viewDistance; z <= cz + viewDistance; z++) {
                ChunkKey key = new ChunkKey(dimension, x, z);
                if (claims.isClaimed(key)) {
                    migrating.add(key);
                }
            }
        }

        ThreadPools.io().execute(() -> {
            try {
                // 1. 玩家数据上行（离组者的新集成服务端从服务端取回）
                byte[] gzip = ChunkUploadService.compress(playerTag);
                if (gzip != null) {
                    JsonObject up = new JsonObject();
                    up.addProperty("playerUuid", departing.toString());
                    up.addProperty("playerName", player.getGameProfile().getName());
                    conn.send(ControlMessage.of(MessageType.PLAYER_DATA_UPLOAD, up, gzip));
                }
                // 2. 分离申请
                JsonObject req = new JsonObject();
                req.addProperty("groupId", groupId.toString());
                req.addProperty("departingClientId", departing.toString());
                JsonArray chunks = new JsonArray();
                for (ChunkKey key : migrating) {
                    chunks.add(key.asString());
                }
                req.add("chunks", chunks);
                ControlMessage ack = conn.request(
                                ControlMessage.of(MessageType.SPLIT_REQUEST, req))
                        .get(30, java.util.concurrent.TimeUnit.SECONDS);
                boolean granted = ack.type() == MessageType.SPLIT_ACK
                        && JsonUtil.getBoolean(ack.json(), "granted", false);
                server.execute(() -> inFlight.remove(departing));
                if (!granted) {
                    LOGGER.warn("分离申请被拒: 玩家 {}（保持现组）", departing);
                    return;
                }
                // 3. 本地摘除已迁移区块 + 抑制合并触发（防震荡）。
                //    离组玩家的隧道会随其客户端切换本地世界自然断开。
                for (ChunkKey key : migrating) {
                    claims.forgetLocal(key);
                }
                MergeTriggers.suppressFor(MERGE_SUPPRESS_MILLIS);
                LOGGER.info("单端分离完成: 玩家 {} 自立新组（迁移 {} 个区块）",
                        departing, JsonUtil.getInt(ack.json(), "migratedChunks", 0));
            } catch (Exception e) {
                server.execute(() -> inFlight.remove(departing));
                LOGGER.warn("分离流程失败（下轮检测重试）: 玩家 {}", departing, e);
            }
        });
    }
}
