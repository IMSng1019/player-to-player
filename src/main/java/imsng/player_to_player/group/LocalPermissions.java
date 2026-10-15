package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mojang.authlib.GameProfile;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.players.PlayerList;
import net.minecraft.server.players.ServerOpListEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地权限位落地（主客户端，{@link MessageType#PERMISSION_RELAY} 的消费面）。
 *
 * <h2>做什么</h2>
 * 物理服务端下发的"组成员 → ops 真实等级"名单在这里落到<b>本机集成服务端</b>的
 * 玩家名单上（{@code PlayerList.getOps()} 加条目 + {@code sendPlayerPermissionLevel}），
 * 从而恢复：
 * <ul>
 *   <li>F3+F4 游戏模式切换（客户端 {@code hasPermissions(2)} 闸门读的是本地权限位）；</li>
 *   <li>指令补全树（原版按权限过滤 {@code Commands.sendCommands} 下发）；</li>
 *   <li>未被拦截的执行路径（告示牌/命令方块/第三方程序化调用）按真实等级执行。</li>
 * </ul>
 * 等级取服务端核出的<b>原值</b>（不钳制）—— 这是 2026-10-15 的定稿口径；跨组广播执行
 * 的权限另有一道钳制（{@code CommandChatHandlers.FORWARD_PERMISSION_CAP}），两者
 * 作用面不同：本地权限位决定"这台客户端本地能看到/能做什么"。
 *
 * <h2>为什么是纯内存、按会话</h2>
 * 集成服务端从不读取 {@code ops.json}（{@code PlayerList} 的构造器不 load），
 * 所以这里加的条目<b>只活在本次集成服务端实例里</b>：进程退出、世界关闭即消失，
 * 没有持久化残留问题（{@code StoredUserList.add} 会顺手把文件写出去，但没有任何
 * 回读路径，属无害副作用）。条目随 {@code PlayerList} 实例销毁，故拆除时只需清
 * 本类的缓存状态。
 *
 * <h2>线程模型</h2>
 * {@code ServerOpList} 内部是非并发 Map，落权必须在集成服务端主线程；
 * {@link #applyScheduled} 负责"同线程直跑、异线程 execute"，与
 * {@code GroupRuntime.beginChunkPreUnload} 同一范式。名单缓存本身是并发的
 * （写入在 Netty 线程，读取在服务器主线程）。
 */
public final class LocalPermissions {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/permission");

    /** 一名成员的下发档（名字用于本地玩家列表按名兜底匹配）。 */
    private record Member(String name, int level) {
    }

    /** 名单所属组（与 {@link GroupRuntime#activeGroupId()} 不符时视为陈旧帧）。 */
    private static volatile UUID groupId;

    /** 玩家 UUID → 权限档（服务端下发的权威值）。 */
    private static volatile Map<UUID, Member> members = Map.of();

    /** 本会话已落地的等级（uuid → level），避免重复 add/send。 */
    private static final Map<UUID, Integer> applied = new ConcurrentHashMap<>();

    private LocalPermissions() {
    }

    /** 注册 PERMISSION_RELAY 入站处理（世界会话建立时与控制连接一起挂接）。 */
    public static void register(HandlerRegistry reg) {
        reg.on(MessageType.PERMISSION_RELAY, (conn, msg) -> update(msg));
    }

    /** 收到服务端名单：整表替换 + 尝试落地（Netty 线程）。 */
    private static void update(ControlMessage msg) {
        JsonObject json = msg.json();
        UUID group = parseUuid(JsonUtil.getString(json, "groupId", ""));
        JsonElement raw = json.get("members");
        if (group == null || raw == null || !raw.isJsonArray()) {
            return; // 入站数据不可信：缺字段直接忽略，保持旧名单
        }
        Map<UUID, Member> next = new ConcurrentHashMap<>();
        JsonArray array = raw.getAsJsonArray();
        for (JsonElement el : array) {
            if (!el.isJsonObject()) {
                continue;
            }
            JsonObject row = el.getAsJsonObject();
            UUID playerId = parseUuid(JsonUtil.getString(row, "uuid", ""));
            if (playerId == null) {
                continue;
            }
            int level = Math.max(0, Math.min(4, JsonUtil.getInt(row, "level", 0)));
            next.put(playerId, new Member(JsonUtil.getString(row, "name", "?"), level));
        }
        groupId = group;
        members = next;
        LOGGER.info("收到权限名单: 组 {} → {} 名成员", group, next.size());
        applyScheduled(GroupRuntime.server());
    }

    /** 会话拆除时清缓存（条目本身随集成服务端实例销毁，无需文件清理）。 */
    public static void reset() {
        groupId = null;
        members = Map.of();
        applied.clear();
    }

    /** 任意线程：把当前名单落到该集成服务端（不为本组/未接管时静默跳过）。 */
    public static void applyScheduled(MinecraftServer server) {
        if (server == null) {
            return;
        }
        try {
            if (server.isSameThread()) {
                applyNow(server);
            } else {
                server.execute(() -> applyNow(server));
            }
        } catch (RuntimeException e) {
            // 服务端正在停止：名单本来就会随实例销毁，无需补救
            LOGGER.debug("本地权限落权调度失败: {}", e.toString());
        }
    }

    /** 落权实现（必须在集成服务端主线程）。 */
    private static void applyNow(MinecraftServer server) {
        UUID active = GroupRuntime.activeGroupId();
        Map<UUID, Member> table = members;
        if (active == null || groupId == null || !active.equals(groupId) || table.isEmpty()) {
            return; // 未接管 / 名单不是本组的 / 名单为空：什么都不做
        }
        PlayerList playerList = server.getPlayerList();
        for (Map.Entry<UUID, Member> entry : table.entrySet()) {
            ServerPlayer player = playerList.getPlayer(entry.getKey());
            if (player == null) {
                player = playerList.getPlayerByName(entry.getValue().name());
            }
            if (player == null) {
                continue; // 还没进本组世界：JOIN 事件会再触发一次
            }
            applyOne(server, playerList, player, entry.getValue().level());
        }
    }

    /** 给单个玩家落权限位（等级 0 = 撤权，避免把普通人标成 op）。 */
    private static void applyOne(MinecraftServer server, PlayerList playerList,
                                 ServerPlayer player, int level) {
        UUID playerId = player.getUUID();
        Integer previous = applied.get(playerId);
        if (previous != null && previous == level) {
            return; // 已落地且未变化
        }
        GameProfile profile = player.getGameProfile();
        if (level >= 1) {
            playerList.getOps().add(new ServerOpListEntry(profile, level, false));
        } else {
            playerList.getOps().remove(profile);
        }
        // 同一次调用既发权限事件包（客户端 hasPermissions 闸门），也重发指令树（补全）
        playerList.sendPlayerPermissionLevel(player);
        applied.put(playerId, level);
        LOGGER.info("本地权限位已落地: {} → 等级 {}（服务端 ops 名单口径）",
                player.getGameProfile().getName(), level);
    }

    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
