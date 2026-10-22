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
 * 等级取服务端核出的<b>真实值</b>（仅限制在原版合法范围 0..4）；跨组广播执行
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
 * （写入在 Netty 线程，读取在服务器主线程）。JOIN 的二次核验通过服务器 tick 事件，
 * 不把 {@code server.execute} 误当成“下一 tick”—— 同线程调用可能立即执行。
 */
public final class LocalPermissions {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/permission");

    /** 一名成员的下发档（名字用于本地玩家列表按名兜底匹配）。 */
    record Member(String name, int level) {
    }

    /** 权威名单和玩家实例的应用记录分别管理，不能由宿主启动一起清空。 */
    private static final LocalPermissionState STATE = new LocalPermissionState();

    private record JoinRecheck(MinecraftServer server, ServerPlayer player, int joinedAtTick) { }

    /** JOIN 后只跨 tick 核验一次；按 UUID 替换并校验对象，防止旧连接影响同 UUID 重连。 */
    private static final Map<UUID, JoinRecheck> JOIN_RECHECKS = new ConcurrentHashMap<>();

    private LocalPermissions() {
    }

    /** 注册 PERMISSION_RELAY 入站处理（世界会话建立时与控制连接一起挂接）。 */
    public static void register(HandlerRegistry reg) {
        reg.on(MessageType.PERMISSION_RELAY, (conn, msg) -> update(msg));
    }

    /** 收到服务端名单：整表替换 + 尝试落地（Netty 线程）。 */
    private static void update(ControlMessage msg) {
        updateFromJson(msg.json());
    }

    /**
     * 从 JSON 应用一份名单（PERMISSION_RELAY 帧与 HELLO_ACK 的附带名单共用）。
     * <p>
     * 两条路径都要有：帧路径是常规通道（角色指派/合并/分离/周期兜底），ACK 路径是
     * 重连时的可靠通道（客户端此刻还没注册处理器，帧会丢）。
     */
    public static void updateFromJson(JsonObject json) {
        UUID group = parseUuid(JsonUtil.getString(json, "groupId",
                JsonUtil.getString(json, "permissionsGroupId", "")));
        Map<UUID, Member> next = parseMembers(json.get("members"));
        if (group == null || next == null) {
            return; // 入站数据不可信：缺字段直接忽略，保持旧名单
        }
        STATE.replace(group, next);
        LOGGER.info("收到权限名单: 组 {} → {} 名成员", group, next.size());
        applyScheduled(GroupRuntime.server());
    }

    /**
     * 解析名单数组（纯函数，可单测）。
     * <p>
     * 入站数据不可信，逐条跳过非法项：非对象条目、缺 uuid、等级越界（钳到 0..4 ——
     * 只有 0-4 是原版语义内的等级，越界值按边界处理而不是直接信任）。
     *
     * @return 解析结果；字段缺失/不是数组返回 null（调用方保持旧名单）
     */
    static Map<UUID, Member> parseMembers(JsonElement raw) {
        if (raw == null || !raw.isJsonArray()) {
            return null;
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
        return next;
    }

    /** 会话拆除时清缓存（条目本身随集成服务端实例销毁，无需文件清理）。 */
    public static void reset() {
        STATE.resetSession();
        JOIN_RECHECKS.clear();
    }

    /** 集成服实例结束/重建：权限名单仍属于当前物理服务端会话，必须留给新实例使用。 */
    public static void resetRuntime() {
        STATE.resetRuntime();
        JOIN_RECHECKS.clear();
    }

    /** 分离/玩家上行使用物理服名单中的规范身份，避免把离线派生UUID提交给服务端。 */
    public static UUID canonicalMemberId(UUID playerId, String playerName) {
        return STATE.memberIdFor(GroupRuntime.activeGroupId(), playerId, playerName);
    }

    /** 任意线程：把当前名单落到该集成服务端（不为本组/未接管时静默跳过）。 */
    public static void applyScheduled(MinecraftServer server) {
        applyScheduled(server, false);
    }

    /** LAN 发布会重置本地客户端权限位；发布完成后即使等级未变也必须重发事件包/指令树。 */
    public static void refreshScheduled(MinecraftServer server) {
        applyScheduled(server, true);
    }

    private static void applyScheduled(MinecraftServer server, boolean forceSend) {
        if (server == null) {
            return;
        }
        try {
            if (server.isSameThread()) {
                applyNow(server, forceSend);
            } else {
                server.execute(() -> applyNow(server, forceSend));
            }
        } catch (RuntimeException e) {
            // 服务端正在停止：名单本来就会随实例销毁，无需补救
            LOGGER.debug("本地权限落权调度失败: {}", e.toString());
        }
    }

    /**
     * Fabric JOIN 早于 PlayerList.players.add，必须使用事件持有的玩家对象直接落权。
     * 原版此后还会发送一次权限包；这里先写 ops，确保原版再次发送时也读到正确等级。
     */
    public static void applyJoiningPlayer(MinecraftServer server, ServerPlayer player) {
        if (player == null || !GroupRuntime.isManagedServer(server)) {
            return;
        }
        applyToPlayer(server, player, true);
        JOIN_RECHECKS.put(player.getUUID(), new JoinRecheck(server, player, server.getTickCount()));
    }

    /** END_SERVER_TICK 调用，明确等到与 JOIN 不同的 tick 后才核验，且不重复写相同 ops。 */
    public static void onServerTick(MinecraftServer server) {
        if (!GroupRuntime.isManagedServer(server) || JOIN_RECHECKS.isEmpty()) {
            return;
        }
        for (Map.Entry<UUID, JoinRecheck> entry : JOIN_RECHECKS.entrySet()) {
            JoinRecheck recheck = entry.getValue();
            if (recheck.server() != server || recheck.joinedAtTick() == server.getTickCount()) {
                continue;
            }
            if (!JOIN_RECHECKS.remove(entry.getKey(), recheck)) {
                continue;
            }
            // 同 UUID 已换连接/已断线时，绝不能把补发包送给旧玩家对象。
            if (server.getPlayerList().getPlayer(entry.getKey()) == recheck.player()) {
                applyToPlayer(server, recheck.player(), true);
            }
        }
    }

    /** 玩家离开只清该连接记录；旧连接迟到的退出事件不能影响重连实例。 */
    public static void onPlayerDisconnect(ServerPlayer player) {
        STATE.forget(player.getUUID(), player);
        JOIN_RECHECKS.computeIfPresent(player.getUUID(),
                (id, recheck) -> recheck.player() == player ? null : recheck);
    }

    /** 遍历在线玩家而非名单：整表移除成员也必须落实撤权。只在服务器主线程调用。 */
    private static void applyNow(MinecraftServer server, boolean forceSend) {
        if (!GroupRuntime.isManagedServer(server)) {
            return;
        }
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            applyToPlayer(server, player, forceSend);
        }
    }

    /** 给单个玩家落权限位（等级 0 = 撤权；未知/不同组的名单不会修改当前权限）。 */
    private static void applyToPlayer(MinecraftServer server, ServerPlayer player, boolean forceSend) {
        UUID playerId = player.getUUID();
        Integer level = STATE.levelFor(GroupRuntime.activeGroupId(), playerId,
                player.getGameProfile().getName());
        if (level == null) {
            LOGGER.debug("等待本组权限名单: 玩家={} 组={}",
                    player.getGameProfile().getName(), GroupRuntime.activeGroupId());
            return;
        }
        PlayerList playerList = server.getPlayerList();
        GameProfile profile = player.getGameProfile();
        ServerOpListEntry existing = playerList.getOps().get(profile);
        boolean opsChanged = level >= 1
                ? existing == null || existing.getLevel() != level
                : existing != null;
        if (!forceSend && !opsChanged && !STATE.needsApply(playerId, player, level)) {
            return;
        }
        if (level >= 1) {
            if (opsChanged) {
                playerList.getOps().add(new ServerOpListEntry(profile, level, false));
            }
        } else if (opsChanged) {
            playerList.getOps().remove(profile);
        }
        // 同一次调用既发权限事件包（客户端 hasPermissions 闸门），也重发指令树（补全）
        playerList.sendPlayerPermissionLevel(player);
        STATE.markApplied(playerId, player, level);
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
