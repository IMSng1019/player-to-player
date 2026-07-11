package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import com.mojang.authlib.GameProfile;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.server.MinecraftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 指令与聊天路由（服务端，Phase 4；规范"服务器的指令处理"与"服务器的信息处理"）。
 *
 * <h2>指令（COMMAND_RELAY）：逐级处理再分级下放</h2>
 * 规范链路 副→主→中转→服务端→其他主→其他副 中，"副→主"是原版语义（副客户端的
 * 指令经隧道在主客户端的集成服务端执行），"主→服务端"是本组执行失败后的上送
 * （{@code mixin.CommandsMixin} 捕获），本类是"服务端"一级：
 * <ol>
 *   <li><b>玩家表特例</b>（规范"服务端可以执行原主客户端不能执行的 tp 指令"）：
 *       {@code /tp <玩家名>} 的目标在玩家表中且发起者权限达标时，直接以目标坐标
 *       应答 {@code action=teleport}，由<b>发起方</b>主客户端在本地执行传送 ——
 *       玩家落到目标组的领地后，加载门控/分离监视自然接管合并或分离，
 *       无需跨组搬运实体；{@code /list} 由玩家表直接应答；{@code /msg} 转跨组私聊；</li>
 *   <li><b>泛化下放</b>：其余指令并发转发给所有其他组的主客户端尝试执行
 *       （规范"其他的主客户端"一级），第一个执行成功者的消息返回发起端；
 *       全部报错则应答 handled=false（发起端已显示本地报错，符合规范
 *       "在指令被传递完所有的端都报错后则返回本地的报错信息"）。</li>
 * </ol>
 * <b>权限口径</b>：转发执行的权限取物理服务端 ops 名单
 * （{@link MinecraftServer#getProfilePermissions}）并钳制到 2（游戏管理指令级）——
 * 主客户端信任服务端下发的权限值，但 /stop、/op 这类 3/4 级指令绝不跨组转发，
 * 防止一台机器的管理员关停别人的集成服务端。
 *
 * <h2>聊天（CHAT_RELAY）：全网分发</h2>
 * 主客户端上送本组聊天（含其副客户端 —— 隧道天然汇聚），服务端记录并广播给
 * <b>其他</b>组的主客户端（发起组自己已经看过原始聊天，不回发防止重复），
 * 各主客户端在本组集成服务端内以系统消息重放，副客户端经原版链路看到。
 * 规范链路中的"中转服务端"一级由控制连接的传输层承担（DESIGN.md 第 3 节），
 * 消息路由不感知。
 * <p>
 * 线程模型：处理器在 Netty 事件循环上只做查表与转发；泛化下放的聚合回调在
 * 各连接的事件循环上以原子量收敛，无锁竞争。
 */
public final class CommandChatHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/cmd-chat");

    /** 上送指令长度上限（入站不可信，防恶意大帧）。 */
    private static final int MAX_COMMAND_LENGTH = 256;

    /** 转发执行的权限钳制（游戏管理级=2；/stop、/op 等 3/4 级指令绝不跨组转发）。 */
    private static final int FORWARD_PERMISSION_CAP = 2;

    private final MinecraftServer server;
    private final GroupTable groups;
    private final PlayerTable players;

    private CommandChatHandlers(MinecraftServer server, GroupTable groups, PlayerTable players) {
        this.server = server;
        this.groups = groups;
        this.players = players;
    }

    /** 注册 COMMAND_RELAY / CHAT_RELAY 处理器。 */
    public static void register(HandlerRegistry reg, MinecraftServer server,
                                GroupTable groups, PlayerTable players) {
        CommandChatHandlers handlers = new CommandChatHandlers(server, groups, players);
        reg.on(MessageType.COMMAND_RELAY, handlers::handleCommandRelay);
        reg.on(MessageType.CHAT_RELAY, handlers::handleChatRelay);
    }

    // ------------------------------------------------------------ 指令上送

    private void handleCommandRelay(ControlConnection conn, ControlMessage msg) {
        UUID requester = conn.peerId();
        if (requester == null) {
            conn.send(error(msg, "not_authenticated", "须先完成 HELLO 握手"));
            return;
        }
        String command = JsonUtil.getString(msg.json(), "command", "").trim();
        UUID playerUuid = parseUuid(JsonUtil.getString(msg.json(), "playerUuid", ""));
        String playerName = JsonUtil.getString(msg.json(), "playerName", "?");
        if (command.isEmpty() || command.length() > MAX_COMMAND_LENGTH || playerUuid == null) {
            conn.send(error(msg, "invalid_request", "command/playerUuid 缺失或非法"));
            return;
        }
        if (command.startsWith("/")) {
            command = command.substring(1);
        }
        UUID originGroup = groups.groupOf(requester);
        LOGGER.info("指令上送: 玩家 {} (组 {}) /{}", playerName, originGroup, command);

        // 发起者在物理服务端 ops 名单中的权限（权威口径），转发时钳制到 2
        int permission = Math.min(FORWARD_PERMISSION_CAP,
                server.getProfilePermissions(new GameProfile(playerUuid, playerName)));

        String[] tokens = command.split("\\s+");
        // ---- 特例一：/list —— 玩家表直接应答（规范：玩家表如同原版统计玩家）----
        if (tokens.length == 1 && "list".equalsIgnoreCase(tokens[0])) {
            replyHandled(conn, msg, buildPlayerList());
            return;
        }
        // ---- 特例二：/tp|/teleport <玩家名> —— 玩家表解析坐标，发起方本地执行 ----
        if (tokens.length == 2 && ("tp".equalsIgnoreCase(tokens[0])
                || "teleport".equalsIgnoreCase(tokens[0]))) {
            PlayerTable.PlayerEntry target = players.byName(tokens[1]);
            if (target != null && permission >= 2) {
                JsonObject out = new JsonObject();
                out.addProperty("handled", true);
                out.addProperty("action", "teleport");
                out.addProperty("dimension", target.dimension());
                out.addProperty("x", target.x());
                out.addProperty("y", target.y());
                out.addProperty("z", target.z());
                out.addProperty("message", "已传送到 " + target.playerName());
                conn.send(msg.reply(MessageType.COMMAND_RELAY, out, null));
                LOGGER.info("跨组 tp: {} → {} ({} {},{},{})", playerName, target.playerName(),
                        target.dimension(), (int) target.x(), (int) target.y(), (int) target.z());
                return;
            }
            // 目标不在线/权限不足：落到泛化下放（其他端也执行不了，最终 handled=false）
        }
        // ---- 特例三：/msg|/tell|/w <玩家名> <内容> —— 跨组私聊投递 ----
        if (tokens.length >= 3 && ("msg".equalsIgnoreCase(tokens[0])
                || "tell".equalsIgnoreCase(tokens[0]) || "w".equalsIgnoreCase(tokens[0]))) {
            PlayerTable.PlayerEntry target = players.byName(tokens[1]);
            if (target != null) {
                String text = command.substring(command.indexOf(tokens[1]) + tokens[1].length()).trim();
                if (deliverPrivateMessage(playerName, target, text)) {
                    replyHandled(conn, msg, "你悄悄地对 " + target.playerName() + " 说: " + text);
                } else {
                    replyUnhandled(conn, msg);
                }
                return;
            }
        }
        // ---- 泛化下放：并发转发给其他组的主客户端，首个成功者胜出 ----
        forwardToOtherPrimaries(conn, msg, originGroup, command, playerName, permission);
    }

    /** 在线玩家清单（/list 应答文本）。 */
    private String buildPlayerList() {
        Map<UUID, PlayerTable.PlayerEntry> snapshot = players.snapshot();
        StringJoiner names = new StringJoiner(", ");
        for (PlayerTable.PlayerEntry entry : snapshot.values()) {
            names.add(entry.playerName());
        }
        return "当前共有 " + snapshot.size() + " 名玩家在线: " + names;
    }

    /** 跨组私聊：推送给目标玩家所在组的主客户端（privateTo 语义见 MessageType）。 */
    private boolean deliverPrivateMessage(String fromName, PlayerTable.PlayerEntry target,
                                          String text) {
        UUID targetGroup = target.groupId() != null
                ? target.groupId() : groups.groupOf(target.playerId());
        UUID targetPrimary = targetGroup != null ? groups.primaryOf(targetGroup) : null;
        ControlConnection c = targetPrimary != null
                ? HelloHandler.connectionOf(targetPrimary) : null;
        if (c == null || !c.isOpen()) {
            return false;
        }
        JsonObject push = new JsonObject();
        push.addProperty("playerName", fromName);
        push.addProperty("text", text);
        push.addProperty("privateTo", target.playerId().toString());
        c.send(ControlMessage.of(MessageType.CHAT_RELAY, push));
        return true;
    }

    /**
     * 泛化下放（规范"服务端→其他的主客户端"）：并发请求全部其他组主客户端执行，
     * 第一个 executed=true 的应答返回发起端；全部失败/超时 → handled=false。
     */
    private void forwardToOtherPrimaries(ControlConnection origin, ControlMessage msg,
                                         UUID originGroup, String command,
                                         String playerName, int permission) {
        List<ControlConnection> targets = new ArrayList<>();
        for (Map.Entry<UUID, UUID> e : groups.primariesSnapshot().entrySet()) {
            if (e.getKey().equals(originGroup)) {
                continue; // 发起组自己已经执行失败过，不回发
            }
            ControlConnection c = HelloHandler.connectionOf(e.getValue());
            if (c != null && c.isOpen()) {
                targets.add(c);
            }
        }
        if (targets.isEmpty()) {
            replyUnhandled(origin, msg);
            return;
        }
        JsonObject forward = new JsonObject();
        forward.addProperty("execute", true);
        forward.addProperty("command", command);
        forward.addProperty("sourceName", playerName);
        forward.addProperty("permissionLevel", permission);

        // 首个成功者胜出：done 保证只回发一次；remaining 归零仍无成功 → handled=false
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicInteger remaining = new AtomicInteger(targets.size());
        for (ControlConnection target : targets) {
            target.request(ControlMessage.of(MessageType.COMMAND_RELAY, forward.deepCopy()))
                    .whenComplete((resp, err) -> {
                        boolean executed = err == null && resp != null
                                && resp.type() == MessageType.COMMAND_RELAY
                                && JsonUtil.getBoolean(resp.json(), "executed", false);
                        if (executed && done.compareAndSet(false, true)) {
                            // 规范"如果被执行 则将执行端的消息返回发出指令的端"
                            replyHandled(origin, msg,
                                    JsonUtil.getString(resp.json(), "message", "指令已在其他组执行"));
                            LOGGER.info("指令 /{} 已由其他组执行（发起者 {}）", command, playerName);
                        }
                        if (remaining.decrementAndGet() == 0 && done.compareAndSet(false, true)) {
                            replyUnhandled(origin, msg); // 所有端都报错：发起端回放本地报错
                        }
                    });
        }
    }

    // ------------------------------------------------------------ 聊天分发

    private void handleChatRelay(ControlConnection conn, ControlMessage msg) {
        UUID sender = conn.peerId();
        if (sender == null) {
            return; // 未握手连接的聊天直接丢弃（无应答消息，静默）
        }
        String playerName = JsonUtil.getString(msg.json(), "playerName", "?");
        String text = JsonUtil.getString(msg.json(), "text", "");
        if (text.isBlank() || text.length() > 1024) {
            return; // 入站不可信：空文本/超长文本丢弃
        }
        UUID originGroup = groups.groupOf(sender);
        // 服务端留痕（规范：服务器和客户端的消息需要让所有客户端看到 —— 服务端是汇聚点）
        LOGGER.info("[跨组聊天] <{}> {}", playerName, text);

        JsonObject push = new JsonObject();
        push.addProperty("playerName", playerName);
        push.addProperty("text", text);
        for (Map.Entry<UUID, UUID> e : groups.primariesSnapshot().entrySet()) {
            if (e.getKey().equals(originGroup)) {
                continue; // 发起组已看过原始聊天
            }
            ControlConnection c = HelloHandler.connectionOf(e.getValue());
            if (c != null && c.isOpen()) {
                c.send(ControlMessage.of(MessageType.CHAT_RELAY, push.deepCopy()));
            }
        }
    }

    // ------------------------------------------------------------ 工具

    private static void replyHandled(ControlConnection conn, ControlMessage msg, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("handled", true);
        out.addProperty("message", message);
        conn.send(msg.reply(MessageType.COMMAND_RELAY, out, null));
    }

    private static void replyUnhandled(ControlConnection conn, ControlMessage msg) {
        JsonObject out = new JsonObject();
        out.addProperty("handled", false);
        conn.send(msg.reply(MessageType.COMMAND_RELAY, out, null));
    }

    /** 防御性 UUID 解析：null / 格式非法返回 null 而非抛异常。 */
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

    /** 构造保留 _rid 的 ERROR 应答。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
