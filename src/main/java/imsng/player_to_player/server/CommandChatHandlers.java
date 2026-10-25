package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import com.mojang.authlib.GameProfile;
import com.mojang.brigadier.ParseResults;
import com.mojang.brigadier.context.CommandContextBuilder;
import com.mojang.brigadier.context.ParsedArgument;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.CommandFeedback;
import imsng.player_to_player.group.CommandRouting;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.TokenBucket;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.minecraft.commands.CommandSource;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.core.registries.Registries;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.ComponentUtils;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.rcon.RconConsoleSource;
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 指令与聊天路由（服务端，Phase 4；规范"服务器的指令处理"与"服务器的信息处理"）。
 *
 * <h2>指令（COMMAND_RELAY）：全量上链 + 服务端裁定 + 分组执行</h2>
 * 口径见 {@code docs/plans/2026-10-15-command-routing-fix-plan.md} 附录 P / P.2
 * （2026-10-15 定稿）：玩家敲的每一条指令都上链，<b>权限裁定唯一在物理服务端</b>，
 * 一条指令的语义 = 按下面的规则在若干组各执行一次。处理顺序：
 * <ol>
 *   <li><b>身份自洽</b>（{@link ConnectionIdentity}）：上报的玩家必须是本连接所在组的
 *       成员 —— 否则任何人都能冒用 op 的 UUID 换取权限、或以他人名义刷位置；</li>
 *   <li><b>权限与解析</b>：以发起者的<b>真实权限等级</b>（物理服 ops 名单口径）
 *       构造解析源做原版解析，解析不通过就回原版报错文案
 *       （"在原版中不合理的指令是不会被执行的"）；</li>
 *   <li><b>三个特例</b>（决策 6）：{@code /list} 用玩家表答全网；{@code /tp <玩家名>}
 *       与具名/坐标 TP 按玩家表规划到被传送玩家的组；{@code /msg <玩家名> <文本>} 跨组投递；</li>
 *   <li><b>3/4 级指令</b>（决策 3）：在<b>服务端</b>执行（真实等级过原版 requires），
 *       结果文案回给发起者 —— 这类指令绝不跨组执行（防一台机器的管理员关停别人的
 *       集成服务端），执行后触发一次权限 diff 推送（{@code /op}、{@code /deop} 等）；</li>
 *   <li><b>0-2 级指令</b>：形状规则 + 归属预检决定目标组（{@link CommandRouting}），
 *       下发 {@code execute=true} 的转发执行，{@link CommandAggregator} 聚合回执。</li>
 * </ol>
 *
 * <h2>聊天（CHAT_RELAY）：全网分发</h2>
 * 主客户端上送本组聊天（含其副客户端 —— 隧道天然汇聚），服务端记录并广播给
 * <b>其他</b>组的主客户端（发起组自己已经看过原始聊天，不回发防止重复），
 * 各主客户端在本组集成服务端内以系统消息重放，副客户端经原版链路看到。
 * 规范链路中的"中转服务端"一级由控制连接的传输层承担（DESIGN.md 第 3 节），
 * 消息路由不感知。
 * <p>
 * 线程模型：处理器在 Netty 事件循环上只做查表与转发；聚合回调在各连接的事件循环上
 * 以 {@link CommandAggregator} 的同步块收敛。服务端执行（3/4 级）回服务器主线程。
 */
public final class CommandChatHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/cmd-chat");

    /** 只为当前物理服暴露控制台入口；实例身份校验阻止集成服或旧存档借用此路由器。 */
    private static volatile CommandChatHandlers consoleHandlers;
    private static boolean consoleLifecycleRegistered;

    /** 上送指令长度上限（入站不可信，防恶意大帧）。 */
    private static final int MAX_COMMAND_LENGTH = 256;

    /** 转发执行的权限钳制（游戏管理级=2；/stop、/op 等 3/4 级指令绝不跨组转发）。 */
    private static final int FORWARD_PERMISSION_CAP = 2;

    /**
     * 每条连接的上链限速（突发 20、约 4 条/秒）。
     * <p>
     * 客户端侧也有同一套限速，但那是"自己人"的礼貌；服务端这道是防被改造的客户端
     * 把"一次上链"放大成"向 N 个组各一次转发"的指令风暴。超限回 ERROR，玩家侧
     * 会看到明确文案而不是静默丢弃。
     */
    private static final int UPLINK_BURST = 20;

    private static final long UPLINK_REFILL_NANOS = 250_000_000L;

    /**
     * 玩家（归一后的成员 ID）→ 令牌桶。
     * <p>
     * 按<b>玩家</b>而不是按连接：一条控制连接代表整个组（主客户端 + 其副客户端都经
     * 它上送），按连接限速会让一个玩家连打把同组其他人卡在"指令过于频繁"。
     */
    private final Map<UUID, TokenBucket> uplinkBuckets = new java.util.concurrent.ConcurrentHashMap<>();

    private final MinecraftServer server;
    private final GroupTable groups;
    private final PlayerTable players;
    private final ChunkRegistry registry;
    private final PermissionRelay permissions;

    private CommandChatHandlers(MinecraftServer server, GroupTable groups, PlayerTable players,
                                ChunkRegistry registry, PermissionRelay permissions) {
        this.server = server;
        this.groups = groups;
        this.players = players;
        this.registry = registry;
        this.permissions = permissions;
    }

    /** 注册 COMMAND_RELAY / CHAT_RELAY 处理器。 */
    public static void register(HandlerRegistry reg, MinecraftServer server,
                                GroupTable groups, PlayerTable players,
                                ChunkRegistry registry, PermissionRelay permissions) {
        CommandChatHandlers handlers = new CommandChatHandlers(
                server, groups, players, registry, permissions);
        reg.on(MessageType.COMMAND_RELAY, handlers::handleCommandRelay);
        reg.on(MessageType.CHAT_RELAY, handlers::handleChatRelay);
        publishConsoleHandlers(handlers);
    }

    private static synchronized void publishConsoleHandlers(CommandChatHandlers handlers) {
        consoleHandlers = handlers;
        if (!consoleLifecycleRegistered) {
            consoleLifecycleRegistered = true;
            ServerLifecycleEvents.SERVER_STOPPED.register(stopped -> {
                CommandChatHandlers current = consoleHandlers;
                if (current != null && current.server == stopped) {
                    consoleHandlers = null;
                }
            });
        }
    }

    /**
     * 仅由 DedicatedServer.handleConsoleInputs 的调用点进入，返回 true 才取消原版执行。
     * 普通玩家、命令方块、函数与内部转发都不经过这个入口，不会再次上链。
     */
    public static boolean routeConsoleInput(CommandSourceStack source, String command) {
        CommandChatHandlers handlers = currentConsoleHandlers(source.getServer());
        if (handlers == null || source.getEntity() != null) {
            return false;
        }
        try {
            CompletableFuture<String> response = handlers.dispatchConsole(source, command);
            if (response == null) {
                return false; // 管理命令与原版解析错误保持原入口执行。
            }
            response.whenComplete((text, error) -> handlers.server.execute(() ->
                    source.sendSystemMessage(Component.literal(error == null
                            ? text : "P2P 指令执行结果未确认: " + error.getMessage()))));
        } catch (RuntimeException error) {
            source.sendFailure(Component.literal("P2P 指令路由失败，未执行: " + error.getMessage()));
            LOGGER.warn("控制台指令路由异常: /{}", command, error);
        }
        return true;
    }

    /**
     * RCON 原入口会同步读取共享缓冲，直接异步发消息会漏回执或串到下次请求。
     * 因此每次请求拥有独立 future，仅 RCON 线程等待；解析与路由仍调度到服务器线程。
     * null 表示原版管理命令，交还原始 runCommand 执行，其权限与反馈路径保持不变。
     */
    public static String routeRconInput(MinecraftServer server, String command) {
        CommandChatHandlers handlers = currentConsoleHandlers(server);
        if (handlers == null) {
            return null;
        }
        if (server.isSameThread()) {
            // 原版 RCON 从专用连接线程调用；模组若在主线程程序化调用此 API，不能等网络。
            return "P2P 控制台路由不能在服务器主线程等待远程执行，请从控制台或 RCON 连接发起";
        }
        CompletableFuture<String> response = new CompletableFuture<>();
        server.execute(() -> {
            if (response.isDone()) {
                return; // 排队期间已经超时的请求不能在稍后突然执行。
            }
            try {
                CommandSourceStack source = new RconConsoleSource(server).createCommandSourceStack();
                CompletableFuture<String> routed = handlers.dispatchConsole(source, command);
                if (routed == null) {
                    response.complete(null);
                } else {
                    routed.whenComplete((text, error) -> response.complete(error == null
                            ? text : "P2P 指令执行结果未确认: " + error.getMessage()));
                }
            } catch (RuntimeException error) {
                response.complete("P2P 指令路由失败，未执行: " + error.getMessage());
            }
        });
        try {
            return response.get(Protocol.REQUEST_TIMEOUT_MILLIS + 5_000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            String message = "P2P 指令等待超时，执行结果未确认";
            response.complete(message);
            return message;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            String message = "P2P 指令等待被中断，执行结果未确认";
            response.complete(message);
            return message;
        } catch (Exception error) {
            return "P2P 指令执行结果未确认: " + error.getMessage();
        }
    }

    private static CommandChatHandlers currentConsoleHandlers(MinecraftServer server) {
        CommandChatHandlers current = consoleHandlers;
        return NodeContext.get().isServer() && current != null && current.server == server
                && !server.isStopped() ? current : null;
    }

    /** 主线程生成一次计划，之后只向唯一目标组下发，反馈沿现有 request/回执聚合器返回。 */
    private CompletableFuture<String> dispatchConsole(CommandSourceStack source, String command) {
        String normalized = command.trim();
        if (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        if ("list".equals(normalized)) {
            return CompletableFuture.completedFuture(buildPlayerList());
        }
        // 全局字段只在中心执行一次；原版最终执行节点负责耐久提交结果补丁。
        if (imsng.player_to_player.metadata.MetadataCommands.isGlobal(normalized)) return null;
        String dimension = source.getLevel().dimension().location().toString();
        ConsoleCommandRouting.Decision plan = ConsoleCommandRouting.plan(normalized,
                server.getCommands().getDispatcher(), source, dimension,
                players::byName, groups::groupOf, registry::ownerOf);
        if (plan.kind() == ConsoleCommandRouting.Kind.LOCAL) {
            return null;
        }
        if (plan.kind() == ConsoleCommandRouting.Kind.REJECT) {
            return CompletableFuture.completedFuture("P2P 指令未执行: " + plan.message());
        }
        if (plan.command().length() > MAX_COMMAND_LENGTH) {
            return CompletableFuture.completedFuture("P2P 指令展开后过长，未执行");
        }
        UUID primary = groups.primaryOf(plan.groupId());
        ControlConnection target = primary == null ? null : HelloHandler.connectionOf(primary);
        if (target == null || !target.isOpen() || !ConnectionIdentity.supportsCommandRouting(primary)) {
            return CompletableFuture.completedFuture("目标玩家所在组当前不可执行指令，未执行");
        }
        JsonObject forward = new JsonObject();
        forward.addProperty("execute", true);
        forward.addProperty("command", plan.command());
        forward.addProperty("sourceName", source.getTextName());
        forward.addProperty("permissionLevel", FORWARD_PERMISSION_CAP);
        forward.addProperty("origin", false); // console 始终无源实体，绝不能借用目标玩家充当 @s。
        forward.addProperty("dimension", plan.dimension());
        if (!plan.ownedChunks().isEmpty()) {
            com.google.gson.JsonArray keys = new com.google.gson.JsonArray();
            plan.ownedChunks().forEach(key -> keys.add(key.asString()));
            forward.add("ownedChunks", keys);
        }
        CompletableFuture<String> response = new CompletableFuture<>();
        CommandAggregator aggregator = new CommandAggregator(1, response::complete);
        LOGGER.info("控制台指令下放: /{} -> 组 {}", normalized, plan.groupId());
        target.request(ControlMessage.of(MessageType.COMMAND_RELAY, forward))
                .whenComplete((reply, error) -> {
                    boolean executed = error == null && reply != null
                            && reply.type() == MessageType.COMMAND_RELAY
                            && JsonUtil.getBoolean(reply.json(), "executed", false);
                    String message = error != null ? "执行组未在规定时间内应答，执行结果未确认"
                            : reply == null ? "执行组没有返回结果"
                            : JsonUtil.getString(reply.json(), "message", "");
                    aggregator.record(false, executed, message);
                });
        return response;
    }

    // ------------------------------------------------------------ 指令上链

    private void handleCommandRelay(ControlConnection conn, ControlMessage msg) {
        UUID requester = conn.peerId();
        if (requester == null) {
            conn.send(error(msg, "not_authenticated", "须先完成 HELLO 握手"));
            return;
        }
        // 没有 command 字段的帧不是上送，而是**迟到的转发执行应答**：服务端对目标组的
        // request 已超时（pending 已移除），应答这才回来。必须静默忽略，不能回 ERROR ——
        // 回 ERROR 会让两端以同一个未知 _rid 无限互回（见 NettyControlConnection 的分发注释）。
        if (!msg.json().has("command")) {
            LOGGER.debug("忽略迟到的指令应答（来自 {}）", requester);
            return;
        }
        String command = JsonUtil.getString(msg.json(), "command", "").trim();
        UUID reportedUuid = parseUuid(JsonUtil.getString(msg.json(), "playerUuid", ""));
        String playerName = JsonUtil.getString(msg.json(), "playerName", "?");
        if (command.isEmpty() || command.length() > MAX_COMMAND_LENGTH || reportedUuid == null) {
            conn.send(error(msg, "invalid_request", "command/playerUuid 缺失或非法"));
            return;
        }
        if (command.startsWith("/")) {
            command = command.substring(1);
        }
        // ---- 身份自洽（复查 G-06 / 2026-10-16 复查）：上报身份必须**配对命中**
        //      本组某个具体成员，并归一到该成员的档案 UUID ----
        UUID playerUuid = ConnectionIdentity.canonicalMemberId(
                groups, requester, reportedUuid, playerName);
        if (playerUuid == null) {
            LOGGER.warn("拒绝越权指令上送: 连接 {} 声称玩家 {}（{}）",
                    requester, reportedUuid, playerName);
            conn.send(error(msg, "not_authorized", "上报的玩家身份与本连接所在组不符"));
            return;
        }
        // 旧客户端（HELLO 未声明指令路由能力）只会在本地执行失败后上送：服务端
        // 不按新语义处理，静默忽略（它本地已经执行过、玩家也已看到本地报错）
        if (!ConnectionIdentity.supportsCommandRouting(requester)) {
            LOGGER.info("忽略旧客户端的指令上送（{} 未声明指令路由能力）: /{}",
                    requester, command);
            return;
        }
        if (!uplinkBuckets.computeIfAbsent(playerUuid,
                id -> new TokenBucket(UPLINK_BURST, UPLINK_REFILL_NANOS)).tryAcquire()) {
            conn.send(error(msg, "too_many_commands", "指令过于频繁，请稍候再试"));
            return;
        }
        UUID originGroup = groups.groupOf(requester);
        String dimension = JsonUtil.getString(msg.json(), "dimension", "minecraft:overworld");
        double x = JsonUtil.getDouble(msg.json(), "x", 0.0);
        double y = JsonUtil.getDouble(msg.json(), "y", 0.0);
        double z = JsonUtil.getDouble(msg.json(), "z", 0.0);
        // 朝向也要：^ 局部坐标以执行者朝向为原点，解析源必须带上它才能结算出
        // 与发起端一致的方块坐标（越界预检依赖这个结算结果）
        float yRot = (float) JsonUtil.getDouble(msg.json(), "yRot", 0.0);
        float xRot = (float) JsonUtil.getDouble(msg.json(), "xRot", 0.0);

        // ---- 权限裁定：物理服务端 ops 名单是唯一权威（决策 3 的实现要点）----
        // 等级取自 PermissionRelay 在服务器主线程维护的快照：ops 名单内部是非并发
        // HashMap，从 Netty 线程直读会与主线程的 /op /deop 形成数据竞争（偶发把合法
        // 指令判成权限不足）。快照在角色指派/HELLO/每 5 秒 diff 与每次服务端执行
        // 3/4 级指令后刷新；未命中按 0（fail-closed，且这种连接本不该有指令上送）。
        Integer cachedLevel = permissions.levelOf(playerUuid);
        int realPermission = cachedLevel != null ? cachedLevel : 0;
        ServerLevel level = resolveLevel(dimension);
        if (level == null) {
            level = server.overworld();
        }
        CommandSourceStack source = new CommandSourceStack(CommandSource.NULL,
                new Vec3(x, y, z), new Vec2(xRot, yRot), level, realPermission, playerName,
                Component.literal(playerName), server, null);
        ParseResults<CommandSourceStack> parse;
        try {
            parse = server.getCommands().getDispatcher().parse(command, source);
        } catch (RuntimeException e) {
            replyMessage(conn, msg, "指令解析失败: " + e.getMessage());
            return;
        }
        try {
            Commands.validateParseResults(parse);
        } catch (CommandSyntaxException e) {
            // 原版口径：权限不足与未知指令都表现为"解析不通过"，文案照原版回给发起者
            replyError(conn, msg, e);
            return;
        }
        LOGGER.info("指令上链: 玩家 {} (组 {}) /{} perm={}",
                playerName, originGroup, command, realPermission);

        String[] tokens = command.split("\\s+");
        // ---- 特例一：/list —— 玩家表直接应答（规范：玩家表如同原版统计玩家）----
        if (tokens.length == 1 && "list".equalsIgnoreCase(tokens[0])) {
            replyMessage(conn, msg, buildPlayerList());
            return;
        }
        // ---- TP 专用路由：目的玩家可以位于其他组，执行者始终是被传送玩家的当前组 ----
        // 不能复用普通世界写入的落点归属预检，否则 /tp x y z 进入其他组领地时会在
        // 认领/预同步/合并发生前直接被拒绝；也不能广播 /tp A B，让各组解析缺失实体。
        TeleportCommandRouting.Plan teleport = TeleportCommandRouting.plan(parse, source,
                dimension, playerName, players::byName, groups::groupOf);
        if (teleport != null && realPermission >= 2) {
            if (teleport.rejection() != null) {
                replyMessage(conn, msg, teleport.rejection());
            } else {
                forwardTeleport(conn, msg, teleport, playerName, realPermission);
            }
            return;
        }
        // ---- 特例三：/msg|/tell|/w <玩家名> <内容> —— 跨组私聊投递 ----
        if (tokens.length >= 3 && ("msg".equalsIgnoreCase(tokens[0])
                || "tell".equalsIgnoreCase(tokens[0]) || "w".equalsIgnoreCase(tokens[0]))) {
            PlayerTable.PlayerEntry target = players.byName(tokens[1]);
            if (target != null) {
                // 文本用解析结果取（原实现用 indexOf 定位目标名，目标名与指令字面量同形时会截错）
                String text = extractTextArgument(parse, "message");
                if (text == null || text.isBlank()) {
                    conn.send(error(msg, "invalid_request", "无法解析消息文本"));
                    return;
                }
                if (deliverPrivateMessage(playerName, target, text)) {
                    replyMessage(conn, msg, "你悄悄地对 " + target.playerName() + " 说: " + text);
                } else {
                    replyMessage(conn, msg, target.playerName() + " 当前不在线");
                }
                return;
            }
        }

        // ---- 3/4 级指令：在服务端执行（真实等级过原版 requires），不跨组 ----
        if ((imsng.player_to_player.metadata.MetadataCommands.isGlobal(command)
                && !imsng.player_to_player.metadata.MetadataCommands.isLocalClockQuery(command))
                || !isGroupExecutable(command, source)) {
            executeOnServer(conn, msg, command, source, playerName);
            return;
        }

        // ---- 0-2 级指令：形状规则 + 归属预检 → 分组广播执行 ----
        route(conn, msg, originGroup, command, playerUuid, playerName, dimension, parse,
                realPermission);
    }

    /**
     * TP 只发往一个执行组，并等该组的原版执行结果后再答复。旧 action=teleport
     * 回执在真正移动玩家前已经写“已传送”，本路径避免把排队或执行失败当成功。
     * 规划时已结算执行源位置、朝向和目标身份，所以接收端使用无实体虚拟源即可。
     */
    private void forwardTeleport(ControlConnection conn, ControlMessage msg,
                                 TeleportCommandRouting.Plan plan, String sourceName,
                                 int permission) {
        UUID primary = groups.primaryOf(plan.groupId());
        ControlConnection target = primary == null ? null : HelloHandler.connectionOf(primary);
        if (target == null || !target.isOpen() || !ConnectionIdentity.supportsCommandRouting(primary)) {
            replyMessage(conn, msg, "被传送玩家所在组当前不可执行指令，未执行");
            return;
        }
        if (plan.command().length() > MAX_COMMAND_LENGTH) {
            replyMessage(conn, msg, "传送指令展开后过长，未执行");
            return;
        }
        JsonObject forward = new JsonObject();
        forward.addProperty("execute", true);
        forward.addProperty("command", plan.command());
        forward.addProperty("sourceName", sourceName);
        forward.addProperty("permissionLevel", Math.min(FORWARD_PERMISSION_CAP, permission));
        forward.addProperty("origin", false);
        forward.addProperty("dimension", plan.dimension());
        CommandAggregator aggregator = new CommandAggregator(1,
                text -> replyMessage(conn, msg, text));
        LOGGER.info("TP 指令下放: /{} -> 组 {}", plan.command(), plan.groupId());
        target.request(ControlMessage.of(MessageType.COMMAND_RELAY, forward))
                .whenComplete((reply, error) -> {
                    boolean executed = error == null && reply != null
                            && reply.type() == MessageType.COMMAND_RELAY
                            && JsonUtil.getBoolean(reply.json(), "executed", false);
                    String text = error != null ? "传送执行组未在规定时间内应答，执行结果未确认"
                            : reply == null ? "传送执行组没有返回结果"
                            : JsonUtil.getString(reply.json(), "message", "");
                    aggregator.record(false, executed, text);
                });
    }

    /**
     * 该指令能否在组世界里执行：用<b>2 级权限</b>的同一解析源再解析一次。
     * <p>
     * 这是判定"3/4 级指令"的免维护做法（决策 3）：命令节点上的 {@code requires}
     * 由原版权限检查本身裁定，不需要维护"哪条指令要几级"的等级表。能在 2 级下
     * 解析通过 ⇒ 属于组世界可执行的 0-2 级；否则（只在高权限下可解析，或依赖
     * 仅服务端才有的上下文）留在服务端执行。
     */
    private boolean isGroupExecutable(String command, CommandSourceStack realSource) {
        return ConsoleCommandRouting.isGroupExecutable(
                command, server.getCommands().getDispatcher(), realSource);
    }

    /** 3/4 级指令的服务端执行（决策 3）：结果文案回给发起者，并刷新权限名单。 */
    private void executeOnServer(ControlConnection conn, ControlMessage msg, String command,
                                 CommandSourceStack realSource, String playerName) {
        server.execute(() -> {
            CommandFeedback feedback = new CommandFeedback();
            CommandSourceStack collecting = realSource.withSource(feedback);
            try {
                imsng.player_to_player.metadata.MetadataService.beginLocalCommand(groups.groupOf(conn.peerId()),
                        parseUuid(JsonUtil.getString(msg.json(), "playerUuid", "")), collecting);
                server.getCommands().performPrefixedCommand(collecting, command);
            } catch (Exception e) {
                LOGGER.warn("服务端执行指令异常: /{}", command, e);
                feedback.sendSystemMessage(Component.literal("全局指令未确认: " + e.getMessage()));
            } finally {
                imsng.player_to_player.metadata.MetadataService.endLocalCommand(server);
            }
            String message = feedback.joined();
            // 审计痕迹：这些指令（/op、/ban、/stop…）在原版会写控制台与 latest.log，
            // 收集器不直发的话服务端侧就只剩一行模组日志 —— 同时发一份给服务端自身
            if (!message.isEmpty()) {
                server.sendSystemMessage(net.minecraft.network.chat.Component.literal(
                        "[" + playerName + ": /" + command + "] " + message));
            } else {
                server.sendSystemMessage(net.minecraft.network.chat.Component.literal(
                        "[" + playerName + " 执行了 3/4 级指令: /" + command + "]"));
            }
            if (message.isEmpty()) {
                message = "指令已在服务端执行: /" + command;
            }
            replyMessage(conn, msg, message);
            LOGGER.info("服务端已执行 3/4 级指令: /{}（发起者 {}）", command, playerName);
            // /op、/deop、/whitelist 之类改动了名单：立刻重算并下发各组的本地权限位
            permissions.pushAll();
        });
    }

    /**
     * 形状规则（决策 2）+ 归属预检（决策 1）：
     * <ul>
     *   <li>引用执行上下文 ⇒ 只在发起组执行（源 = 发起玩家实体）；</li>
     *   <li>无坐标且不引用上下文 ⇒ 所有组各执行一次；</li>
     *   <li>带绝对坐标 ⇒ 只发给真正持有这些区块的组：无人持有则交给发起组
     *       （按需申请，与原版本地执行同一条加载门控），归属跨越多个组则拒绝执行
     *       （没有一个组能安全地整体执行，硬执行会让主线程永久忙等）。</li>
     * </ul>
     */
    private void route(ControlConnection conn, ControlMessage msg, UUID originGroup,
                       String command, UUID playerUuid, String playerName, String dimension,
                       ParseResults<CommandSourceStack> parse, int realPermission) {
        CommandRouting.Analysis analysis = CommandRouting.analyze(parse);
        int forwardPermission = Math.min(FORWARD_PERMISSION_CAP, realPermission);
        List<UUID> targetGroups = new ArrayList<>();
        // 服务端核过的"持有区块"清单：随转发下发，接收端据此复核归属在预检之后没有变化
        List<ChunkKey> ownedChunks = new ArrayList<>();
        if (analysis.contextBound() || imsng.player_to_player.metadata.MetadataCommands.containsGlobal(parse)) {
            // 引用执行上下文的指令只在发起组执行（源 = 发起玩家实体），但**越界仍要拦**：
            // /execute positioned … run fill ~ ~ ~ ~45 32 32 这类形态的绝对坐标同样会
            // 伸进别人的领地 —— 发起组执行时就地冻结，比"干净拒绝"糟糕得多
            CommandRouting.ChunkRange range = analysis.chunkRange();
            if (originGroup != null && range != null) {
                String veto = CommandOwnershipRouter.checkExecutableBy(
                        range, dimension, originGroup, registry::ownerOf);
                if (veto != null) {
                    replyMessage(conn, msg, veto);
                    return;
                }
            }
            if (originGroup != null) {
                targetGroups.add(originGroup);
            }
        } else if (analysis.blockPositions().isEmpty()) {
            targetGroups.addAll(groups.primariesSnapshot().keySet());
        } else {
            // 归属裁决：按坐标包围盒逐区块查（/fill 只声明角点，内部区块必须一并覆盖）
            CommandOwnershipRouter.Decision decision = CommandOwnershipRouter.decide(
                    analysis.chunkRange(), dimension, registry::ownerOf, originGroup);
            if (decision.rejected()) {
                replyMessage(conn, msg, decision.rejection());
                return;
            }
            if (decision.unowned()) {
                if (originGroup != null) {
                    targetGroups.add(originGroup); // 无人持有：由发起组按需申请（原版路径）
                }
            } else {
                targetGroups.add(decision.owner());
                ownedChunks.addAll(decision.ownedChunks());
                LOGGER.debug("指令 /{} 归属组 {}（核过 {} 个持有区块）",
                        command, decision.owner(), decision.ownedChunks().size());
            }
        }

        List<Target> targets = new ArrayList<>();
        for (UUID groupId : targetGroups) {
            UUID primary = groups.primaryOf(groupId);
            ControlConnection c = primary != null ? HelloHandler.connectionOf(primary) : null;
            if (c != null && c.isOpen()) {
                targets.add(new Target(c, groupId.equals(originGroup)));
            }
        }
        if (targets.isEmpty()) {
            replyMessage(conn, msg, "没有可执行该指令的组在线");
            return;
        }

        CommandAggregator aggregator = new CommandAggregator(targets.size(),
                text -> replyMessage(conn, msg, text));
        for (Target target : targets) {
            JsonObject forward = new JsonObject();
            forward.addProperty("execute", true);
            forward.addProperty("command", command);
            forward.addProperty("sourceUuid", playerUuid.toString());
            forward.addProperty("sourceName", playerName);
            forward.addProperty("permissionLevel", forwardPermission);
            forward.addProperty("origin", target.origin());
            forward.addProperty("dimension", dimension);
            if (!ownedChunks.isEmpty()) {
                com.google.gson.JsonArray keys = new com.google.gson.JsonArray();
                for (ChunkKey key : ownedChunks) {
                    keys.add(key.asString());
                }
                forward.add("ownedChunks", keys);
            }
            target.conn().request(ControlMessage.of(MessageType.COMMAND_RELAY, forward))
                    .whenComplete((resp, err) -> {
                        boolean executed = err == null && resp != null
                                && resp.type() == MessageType.COMMAND_RELAY
                                && JsonUtil.getBoolean(resp.json(), "executed", false);
                        String message = executed
                                ? JsonUtil.getString(resp.json(), "message", "") : "";
                        if (err != null) {
                            message = "执行组未在规定时间内应答";
                        } else if (!executed && resp != null) {
                            message = JsonUtil.getString(resp.json(), "message", "");
                        }
                        aggregator.record(target.origin(), executed, message);
                    });
        }
    }

    /** 一个执行目标（连接 + 是否为发起组自己那一次）。 */
    private record Target(ControlConnection conn, boolean origin) {
    }

    /** 在线玩家清单（/list 应答文本；玩家表由 HELLO 声明建立，断连即移除）。 */
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
        // 目标名字一并下发：集成服里副客户端的实体 UUID 是原版离线派生值，
        // 接收端按成员档案 UUID 查不到实体时靠它兜底（否则私聊静默丢失）
        push.addProperty("targetName", target.playerName());
        c.send(ControlMessage.of(MessageType.CHAT_RELAY, push));
        return true;
    }

    /**
     * 从解析结果取文本参数原文（如 {@code /msg <玩家名> <文本>} 的 message）。
     * <p>
     * 原实现用 {@code command.indexOf(tokens[1])} 定位目标名，目标名与指令字面量
     * 同形时会截错（复查 G-08：{@code /tell tell hi} 会截出 {@code "tell hi"}）；
     * 解析结果里实参的字符区间是权威的。
     */
    private static String extractTextArgument(ParseResults<CommandSourceStack> parse,
                                             String argumentName) {
        String input = parse.getReader() != null ? parse.getReader().getString() : "";
        for (CommandContextBuilder<CommandSourceStack> builder = parse.getContext();
             builder != null; builder = builder.getChild()) {
            ParsedArgument<CommandSourceStack, ?> argument =
                    builder.getArguments().get(argumentName);
            if (argument == null || argument.getRange() == null
                    || argument.getRange().isEmpty()) {
                continue;
            }
            int start = Math.max(0, argument.getRange().getStart());
            int end = Math.min(input.length(), argument.getRange().getEnd());
            if (start < end) {
                return input.substring(start, end);
            }
        }
        return null;
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
        // 与指令上送同口径的身份校验：聊天帧的 playerName 不能是任意自报值
        //（否则任何已握手连接都能以管理员名义跨组发言）。帧里本来就带 playerUuid
        //（集成服实体 UUID），必须一并传入 —— 只比名字会让本组任意成员都能冒用他人
        // 名义发言；带上 UUID 后"配对判据 + 离线派生化校验"才真正生效。
        UUID senderUuid = parseUuid(JsonUtil.getString(msg.json(), "playerUuid", ""));
        if (ConnectionIdentity.canonicalMemberId(groups, sender, senderUuid, playerName) == null) {
            LOGGER.warn("拒绝越权聊天上送: 连接 {} 声称玩家 {}（{}）",
                    sender, playerName, senderUuid);
            return;
        }
        UUID originGroup = groups.groupOf(sender);
        // 服务端留痕（规范：服务器和客户端的消息需要让所有客户端看到 —— 服务端是汇聚点）
        LOGGER.info("[跨组聊天] <{}> {}", playerName, text);

        JsonObject push = new JsonObject();
        push.addProperty("playerName", playerName);
        push.addProperty("text", text);
        for (Map.Entry<UUID, UUID> e : groups.primariesSnapshot().entrySet()) {
            if (originGroup != null && e.getKey().equals(originGroup)) {
                continue; // 发起组已看过原始聊天
            }
            ControlConnection c = HelloHandler.connectionOf(e.getValue());
            if (c != null && c.isOpen()) {
                c.send(ControlMessage.of(MessageType.CHAT_RELAY, push.deepCopy()));
            }
        }
    }

    // ------------------------------------------------------------ 工具

    /** 维度名 → ServerLevel（非法/不存在返回 null）。 */
    private ServerLevel resolveLevel(String dimension) {
        try {
            ResourceLocation location = ResourceLocation.tryParse(dimension);
            return location == null ? null
                    : server.getLevel(ResourceKey.create(Registries.DIMENSION, location));
        } catch (Exception e) {
            return null; // 入站不可信
        }
    }

    private static void replyMessage(ControlConnection conn, ControlMessage msg, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("handled", true);
        out.addProperty("message", message == null ? "" : message);
        conn.send(msg.reply(MessageType.COMMAND_RELAY, out, null));
    }

    /**
     * 回原版报错：文本按原版原文（{@code rawMessage}），另附可本地化的组件 JSON
     * （{@code messageJson}）—— 客户端优先用它，这样"未知指令/权限不足"这类
     * 原版文案在玩家端仍是本地语言，而不是服务端的英文兜底。
     */
    private static void replyError(ControlConnection conn, ControlMessage msg,
                                   CommandSyntaxException error) {
        JsonObject out = new JsonObject();
        out.addProperty("handled", true);
        String raw = error.getRawMessage() != null
                ? error.getRawMessage().getString() : "指令被拒绝";
        out.addProperty("message", raw);
        try {
            Component component = ComponentUtils.fromMessage(error.getRawMessage());
            out.addProperty("messageJson",
                    net.minecraft.network.chat.Component.Serializer.toJson(component));
        } catch (Exception ignored) {
            // 序列化失败只影响本地化，文本已经给出
        }
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
