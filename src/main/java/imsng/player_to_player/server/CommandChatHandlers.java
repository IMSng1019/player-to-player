package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import com.mojang.authlib.GameProfile;
import com.mojang.brigadier.ParseResults;
import com.mojang.brigadier.context.CommandContextBuilder;
import com.mojang.brigadier.context.ParsedArgument;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import imsng.player_to_player.group.CommandFeedback;
import imsng.player_to_player.group.CommandRouting;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.TokenBucket;
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
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

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
 *       按玩家表解析坐标、由发起端本地传送；{@code /msg <玩家名> <文本>} 跨组投递；</li>
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

    /** 连接身份（clientId）→ 令牌桶。 */
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
    }

    // ------------------------------------------------------------ 指令上链

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
        // ---- 身份自洽（复查 G-06）：上报的玩家必须是本连接所在组的成员 ----
        if (!ConnectionIdentity.isOwnGroupMember(groups, requester, playerUuid, playerName)) {
            LOGGER.warn("拒绝越权指令上送: 连接 {} 声称玩家 {}（{}）",
                    requester, playerUuid, playerName);
            conn.send(error(msg, "not_authorized", "上报的玩家身份与本连接所在组不符"));
            return;
        }
        if (!uplinkBuckets.computeIfAbsent(requester,
                id -> new TokenBucket(UPLINK_BURST, UPLINK_REFILL_NANOS)).tryAcquire()) {
            conn.send(error(msg, "too_many_commands", "指令过于频繁，请稍候再试"));
            return;
        }
        UUID originGroup = groups.groupOf(requester);
        String dimension = JsonUtil.getString(msg.json(), "dimension", "minecraft:overworld");
        double x = JsonUtil.getDouble(msg.json(), "x", 0.0);
        double y = JsonUtil.getDouble(msg.json(), "y", 0.0);
        double z = JsonUtil.getDouble(msg.json(), "z", 0.0);

        // ---- 权限裁定：物理服务端 ops 名单是唯一权威（决策 3 的实现要点）----
        int realPermission = server.getProfilePermissions(
                new GameProfile(playerUuid, playerName));
        ServerLevel level = resolveLevel(dimension);
        if (level == null) {
            level = server.overworld();
        }
        CommandSourceStack source = new CommandSourceStack(CommandSource.NULL,
                new Vec3(x, y, z), Vec2.ZERO, level, realPermission, playerName,
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
        // ---- 特例二：/tp|/teleport <玩家名> —— 玩家表解析坐标，发起方本地执行 ----
        if (tokens.length == 2 && ("tp".equalsIgnoreCase(tokens[0])
                || "teleport".equalsIgnoreCase(tokens[0]))) {
            PlayerTable.PlayerEntry target = players.byName(tokens[1]);
            if (target != null && realPermission >= 2) {
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
            // 目标不在玩家表：落到下面的通则（各组的虚拟源没有实体，会干净报错而不是错搬人）
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
        if (!isGroupExecutable(command, source)) {
            executeOnServer(conn, msg, command, source, playerName);
            return;
        }

        // ---- 0-2 级指令：形状规则 + 归属预检 → 分组广播执行 ----
        route(conn, msg, originGroup, command, playerUuid, playerName, dimension, parse,
                realPermission);
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
        CommandSourceStack capped = realSource.withPermission(FORWARD_PERMISSION_CAP);
        ParseResults<CommandSourceStack> parse =
                server.getCommands().getDispatcher().parse(command, capped);
        try {
            Commands.validateParseResults(parse);
            return true;
        } catch (CommandSyntaxException e) {
            return false;
        }
    }

    /** 3/4 级指令的服务端执行（决策 3）：结果文案回给发起者，并刷新权限名单。 */
    private void executeOnServer(ControlConnection conn, ControlMessage msg, String command,
                                 CommandSourceStack realSource, String playerName) {
        server.execute(() -> {
            CommandFeedback feedback = new CommandFeedback();
            CommandSourceStack collecting = realSource.withSource(feedback);
            try {
                server.getCommands().performPrefixedCommand(collecting, command);
            } catch (Exception e) {
                LOGGER.warn("服务端执行指令异常: /{}", command, e);
            }
            String message = feedback.joined();
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
        if (analysis.contextBound()) {
            if (originGroup != null) {
                targetGroups.add(originGroup);
            }
        } else if (analysis.blockPositions().isEmpty()) {
            targetGroups.addAll(groups.primariesSnapshot().keySet());
        } else {
            CommandRouting.ChunkRange range = analysis.chunkRange();
            if (range == null) {
                replyMessage(conn, msg, "无法确定指令影响的区块范围，未执行");
                return;
            }
            if (range.count() > CommandRouting.MAX_ROUTING_CHUNKS) {
                replyMessage(conn, msg, "指令影响的区块范围过大（" + range.count()
                        + " 个区块），未执行");
                return;
            }
            // 按包围盒逐区块查归属：/fill 只声明角点，内部方块必须一并覆盖
            Set<UUID> owners = new LinkedHashSet<>();
            for (int cx = range.minX(); cx <= range.maxX(); cx++) {
                for (int cz = range.minZ(); cz <= range.maxZ(); cz++) {
                    UUID owner = registry.ownerOf(new ChunkKey(dimension, cx, cz));
                    if (owner != null) {
                        owners.add(owner);
                        if (owners.size() > 1) {
                            break;
                        }
                    }
                }
                if (owners.size() > 1) {
                    break;
                }
            }
            if (owners.isEmpty()) {
                if (originGroup != null) {
                    targetGroups.add(originGroup); // 无人持有：由发起组按需申请（原版路径）
                }
            } else if (owners.size() == 1) {
                UUID owner = owners.iterator().next();
                targetGroups.add(owner);
                for (int cx = range.minX(); cx <= range.maxX(); cx++) {
                    for (int cz = range.minZ(); cz <= range.maxZ(); cz++) {
                        ChunkKey key = new ChunkKey(dimension, cx, cz);
                        if (owner.equals(registry.ownerOf(key))) {
                            ownedChunks.add(key);
                        }
                    }
                }
                LOGGER.debug("指令 /{} 归属组 {}（覆盖 {} 个区块）", command, owner, range.count());
            } else {
                replyMessage(conn, msg,
                        "指令跨越了多个组的区块归属（" + owners.size() + " 组），未执行");
                return;
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
