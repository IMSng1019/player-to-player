package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import com.mojang.brigadier.ParseResults;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.commands.CommandResultCallback;
import net.minecraft.commands.CommandSource;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.core.registries.Registries;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 指令逐级路由（主客户端侧，Phase 4；规范"服务器的指令处理"）。
 * <p>
 * 与 {@code mixin.CommandsMixin} 配合完成"逐级处理再分级下放"的客户端半边：
 * <ol>
 *   <li><b>本地探针</b>：{@link #onPerformCommand}/{@link #afterPerformCommand} 在
 *       集成服务端执行玩家指令时挂结果回调 —— 执行成功走原版，无事发生；
 *       执行失败（含解析失败：回调从未发火）且是玩家发出 → 上送服务端
 *       （规范基础"在原版中不合理的指令是不会被执行的"——本地能执行的指令
 *       天然不上送）。副客户端的指令经隧道在本集成服务端执行，同一探针覆盖，
 *       即规范链路的"副→主"一级；</li>
 *   <li><b>上送</b>：COMMAND_RELAY 请求 → 服务端按玩家表特例处理（跨组 tp/list/msg）
 *       或泛化下放到其他主客户端；应答带 handled/message/action。本地报错原版
 *       已即时显示（工程取舍：不压制原版反馈，避免吞掉正当提示；远端成功时
 *       补发执行端消息 —— 规范"如果被执行 则将执行端的消息返回发出指令的端"）；</li>
 *   <li><b>转发执行</b>：{@link #register} 处理服务端转发来的其他组指令 —— 以
 *       收集输出的虚拟指令源在本集成服务端执行（权限取服务端下发值并钳制到 2，
 *       信任链：物理服务端 ops 名单是权威），执行结果与输出文本回传。</li>
 * </ol>
 * 线程模型：探针在服务器主线程（performCommand 所在线程，1.20.4 指令队列在
 * 调用内同步跑完，RETURN 时结果已定）；上送/应答在 io/Netty 线程，回主线程投递。
 */
public final class CommandRelayClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/cmd-relay");

    /** 同一玩家两次上送的最小间隔（毫秒）：打错指令连打不该刷爆服务端。 */
    private static final long ESCALATE_INTERVAL_MILLIS = 2_000;

    /** 上送指令长度上限（与服务端一致）。 */
    private static final int MAX_COMMAND_LENGTH = 256;

    /** 转发执行的权限钳制（与服务端 FORWARD_PERMISSION_CAP 口径一致）。 */
    private static final int FORWARD_PERMISSION_CAP = 2;

    /**
     * 执行结果探针：挂在指令源的结果回调上。1.20.4 的指令执行经
     * {@code ContextChain.runExecutable} 把每个叶子命令的成败回报给源回调
     * （已用 javap 核实 {@code CommandSourceStack.callback()/withCallback} 与
     * {@code CommandResultCallback.chain} 签名）——任一叶子成功即视为执行成功
     * （选择器多目标指令部分成功也算成功，与原版反馈口径一致）。
     */
    private static final class Probe implements CommandResultCallback {
        final UUID playerUuid;
        final String playerName;
        final String command;
        volatile boolean anySuccess;

        Probe(UUID playerUuid, String playerName, String command) {
            this.playerUuid = playerUuid;
            this.playerName = playerName;
            this.command = command;
        }

        @Override
        public void onResult(boolean success, int result) {
            if (success) {
                anySuccess = true;
            }
        }
    }

    /** 跳过标记（非玩家指令/未接管/限速中）：保证 begin/after 一一配对。 */
    private static final Probe SKIP = new Probe(null, null, null);

    /**
     * 每线程的探针栈：performCommand 理论上可重入（函数/数据包触发），
     * 栈结构保证 HEAD 与 RETURN 注入点严格配对。
     */
    private static final ThreadLocal<Deque<Probe>> PROBES =
            ThreadLocal.withInitial(ArrayDeque::new);

    /** 限速表：玩家 → 上次上送时刻。 */
    private static final Map<UUID, Long> lastEscalation = new ConcurrentHashMap<>();

    private CommandRelayClient() {
    }

    /** 世界会话拆除时清理限速状态（幂等）。 */
    public static void reset() {
        lastEscalation.clear();
    }

    // ------------------------------------------------------------ 本地探针（Mixin 入口）

    /**
     * performCommand HEAD（CommandsMixin @ModifyVariable）：符合条件的玩家指令
     * 换上挂了探针回调的指令源；其余场景原样放行并压入 SKIP 保证配对。
     */
    public static ParseResults<CommandSourceStack> onPerformCommand(
            ParseResults<CommandSourceStack> parseResults, String command) {
        Deque<Probe> stack = PROBES.get();
        try {
            CommandSourceStack source = parseResults.getContext().getSource();
            MinecraftServer server = source.getServer();
            Entity entity = source.getEntity();
            // 只探测被接管集成服务端上的玩家指令（转发执行的虚拟源 entity=null，
            // 天然被排除 —— 无递归上送风险）
            if (!(entity instanceof ServerPlayer player) || !GroupRuntime.isManagedServer(server)) {
                stack.push(SKIP);
                return parseResults;
            }
            ControlConnection conn = GroupRuntime.conn();
            if (conn == null || !conn.isOpen() || command.length() > MAX_COMMAND_LENGTH) {
                stack.push(SKIP);
                return parseResults;
            }
            Long last = lastEscalation.get(player.getUUID());
            if (last != null && System.currentTimeMillis() - last < ESCALATE_INTERVAL_MILLIS) {
                stack.push(SKIP);
                return parseResults;
            }
            Probe probe = new Probe(player.getUUID(),
                    player.getGameProfile().getName(), command);
            stack.push(probe);
            // chain 保留源上已有的回调（/execute store 等），探针只旁听不改语义
            return Commands.mapSource(parseResults,
                    s -> s.withCallback(CommandResultCallback.chain(s.callback(), probe)));
        } catch (Exception e) {
            // 探针失败绝不能影响原版指令执行：压 SKIP 放行
            LOGGER.warn("指令探针挂接失败，本条指令不参与逐级路由", e);
            stack.push(SKIP);
            return parseResults;
        }
    }

    /**
     * performCommand RETURN（CommandsMixin @Inject）：弹出探针结算。
     * 1.20.4 的执行队列在 performCommand 内同步跑完，此刻成败已定；
     * 回调从未发火 = 解析失败（unknown command 等），同样按失败上送。
     */
    public static void afterPerformCommand() {
        Probe probe = PROBES.get().poll();
        if (probe == null || probe == SKIP || probe.anySuccess) {
            return;
        }
        lastEscalation.put(probe.playerUuid, System.currentTimeMillis());
        ThreadPools.io().execute(() -> escalate(probe));
    }

    // ------------------------------------------------------------ 上送

    /** 上送失败指令并按应答行动（io 线程）。 */
    private static void escalate(Probe probe) {
        ControlConnection conn = GroupRuntime.conn();
        MinecraftServer server = GroupRuntime.server();
        if (conn == null || !conn.isOpen() || server == null) {
            return;
        }
        JsonObject json = new JsonObject();
        json.addProperty("playerUuid", probe.playerUuid.toString());
        json.addProperty("playerName", probe.playerName);
        json.addProperty("command", probe.command);
        conn.request(ControlMessage.of(MessageType.COMMAND_RELAY, json))
                .whenComplete((resp, err) -> {
                    if (err != null || resp == null || resp.type() != MessageType.COMMAND_RELAY
                            || !JsonUtil.getBoolean(resp.json(), "handled", false)) {
                        // 全链报错：规范"返回本地的报错信息"——原版已即时显示，无补发
                        return;
                    }
                    String message = JsonUtil.getString(resp.json(), "message", "");
                    if ("teleport".equals(JsonUtil.getString(resp.json(), "action", ""))) {
                        performTeleport(server, probe.playerUuid, resp.json(), message);
                    } else if (!message.isEmpty()) {
                        deliverMessage(server, probe.playerUuid, message);
                    }
                });
    }

    /**
     * 跨组 tp（服务端按玩家表解析出目标坐标）：在本集成服务端把玩家传送过去
     * （可跨维度）。落点若属其他组的领地，加载门控的申请被拒会触发预连接/合并，
     * 分离监视会在 10s 后处理留守成员 —— 规范"玩家大幅度移动"的既有机制接管。
     */
    private static void performTeleport(MinecraftServer server, UUID playerUuid,
                                        JsonObject json, String message) {
        String dimension = JsonUtil.getString(json, "dimension", "");
        double x = JsonUtil.getDouble(json, "x", Double.NaN);
        double y = JsonUtil.getDouble(json, "y", Double.NaN);
        double z = JsonUtil.getDouble(json, "z", Double.NaN);
        if (dimension.isEmpty() || Double.isNaN(x) || Double.isNaN(y) || Double.isNaN(z)) {
            return; // 应答不完整：放弃（本地报错已显示过）
        }
        server.execute(() -> {
            ServerPlayer player = server.getPlayerList().getPlayer(playerUuid);
            if (player == null) {
                return; // 玩家已下线
            }
            ServerLevel level;
            try {
                level = server.getLevel(
                        ResourceKey.create(Registries.DIMENSION, new ResourceLocation(dimension)));
            } catch (Exception e) {
                return; // 维度名非法（入站不可信）
            }
            if (level == null) {
                return;
            }
            player.teleportTo(level, x, y, z, player.getYRot(), player.getXRot());
            if (!message.isEmpty()) {
                player.sendSystemMessage(Component.literal(message));
            }
            LOGGER.info("跨组 tp 已执行: 玩家 {} → {} ({}, {}, {})",
                    playerUuid, dimension, (int) x, (int) y, (int) z);
        });
    }

    /** 把执行端的消息投递给发起指令的玩家（规范"将执行端的消息返回发出指令的端"）。 */
    private static void deliverMessage(MinecraftServer server, UUID playerUuid, String message) {
        server.execute(() -> {
            ServerPlayer player = server.getPlayerList().getPlayer(playerUuid);
            if (player != null) {
                player.sendSystemMessage(Component.literal(message));
            }
        });
    }

    // ------------------------------------------------------------ 转发执行（入站）

    /** 收集输出的虚拟指令源（转发执行的反馈文本要回传发起端）。 */
    private static final class CollectingSource implements CommandSource {
        final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        public void sendSystemMessage(Component component) {
            if (lines.size() < 8) { // 反馈行数上限：回传消息不做全量控制台转储
                lines.add(component.getString());
            }
        }

        @Override
        public boolean acceptsSuccess() {
            return true;
        }

        @Override
        public boolean acceptsFailure() {
            return true;
        }

        @Override
        public boolean shouldInformAdmins() {
            return false; // 不打扰本组管理员：这是别的组的指令回执
        }

        String joined() {
            String joined = String.join("\n", lines);
            return joined.length() > 512 ? joined.substring(0, 512) : joined;
        }
    }

    /** 注册入站 COMMAND_RELAY 处理器（世界会话建立时挂到控制连接上）。 */
    public static void register(HandlerRegistry reg) {
        reg.on(MessageType.COMMAND_RELAY, (conn, msg) -> {
            if (!JsonUtil.getBoolean(msg.json(), "execute", false)) {
                return; // 上送应答经 _rid 直达 future，不会进 handler；防御性忽略
            }
            MinecraftServer server = GroupRuntime.server();
            String command = JsonUtil.getString(msg.json(), "command", "").trim();
            if (server == null || command.isEmpty() || command.length() > MAX_COMMAND_LENGTH) {
                JsonObject out = new JsonObject();
                out.addProperty("executed", false);
                conn.send(msg.reply(MessageType.COMMAND_RELAY, out, null));
                return;
            }
            String sourceName = JsonUtil.getString(msg.json(), "sourceName", "?");
            // 权限：信任服务端下发值（物理服务端 ops 名单是权威），本地再钳制一次
            int permission = Math.max(0, Math.min(FORWARD_PERMISSION_CAP,
                    JsonUtil.getInt(msg.json(), "permissionLevel", 0)));
            server.execute(() -> {
                CollectingSource collector = new CollectingSource();
                Probe probe = new Probe(null, sourceName, command);
                ServerLevel level = server.overworld();
                // 虚拟源：以发起玩家为名、主世界出生点为位（跨组指令没有本地实体锚点），
                // entity=null 使 CommandsMixin 的探针天然跳过（无递归上送）
                CommandSourceStack stack = new CommandSourceStack(collector,
                        Vec3.atBottomCenterOf(level.getSharedSpawnPos()), Vec2.ZERO, level,
                        permission, sourceName, Component.literal(sourceName), server, null)
                        .withCallback(probe);
                try {
                    server.getCommands().performPrefixedCommand(stack, command);
                } catch (Exception e) {
                    LOGGER.warn("转发指令执行异常: /{}", command, e);
                }
                JsonObject out = new JsonObject();
                out.addProperty("executed", probe.anySuccess);
                out.addProperty("message", collector.joined());
                conn.send(msg.reply(MessageType.COMMAND_RELAY, out, null));
                if (probe.anySuccess) {
                    LOGGER.info("已代执行其他组指令: /{}（发起者 {}）", command, sourceName);
                }
            });
        });
    }
}
