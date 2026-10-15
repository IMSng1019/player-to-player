package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import imsng.player_to_player.util.TokenBucket;
import net.minecraft.commands.CommandResultCallback;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.core.registries.Registries;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
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

/**
 * 指令全量上链的客户端半边（规范"服务器的指令处理"；口径见
 * {@code docs/plans/2026-10-15-command-routing-fix-plan.md} 附录 P / P.2）。
 *
 * <h2>上链（拦截面）</h2>
 * 玩家在聊天栏敲的<b>每一条</b>指令，在集成服务端的
 * {@code ServerGamePacketListenerImpl.performChatCommand} 里于本地执行前被拦下
 * （签名校验已经完成，见 {@code client.mixin.CommandDispatchMixin}），
 * 无条件上送服务端；本地<b>不执行</b>。副客户端的指令经隧道汇聚到主客户端的
 * 集成服务端，同一入口覆盖 —— 这就是规范链路"副→主"那一级。
 * <p>
 * 拦截的判据是"玩家敲的指令"（数据包入口），不是 {@code Commands.performCommand}：
 * 告示牌、命令方块、数据包函数与第三方程序化调用都不经过这里，它们照旧在本地
 * 按服务端下发的真实权限位执行（{@link LocalPermissions}）。
 *
 * <h2>接收面（转发执行）</h2>
 * 服务端按形状规则与区块归属决定"谁执行"后，向目标组的主客户端下发
 * {@code execute=true} 的 COMMAND_RELAY：
 * <ul>
 *   <li>{@code origin=true}：发起组自己那一次。指令源是<b>发起玩家实体</b>
 *       （{@code @s}/{@code @p}/{@code ~} 保持原版语义）；</li>
 *   <li>{@code origin=false}：其余组。指令源是虚拟源（该维度出生点、entity=null），
 *       只服务世界状态类/绝对坐标/具名目标类指令；执行前再做一次<b>区块归属复核</b>
 *       （本地 claim 集），把服务端预检与执行之间的归属变化兜住 —— 集成服主线程
 *       对"本组不持有"的区块会永久忙等，这道复核是防冻结的最后一道闸。</li>
 * </ul>
 * 执行反馈一律经 {@link CommandFeedback} 收集后随应答回传，绝不直发玩家：
 * 玩家看到的输出只有服务端聚合后的那条回执。
 */
public final class CommandRelayClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/cmd-relay");

    /** 指令长度上限（与服务端一致，入站出站都查）。 */
    private static final int MAX_COMMAND_LENGTH = 256;

    /** 转发执行的权限钳制（与服务端 FORWARD_PERMISSION_CAP 口径一致）。 */
    private static final int FORWARD_PERMISSION_CAP = 2;

    /** 令牌桶容量（突发额度）：连续敲几条指令不该被限速。 */
    private static final int TOKEN_BURST = 12;

    /** 令牌补充间隔（纳秒）：约每秒 4 条，打错指令连打不会刷爆服务端。 */
    private static final long TOKEN_REFILL_NANOS = 250_000_000L;

    /** 每玩家的令牌桶（{@link #reset()} 随会话清理）。 */
    private static final Map<UUID, TokenBucket> buckets = new ConcurrentHashMap<>();

    /**
     * 服务端是否声明支持"全量上链"语义（HELLO_ACK 的 {@code commandRouting} 字段）。
     * <p>
     * 混版本安全网：老服务端按旧语义理解上送（只把它当"本地失败才上送"的补充），
     * 新客户端若已经取消本地执行就会漏执行 —— 所以看不到这个能力位就退回老行为
     * （本地执行、失败才上送）。
     */
    private static volatile boolean serverRoutingSupported;

    /** 每玩家暂存的回执（集成服在切换窗口内不可用时的补投队列）。 */
    private static final Map<UUID, Deque<Component>> pendingFeedback = new ConcurrentHashMap<>();

    /** 暂存队列上限（每玩家）。 */
    private static final int MAX_PENDING_FEEDBACK = 8;

    /** 执行成败探针（回传 executed 字段；上下文链保证 /execute 等形态也能收敛）。 */
    private static final class Probe implements CommandResultCallback {
        volatile boolean anySuccess;

        @Override
        public void onResult(boolean success, int result) {
            if (success) {
                anySuccess = true;
            }
        }
    }

    private CommandRelayClient() {
    }

    /** 世界会话拆除时清限速与暂存状态（幂等）。 */
    public static void reset() {
        buckets.clear();
        pendingFeedback.clear();
        serverRoutingSupported = false;
    }

    /** 记录服务端的指令路由能力位（HELLO_ACK 收到时调用）。 */
    public static void setServerRoutingSupported(boolean supported) {
        serverRoutingSupported = supported;
    }

    /** 当前服务端是否声明支持"全量上链"（日志/诊断用）。 */
    public static boolean isServerRoutingSupported() {
        return serverRoutingSupported;
    }

    // ------------------------------------------------------------ 拦截面（玩家敲的指令）

    /**
     * 拦截一条玩家指令：能上链就上链并返回 true（调用方取消本地执行），
     * 否则返回 false（退回原版直通）。
     * <p>
     * 必须在集成服务端主线程调用（由 mixin 在 {@code performChatCommand} 内保证）。
     * 这里只做判断与发包调度，绝不阻塞主线程。
     *
     * @param player  发起指令的玩家（主客户端本人或其组里的副客户端）
     * @param command 指令原文（不含前导斜杠）
     * @return true = 已上链，本地不得执行
     */
    public static boolean interceptPlayerCommand(ServerPlayer player, String command) {
        try {
            if (player == null || command == null || command.isBlank()
                    || command.length() > MAX_COMMAND_LENGTH) {
                return false;
            }
            MinecraftServer server = player.getServer();
            if (server == null || !GroupRuntime.isManagedServer(server)) {
                return false; // 非组世界（单人/物理服）：原版直通
            }
            if (!serverRoutingSupported) {
                return false; // 服务端未声明该能力（老服务端）：退回老行为，避免漏执行
            }
            ControlConnection conn = GroupRuntime.conn();
            if (conn == null || !conn.isOpen()) {
                return false; // 控制连接不可用：退回本地执行（宁可本地生效，也不要指令凭空消失）
            }
            if (!buckets.computeIfAbsent(player.getUUID(),
                    id -> new TokenBucket(TOKEN_BURST, TOKEN_REFILL_NANOS)).tryAcquire()) {
                // 限速命中：本次不上链，退回本地执行（此时链路上没有第二条执行路径，
                // 不存在"本地 + 广播"重复执行），而不是把指令整条丢掉
                player.sendSystemMessage(Component.literal(
                        "[P2P] 指令过于频繁，本次仅在本地执行"));
                return false;
            }
            String normalized = command.startsWith("/") ? command.substring(1) : command;
            JsonObject json = new JsonObject();
            json.addProperty("playerUuid", player.getUUID().toString());
            json.addProperty("playerName", player.getGameProfile().getName());
            json.addProperty("command", normalized);
            json.addProperty("dimension", player.serverLevel().dimension().location().toString());
            json.addProperty("x", player.getX());
            json.addProperty("y", player.getY());
            json.addProperty("z", player.getZ());
            // 朝向：服务端据此把 ^ 局部坐标结算成与发起端一致的方块坐标（越界预检用）
            json.addProperty("yRot", player.getYRot());
            json.addProperty("xRot", player.getXRot());
            UUID playerId = player.getUUID();
            // 发包走 io 线程：Netty 写本身线程安全，但避免在服务器主线程上做排队等待
            ThreadPools.io().execute(() -> uplink(conn, playerId, json));
            return true;
        } catch (Exception e) {
            // 拦截失败绝不能吞掉玩家的指令：退回原版直通
            LOGGER.warn("指令拦截失败，本条指令按原版本地执行", e);
            return false;
        }
    }

    /** 上送并按回执行事（io 线程）。 */
    private static void uplink(ControlConnection conn, UUID playerId, JsonObject json) {
        conn.request(ControlMessage.of(MessageType.COMMAND_RELAY, json))
                .whenComplete((resp, err) -> {
                    MinecraftServer server = GroupRuntime.server();
                    if (resp != null && resp.type() == MessageType.ERROR) {
                        String code = JsonUtil.getString(resp.json(), "code", "");
                        String text = JsonUtil.getString(resp.json(), "message", "指令被服务端拒绝");
                        if ("too_many_commands".equals(code)) {
                            // 服务端在限速处就拒了，保证没有执行过 ⇒ 本地补执行一次是安全的
                            //（比"指令凭空消失"好），并给出可见文案
                            String command = JsonUtil.getString(json, "command", "");
                            scheduleLocalFallback(server, playerId, command);
                            deliverMessage(server, playerId,
                                    Component.literal("[P2P] 服务端限速，本次仅在本地执行"));
                            return;
                        }
                        deliverMessage(server, playerId, Component.literal(text));
                        return;
                    }
                    if (err != null || resp == null
                            || resp.type() != MessageType.COMMAND_RELAY) {
                        // 超时/断连：本地执行已被取消，必须给出明确交代（不回退本地执行，
                        // 否则会与广播执行竞态）
                        deliverMessage(server, playerId, "[P2P] 指令未能送达服务端：与服务端的连接不可用");
                        return;
                    }
                    JsonObject out = resp.json();
                    String message = JsonUtil.getString(out, "message", "");
                    if ("teleport".equals(JsonUtil.getString(out, "action", ""))) {
                        performTeleport(server, playerId, out, message);
                        return;
                    }
                    // 原版报错（未知指令/权限不足）带可本地化的组件 JSON：优先用它，
                    // 玩家看到的文案与真人原版一致（而不是服务端语言的兜底文本）
                    String messageJson = JsonUtil.getString(out, "messageJson", "");
                    if (!message.isEmpty() || !messageJson.isEmpty()) {
                        deliverMessage(server, playerId, decodeComponent(messageJson, message));
                    }
                });
    }

    /** 限速兜底：在本地以玩家自身指令源执行一次（服务端已确认未执行）。 */
    private static void scheduleLocalFallback(MinecraftServer server, UUID playerId,
                                              String command) {
        if (server == null || command == null || command.isBlank()) {
            return;
        }
        server.execute(() -> {
            ServerPlayer player = server.getPlayerList().getPlayer(playerId);
            if (player == null) {
                return;
            }
            try {
                server.getCommands().performPrefixedCommand(
                        player.createCommandSourceStack(), command);
            } catch (Exception e) {
                LOGGER.warn("限速兜底本地执行失败: /{}", command, e);
            }
        });
    }

    /** 组件 JSON → Component（失败回退纯文本；入站数据不可信）。 */
    private static Component decodeComponent(String json, String fallback) {
        if (json != null && !json.isEmpty()) {
            try {
                var parsed = net.minecraft.network.chat.Component.Serializer.fromJson(json);
                if (parsed != null) {
                    return parsed;
                }
            } catch (Exception ignored) {
                // 落到纯文本
            }
        }
        return Component.literal(fallback == null ? "" : fallback);
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
            return; // 应答不完整：放弃（服务端文案已单独回给玩家）
        }
        if (server == null) {
            // 切换窗口：跨组 tp 这一次没法做，给玩家一句明确交代（不静默）
            deliverMessage((MinecraftServer) null, playerUuid,
                    Component.literal("[P2P] 跨组传送未执行：组世界正在切换"));
            return;
        }
        server.execute(() -> {
            ServerPlayer player = server.getPlayerList().getPlayer(playerUuid);
            if (player == null) {
                return; // 玩家已下线
            }
            ServerLevel level = resolveLevel(server, dimension);
            if (level == null) {
                return; // 维度名非法（入站不可信）
            }
            player.teleportTo(level, x, y, z, player.getYRot(), player.getXRot());
            flushPendingFeedback(player); // 同一玩家在窗口内暂存的回执一并补投
            if (!message.isEmpty()) {
                player.sendSystemMessage(Component.literal(message));
            }
            LOGGER.info("跨组 tp 已执行: 玩家 {} → {} ({}, {}, {})",
                    playerUuid, dimension, (int) x, (int) y, (int) z);
        });
    }

    /** 把执行端的消息投递给发起指令的玩家（规范"将执行端的消息返回发出指令的端"）。 */
    private static void deliverMessage(MinecraftServer server, UUID playerUuid, String message) {
        deliverMessage(server, playerUuid,
                Component.literal(message == null ? "" : message));
    }

    /**
     * 投递已构造好的组件（本地化报错走这条）。
     * <p>
     * 回执到达时**集成服务端可能已经不在**（合并让出/退出事务窗口：玩家还在游戏里，
     * 但本地世界已经交出去了）。此时本地执行早已被取消，回执是玩家唯一的输出 ——
     * 丢一次就是"指令凭空消失"，所以暂存起来，等玩家重新进入某个组世界时补投
     * （{@link #flushPendingFeedback}）。
     */
    private static void deliverMessage(MinecraftServer server, UUID playerUuid,
                                       Component message) {
        if (message == null) {
            return;
        }
        if (server == null) {
            queueFeedback(playerUuid, message);
            return;
        }
        server.execute(() -> {
            ServerPlayer player = server.getPlayerList().getPlayer(playerUuid);
            if (player != null) {
                player.sendSystemMessage(message);
            } else {
                // 玩家已不在本集成服（切换中/已下线）：暂存，等他回来再给
                queueFeedback(playerUuid, message);
            }
        });
    }

    /** 暂存回执（每玩家有界；切换窗口通常只有一两条）。 */
    private static void queueFeedback(UUID playerId, Component message) {
        if (playerId == null) {
            return;
        }
        Deque<Component> queue = pendingFeedback.computeIfAbsent(
                playerId, id -> new ArrayDeque<>());
        synchronized (queue) {
            if (queue.size() >= MAX_PENDING_FEEDBACK) {
                queue.pollFirst();
            }
            queue.addLast(message);
        }
    }

    /**
     * 补投暂存的回执（玩家进入组世界时调用；须在集成服务端主线程）。
     * 覆盖"合并让出/退出事务窗口内收到回执"的缺口。
     */
    public static void flushPendingFeedback(ServerPlayer player) {
        if (player == null) {
            return;
        }
        Deque<Component> queue = pendingFeedback.remove(player.getUUID());
        if (queue == null) {
            return;
        }
        synchronized (queue) {
            Component message;
            while ((message = queue.pollFirst()) != null) {
                player.sendSystemMessage(message);
            }
        }
    }

    // ------------------------------------------------------------ 接收面（服务端下发的转发执行）

    /** 注册入站 COMMAND_RELAY 处理器（世界会话建立时挂到控制连接上）。 */
    public static void register(HandlerRegistry reg) {
        reg.on(MessageType.COMMAND_RELAY, (conn, msg) -> {
            if (!JsonUtil.getBoolean(msg.json(), "execute", false)) {
                return; // 上送应答经 _rid 直达 future，不会进 handler；防御性忽略
            }
            MinecraftServer server = GroupRuntime.server();
            String command = JsonUtil.getString(msg.json(), "command", "").trim();
            if (server == null || command.isEmpty() || command.length() > MAX_COMMAND_LENGTH) {
                reply(conn, msg, false, "指令不可执行");
                return;
            }
            boolean origin = JsonUtil.getBoolean(msg.json(), "origin", false);
            String sourceName = JsonUtil.getString(msg.json(), "sourceName", "?");
            UUID sourceUuid = parseUuid(JsonUtil.getString(msg.json(), "sourceUuid", ""));
            String dimension = JsonUtil.getString(msg.json(), "dimension", "minecraft:overworld");
            // 权限：信任服务端下发值（物理服务端 ops 名单是权威），本地再钳制一次
            int permission = Math.max(0, Math.min(FORWARD_PERMISSION_CAP,
                    JsonUtil.getInt(msg.json(), "permissionLevel", 0)));
            server.execute(() -> execute(server, conn, msg, command, origin,
                    sourceUuid, sourceName, dimension, permission));
        });
    }

    /** 转发执行实现（集成服务端主线程）。 */
    private static void execute(MinecraftServer server, ControlConnection conn,
                                ControlMessage msg, String command, boolean origin,
                                UUID sourceUuid, String sourceName, String dimension,
                                int permission) {
        if (GroupRuntime.isExitDraining()) {
            // 退出事务冻结期：本组的认领会被注册表直接拒绝（exitFrozenGroups），
            // 执行指令只会撞上"门控无限重试"的忙等面 —— 干净跳过并说明
            reply(conn, msg, false, "本组正在退出世界，已跳过执行");
            return;
        }
        // 区块归属复核：只要服务端下发了"持有区块"清单就要复核（含发起组自己那一次）
        String verdict = verifyOwnedChunks(msg.json());
        if (verdict != null) {
            LOGGER.info("拒绝执行越界的转发指令（{}）: /{}", verdict, command);
            reply(conn, msg, false, verdict);
            return;
        }
        CommandFeedback feedback = new CommandFeedback();
        Probe probe = new Probe();
        CommandSourceStack stack;
        if (origin) {
            // 发起组自己那一次：指令源必须是发起玩家实体，原版语义（@s/@p/~）才成立
            ServerPlayer player = sourceUuid != null
                    ? server.getPlayerList().getPlayer(sourceUuid) : null;
            if (player == null && sourceName != null && !sourceName.isBlank()) {
                player = server.getPlayerList().getPlayerByName(sourceName);
            }
            if (player == null) {
                reply(conn, msg, false, "发起玩家已不在本组世界");
                return;
            }
            // 维度核对：发起分支用的是玩家**此刻**所在等级，而服务端的越界预检按上链时
            // 自报的维度做 —— 两者不一致时预检查的是别的维度的注册表，等于没查
            //（穿传送门、或改装客户端谎报维度都会走到这里）→ 干净拒绝，不放行
            String actualDimension = player.serverLevel().dimension().location().toString();
            if (!actualDimension.equals(dimension)) {
                reply(conn, msg, false, "玩家所在维度与上链时不一致（" + dimension
                        + " -> " + actualDimension + "），已跳过执行");
                return;
            }
            stack = player.createCommandSourceStack()
                    .withSource(feedback)
                    .withPermission(permission)
                    .withCallback(probe);
        } else {
            ServerLevel level = resolveLevel(server, dimension);
            if (level == null) {
                level = server.overworld();
            }
            // 虚拟源：以发起玩家为名、出生点为位（跨组指令没有本地实体锚点），
            // entity=null 使涉及执行者自身的指令在这里干净报错而不是错落到本组
            stack = new CommandSourceStack(feedback,
                    Vec3.atBottomCenterOf(level.getSharedSpawnPos()), Vec2.ZERO, level,
                    permission, sourceName, Component.literal(sourceName), server, null)
                    .withCallback(probe);
        }
        try {
            server.getCommands().performPrefixedCommand(stack, command);
        } catch (Exception e) {
            LOGGER.warn("转发指令执行异常: /{}", command, e);
        }
        reply(conn, msg, probe.anySuccess, feedback.joined());
        if (probe.anySuccess) {
            LOGGER.info("已代执行{}指令: /{}（发起者 {}）",
                    origin ? "发起组" : "其他组", command, sourceName);
        }
    }

    /**
     * 接收端区块归属复核：服务端随转发下发的 {@code ownedChunks}（它按包围盒核出的
     * "本组持有"清单）必须仍是本组持有。
     * <p>
     * 复核的是"服务端预检 → 实际执行"之间的归属变化（合并/分离/退出事务期间会发生）。
     * 不通过则<b>不执行</b>：集成服主线程一旦碰触"本组不持有"的区块，加载 future
     * 会永远悬置，主线程在 {@code managedBlock + join} 里永久忙等（集成服没有 watchdog）。
     * <p>
     * 清单为空/缺省（无坐标指令，或"无人持有 → 交回发起组按需申请"）时无需复核：
     * 那种情形走的正是原版本地执行的同一条加载门控路径。
     *
     * @return null = 放行；否则为拒绝文案
     */
    private static String verifyOwnedChunks(JsonObject json) {
        List<ChunkKey> keys = CommandRouting.parseChunkKeys(json.get("ownedChunks"));
        ChunkClaimClient claims = GroupRuntime.claims();
        if (claims == null) {
            return null; // 尚未接入 claim 客户端：由服务端预检负责（不应发生）
        }
        return CommandRouting.verifyOwnedChunks(keys, claims::isClaimed);
    }

    /** 维度名 → ServerLevel；非法/不存在返回 null。 */
    private static ServerLevel resolveLevel(MinecraftServer server, String dimension) {
        if (server == null || dimension == null || dimension.isBlank()) {
            return null;
        }
        try {
            ResourceLocation location = ResourceLocation.tryParse(dimension);
            if (location == null) {
                return null;
            }
            return server.getLevel(ResourceKey.create(Registries.DIMENSION, location));
        } catch (Exception e) {
            return null; // 维度名非法（入站不可信）
        }
    }

    /** 回传执行结果（保留 _rid，使服务端 request() 的 future 完成）。 */
    private static void reply(ControlConnection conn, ControlMessage msg,
                              boolean executed, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("executed", executed);
        out.addProperty("message", message == null ? "" : message);
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
}
