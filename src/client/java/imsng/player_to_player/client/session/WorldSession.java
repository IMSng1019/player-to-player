package imsng.player_to_player.client.session;

import com.google.gson.JsonObject;
import imsng.player_to_player.client.boot.ClientBootstrap;
import imsng.player_to_player.client.group.GroupHost;
import imsng.player_to_player.client.group.LocalWorldLauncher;
import imsng.player_to_player.client.group.MergeClient;
import imsng.player_to_player.client.group.SecondaryJoiner;
import imsng.player_to_player.client.group.WorldSwitcher;
import imsng.player_to_player.client.net.PortSyncClient;
import imsng.player_to_player.client.env.EnvironmentPreflightClient;
import imsng.player_to_player.runtime.WorkerProcess;
import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.config.WorldConfig;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.EnvSyncClient;
import imsng.player_to_player.env.ModPrefixResolver;
import imsng.player_to_player.group.ChatRelay;
import imsng.player_to_player.group.CommandRelayClient;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.ExitTransactionClient;
import imsng.player_to_player.group.LocalPermissions;
import imsng.player_to_player.group.PlayerDataClient;
import imsng.player_to_player.group.PearlHandoff;
import imsng.player_to_player.group.PortalPreloader;
import imsng.player_to_player.netproto.ControlClient;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.p2p.NatInfo;
import imsng.player_to_player.p2p.P2PSessions;
import imsng.player_to_player.p2p.RelayConnector;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ServerData;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.network.chat.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 世界会话（客户端侧，对应规范"玩家加入世界"事件的完整客户端流程；
 * DESIGN.md 第 2/4/7 节）。由 {@link ClientBootstrap} 的 JOIN/DISCONNECT 钩子开合：
 * <ol>
 *   <li>解析目标服务端 IP（单人世界/本地联机没有 P2P 服务端，直接跳过）；</li>
 *   <li>连接服务端控制端口并 HELLO 握手（version, clientId=玩家 UUID, playerName,
 *       mode, compute（就绪则带上，最多等 20 秒）, nat）；</li>
 *   <li>按 HELLO_ACK 下发的世界名建立 {@code <IP>+<世界名>} 世界文件夹与世界配置；</li>
 *   <li>环境同步：每次加入都对本地环境做真实校验（拉清单→本地扫描→diff→零差异零下载）——
 *       规范"主客户端和副客户端随时可以切换 所以下载环境文件时需要同时下载"，
 *       对 PRIMARY_CLIENT / SECONDARY_CLIENT 两个目标各同步一套；
 *       服务端未就绪或同步失败时带退避重试（见 {@link #syncEnvironmentsWithRetry}）；</li>
 *   <li>挂接 P2P 撮合处理器（P2P_ENDPOINT_EXCHANGE）并上报算力（COMPUTE_REPORT）。</li>
 * </ol>
 * <p>
 * 线程模型：onJoin 在客户端主线程被调，立刻转交 {@link ThreadPools#io()} 全程后台执行，
 * 绝不阻塞渲染；onLeave 关闭控制连接与全部 P2P 会话（异步安全）。
 * 静态单例会话，open/close 幂等。
 */
public final class WorldSession {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/session");

    /** HELLO 前等待算力检测结果的最长时间（秒）；超时则不携带算力信息降级握手。 */
    private static final long COMPUTE_WAIT_SECONDS = 20;

    /** 环境同步失败/服务端未就绪时的重试间隔（秒）。 */
    private static final long ENV_SYNC_RETRY_SECONDS = 10;

    /** 环境同步最大尝试次数（10 秒一次共约 5 分钟，覆盖服务端启动扫描的合理耗时）。 */
    private static final int ENV_SYNC_MAX_ATTEMPTS = 30;

    /**
     * 会话锁：把 {代数校验 + controlClient 发布} 与 onLeave 的
     * {generation++ + 摘取关闭} 两组复合操作各自原子化（竞态时序见 openSession 内注释）。
     */
    private static final Object SESSION_LOCK = new Object();

    /** 当前活跃会话的控制客户端；null = 无会话。读写均在 {@link #SESSION_LOCK} 内。 */
    private static volatile ControlClient controlClient;

    /** 退出事务断线重连所需的物理服会话身份；均在 SESSION_LOCK 内读写。 */
    private static String sessionHost;
    /** 本次会话实际使用的控制端口（服务端同步/本地配置回退）；耐久退出重连复用。 */
    private static int sessionControlPort;
    private static UUID sessionPlayerId;
    private static String sessionPlayerName;
    /** 由登录预检绑定版本的中心游戏设置，只供独立 worker 生成受控 server.properties。 */
    private static volatile java.util.Map<String, String> runtimeProperties = java.util.Map.of();

    public static java.util.Map<String, String> runtimeProperties() { return runtimeProperties; }

    /**
     * 会话代数：快速离开再加入时，旧的后台建连/重试流程据此发现自己已过期。
     * 写入在 {@link #SESSION_LOCK} 内；保留 volatile 供重试循环无锁读取
     * （读到瞬时旧值最多多跑一轮判断，不影响正确性）。
     */
    private static volatile long generation;

    private WorldSession() {
    }

    /** 后台准备任务在投递切换前后均验证连接，旧世界迟到任务不得切走新世界。 */
    public static boolean isCurrentConnection(ControlConnection conn) {
        // 合并处理器可能持有 MergeClient.class 调用此方法；只读volatile快照，
        // 避免与拆除期间 SESSION_LOCK→MergeClient.reset 的锁顺序形成环。
        ControlClient active = controlClient;
        return conn != null && conn.isOpen() && active != null && active.connection() == conn;
    }

    // ------------------------------------------------------------------ 加入

    /** 分离等待只属于当前物理服会话；迟到通知不能把其他世界切走。 */
    private static volatile UUID pendingSplit;
    private static volatile long pendingSplitDeadline;

    private static void handleSplitPrepare(ControlConnection conn, ControlMessage message) {
        if (!isCurrentConnection(conn)) return;
        UUID id;
        UUID source;
        try {
            id = UUID.fromString(JsonUtil.getString(message.json(), "splitId", ""));
            source = UUID.fromString(JsonUtil.getString(message.json(), "groupId", ""));
        } catch (IllegalArgumentException invalid) { return; }
        Minecraft client = Minecraft.getInstance();
        client.execute(() -> {
            NodeContext context = NodeContext.get();
            if (!isCurrentConnection(conn) || context.clientRole() != ClientRole.SECONDARY
                    || !source.equals(context.groupId()) || id.equals(pendingSplit)) return;
            pendingSplit = id;
            pendingSplitDeadline = System.currentTimeMillis() + 180_000;
            SwitchTelemetry.begin("split", id.toString());
            // 全体离组成员都先关闭旧游戏连接（包括尚未指定的新副客户端），
            // 这样原组的 PLAYER 票据才能撤销，避免等待新主就绪的循环依赖。
            if (client.level != null) {
                WorldSwitcher.markSwitching();
                client.level.disconnect();
                client.disconnect();
            }
            client.setScreen(new JoinProgressScreen());
        });
    }

    /** 即使控制连接或服务端通知丢失，等待界面也有明确失败终点。 */
    static void tickPendingSplit(Minecraft client) {
        if (pendingSplit != null && System.currentTimeMillis() > pendingSplitDeadline) {
            transitionFailed(null, "等待组世界交接超时，请稍后重新连接。");
        }
    }

    public static void cancelWaiting() {
        Minecraft client = Minecraft.getInstance();
        pendingSplit = null;
        teardownSession();
        if (client.level != null) {
            client.level.disconnect();
            client.disconnect();
        }
        client.setScreen(new net.minecraft.client.gui.screens.TitleScreen());
    }

    /** 失败仅结束本客户端会话，不在提交结果不明时恢复旧世界写者。 */
    public static void transitionFailed(ControlConnection conn, String reason) {
        transitionFailed(conn, reason, generation);
    }

    /** 把失败绑定到发起操作的会话代数，排队期间重连也不得关闭新会话。 */
    private static void transitionFailed(ControlConnection conn, String reason, long expectedGeneration) {
        Minecraft client = Minecraft.getInstance();
        client.execute(() -> {
            if (expectedGeneration != generation) return;
            if (conn != null && !isCurrentConnection(conn)) return;
            // 已经进入目标世界后不接受迟到准备错误；实际断开错误由原版网络界面呈现。
            ServerData currentServer = client.getCurrentServer();
            boolean inGroupWorld = client.isLocalServer() || (currentServer != null
                    && WorldSwitcher.isTunnelAddress(currentServer.ip));
            if (client.level != null && inGroupWorld
                    && !(client.screen instanceof JoinProgressScreen)) return;
            pendingSplit = null;
            teardownSession();
            if (client.level != null) {
                client.level.disconnect();
                client.disconnect();
            }
            client.setScreen(new net.minecraft.client.gui.screens.DisconnectedScreen(
                    new net.minecraft.client.gui.screens.TitleScreen(),
                    net.minecraft.network.chat.Component.literal("无法完成世界交接"),
                    net.minecraft.network.chat.Component.literal(reason)));
        });
    }

    /** 加入世界（客户端主线程回调；立即转后台）。 */
    public static void onJoin(Minecraft client) {
        // 单人/本地联机世界没有 P2P 服务端可连，直接忽略。
        // 注意主客户端自己的本地组世界也命中 isLocalServer —— 正确：它的会话
        // 早在加入物理服务端时就建立了，切换期间被切换旗标保住，无需重建。
        ServerData serverData = client.getCurrentServer();
        if (serverData == null || client.isLocalServer()) {
            if (WorkerProcess.isRunning()) {
                pendingSplit = null;
                SwitchTelemetry.joined();
            }
            LOGGER.info("单人或本地联机世界，P2P 会话不启动");
            return;
        }
        String rawAddress = serverData.ip != null ? serverData.ip : "";
        // 副客户端经 127.0.0.1 隧道口连接主客户端（Phase 2）：这是组内连接，
        // 与物理服务端的会话仍然活跃，绝不能对隧道口再开一个会话
        if (WorldSwitcher.isTunnelAddress(rawAddress)) {
            pendingSplit = null;
            SwitchTelemetry.joined();
            LOGGER.info("经 P2P 隧道加入主客户端组世界，物理服务端会话保持");
            return;
        }
        // 普通/旧版服务器没有环境登录查询，不能猜测其 25580 端口，更不能盖住正常游戏界面。
        if (EnvironmentPreflightClient.preparedFor(rawAddress) == null) {
            LOGGER.debug("此连接没有 P2P 环境预检票据，保留普通服务器行为");
            return;
        }
        // 玩家身份：clientId 使用玩家 UUID（规范；服务端/中转端才用随机 UUID）
        UUID playerId = client.getUser().getProfileId();
        String playerName = client.getUser().getName();

        // 物理服加入：开启新一代端口等待。服务端会在收到本端的频道注册包后，
        // 经 MC 自定义通道下发它实际绑定的 controlPort/relayPort（见 PortSyncClient）；
        // 本方法仍在主线程，真正的等待发生在下面的 join-flow 阻塞线程里。
        // 注意必须在 SESSION_LOCK 的代数自增之后开启：旧 join-flow 若因新加入而
        // 从等待中返回，必须能看到"自己的代数已过期"，从而不再配置中转/建连。
        long myGeneration;
        synchronized (SESSION_LOCK) {
            // 代数自增与 onLeave 的"++并摘取关闭"同锁，使 openSession 的
            // "校验代数+发布连接"能与离开流程严格互斥
            myGeneration = ++generation;
        }
        PortSyncClient.beginJoin();
        SwitchTelemetry.begin("join", Long.toString(myGeneration));
        client.setScreen(new JoinProgressScreen());
        // 加入流程是一条长工作流（环境同步/角色申请/本地存档准备，含 .join() 等待 io 池上的
        // 子任务）：走专用阻塞线程，避免"池内任务等池内任务"占用名额（见 ThreadPools#startBlocking）
        ThreadPools.startBlocking("join-flow", () -> openSession(myGeneration, rawAddress, playerId, playerName));
    }

    /** 离开世界（客户端主线程回调）。 */
    public static void onLeave(Minecraft client) {
        // Phase 2 世界切换：主/副客户端从物理服务端"编排性断开"再进组世界，
        // 与服务端的控制连接必须活到组世界的整个生命周期 —— 旗标一次性消费
        if (WorldSwitcher.consumeSwitchFlag()) {
            LOGGER.info("世界切换中的编排性断开，P2P 会话保持");
            return;
        }
        if (ClientExitCoordinator.isActive()) {
            LOGGER.info("主客户端耐久退出进行中，P2P 会话等待最终提交确认");
            return;
        }
        if (WorkerProcess.isStarting()) {
            // 玩家在进程准备期间离开必须立即撤销本次意图；不能等待迟到READY再把玩家切回世界。
            teardownSession();
            return;
        }
        // 主客户端退出本地组世界：DISCONNECT 事件先于集成服务端完全停止，
        // 而停服 saveAll 的区块上行还要走控制连接 —— 延迟到 SERVER_STOPPED
        // 后由 GroupRuntime 的停止回调（ClientBootstrap 挂接）执行拆除
        if (WorkerProcess.isRunning()) {
            LOGGER.info("集成服务端仍在停止流程中，P2P 会话延迟拆除（等待最终区块上行）");
            return;
        }
        teardownSession();
    }

    /**
     * 拆除世界会话（幂等；主线程/io 线程/服务器线程均可调）：关闭控制连接、
     * 全部 P2P 会话、中转连接，清空组上下文。
     */
    public static void teardownSession() {
        teardownSession(null);
    }

    /** 停止回调只可拆除其来源会话；身份核对、失效、全局资源清理与新连接发布同锁串行。 */
    public static void teardownSession(ControlConnection expected) {
        synchronized (SESSION_LOCK) {
            if (expected != null && (controlClient == null || controlClient.connection() != expected)) return;
            SwitchTelemetry.finish("session_closed");
            EnvironmentLiveUpdates.stop();
            WorkerProcess.closeSession();
            pendingSplit = null;
            generation++; // 使仍在后台跑的建连/重试流程过期
            ControlClient cc = controlClient;
            controlClient = null;
            sessionHost = null;
            sessionControlPort = 0;
            sessionPlayerId = null;
            sessionPlayerName = null;
            runtimeProperties = java.util.Map.of();
            if (cc != null) {
                cc.close();
                LOGGER.info("P2P 世界会话已关闭");
            }
            // 全局子系统也在发布锁内清理，不能在释放锁后把新会话的P2P/分组状态一起清掉。
            GroupHost.reset();
            LocalPermissions.reset();
            P2PSessions.closeAll();
            RelayConnector.closeAll();
            MergeClient.reset();
            CommandRelayClient.reset();
            PearlHandoff.reset();
            PortalPreloader.reset();
            WorldSwitcher.markTunnelPort(0);
            NodeContext ctx = NodeContext.get();
            ctx.setGroupId(null);
            ctx.setClientRole(ClientRole.UNASSIGNED);
        }
    }

    // ------------------------------------------------------------ 会话主流程

    /** 会话建立主流程（io 线程，可阻塞）；连接资源的清理由本方法自理。 */
    private static void openSession(long myGeneration, String rawAddress,
                                    UUID playerId, String playerName) {
        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        P2PPaths paths = ctx.paths();
        if (config == null || paths == null) {
            LOGGER.error("公共引导未完成（config/paths 为空），会话不启动");
            return;
        }
        ctx.setClientId(playerId);
        ctx.setPlayerName(playerName); // 日志上传等落盘命名用（Phase 4）

        // ---- 1. 解析服务端主机（地址可能是 "host:MC端口"，控制端口独立于 MC 端口）----
        EnvironmentPreflightClient.Prepared preflight = EnvironmentPreflightClient.preparedFor(rawAddress);
        if (preflight == null) return;
        String host = preflight.host();
        if (host.isEmpty()) {
            LOGGER.warn("服务器地址为空，P2P 会话不启动");
            return;
        }

        // ---- 2. 解析控制端口（优先服务端经 MC 通道同步）+ 连接 + HELLO 握手 ----
        ResolvedPorts ports = new ResolvedPorts(preflight.info().controlPort(), preflight.info().relayPort(), true);
        if (myGeneration != generation) {
            // 等待端口期间玩家已离开/重进：旧 join-flow 绝不能再用旧 host 建连，
            // 更不能改写 RelayConnector —— 新会话可能已经用新端口建立了自己的配置。
            LOGGER.info("会话已过期，放弃本次控制面建连");
            return;
        }
        // 同步到的 relayPort 稍后预填给 RelayConnector；这里先只解析端口。
        // （预填动作放在会话发布之后，避免过期 join-flow 改写新会话的中转配置。）
        int controlPort = ports.controlPort();
        ControlClient cc = new ControlClient(host, controlPort);
        try {
            ControlConnection connected;
            try {
                connected = cc.connect();
            } catch (IOException first) {
                // 兼容旧部署：服务端可能经 NAT/端口转发把控制面映射成另一个外部端口，
                // 同步下发的"实际监听端口"在外网不可达。此时用本地配置端口重试一次
                // （仅当确有同步值且与本地配置不同；1:1 部署不会走到这里）。
                int fallbackPort = config.controlPort;
                if (!ports.synced() || fallbackPort <= 0 || fallbackPort == controlPort) {
                    throw first;
                }
                LOGGER.warn("连接服务端同步端口 {} 失败，回退本地 controlPort={} 重试: {}",
                        controlPort, fallbackPort, first.toString());
                cc.close();
                cc = new ControlClient(host, fallbackPort);
                connected = cc.connect();
                controlPort = fallbackPort;
            }
            // ROLE_ASSIGN 推送处理器会捕获该引用，故发布后不再重新赋值
            final ControlConnection conn = connected;

            // —— 竞态时序（为何"校验+发布"必须持锁）——
            // 旧实现先无锁校验代数、通过后再写 controlClient，存在竞窗：
            //   io 线程: 校验 myGeneration == generation → 通过
            //   主线程 : onLeave → generation++ → 摘取 controlClient（此刻还是 null）→ 无可关闭
            //   io 线程: controlClient = cc → 过期连接（含其 NioEventLoopGroup）被发布，从此无人关闭
            // 现把"校验+发布"放进 SESSION_LOCK，与 onLeave 的"++代数+摘取关闭"互斥，竞窗消除。
            boolean expired;
            synchronized (SESSION_LOCK) {
                expired = myGeneration != generation;
                if (!expired) {
                    controlClient = cc;
                    sessionHost = host;
                    sessionControlPort = controlPort;
                    sessionPlayerId = playerId;
                    sessionPlayerName = playerName;
                }
            }
            if (expired) {
                cc.close(); // 玩家已经离开世界：立即关闭刚建的连接（尚未发布，不影响新会话）
                return;
            }
            // 会话已确认发布：用同步到的端口预填中转端点。HELLO_ACK 到达后仍会以
            // 服务端下发的 relayAddress/relayPort 覆盖（含独立中转地址的场景）。
            if (ports.synced()) {
                RelayConnector.configure(host, ports.relayPort());
            }

            ControlMessage ack = conn.request(ControlMessage.of(MessageType.HELLO,
                            buildHello(playerId, playerName, ctx)))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            JsonObject ackJson = ack.json();
            if (!JsonUtil.getBoolean(ackJson, "accepted", false)) {
                LOGGER.warn("服务端拒绝握手: {}", JsonUtil.getString(ackJson, "reason", "未知原因"));
                closeLocalConnection(cc);
                transitionFailed(null,
                        "服务端拒绝连接：" + JsonUtil.getString(ackJson, "reason", "请检查两端版本与配置"), myGeneration);
                return;
            }
            String worldName = JsonUtil.getString(ackJson, "worldName", "world");
            if (!worldName.equals(preflight.info().worldName()))
                throw new IOException("服务器世界在预检后发生变化，请重新连接");
            runtimeProperties = preflight.info().worldProperties();
            String envHash = JsonUtil.getString(ackJson, "envHash", "");
            boolean envReady = JsonUtil.getBoolean(ackJson, "envReady", false);
            LOGGER.info("握手成功: 世界={} envReady={} 中转={}:{}", worldName, envReady,
                    JsonUtil.getString(ackJson, "relayAddress", "(同服务端)"),
                    JsonUtil.getInt(ackJson, "relayPort", 0));

            // 服务端能力位：只有它声明支持"全量上链"语义，本端才在本地执行前拦截
            CommandRelayClient.setServerRoutingSupported(
                    JsonUtil.getInt(ackJson, "commandRouting", 0) >= 1);
            LOGGER.info("服务端指令路由能力: {}", CommandRelayClient.isServerRoutingSupported()
                    ? "支持（全量上链）" : "不支持（保持原版本地执行语义）");

            // ---- 3. 世界文件夹（<IP>+<世界名>）与世界配置 ----
            Path worldFolder = preflight.worldFolder();
            paths.ensureWorldDirs(worldFolder);
            WorldConfig worldConfig = WorldConfig.loadOrCreate(
                    paths.worldConfig(worldFolder), preflight.address(), worldName);
            worldConfig.lastJoinedEpochMillis = System.currentTimeMillis();
            worldConfig.save(paths.worldConfig(worldFolder));

            // ---- 4. 挂接 P2P 撮合处理器（服务端可能随时推 P2P_ENDPOINT_EXCHANGE）
            //         并配置中转端点（打洞失败降级 / 经中转环境同步共用）----
            P2PSessions.register(cc, conn);
            // 合并/分离客户端（Phase 3）：MERGE_PLAN/ABORT/COMMIT 处理器 +
            // 区块拒绝触发面（服务端可能在角色指派后的任意时刻下发合并计划）
            MergeClient.register(cc, conn);
            // 世界逻辑消息由实际运行独立服务端的 worker 消费，渲染进程仅持有中心会话。
            WorkerProcess.registerForwarders(cc, conn);
            WorkerProcess.setMetadataRestartListener((connection, fields) -> Minecraft.getInstance().execute(() -> {
                if (!isCurrentConnection(connection)) return;
                Minecraft client = Minecraft.getInstance();
                String reason = "世界启动设置已更新（" + fields + "）。已暂停模拟并保存世界，请重新连接以应用最新快照。";
                var screen = new net.minecraft.client.gui.screens.DisconnectedScreen(
                        new net.minecraft.client.gui.screens.TitleScreen(),
                        net.minecraft.network.chat.Component.literal("世界需要重新加载"),
                        net.minecraft.network.chat.Component.literal(reason));
                // 沿用EXIT_BEGIN/最终尾部/EXIT_COMMIT屏障，不能直接杀掉正在写世界的子进程。
                if (!ClientExitCoordinator.interceptDisconnect(client, screen)) transitionFailed(connection, reason);
            }));
            // 分离晋升（Phase 3）：服务端主动推送的 ROLE_ASSIGN（不带 _rid，
            // 不会与 requestRoleAndSwitch 的 reply 路径冲突 —— 带 _rid 的应答
            // 在连接层被 pending future 消费，永远不进处理器分发）
            cc.on(MessageType.ROLE_ASSIGN, (c, m) ->
                    handleUnsolicitedRoleAssign(conn, worldFolder, worldName, m));
            cc.on(MessageType.SPLIT_PREPARE, (c, m) -> handleSplitPrepare(conn, m));
            cc.on(MessageType.SPLIT_ABORT, (c, m) -> {
                if (!isCurrentConnection(conn)) return;
                try { WorkerProcess.cancelSplit(
                        UUID.fromString(JsonUtil.getString(m.json(), "splitId", ""))); }
                catch (IllegalArgumentException ignored) { }
            });
            cc.on(MessageType.SPLIT_FAILED, (c, m) -> {
                if (!isCurrentConnection(conn) || pendingSplit == null) return;
                String id = JsonUtil.getString(m.json(), "splitId", "");
                if (!id.isEmpty() && !pendingSplit.toString().equals(id)) return;
                transitionFailed(conn, JsonUtil.getString(m.json(), "message", "分离交接失败"));
            });
            String relayAddress = JsonUtil.getString(ackJson, "relayAddress", "");
            int relayPort = JsonUtil.getInt(ackJson, "relayPort", 0);
            // relayAddress 空串 = 服务端兼任中转：复用已知可达的服务端主机
            RelayConnector.configure(relayAddress.isEmpty() ? host : relayAddress, relayPort);

            // ---- 5. 接续已经完成的登录前环境校验，并挂接运行期更新服务 ----
            // 登录前已经下载并完整校验两套目录。这里复用其同一 snapshot，绝不在通过
            // 模组/注册表校验后再换一套新环境；静态更新由后台 pending 目录等待重连生效。
            if (myGeneration != generation) return;
            worldConfig.lastEnvironmentHash = preflight.environments().snapshotId();
            worldConfig.save(paths.worldConfig(worldFolder));
            EnvironmentLiveUpdates.start(cc, conn, worldFolder, preflight.info(), preflight.environments());

            // ---- 6. 角色申请与世界切换（Phase 2）----
            requestRoleAndSwitch(conn, worldFolder, worldName, myGeneration);
        } catch (Exception e) {
            LOGGER.error("P2P 世界会话建立失败（模组功能降级，不影响正常游玩）", e);
            // cc 是本方法局部创建的连接，此处无条件关闭：会话过期时它必然不是
            // 新会话的 controlClient（新会话自建实例），关闭不会误伤新会话；
            // 未过期时摘除发布引用再关闭，避免半初始化连接悬挂
            closeLocalConnection(cc);
            transitionFailed(null, "连接或准备组世界失败，请检查日志后重试。", myGeneration);
        }
    }

    /** 组装 HELLO 消息体（compute 未就绪时限时等待，超时降级为不携带）。 */
    private static JsonObject buildHello(UUID playerId, String playerName, NodeContext ctx) {
        JsonObject hello = new JsonObject();
        hello.addProperty("version", Protocol.VERSION);
        hello.addProperty("clientId", playerId.toString());
        hello.addProperty("playerName", playerName);
        hello.addProperty("mode", ctx.mode().id());
        // 环境版本贯穿正式控制会话，服务端选主/合并只能组合实际运行同一静态版本的节点。
        hello.addProperty("runtimeHash", EnvironmentPreflightClient.activeRuntimeHash());
        // 指令路由能力位：服务端据此决定是否按"全量上链"语义处理上送
        //（缺省 0 = 老客户端语义；新服务端会忽略未声明的上送，避免重复执行）
        hello.addProperty("commandRouting", 1);

        // 算力：规范"玩家加入世界时向服务端给出算力能力"——未就绪则限时等待检测完成
        ComputeScore score = ctx.computeScore();
        if (score == null) {
            try {
                score = ClientBootstrap.computeScoreFuture()
                        .get(COMPUTE_WAIT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.warn("等待算力检测超时/失败，HELLO 不携带算力信息: {}", e.toString());
            }
        }
        if (score != null) {
            hello.add("compute", score.toJson());
        }
        NatInfo nat = ctx.natInfo();
        if (nat != null) {
            hello.add("nat", nat.toJson());
        }
        return hello;
    }

    /**
     * 解析本次会话的控制端口（io 线程，可阻塞）。
     * <p>
     * 优先使用服务端经 MC 自定义通道同步的端口：服务端可能把 {@code controlPort}
     * 配成非默认值，客户端本地配置无从得知，旧实现会因此完全连不上控制面。
     * 服务端未运行本模组 / 版本过旧 / 超过等待上限时，回退本地配置（保持旧行为）。
     * <p>
     * 本方法<b>不产生任何副作用</b>（不配置 RelayConnector、不建连）：等待期间
     * 会话可能已经过期，副作用必须由调用方在代数校验之后执行。
     */
    private static ResolvedPorts resolvePorts(GlobalConfig config) {
        PortSyncClient.Ports ports = PortSyncClient.awaitForJoin();
        if (ports != null) {
            LOGGER.info("使用服务端同步的端口: controlPort={}, relayPort={}",
                    ports.controlPort(), ports.relayPort());
            return new ResolvedPorts(ports.controlPort(), ports.relayPort(), true);
        }
        LOGGER.warn("未收到服务端端口同步（服务端未运行本模组或超时），回退本地 controlPort={}",
                config.controlPort);
        return new ResolvedPorts(config.controlPort, 0, false);
    }

    /** 解析结果：{@code synced=false} 表示回退本地配置，调用方不得预填中转端点。 */
    private record ResolvedPorts(int controlPort, int relayPort, boolean synced) {
    }

    /**
     * 环境同步（带重试，io 线程）：主/副两套目录各同步一次（见类 Javadoc 第 4 步）。
     * <p>
     * 每轮先尝试经中转端同步（{@link GlobalConfig#envSyncViaRelay} 开启且存在独立
     * 中转时；规范：中转服务端"同时可以给主客户端和副客户端分发模组文件以及
     * 配置文件"），失败自动回退直连服务端 —— 可用性不依赖中转。
     * <p>
     * 服务端启动初期环境扫描未完成（HELLO_ACK envReady=false，期间 ENV_MANIFEST_REQUEST
     * 会被服务端以 ERROR 拒绝）或同步中途失败时，每 {@value #ENV_SYNC_RETRY_SECONDS} 秒
     * 重试一次、至多 {@value #ENV_SYNC_MAX_ATTEMPTS} 次 —— 否则服务端启动初期加入的玩家
     * 整局都不会同步环境。每轮动手前校验会话代数，玩家离开世界立即停止。
     *
     * @return 是否同步成功（false = 放弃/会话过期，调用方不得继续角色流程）
     */
    private static boolean syncEnvironmentsWithRetry(ControlConnection conn, GlobalConfig config,
                                                     P2PPaths paths, Path worldFolder,
                                                     WorldConfig worldConfig, String envHash,
                                                     boolean envReadyAtHello, long myGeneration,
                                                     String relayAddress, int relayPort) {
        SwitchTelemetry.mark("environment_begin");
        EnvSyncClient sync = new EnvSyncClient(conn, config, EnvironmentPreflightClient.worldPath());
        if (!envReadyAtHello) {
            // HELLO 已声明环境未就绪：首轮请求注定失败，先等一个周期再开始
            LOGGER.info("服务端环境尚未就绪，{} 秒后开始环境同步", ENV_SYNC_RETRY_SECONDS);
            if (!sleepRetryInterval()) {
                return false;
            }
        }
        for (int attempt = 1; attempt <= ENV_SYNC_MAX_ATTEMPTS; attempt++) {
            // 每轮动手前校验会话代数：玩家已离开世界（或重进触发了新会话）则立即停止
            if (myGeneration != generation) {
                LOGGER.info("会话已过期，环境同步停止");
                return false;
            }
            try {
                // 优先经中转端分发（减轻服务端上行带宽），失败回退直连
                if (!trySyncViaProxy(config, paths, worldFolder, relayAddress, relayPort)) {
                    // io 线程上串行两次同步（本方法已在 io 池内，join 安全）
                    sync.syncPair(paths.environmentDir(worldFolder, ClientRole.PRIMARY),
                            paths.environmentDir(worldFolder, ClientRole.SECONDARY)).join();
                }
                if (myGeneration == generation && !envHash.isEmpty()) {
                    // lastEnvironmentHash 仅作诊断记录（最近一次同步成功时服务端的 envHash），
                    // 不再参与"是否跳过同步"的决策（见 openSession 第 5 步说明）
                    worldConfig.lastEnvironmentHash = envHash;
                    worldConfig.save(paths.worldConfig(worldFolder));
                }
                LOGGER.info("主/副两套环境同步完成（第 {} 次尝试）", attempt);
                SwitchTelemetry.mark("environment_ready");
                return true;
            } catch (Exception e) {
                LOGGER.warn("环境同步失败（第 {}/{} 次），{} 秒后重试: {}",
                        attempt, ENV_SYNC_MAX_ATTEMPTS, ENV_SYNC_RETRY_SECONDS, e.toString());
            }
            if (attempt < ENV_SYNC_MAX_ATTEMPTS && !sleepRetryInterval()) {
                return false;
            }
        }
        LOGGER.error("环境同步重试 {} 次后仍未成功，本次会话放弃（重进世界会再次尝试）",
                ENV_SYNC_MAX_ATTEMPTS);
        return false;
    }

    /**
     * 尝试经中转端同步环境（Phase 2"中转端环境分发"的客户端消费侧；io 线程）。
     * <p>
     * 仅当配置开启且 HELLO_ACK 下发了<b>独立</b>中转地址时才尝试（服务端兼任
     * 中转时其控制端口本就直接提供环境同步，走中转没有意义）。中转端的控制
     * 服务器要求先 RELAY_REGISTER 登记身份（帧序保证后续 ENV_* 请求已通过鉴权门）。
     *
     * @return true = 已经中转完成两套同步；false = 未启用/失败（调用方回退直连）
     */
    private static boolean trySyncViaProxy(GlobalConfig config, P2PPaths paths, Path worldFolder,
                                           String relayAddress, int relayPort) {
        if (!config.envSyncViaRelay || relayAddress == null || relayAddress.isEmpty()
                || relayPort <= 0) {
            return false;
        }
        ControlClient pc = new ControlClient(relayAddress, relayPort);
        try {
            ControlConnection conn = pc.connect();
            JsonObject reg = new JsonObject();
            reg.addProperty("clientId", NodeContext.get().clientId().toString());
            conn.send(ControlMessage.of(MessageType.RELAY_REGISTER, reg));
            EnvSyncClient sync = new EnvSyncClient(conn, config, EnvironmentPreflightClient.worldPath());
            sync.syncPair(paths.environmentDir(worldFolder, ClientRole.PRIMARY),
                    paths.environmentDir(worldFolder, ClientRole.SECONDARY)).join();
            LOGGER.info("环境同步经中转端 {}:{} 完成", relayAddress, relayPort);
            return true;
        } catch (Exception e) {
            LOGGER.warn("经中转端环境同步失败，回退直连服务端: {}", e.toString());
            return false;
        } finally {
            pc.close(); // 环境同步是一次性拉取，不留常驻中转连接
        }
    }

    // ------------------------------------------------------ 角色申请与世界切换

    /**
     * 服务端主动推送的 ROLE_ASSIGN（Phase 3 分离晋升；Netty 线程 → 重活转 io）。
     * <p>
     * 单端分离达标后服务端把离组副客户端晋升为主（SplitMonitor → SPLIT_ACK →
     * 服务端直发 ROLE_ASSIGN(primary)）：本端此刻还连着原主的隧道世界，需要
     * 切到自己的本地组世界。区块数据由服务端供给（原主的实时上行保证新鲜），
     * 走 LocalWorldLauncher 的既有管线。
     */
    private static void handleUnsolicitedRoleAssign(ControlConnection conn, Path worldFolder,
                                                    String worldName, ControlMessage msg) {
        if (!isCurrentConnection(conn)) return;
        String role = JsonUtil.getString(msg.json(), "role", "");
        if ("secondary".equals(role)) {
            UUID primary;
            UUID assignedGroup;
            try {
                primary = UUID.fromString(JsonUtil.getString(msg.json(), "primaryClientId", ""));
                assignedGroup = UUID.fromString(JsonUtil.getString(msg.json(), "groupId", ""));
            } catch (IllegalArgumentException invalid) { return; }
            ThreadPools.startBlocking("split-follow", () -> {
                try {
                    if (!isCurrentConnection(conn)) return;
                    NodeContext context = NodeContext.get();
                    context.setGroupId(assignedGroup);
                    context.setClientRole(ClientRole.SECONDARY);
                    var prepared = SecondaryJoiner.prepare(conn, primary, UUID.randomUUID());
                    SecondaryJoiner.activate(Minecraft.getInstance(), conn, primary, prepared, () -> { });
                } catch (Exception failure) {
                    LOGGER.error("分离副客户端连接失败", failure);
                    transitionFailed(conn, "无法连接组内主客户端，请稍后重新连接。");
                }
            });
            return;
        }
        if (!"primary".equals(role)) {
            return;
        }
        NodeContext ctx = NodeContext.get();
        if (ctx.clientRole() == ClientRole.PRIMARY) {
            return; // 已是主客户端：重复/迟到推送，忽略
        }
        UUID groupId;
        try {
            groupId = UUID.fromString(JsonUtil.getString(msg.json(), "groupId", ""));
        } catch (IllegalArgumentException e) {
            return;
        }
        ThreadPools.startBlocking("split-promote", () -> {
            try {
                SwitchTelemetry.begin("split", groupId.toString());
                LocalWorldLauncher.PreparedWorld prepared = prepareAuthoritativeWorld(
                        conn, worldFolder, worldName);
                if (!isCurrentConnection(conn)) return;
                ctx.setGroupId(groupId);
                ctx.setClientRole(ClientRole.PRIMARY);
                LOGGER.info("收到分离晋升指派: 本端成为组 {} 的主客户端，切换到本地组世界",
                        groupId);
                LocalWorldLauncher.launchPrepared(
                        Minecraft.getInstance(), conn, prepared, groupId);
            } catch (Exception e) {
                LOGGER.error("分离晋升前准备权威玩家状态失败", e);
                transitionFailed(conn, "准备分离后的世界失败，请检查日志后重新连接。");
            }
        });
    }

    /** 取得物理服实时玩家状态，并把本地主缓存准备到可直接启动的状态。 */
    private static LocalWorldLauncher.PreparedWorld prepareAuthoritativeWorld(
            ControlConnection conn, Path worldFolder, String worldName) throws Exception {
        NodeContext ctx = NodeContext.get();
        SwitchTelemetry.mark("player_state_begin");
        CompoundTag playerState = PlayerDataClient.requestAuthoritative(conn, ctx.clientId());
        SwitchTelemetry.mark("player_state_ready");
        return LocalWorldLauncher.prepare(
                conn, worldFolder, worldName, ctx.clientId(), playerState);
    }

    /**
     * 角色申请与世界切换（Phase 2 主流程收尾；io 线程）。
     * <p>
     * ROLE_REQUEST → ROLE_ASSIGN：服务端按玩家存档位置查区块注册表/组表决定主副 ——
     * <ul>
     *   <li>primary → {@link LocalWorldLauncher}：构建本地存档并打开（集成服务端接管）；</li>
     *   <li>secondary → {@link SecondaryJoiner}：P2P 连接主客户端，经隧道加入其世界。</li>
     * </ul>
     * 失败时结束当前准备会话并显示原因，避免永久停留在准备界面；
     * 过期会话的失败不得影响新连接。
     */
    private static void requestRoleAndSwitch(ControlConnection conn, Path worldFolder,
                                             String worldName, long myGeneration) {
        NodeContext ctx = NodeContext.get();
        LocalWorldLauncher.PreparedWorld prepared;
        try {
            // 必须在 ROLE_REQUEST 前完成：失败时服务端尚未创建组，玩家仍留在物理服。
            prepared = prepareAuthoritativeWorld(conn, worldFolder, worldName);
        } catch (Exception e) {
            LOGGER.error("主客户端切换前无法取得或准备物理服务器权威玩家状态", e);
            transitionFailed(conn, "无法取得或准备服务器玩家状态，请稍后重新连接。", myGeneration);
            return;
        }
        if (myGeneration != generation) {
            return;
        }

        try {
            ControlMessage assign = conn.request(ControlMessage.of(MessageType.ROLE_REQUEST,
                            new JsonObject()))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (assign.type() != MessageType.ROLE_ASSIGN) {
                LOGGER.warn("角色申请被服务端拒绝: {}", assign.json());
                transitionFailed(conn, "服务端拒绝角色分配，请检查日志后重新连接。", myGeneration);
                return;
            }
            String role = JsonUtil.getString(assign.json(), "role", "");
            UUID groupId = UUID.fromString(JsonUtil.getString(assign.json(), "groupId", ""));
            UUID primaryId = UUID.fromString(
                    JsonUtil.getString(assign.json(), "primaryClientId", ""));
            if (myGeneration != generation) {
                return; // 玩家已离开世界，指派作废（服务端断连清理会收回组登记）
            }
            Minecraft minecraft = Minecraft.getInstance();
            switch (role) {
                case "primary" -> {
                    ctx.setGroupId(groupId);
                    ctx.setClientRole(ClientRole.PRIMARY);
                    LOGGER.info("角色指派: 主客户端（组 {}），启动本地组世界", groupId);
                    LocalWorldLauncher.launchPrepared(minecraft, conn, prepared, groupId);
                }
                case "secondary" -> {
                    ctx.setGroupId(groupId);
                    ctx.setClientRole(ClientRole.SECONDARY);
                    LOGGER.info("角色指派: 副客户端（组 {}，主客户端 {}），发起 P2P 加入",
                            groupId, primaryId);
                    SecondaryJoiner.join(minecraft, conn, primaryId);
                }
                default -> transitionFailed(conn, "服务端返回未知角色，请确认各端版本一致。", myGeneration);
            }
        } catch (Exception e) {
            LOGGER.error("角色申请失败", e);
            transitionFailed(conn, "角色分配或组世界启动失败，请检查日志后重新连接。", myGeneration);
        }
    }

    // ------------------------------------------------------------------ 工具

    /**
     * 关闭 openSession 局部创建的连接：先在会话锁内摘除发布引用（若它恰是当前会话的），
     * 再无条件 close —— cc 是局部对象，会话过期时它必然不等于新会话的 controlClient，
     * 无条件关闭不会误伤新会话。
     */
    private static void closeLocalConnection(ControlClient cc) {
        synchronized (SESSION_LOCK) {
            if (controlClient == cc) {
                controlClient = null;
            }
        }
        cc.close();
    }

    /**
     * 创建耐久退出发送器。连接工厂优先复用当前控制连接；断线时用保存的物理服端点
     * 重新 TCP 建连并完成 HELLO。重试次数没有上限，等待器只提供封顶退避。
     */
    public static ExitTransactionClient createExitTransactionClient() {
        return new ExitTransactionClient(new ExitTransactionClient.ConnectionFactory() {
            @Override
            public ControlConnection connect() throws Exception {
                return ensureExitConnection();
            }

            @Override
            public void invalidate(ControlConnection connection) {
                invalidateExitConnection();
            }
        }, WorldSession::waitBeforeExitRetry);
    }

    private static ControlConnection ensureExitConnection() throws Exception {
        ControlClient existing;
        String host;
        UUID playerId;
        String playerName;
        int controlPort;
        synchronized (SESSION_LOCK) {
            existing = controlClient;
            host = sessionHost;
            playerId = sessionPlayerId;
            playerName = sessionPlayerName;
            controlPort = sessionControlPort;
        }
        if (existing != null) {
            ControlConnection open = existing.connection();
            if (open != null && open.isOpen()) {
                return open;
            }
        }
        if (host == null || host.isBlank() || playerId == null || playerName == null) {
            throw new IllegalStateException("退出重连缺少物理服会话身份");
        }

        NodeContext ctx = NodeContext.get();
        // 复用会话建立时实际生效的端口（可能是服务端同步值，本地配置并不相同）
        int reconnectPort = controlPort > 0 ? controlPort : ctx.config().controlPort;
        ControlClient replacement = new ControlClient(host, reconnectPort);
        try {
            ControlConnection connection = replacement.connect();
            ControlMessage ack = connection.request(ControlMessage.of(
                            MessageType.HELLO, buildHello(playerId, playerName, ctx)))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (ack.type() != MessageType.HELLO_ACK
                    || !JsonUtil.getBoolean(ack.json(), "accepted", false)) {
                throw new IllegalStateException("退出重连 HELLO 被物理服务端拒绝");
            }
            ControlClient previous;
            synchronized (SESSION_LOCK) {
                previous = controlClient;
                controlClient = replacement;
                sessionControlPort = reconnectPort;
            }
            if (previous != null && previous != replacement) {
                previous.close();
            }
            LOGGER.info("耐久退出控制连接已恢复: {}:{}", host, reconnectPort);
            return connection;
        } catch (Exception e) {
            replacement.close();
            throw e;
        }
    }

    private static void invalidateExitConnection() {
        ControlClient failed;
        synchronized (SESSION_LOCK) {
            failed = controlClient;
            controlClient = null;
        }
        if (failed != null) {
            failed.close();
        }
    }

    /** 1 秒起、最高 30 秒；忽略中断，因为用户已要求退出提交不可取消。 */
    private static void waitBeforeExitRetry(int attempt) {
        long delayMillis = Math.min(30_000L,
                1_000L << Math.min(5, Math.max(0, attempt - 1)));
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMillis);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(remaining);
            } catch (InterruptedException ignored) {
                // 清除中断并继续等满退避时间；退出事务只有服务端 committed 才结束。
            }
        }
    }

    /** 退避等待一个重试周期；线程被中断时恢复中断标志并返回 false（调用方随即退出）。 */
    private static boolean sleepRetryInterval() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(ENV_SYNC_RETRY_SECONDS));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 从 "host:port" 剥掉端口，仅当恰有一个冒号时拆分
     * （与 HelloHandler.appendRelayInfo 同策略，避免误截 IPv6 多冒号地址）。
     */
    private static String stripPort(String address) {
        String trimmed = address == null ? "" : address.trim();
        int colon = trimmed.lastIndexOf(':');
        if (colon > 0 && trimmed.indexOf(':') == colon) {
            return trimmed.substring(0, colon);
        }
        return trimmed;
    }
}
