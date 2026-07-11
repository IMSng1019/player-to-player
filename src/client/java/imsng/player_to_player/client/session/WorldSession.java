package imsng.player_to_player.client.session;

import com.google.gson.JsonObject;
import imsng.player_to_player.client.boot.ClientBootstrap;
import imsng.player_to_player.client.group.LocalWorldLauncher;
import imsng.player_to_player.client.group.MergeClient;
import imsng.player_to_player.client.group.SecondaryJoiner;
import imsng.player_to_player.client.group.WorldSwitcher;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    private static ControlClient controlClient;

    /**
     * 会话代数：快速离开再加入时，旧的后台建连/重试流程据此发现自己已过期。
     * 写入在 {@link #SESSION_LOCK} 内；保留 volatile 供重试循环无锁读取
     * （读到瞬时旧值最多多跑一轮判断，不影响正确性）。
     */
    private static volatile long generation;

    private WorldSession() {
    }

    // ------------------------------------------------------------------ 加入

    /** 加入世界（客户端主线程回调；立即转后台）。 */
    public static void onJoin(Minecraft client) {
        // 单人/本地联机世界没有 P2P 服务端可连，直接忽略。
        // 注意主客户端自己的本地组世界也命中 isLocalServer —— 正确：它的会话
        // 早在加入物理服务端时就建立了，切换期间被切换旗标保住，无需重建。
        ServerData serverData = client.getCurrentServer();
        if (serverData == null || client.isLocalServer()) {
            LOGGER.info("单人或本地联机世界，P2P 会话不启动");
            return;
        }
        String rawAddress = serverData.ip != null ? serverData.ip : "";
        // 副客户端经 127.0.0.1 隧道口连接主客户端（Phase 2）：这是组内连接，
        // 与物理服务端的会话仍然活跃，绝不能对隧道口再开一个会话
        if (WorldSwitcher.isTunnelAddress(rawAddress)) {
            LOGGER.info("经 P2P 隧道加入主客户端组世界，物理服务端会话保持");
            return;
        }
        // 玩家身份：clientId 使用玩家 UUID（规范；服务端/中转端才用随机 UUID）
        UUID playerId = client.getUser().getProfileId();
        String playerName = client.getUser().getName();

        long myGeneration;
        synchronized (SESSION_LOCK) {
            // 代数自增与 onLeave 的"++并摘取关闭"同锁，使 openSession 的
            // "校验代数+发布连接"能与离开流程严格互斥
            myGeneration = ++generation;
        }
        ThreadPools.io().execute(() -> openSession(myGeneration, rawAddress, playerId, playerName));
    }

    /** 离开世界（客户端主线程回调）。 */
    public static void onLeave(Minecraft client) {
        // Phase 2 世界切换：主/副客户端从物理服务端"编排性断开"再进组世界，
        // 与服务端的控制连接必须活到组世界的整个生命周期 —— 旗标一次性消费
        if (WorldSwitcher.consumeSwitchFlag()) {
            LOGGER.info("世界切换中的编排性断开，P2P 会话保持");
            return;
        }
        // 主客户端退出本地组世界：DISCONNECT 事件先于集成服务端完全停止，
        // 而停服 saveAll 的区块上行还要走控制连接 —— 延迟到 SERVER_STOPPED
        // 后由 GroupRuntime 的停止回调（ClientBootstrap 挂接）执行拆除
        if (GroupRuntime.isArmedOrActive()) {
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
        ControlClient cc;
        synchronized (SESSION_LOCK) {
            generation++; // 使仍在后台跑的建连/重试流程过期
            cc = controlClient;
            controlClient = null;
        }
        if (cc != null) {
            cc.close();
            LOGGER.info("P2P 世界会话已关闭");
        }
        // P2P 直连/中转会话独立于控制连接，必须整体关闭 —— 否则打洞成功的 UDP socket、
        // 阻塞 receive 的 io 线程、keepalive 定时任务在离开世界后全部残留，
        // 多次进出世界会持续累积泄漏（P2PChannel/RelayClient 的 close 均幂等，重复关闭安全）
        P2PSessions.closeAll();
        RelayConnector.closeAll();
        MergeClient.reset(); // 合并触发面/预同步暂存不得跨会话残留
        // Phase 4 各子系统的限速/在途状态同样不得跨会话残留（全部幂等）
        CommandRelayClient.reset();
        PearlHandoff.reset();
        PortalPreloader.reset();
        WorldSwitcher.markTunnelPort(0); // 清除隧道口登记，防旧端口误匹配下次加入
        NodeContext ctx = NodeContext.get();
        ctx.setGroupId(null);
        ctx.setClientRole(ClientRole.UNASSIGNED);
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
        String host = stripPort(rawAddress);
        if (host.isEmpty()) {
            LOGGER.warn("服务器地址为空，P2P 会话不启动");
            return;
        }

        // ---- 2. 连接控制端口 + HELLO 握手 ----
        ControlClient cc = new ControlClient(host, config.controlPort);
        try {
            ControlConnection conn = cc.connect();

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
                }
            }
            if (expired) {
                cc.close(); // 玩家已经离开世界：立即关闭刚建的连接（尚未发布，不影响新会话）
                return;
            }

            ControlMessage ack = conn.request(ControlMessage.of(MessageType.HELLO,
                            buildHello(playerId, playerName, ctx)))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            JsonObject ackJson = ack.json();
            if (!JsonUtil.getBoolean(ackJson, "accepted", false)) {
                LOGGER.warn("服务端拒绝握手: {}", JsonUtil.getString(ackJson, "reason", "未知原因"));
                closeLocalConnection(cc);
                return;
            }
            String worldName = JsonUtil.getString(ackJson, "worldName", "world");
            String envHash = JsonUtil.getString(ackJson, "envHash", "");
            boolean envReady = JsonUtil.getBoolean(ackJson, "envReady", false);
            LOGGER.info("握手成功: 世界={} envReady={} 中转={}:{}", worldName, envReady,
                    JsonUtil.getString(ackJson, "relayAddress", "(同服务端)"),
                    JsonUtil.getInt(ackJson, "relayPort", 0));

            // ---- 3. 世界文件夹（<IP>+<世界名>）与世界配置 ----
            Path worldFolder = paths.worldFolder(host, worldName);
            paths.ensureWorldDirs(worldFolder);
            WorldConfig worldConfig = WorldConfig.loadOrCreate(
                    paths.worldConfig(worldFolder), host, worldName);
            worldConfig.lastJoinedEpochMillis = System.currentTimeMillis();
            worldConfig.save(paths.worldConfig(worldFolder));

            // ---- 4. 挂接 P2P 撮合处理器（服务端可能随时推 P2P_ENDPOINT_EXCHANGE）
            //         并配置中转端点（打洞失败降级 / 经中转环境同步共用）----
            P2PSessions.register(cc, conn);
            // 合并/分离客户端（Phase 3）：MERGE_PLAN/ABORT/COMMIT 处理器 +
            // 区块拒绝触发面（服务端可能在角色指派后的任意时刻下发合并计划）
            MergeClient.register(cc, conn);
            // Phase 4 逐级路由入站面：其他组指令的转发执行、跨组聊天/私聊重放、
            // 末影珍珠交接/落点回报（处理器内部按"是否有被接管集成服务端"门控，
            // 副客户端收到这些推送时 GroupRuntime.server()==null，安全忽略）
            CommandRelayClient.register(cc);
            ChatRelay.register(cc);
            PearlHandoff.register(cc);
            // 分离晋升（Phase 3）：服务端主动推送的 ROLE_ASSIGN（不带 _rid，
            // 不会与 requestRoleAndSwitch 的 reply 路径冲突 —— 带 _rid 的应答
            // 在连接层被 pending future 消费，永远不进处理器分发）
            cc.on(MessageType.ROLE_ASSIGN, (c, m) ->
                    handleUnsolicitedRoleAssign(conn, worldFolder, worldName, m));
            String relayAddress = JsonUtil.getString(ackJson, "relayAddress", "");
            int relayPort = JsonUtil.getInt(ackJson, "relayPort", 0);
            // relayAddress 空串 = 服务端兼任中转：复用已知可达的服务端主机
            RelayConnector.configure(relayAddress.isEmpty() ? host : relayAddress, relayPort);

            // ---- 5. 环境同步：每次加入都做真实校验 ----
            // 规范要求"校验本地环境是否与服务端的环境的哈希值相同"——校验对象是本地环境的
            // 真实状态。不再用 worldConfig.lastEnvironmentHash 缓存短路：玩家在两次加入之间
            // 改/删过本地环境文件时，缓存哈希与服务端 envHash 相等并不代表本地真实一致。
            // EnvSyncClient 内部本来就是"拉清单→本地扫描→diff→零差异零下载"，
            // 本地一致时的代价只是一次目录扫描。缓存哈希降级为日志提示。
            if (!envHash.isEmpty() && envHash.equals(worldConfig.lastEnvironmentHash)) {
                LOGGER.info("环境哈希与上次同步记录一致，仍执行本地真实校验（防本地文件被改动）");
            }
            boolean synced = syncEnvironmentsWithRetry(conn, config, paths, worldFolder,
                    worldConfig, envHash, envReady, myGeneration, relayAddress, relayPort);
            if (!synced || myGeneration != generation) {
                return;
            }

            // ---- 6. 角色申请与世界切换（Phase 2）----
            requestRoleAndSwitch(conn, worldFolder, worldName, myGeneration);
        } catch (Exception e) {
            LOGGER.error("P2P 世界会话建立失败（模组功能降级，不影响正常游玩）", e);
            // cc 是本方法局部创建的连接，此处无条件关闭：会话过期时它必然不是
            // 新会话的 controlClient（新会话自建实例），关闭不会误伤新会话；
            // 未过期时摘除发布引用再关闭，避免半初始化连接悬挂
            closeLocalConnection(cc);
        }
    }

    /** 组装 HELLO 消息体（compute 未就绪时限时等待，超时降级为不携带）。 */
    private static JsonObject buildHello(UUID playerId, String playerName, NodeContext ctx) {
        JsonObject hello = new JsonObject();
        hello.addProperty("version", Protocol.VERSION);
        hello.addProperty("clientId", playerId.toString());
        hello.addProperty("playerName", playerName);
        hello.addProperty("mode", ctx.mode().id());

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
        EnvSyncClient sync = new EnvSyncClient(conn, config);
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
                    sync.syncTo(paths.environmentDir(worldFolder, ClientRole.PRIMARY),
                            ModPrefixResolver.Target.PRIMARY_CLIENT).join();
                    sync.syncTo(paths.environmentDir(worldFolder, ClientRole.SECONDARY),
                            ModPrefixResolver.Target.SECONDARY_CLIENT).join();
                }
                if (myGeneration == generation && !envHash.isEmpty()) {
                    // lastEnvironmentHash 仅作诊断记录（最近一次同步成功时服务端的 envHash），
                    // 不再参与"是否跳过同步"的决策（见 openSession 第 5 步说明）
                    worldConfig.lastEnvironmentHash = envHash;
                    worldConfig.save(paths.worldConfig(worldFolder));
                }
                LOGGER.info("主/副两套环境同步完成（第 {} 次尝试）", attempt);
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
            EnvSyncClient sync = new EnvSyncClient(conn, config);
            sync.syncTo(paths.environmentDir(worldFolder, ClientRole.PRIMARY),
                    ModPrefixResolver.Target.PRIMARY_CLIENT).join();
            sync.syncTo(paths.environmentDir(worldFolder, ClientRole.SECONDARY),
                    ModPrefixResolver.Target.SECONDARY_CLIENT).join();
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
        String role = JsonUtil.getString(msg.json(), "role", "");
        if (!"primary".equals(role)) {
            return; // 主动推送目前只有"晋升为主"一种（降级走 MERGE_COMMIT）
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
        LOGGER.info("收到分离晋升指派: 本端成为组 {} 的主客户端，切换到本地组世界", groupId);
        ctx.setGroupId(groupId);
        ctx.setClientRole(ClientRole.PRIMARY);
        ThreadPools.io().execute(() -> {
            // 晋升前先取回本人最新玩家数据（分离时原主已 PLAYER_DATA_UPLOAD 上传），
            // 写进本地主存档 —— 否则本地集成服务端会用加入时同步的旧骨架 playerdata
            // 把玩家放回过时位置。失败只告警（旧数据能玩，位置回退可接受）。
            pullOwnPlayerData(conn, worldFolder, worldName);
            LocalWorldLauncher.launch(Minecraft.getInstance(), conn, worldFolder, worldName, groupId);
        });
    }

    /** 拉取本人玩家数据写入本地主存档（io 线程；尽力而为，失败不阻断晋升）。 */
    private static void pullOwnPlayerData(ControlConnection conn, Path worldFolder,
                                          String worldName) {
        NodeContext ctx = NodeContext.get();
        try {
            JsonObject req = new JsonObject();
            req.addProperty("playerUuid", ctx.clientId().toString());
            ControlMessage resp = conn.request(
                            ControlMessage.of(MessageType.PLAYER_DATA_REQUEST, req))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (resp.type() != MessageType.PLAYER_DATA
                    || !JsonUtil.getBoolean(resp.json(), "exists", false)
                    || resp.binary().length == 0) {
                LOGGER.warn("服务端无本人玩家数据（分离上传未达？），本地存档沿用既有数据");
                return;
            }
            // 本地主存档 playerdata：data/primary/<存档名>/playerdata/<uuid>.dat
            //（下发内容本就是 gzip NBT，原样落盘；临时文件 + 原子替换防半写）
            Path playerData = ctx.paths().dataDir(worldFolder, ClientRole.PRIMARY)
                    .resolve(P2PPaths.sanitize(worldName)).resolve("playerdata");
            Files.createDirectories(playerData);
            Path tmp = playerData.resolve(ctx.clientId() + ".dat.p2p-tmp");
            Files.write(tmp, resp.binary());
            Files.move(tmp, playerData.resolve(ctx.clientId() + ".dat"),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            LOGGER.info("已取回最新玩家数据写入本地存档（{} 字节压缩）", resp.binary().length);
        } catch (Exception e) {
            LOGGER.warn("取回玩家数据失败，本地存档沿用既有数据: {}", e.toString());
        }
    }

    /**
     * 角色申请与世界切换（Phase 2 主流程收尾；io 线程）。
     * <p>
     * ROLE_REQUEST → ROLE_ASSIGN：服务端按玩家存档位置查区块注册表/组表决定主副 ——
     * <ul>
     *   <li>primary → {@link LocalWorldLauncher}：构建本地存档并打开（集成服务端接管）；</li>
     *   <li>secondary → {@link SecondaryJoiner}：P2P 连接主客户端，经隧道加入其世界。</li>
     * </ul>
     * 失败语义：任何失败只记日志，玩家留在物理服务端的（挂起演算的）世界里，
     * 控制连接保持，重进世界可重试。
     */
    private static void requestRoleAndSwitch(ControlConnection conn, Path worldFolder,
                                             String worldName, long myGeneration) {
        NodeContext ctx = NodeContext.get();
        try {
            ControlMessage assign = conn.request(ControlMessage.of(MessageType.ROLE_REQUEST,
                            new JsonObject()))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (assign.type() != MessageType.ROLE_ASSIGN) {
                LOGGER.warn("角色申请被服务端拒绝: {}", assign.json());
                return;
            }
            String role = JsonUtil.getString(assign.json(), "role", "");
            UUID groupId = UUID.fromString(JsonUtil.getString(assign.json(), "groupId", ""));
            UUID primaryId = UUID.fromString(
                    JsonUtil.getString(assign.json(), "primaryClientId", ""));
            if (myGeneration != generation) {
                return; // 玩家已离开世界，指派作废（服务端断连清理会收回组登记）
            }
            ctx.setGroupId(groupId);
            Minecraft minecraft = Minecraft.getInstance();
            switch (role) {
                case "primary" -> {
                    ctx.setClientRole(ClientRole.PRIMARY);
                    LOGGER.info("角色指派: 主客户端（组 {}），启动本地组世界", groupId);
                    LocalWorldLauncher.launch(minecraft, conn, worldFolder, worldName, groupId);
                }
                case "secondary" -> {
                    ctx.setClientRole(ClientRole.SECONDARY);
                    LOGGER.info("角色指派: 副客户端（组 {}，主客户端 {}），发起 P2P 加入",
                            groupId, primaryId);
                    SecondaryJoiner.join(minecraft, conn, primaryId);
                }
                default -> LOGGER.warn("未知角色指派: {}", role);
            }
        } catch (Exception e) {
            LOGGER.error("角色申请失败（玩家留在物理服务端世界，重进可重试）", e);
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
