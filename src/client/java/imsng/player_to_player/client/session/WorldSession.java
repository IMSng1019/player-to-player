package imsng.player_to_player.client.session;

import com.google.gson.JsonObject;
import imsng.player_to_player.client.boot.ClientBootstrap;
import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.config.WorldConfig;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.EnvSyncClient;
import imsng.player_to_player.env.ModPrefixResolver;
import imsng.player_to_player.netproto.ControlClient;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.p2p.NatInfo;
import imsng.player_to_player.p2p.P2PSessions;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ServerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        // 单人/本地联机世界没有 P2P 服务端可连，直接忽略
        ServerData serverData = client.getCurrentServer();
        if (serverData == null || client.isLocalServer()) {
            LOGGER.info("单人或本地联机世界，P2P 会话不启动");
            return;
        }
        String rawAddress = serverData.ip != null ? serverData.ip : "";
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
        NodeContext.get().setGroupId(null);
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

            // ---- 4. 挂接 P2P 撮合处理器（服务端可能随时推 P2P_ENDPOINT_EXCHANGE）----
            P2PSessions.register(cc, conn);

            // ---- 5. 环境同步：每次加入都做真实校验 ----
            // 规范要求"校验本地环境是否与服务端的环境的哈希值相同"——校验对象是本地环境的
            // 真实状态。不再用 worldConfig.lastEnvironmentHash 缓存短路：玩家在两次加入之间
            // 改/删过本地环境文件时，缓存哈希与服务端 envHash 相等并不代表本地真实一致。
            // EnvSyncClient 内部本来就是"拉清单→本地扫描→diff→零差异零下载"，
            // 本地一致时的代价只是一次目录扫描。缓存哈希降级为日志提示。
            if (!envHash.isEmpty() && envHash.equals(worldConfig.lastEnvironmentHash)) {
                LOGGER.info("环境哈希与上次同步记录一致，仍执行本地真实校验（防本地文件被改动）");
            }
            syncEnvironmentsWithRetry(conn, config, paths, worldFolder, worldConfig,
                    envHash, envReady, myGeneration);
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
     * 服务端启动初期环境扫描未完成（HELLO_ACK envReady=false，期间 ENV_MANIFEST_REQUEST
     * 会被服务端以 ERROR 拒绝）或同步中途失败时，每 {@value #ENV_SYNC_RETRY_SECONDS} 秒
     * 重试一次、至多 {@value #ENV_SYNC_MAX_ATTEMPTS} 次 —— 否则服务端启动初期加入的玩家
     * 整局都不会同步环境。每轮动手前校验会话代数，玩家离开世界立即停止。
     */
    private static void syncEnvironmentsWithRetry(ControlConnection conn, GlobalConfig config,
                                                  P2PPaths paths, Path worldFolder,
                                                  WorldConfig worldConfig, String envHash,
                                                  boolean envReadyAtHello, long myGeneration) {
        EnvSyncClient sync = new EnvSyncClient(conn, config);
        if (!envReadyAtHello) {
            // HELLO 已声明环境未就绪：首轮请求注定失败，先等一个周期再开始
            LOGGER.info("服务端环境尚未就绪，{} 秒后开始环境同步", ENV_SYNC_RETRY_SECONDS);
            if (!sleepRetryInterval()) {
                return;
            }
        }
        for (int attempt = 1; attempt <= ENV_SYNC_MAX_ATTEMPTS; attempt++) {
            // 每轮动手前校验会话代数：玩家已离开世界（或重进触发了新会话）则立即停止
            if (myGeneration != generation) {
                LOGGER.info("会话已过期，环境同步停止");
                return;
            }
            try {
                // io 线程上串行两次同步（本方法已在 io 池内，join 安全）
                sync.syncTo(paths.environmentDir(worldFolder, imsng.player_to_player.core.ClientRole.PRIMARY),
                        ModPrefixResolver.Target.PRIMARY_CLIENT).join();
                sync.syncTo(paths.environmentDir(worldFolder, imsng.player_to_player.core.ClientRole.SECONDARY),
                        ModPrefixResolver.Target.SECONDARY_CLIENT).join();
                if (myGeneration == generation && !envHash.isEmpty()) {
                    // lastEnvironmentHash 仅作诊断记录（最近一次同步成功时服务端的 envHash），
                    // 不再参与"是否跳过同步"的决策（见 openSession 第 5 步说明）
                    worldConfig.lastEnvironmentHash = envHash;
                    worldConfig.save(paths.worldConfig(worldFolder));
                }
                LOGGER.info("主/副两套环境同步完成（第 {} 次尝试）", attempt);
                return;
            } catch (Exception e) {
                LOGGER.warn("环境同步失败（第 {}/{} 次），{} 秒后重试: {}",
                        attempt, ENV_SYNC_MAX_ATTEMPTS, ENV_SYNC_RETRY_SECONDS, e.toString());
            }
            if (attempt < ENV_SYNC_MAX_ATTEMPTS && !sleepRetryInterval()) {
                return;
            }
        }
        LOGGER.error("环境同步重试 {} 次后仍未成功，本次会话放弃（重进世界会再次尝试）",
                ENV_SYNC_MAX_ATTEMPTS);
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
