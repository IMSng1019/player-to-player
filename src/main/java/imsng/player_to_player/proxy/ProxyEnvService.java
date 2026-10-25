package imsng.player_to_player.proxy;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.EnvSyncClient;
import imsng.player_to_player.env.EnvSyncServerHandlers;
import imsng.player_to_player.env.EnvironmentSnapshotManager;
import imsng.player_to_player.env.EnvironmentPreflightInfo;
import imsng.player_to_player.env.EnvironmentPathPolicy;
import imsng.player_to_player.netproto.ControlClient;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.ControlServer;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 中转端环境分发服务（Phase 2；规范：中转服务端"同时可以给主客户端和副客户端
 * 分发模组文件以及配置文件使其环境相同"）。
 * <p>
 * 工作方式（DESIGN.md 路线图"中转端环境分发"）：
 * <ol>
 *   <li><b>上游同步</b>：按 {@link GlobalConfig#parentServerAddress} 连接上级
 *       服务端控制端口，HELLO（mode=proxy_server）后以<b>全量视图</b>
 *       （{@link EnvSyncClient#syncTo} 的 null 目标 —— 中转端要向任意目标端
 *       二次分发，必须持有全量环境）同步到 {@link P2PPaths#proxyEnvDir()}；</li>
 *   <li><b>本地清单</b>：同步完成后扫描缓存目录生成清单（volatile 发布，
 *       未就绪期间对客户端的清单请求由 EnvSyncServerHandlers 回 ERROR，
 *       客户端带退避重试）；</li>
 *   <li><b>对外分发</b>：把 {@link EnvSyncServerHandlers} 经
 *       {@link RelayCore#setExtraHandlers} 挂到中转控制端口上 —— 客户端先
 *       RELAY_REGISTER 登记（过未鉴权白名单门）再发 ENV_* 请求，
 *       目标端前缀过滤逻辑与服务端完全同款；</li>
 *   <li><b>定期校验</b>：每 {@link GlobalConfig#proxyEnvResyncMinutes} 分钟
 *       重跑一次同步（EnvSyncClient 本就是"清单→diff→零差异零下载"，
 *       上游无变化时代价只是一次目录扫描），上游更新自动跟进。</li>
 * </ol>
 * 上游不可达时按环境同步重试间隔退避重试，中转/打洞功能不受影响（分发能力
 * 独立降级）。未配置 parentServerAddress 时本服务不启动（只做纯中转）。
 */
public final class ProxyEnvService {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/proxy-env");

    /** 上游同步失败的退避重试间隔（秒）。 */
    private static final long RETRY_SECONDS = 30;

    private final GlobalConfig config;
    private final P2PPaths paths;

    /** 下游分发使用的不可变快照管理器；首次上游同步成功前不启动。 */
    private volatile EnvironmentSnapshotManager snapshots;
    private volatile String worldPath;
    private volatile java.util.Map<MessageType, imsng.player_to_player.netproto.MessageHandler> environmentHandlers = java.util.Map.of();

    /** 定期重同步任务；stop 时取消。 */
    private volatile ScheduledFuture<?> resyncTask;

    private volatile boolean running;

    public ProxyEnvService(GlobalConfig config, P2PPaths paths) {
        this.config = config;
        this.paths = paths;
    }

    /**
     * 启动环境分发：把 ENV_* 处理器挂到中转控制服务器，并开始上游同步循环。
     * <b>必须在 {@code relay.start()} 之前调用</b>（setExtraHandlers 的生效前提）。
     *
     * @return 是否启用（未配置上级服务端地址时 false）
     */
    public boolean attachAndStart(RelayCore relay) {
        String parent = config.parentServerAddress == null ? "" : config.parentServerAddress.trim();
        if (parent.isEmpty()) {
            LOGGER.info("未配置 parentServerAddress，中转端只做打洞协助与中转，不分发环境文件");
            return false;
        }
        running = true;
        // 对外分发面始终读取不可变快照；首次上游同步完成前 current=null，
        // EnvSyncServerHandlers 会明确返回 env_not_ready。
        relay.setExtraHandlers((ControlServer control) -> {
            for (MessageType type : List.of(MessageType.ENV_MANIFEST_REQUEST, MessageType.ENV_FILE_REQUEST, MessageType.MOD_LIST_REQUEST)) {
                control.on(type, (connection, request) -> {
                    var handler = environmentHandlers.get(type);
                    if (handler != null) handler.handle(connection, request);
                    else {
                        JsonObject error = new JsonObject(); error.addProperty("code", "env_not_ready");
                        error.addProperty("message", "中转端尚未取得上游实际世界路径及环境快照");
                        connection.send(request.reply(MessageType.ERROR, error, null));
                    }
                });
            }
        });

        // 上游同步循环：首轮立即，失败退避重试；成功后转定期校验。整条循环在成功前不会
        // 返回（"直到成功或服务停止"），属长驻阻塞任务 —— 专用线程，不占有界 io 池。
        ThreadPools.startBlocking("proxy-env-sync", () -> initialSyncLoop(parent));
        return true;
    }

    /** 首次同步循环（io 线程）：直到成功或服务停止。 */
    private void initialSyncLoop(String parent) {
        while (running) {
            if (syncFromParent(parent)) {
                // 定期校验：上游环境更新（服务端重启换清单）自动跟进
                long minutes = Math.max(1, config.proxyEnvResyncMinutes);
                // 定期轮次同样走专用线程：syncFromParent 内部 join 的是 io 池上的同步任务
                // （EnvSyncClient.syncTo），放回 io 池就是"池内任务等池内任务"
                resyncTask = ThreadPools.scheduler().scheduleWithFixedDelay(
                        () -> ThreadPools.startBlocking("proxy-env-resync",
                                () -> syncFromParent(parent)),
                        minutes, minutes, TimeUnit.MINUTES);
                return;
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(RETRY_SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * 执行一轮上游同步（io 线程，可阻塞）：连接 → HELLO → 全量 syncTo → 本地扫描发布。
     * 每轮独立建连、用完即关 —— 中转端与上级服务端之间不保持常驻连接，
     * 上级重启对中转端无感（下一轮重连即可）。
     *
     * @return 是否成功（失败由调用方决定重试节奏）
     */
    private synchronized boolean syncFromParent(String parent) {
        if (!running) {
            return false;
        }
        String host = parent;
        int port = config.controlPort;
        // 支持 "host:端口" 与纯 "host"；仅恰有一个冒号时拆分（防误截 IPv6）
        int colon = parent.lastIndexOf(':');
        if (colon > 0 && parent.indexOf(':') == colon && colon < parent.length() - 1) {
            try {
                port = Integer.parseInt(parent.substring(colon + 1));
                host = parent.substring(0, colon);
            } catch (NumberFormatException ignored) {
                // 冒号后不是数字：整串当主机名
            }
        }
        ControlClient cc = new ControlClient(host, port);
        boolean sourceUpdateBegun = false;
        boolean publishUpdate = false;
        EnvironmentSnapshotManager manager = null;
        try {
            ControlConnection conn = cc.connect();
            // v9使用只读预检取得真实worldPath；不能按worldName猜路径，也不必为下载注册玩家身份。
            JsonObject hello = new JsonObject();
            hello.addProperty("version", Protocol.VERSION);
            ControlMessage ack = conn.request(ControlMessage.of(MessageType.ENV_PREFLIGHT, hello))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (ack.type() != MessageType.ENV_PREFLIGHT || !JsonUtil.getBoolean(ack.json(), "accepted", false)) {
                LOGGER.warn("上级服务端拒绝握手: {}",
                        JsonUtil.getString(ack.json(), "reason", "未知原因"));
                return false;
            }
            if (!JsonUtil.getBoolean(ack.json(), "envReady", false)) {
                LOGGER.info("上级服务端环境清单尚未就绪，稍后重试");
                return false;
            }
            EnvironmentPreflightInfo info = EnvironmentPreflightInfo.fromJson(ack.json());
            if (!running) return false;
            if (snapshots == null || !info.worldPath().equals(worldPath)) {
                environmentHandlers = java.util.Map.of();
                if (snapshots != null) snapshots.stop();
                manager = new EnvironmentSnapshotManager(paths.proxyEnvDir(), paths.environmentSnapshotsDir(),
                        EnvironmentPathPolicy.worldMetadataExclusions(info.worldPath()), config.envSnapshotRetentionMinutes);
                snapshots = manager;
                worldPath = info.worldPath();
                java.util.Map<MessageType, imsng.player_to_player.netproto.MessageHandler> handlers = new java.util.HashMap<>();
                EnvSyncServerHandlers.register(handlers::put, manager, config);
                environmentHandlers = java.util.Map.copyOf(handlers);
            } else manager = snapshots;
            // 全量视图同步（target=null：不过滤，中转端持有整仓）
            manager.beginSourceUpdate();
            sourceUpdateBegun = true;
            new EnvSyncClient(conn, config, info.worldPath()).syncTo(paths.proxyEnvDir(), null, info.snapshotId()).join();
            // 首轮成功后才启动管理器，避免把空缓存发布成“就绪环境”；后续调用幂等。
            if (!running) return false;
            manager.start();
            if (!running) { manager.stop(); return false; }
            publishUpdate = true;
            LOGGER.info("中转端上游环境同步完成，已触发不可变快照发布");
            return true;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOGGER.warn("中转端环境同步失败（{} 秒后重试）: {}", RETRY_SECONDS, e.toString());
            return false;
        } finally {
            if (sourceUpdateBegun) {
                manager.endSourceUpdate(publishUpdate);
            }
            cc.close();
        }
    }

    /** 停止服务（幂等）：取消定期任务；清单保留（重启前仍可继续分发旧版本）。 */
    public void stop() {
        running = false;
        ScheduledFuture<?> task = resyncTask;
        resyncTask = null;
        if (task != null) {
            task.cancel(false);
        }
        EnvironmentSnapshotManager manager = snapshots;
        if (manager != null) manager.stop();
    }
}
