package imsng.player_to_player.proxy;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 中转服务端主服务（{@code mode=proxy_server}，DESIGN.md 第 1 节）。
 * <p>
 * 规范"中转服务端"：辅助 p2p 打洞、打洞不成功则充当中转、
 * 同时可以给主客户端和副客户端分发模组文件以及配置文件使其环境相同。
 * 中转服务端宿主在一个 fabric 服务端实例上（见 P2PBootstrap：
 * SERVER_STARTED → start()，SERVER_STOPPING → stop()），
 * 但不承担任何世界运算。
 * <p>
 * <b>分期口径（与 DESIGN.md 第 0 节路线图一致）</b>：Phase 1 只启动
 * <b>分期口径（与 DESIGN.md 第 0 节路线图一致）</b>：Phase 1 启动
 * {@link RelayCore}（打洞协助 + 中转）；Phase 2 增加环境文件分发
 * （{@link ProxyEnvService}：按 parentServerAddress 从上级服务端全量同步环境，
 * 复用 EnvSyncServerHandlers 在中转控制端口上向客户端分发）。
 * <p>
 * 静态单例状态（volatile + synchronized 双保险），start/stop 幂等。
 */
public final class ProxyServerService {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/proxy");

    /** 运行中的中转核心；null 表示服务未启动。 */
    private static volatile RelayCore relay;

    /** 运行中的环境分发服务；null = 未启用（未配置上级服务端地址）。 */
    private static volatile ProxyEnvService envService;

    private ProxyServerService() {
    }

    /** 启动中转服务（幂等：重复调用忽略）。 */
    public static synchronized void start() {
        if (relay != null) {
            return;
        }
        // 配置来自全局上下文（P2PBootstrap 已在模组初始化时填充）
        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        RelayCore core = new RelayCore(config.relayPort);

        // Phase 2：环境分发（规范：中转端也向客户端分发模组/配置文件）。
        // 必须在 core.start() 之前挂接 —— ENV_* 处理器要注册进同一个 ControlServer
        ProxyEnvService env = new ProxyEnvService(config, ctx.paths());
        if (env.attachAndStart(core)) {
            envService = env;
            LOGGER.info("中转端环境分发已启用 (上级服务端={})", config.parentServerAddress);
        }

        core.start();
        relay = core;
        LOGGER.info("中转服务端已启动 (relayPort={})", config.relayPort);

        // TODO Phase 4: 指令/聊天逐级路由的中转段（副客户端→主客户端→中转服务端→服务端）。
    }

    /** 停止中转服务（幂等：未启动/已停止忽略）。 */
    public static synchronized void stop() {
        ProxyEnvService env = envService;
        envService = null;
        if (env != null) {
            env.stop();
        }
        RelayCore core = relay;
        relay = null;
        if (core != null) {
            core.stop();
            LOGGER.info("中转服务端已停止");
        }
    }
}
