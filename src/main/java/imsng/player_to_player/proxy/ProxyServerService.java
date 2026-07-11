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
 * {@link RelayCore}（打洞协助 + 中转）；<b>中转端环境文件分发属 Phase 2</b> ——
 * 届时中转端按 {@link imsng.player_to_player.config.GlobalConfig#parentServerAddress}
 * 从其上级服务端同步环境文件，再复用 EnvSyncServerHandlers 向客户端分发。
 * <p>
 * 静态单例状态（volatile + synchronized 双保险），start/stop 幂等。
 */
public final class ProxyServerService {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/proxy");

    /** 运行中的中转核心；null 表示服务未启动。 */
    private static volatile RelayCore relay;

    private ProxyServerService() {
    }

    /** 启动中转服务（幂等：重复调用忽略）。 */
    public static synchronized void start() {
        if (relay != null) {
            return;
        }
        // 配置来自全局上下文（P2PBootstrap 已在模组初始化时填充）
        GlobalConfig config = NodeContext.get().config();
        RelayCore core = new RelayCore(config.relayPort);
        core.start();
        relay = core;
        LOGGER.info("中转服务端已启动 (relayPort={})", config.relayPort);

        // TODO Phase 2: 中转端环境文件分发（规范：中转端也向客户端分发模组/配置文件）——
        //  按 parentServerAddress 从上级服务端同步环境文件后，把 EnvSyncServerHandlers
        //  挂到 RelayCore 的 ControlServer 或独立控制端口上。

        // TODO Phase 4: 指令/聊天逐级路由的中转段（副客户端→主客户端→中转服务端→服务端）。
    }

    /** 停止中转服务（幂等：未启动/已停止忽略）。 */
    public static synchronized void stop() {
        RelayCore core = relay;
        relay = null;
        if (core != null) {
            core.stop();
            LOGGER.info("中转服务端已停止");
        }
    }
}
