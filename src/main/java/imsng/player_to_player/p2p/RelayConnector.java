package imsng.player_to_player.p2p;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlClient;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * 中转连接管理器（客户端侧，Phase 2）：维护到中转端（{@code RelayCore}）的
 * <b>共享</b>控制连接，供多条 {@link RelayClient} 中转会话复用。
 * <p>
 * 端点来源：HELLO_ACK 的 relayAddress/relayPort（{@code WorldSession} 在握手后
 * 调用 {@link #configure} 填入；relayAddress 空串 = 服务端兼任中转，复用服务端主机）。
 * <p>
 * 生命周期与世界会话一致：{@code WorldSession.onLeave} 调用 {@link #closeAll}。
 * 连接懒建立（首次需要中转时才连），建立后：
 * <ol>
 *   <li>挂 {@link RelayClient#registerDispatcher}（每条连接一次，RELAY_FORWARD
 *       入站按 sessionId 分发到各中转会话）；</li>
 *   <li>发 RELAY_REGISTER 登记本机 clientId（使本机可被对端经中转寻址）。</li>
 * </ol>
 * 线程模型：{@link #getOrConnect} 阻塞建连，只能在 io/后台线程调用；
 * 全部方法以类锁串行化（低频操作，不需要更细粒度）。
 */
public final class RelayConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/relay-connector");

    /** 配置的中转端点（configure 填入；host 空串表示未配置/不可用）。 */
    private static String host = "";
    private static int port;

    /** 共享中转连接；null = 尚未建立。 */
    private static ControlClient client;

    private RelayConnector() {
    }

    /**
     * 配置中转端点（WorldSession 握手后调用）。
     *
     * @param relayHost 中转主机；空串 = 无独立中转（调用方应传服务端主机作兜底）
     * @param relayPort 中转端口；&le;0 表示中转不可用
     */
    public static synchronized void configure(String relayHost, int relayPort) {
        host = relayHost == null ? "" : relayHost.trim();
        port = relayPort;
    }

    /** 中转是否已配置且可用。 */
    public static synchronized boolean available() {
        return !host.isEmpty() && port > 0;
    }

    /**
     * 获取（或懒建立）共享中转连接（阻塞，io/后台线程调用）。
     *
     * @throws IOException 未配置中转或建连失败
     */
    public static synchronized ControlConnection getOrConnect() throws IOException {
        if (host.isEmpty() || port <= 0) {
            throw new IOException("中转端点未配置或不可用");
        }
        if (client != null) {
            ControlConnection existing = client.connection();
            if (existing != null) {
                return existing;
            }
            // 旧连接已死：整体丢弃重建（ControlClient 的事件循环随 close 释放）
            client.close();
            client = null;
        }
        ControlClient cc = new ControlClient(host, port);
        // RELAY_FORWARD 入站分发器：每条连接只注册一次（HandlerRegistry 拒绝重复注册）
        RelayClient.registerDispatcher(cc);
        ControlConnection conn = cc.connect();
        // 登记本机 clientId：使对端发往本机的 RELAY_FORWARD 可被中转端寻址
        UUID selfId = NodeContext.get().clientId();
        JsonObject reg = new JsonObject();
        reg.addProperty("clientId", selfId.toString());
        conn.send(ControlMessage.of(MessageType.RELAY_REGISTER, reg));
        client = cc;
        LOGGER.info("中转连接已建立并登记: {}:{} (clientId={})", host, port, selfId);
        return conn;
    }

    /** 关闭共享中转连接（世界会话结束时调用；幂等）。 */
    public static synchronized void closeAll() {
        if (client != null) {
            client.close();
            client = null;
            LOGGER.info("中转连接已关闭");
        }
    }
}
