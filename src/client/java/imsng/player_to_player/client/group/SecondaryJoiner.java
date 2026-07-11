package imsng.player_to_player.client.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.p2p.P2PSessions;
import imsng.player_to_player.p2p.P2PTransport;
import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.p2p.TcpTunnel;
import net.minecraft.client.Minecraft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 副客户端加入器（Phase 2，规范"副客户端……通过 p2p 连接主客户端"）。
 * <p>
 * 被指派为副客户端后（io 线程调用 {@link #join}）：
 * <ol>
 *   <li>先挂 {@link P2PSessions} 会话监听器（按对端 clientId 过滤），<b>再</b>发
 *       P2P_CONNECT_REQUEST —— sessionId 由服务端生成、经 P2P_ENDPOINT_EXCHANGE
 *       才回到本端，先监听后请求才不会漏事件；</li>
 *   <li>等待会话就绪（直连打洞，失败时服务端自动指示中转降级，两条路都走
 *       同一个监听器，上限 {@value #CONNECT_TIMEOUT_SECONDS} 秒）；</li>
 *   <li>可靠通道上送会话头 {@code op=tunnel_join}，在 127.0.0.1 开随机监听口
 *       建立 TCP 隧道；</li>
 *   <li>主线程编排性断开物理服务端，经隧道口连接主客户端的集成服务端。</li>
 * </ol>
 * 失败语义：任何一步失败都只记日志并保持现状 —— 玩家仍留在物理服务端的
 * （挂起演算的）世界里，与服务端的控制连接不受影响，重进世界可重试。
 */
public final class SecondaryJoiner {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/secondary");

    /** 等待 P2P 会话就绪的上限（秒）：覆盖打洞（约 5 秒）+ 中转降级建连。 */
    private static final long CONNECT_TIMEOUT_SECONDS = 30;

    private SecondaryJoiner() {
    }

    /**
     * 以副客户端身份加入主客户端的组世界（io 线程，可阻塞）。
     *
     * @param conn            与物理服务端的控制连接
     * @param primaryClientId 主客户端 clientId（ROLE_ASSIGN 下发）
     */
    public static void join(Minecraft minecraft, ControlConnection conn, UUID primaryClientId) {
        // ---- 1. 先监听后请求（时序见类 Javadoc）----
        CompletableFuture<P2PTransport> ready = new CompletableFuture<>();
        P2PSessions.SessionListener listener = (sessionId, peerClientId, transport) -> {
            if (primaryClientId.equals(peerClientId)) {
                ready.complete(transport); // 已完成时的重复 complete 是 no-op
            }
        };
        P2PSessions.addListener(listener);
        try {
            // ---- 2. 请求撮合并等待传输就绪 ----
            JsonObject req = new JsonObject();
            req.addProperty("targetClientId", primaryClientId.toString());
            conn.send(ControlMessage.of(MessageType.P2P_CONNECT_REQUEST, req));
            LOGGER.info("已请求与主客户端 {} 建立 P2P，等待打洞/中转…", primaryClientId);

            P2PTransport transport;
            try {
                transport = ready.get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                LOGGER.error("与主客户端 {} 的 P2P 在 {} 秒内未就绪（打洞失败且中转不可用？），"
                        + "加入组世界放弃，玩家留在物理服务端世界", primaryClientId, CONNECT_TIMEOUT_SECONDS);
                return;
            }

            // ---- 3. 会话头 + 本机隧道 ----
            ReliableChannel channel = new ReliableChannel(transport, "join:" + primaryClientId);
            JsonObject header = new JsonObject();
            header.addProperty("op", "tunnel_join");
            header.addProperty("clientId", NodeContext.get().clientId().toString());
            header.addProperty("playerName", minecraft.getUser().getName());
            TcpTunnel.writeHeader(channel, header.toString());
            int port = TcpTunnel.listenAndBridge(channel, "join:" + primaryClientId);
            WorldSwitcher.markTunnelPort(port);

            // ---- 4. 主线程切换 ----
            minecraft.execute(() -> WorldSwitcher.switchToTunnel(
                    minecraft, port, primaryClientId.toString()));
        } catch (Exception e) {
            LOGGER.error("加入组世界失败（玩家留在物理服务端世界，重进可重试）", e);
        } finally {
            P2PSessions.removeListener(listener);
        }
    }
}
