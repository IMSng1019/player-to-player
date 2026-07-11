package imsng.player_to_player.client.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.p2p.P2PSessions;
import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.p2p.TcpTunnel;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.client.Minecraft;
import net.minecraft.client.server.IntegratedServer;
import net.minecraft.server.MinecraftServer;
import net.minecraft.util.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 组宿主（主客户端，Phase 2）：集成服务端被接管后的对外接待面。
 * <ol>
 *   <li><b>LAN 发布</b>：{@code IntegratedServer.publishServer}（原版"对局域网开放"
 *       的同款入口，签名已 javap 核实）把集成服务端绑到本机随机 TCP 端口 ——
 *       副客户端的隧道最终桥接到这个端口；</li>
 *   <li><b>接待副客户端</b>：挂 {@link P2PSessions} 会话监听器，每当一条 P2P
 *       会话就绪（直连打洞成功或中转降级），套 {@link ReliableChannel} 读会话头：
 *       {@code op=tunnel_join} → 桥接到 LAN 端口（副客户端的 MC 连接从此经隧道
 *       进入集成服务端）；其他 op 留给 Phase 3（预同步等）扩展。</li>
 * </ol>
 * 会话监听器随 {@code P2PSessions.closeAll()}（世界会话拆除）一起清理；
 * LAN 发布随集成服务端停止自然失效。
 */
public final class GroupHost {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/group-host");

    /** 等待对端发来会话头的超时（毫秒）：超时视为死会话，关闭通道。 */
    private static final long HEADER_TIMEOUT_MILLIS = 15_000;

    /** LAN 端口（volatile：发布在主线程，隧道桥接在 io 线程读）。 */
    private static volatile int lanPort;

    private GroupHost() {
    }

    /**
     * 组宿主启动（{@link imsng.player_to_player.group.GroupRuntime#tryAttach} 的
     * 接管回调；服务器主线程，不得阻塞 —— LAN 发布转客户端主线程，接待转 io）。
     */
    public static void start(MinecraftServer server) {
        if (!(server instanceof IntegratedServer integrated)) {
            LOGGER.error("组宿主只能运行在集成服务端上（当前: {}）", server.getClass().getName());
            return;
        }
        Minecraft minecraft = Minecraft.getInstance();
        // publishServer 是客户端侧动作（原版从 ShareToLanScreen 调）：转客户端主线程
        minecraft.execute(() -> {
            int port = HttpUtil.getAvailablePort();
            // 游戏模式沿用世界默认；不开放作弊（组世界的权威演算不该被 /指令 干预）
            if (integrated.publishServer(server.getDefaultGameType(), false, port)) {
                lanPort = port;
                LOGGER.info("集成服务端已发布 LAN 端口 {}，等待副客户端经隧道加入", port);
            } else {
                LOGGER.error("集成服务端 LAN 发布失败（端口 {}），副客户端将无法加入", port);
            }
        });

        // 接待副客户端：会话就绪 → 读会话头 → 桥接
        P2PSessions.addListener((sessionId, peerClientId, transport) ->
                ThreadPools.io().execute(() -> accept(sessionId, String.valueOf(peerClientId), transport)));
        LOGGER.info("组宿主已就绪（会话监听器已挂接）");
    }

    /** 接待一条新 P2P 会话（io 线程，可阻塞）。 */
    private static void accept(String sessionId, String peer,
                               imsng.player_to_player.p2p.P2PTransport transport) {
        ReliableChannel channel = new ReliableChannel(transport, "host:" + sessionId);
        try {
            String headerJson = readHeaderWithTimeout(channel);
            JsonObject header = JsonUtil.parseObject(headerJson);
            String op = JsonUtil.getString(header, "op", "");
            if (!"tunnel_join".equals(op)) {
                // 未知用途的会话（Phase 3 的预同步等在这里分派）：Phase 2 直接拒绝
                LOGGER.warn("会话 {} 的用途未知（op={}），关闭", sessionId, op);
                channel.close();
                return;
            }
            int port = lanPort;
            if (port <= 0) {
                LOGGER.warn("LAN 端口尚未发布，无法接待副客户端 {}（会话 {} 关闭）", peer, sessionId);
                channel.close();
                return;
            }
            LOGGER.info("副客户端 {} 经会话 {} 请求加入，桥接到 LAN 端口 {}",
                    JsonUtil.getString(header, "playerName", peer), sessionId, port);
            TcpTunnel.bridgeToLocalPort(channel, port, "host:" + sessionId);
        } catch (Exception e) {
            LOGGER.warn("接待会话 {} 失败: {}", sessionId, e.toString());
            channel.close();
        }
    }

    /** 读会话头（带总超时：inputStream 无数据时最多阻塞到超时）。 */
    private static String readHeaderWithTimeout(ReliableChannel channel) throws Exception {
        // TcpTunnel.readHeader 阻塞在可靠通道输入流上；用 future 限时兜底 ——
        // 死会话（对端建完连接即消失）不该占住 io 线程与通道资源
        var future = new java.util.concurrent.CompletableFuture<String>();
        ThreadPools.io().execute(() -> {
            try {
                future.complete(TcpTunnel.readHeader(channel));
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        try {
            return future.get(HEADER_TIMEOUT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            channel.close(); // 关通道使阻塞中的 readHeader 线程解除阻塞退出
            throw new java.io.IOException("等待会话头超时");
        }
    }
}
