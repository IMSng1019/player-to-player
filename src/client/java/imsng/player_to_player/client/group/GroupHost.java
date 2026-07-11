package imsng.player_to_player.client.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
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

import java.util.UUID;

/**
 * 组宿主（主客户端，Phase 2/3）：集成服务端被接管后的对外接待面。
 * <ol>
 *   <li><b>LAN 发布</b>：{@code IntegratedServer.publishServer}（原版"对局域网开放"
 *       的同款入口，签名已 javap 核实）把集成服务端绑到本机随机 TCP 端口 ——
 *       副客户端的隧道最终桥接到这个端口；</li>
 *   <li><b>接待副客户端</b>：挂 {@link P2PSessions} 会话监听器，每当一条<b>入站</b>
 *       P2P 会话就绪（本端非发起方；发起方标记见 SessionListener Javadoc —— 本端
 *       主动发起的预同步会话由 MergeClient 认领，不得被接待面误抢），套
 *       {@link ReliableChannel} 读会话头：{@code op=tunnel_join} → 桥接到 LAN 端口；
 *       {@code op=presync}（Phase 3 合并接管方）→ 移交 {@link #presyncHandler}。</li>
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

    /**
     * 预同步会话处理器（Phase 3；MergeClient 挂接）：参数为会话头 JSON 与已就绪
     * 的可靠通道（所有权移交处理器）。null = 未挂接（op=presync 一律拒绝）。
     */
    private static volatile java.util.function.BiConsumer<JsonObject, ReliableChannel> presyncHandler;

    private GroupHost() {
    }

    /** 挂接预同步会话处理器（MergeClient 注册时调用；null 清除）。 */
    public static void setPresyncHandler(
            java.util.function.BiConsumer<JsonObject, ReliableChannel> handler) {
        presyncHandler = handler;
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
                // Phase 4 组分离：向服务端通告"组世界就绪"——它据此冲刷分离时
                // 暂存的副客户端重定向（早于此刻推送 ROLE_ASSIGN 隧道必然扑空）。
                // 无暂存项时服务端按空操作处理，普通开组也发无妨
                ControlConnection conn = GroupRuntime.conn();
                UUID groupId = GroupRuntime.activeGroupId();
                if (conn != null && conn.isOpen() && groupId != null) {
                    JsonObject ready = new JsonObject();
                    ready.addProperty("groupId", groupId.toString());
                    conn.send(ControlMessage.of(MessageType.GROUP_WORLD_READY, ready));
                }
            } else {
                LOGGER.error("集成服务端 LAN 发布失败（端口 {}），副客户端将无法加入", port);
            }
        });

        // 接待副客户端：入站会话就绪 → 读会话头 → 按 op 分派。
        // 只认非发起方会话：本端主动发起的（合并预同步）由 MergeClient 认领
        P2PSessions.addListener((sessionId, peerClientId, transport, initiator) -> {
            if (initiator) {
                return;
            }
            ThreadPools.io().execute(() -> accept(sessionId, String.valueOf(peerClientId), transport));
        });
        LOGGER.info("组宿主已就绪（会话监听器已挂接）");
    }

    /** 接待一条新入站 P2P 会话（io 线程，可阻塞）。 */
    private static void accept(String sessionId, String peer,
                               imsng.player_to_player.p2p.P2PTransport transport) {
        ReliableChannel channel = new ReliableChannel(transport, "host:" + sessionId);
        try {
            String headerJson = readHeaderWithTimeout(channel);
            JsonObject header = JsonUtil.parseObject(headerJson);
            String op = JsonUtil.getString(header, "op", "");
            switch (op) {
                case "tunnel_join" -> {
                    int port = lanPort;
                    if (port <= 0) {
                        LOGGER.warn("LAN 端口尚未发布，无法接待副客户端 {}（会话 {} 关闭）",
                                peer, sessionId);
                        channel.close();
                        return;
                    }
                    LOGGER.info("副客户端 {} 经会话 {} 请求加入，桥接到 LAN 端口 {}",
                            JsonUtil.getString(header, "playerName", peer), sessionId, port);
                    TcpTunnel.bridgeToLocalPort(channel, port, "host:" + sessionId);
                }
                case "presync" -> {
                    // Phase 3 合并：让出方 A 主动连到接管方 B 推送预同步流
                    var handler = presyncHandler;
                    if (handler == null) {
                        LOGGER.warn("收到预同步会话 {} 但本端无合并进行中，关闭", sessionId);
                        channel.close();
                        return;
                    }
                    handler.accept(header, channel); // 通道所有权移交处理器
                }
                default -> {
                    LOGGER.warn("会话 {} 的用途未知（op={}），关闭", sessionId, op);
                    channel.close();
                }
            }
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
