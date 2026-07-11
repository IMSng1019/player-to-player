package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.p2p.NatInfo;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * P2P 打洞协助处理器（服务端"信令面"，DESIGN.md 第 7 节打洞流程的服务端部分）。
 * <p>
 * 规范：中转服务端/服务端辅助 p2p 打洞 —— A、B 均与服务端保持控制连接，
 * 服务端向双方交换对方的公网 endpoint（P2P_ENDPOINT_EXCHANGE），
 * 双方随后同时向对方 endpoint 发 UDP 探测完成打洞。
 * <ul>
 *   <li>{@code P2P_CONNECT_REQUEST}（json: targetClientId）：请求方希望与目标客户端
 *       建立 P2P。服务端查 {@link HelloHandler} 的在线映射，目标不在线回 ERROR；
 *       否则生成 sessionId 并向<b>双方</b>各发一条 P2P_ENDPOINT_EXCHANGE，
 *       内容是<b>对方</b>的 NAT 观测 ip/port/natType；请求方 initiator=true
 *       （双方需要知道谁先发探测包，避免探测风暴且便于日志排查）。</li>
 *   <li>{@code P2P_RESULT}（json: sessionId, success）：打洞结果回报。
 *       Phase 1 只记录日志；Phase 3 的合并流程会消费该结果决定
 *       继续合并还是降级走中转（RELAY_FORWARD）。</li>
 * </ul>
 * <p>
 * 线程模型：全部在 Netty 事件循环上完成（纯内存查表 + 发消息，无阻塞）。
 */
public final class P2PBrokerHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/p2p-broker");

    private P2PBrokerHandlers() {
    }

    /** 把打洞协助处理器挂到控制服务器的分发表上。 */
    public static void register(HandlerRegistry reg) {
        reg.on(MessageType.P2P_CONNECT_REQUEST, P2PBrokerHandlers::handleConnectRequest);
        reg.on(MessageType.P2P_RESULT, P2PBrokerHandlers::handleResult);
    }

    /** P2P_CONNECT_REQUEST：撮合双方，交换公网 endpoint。 */
    private static void handleConnectRequest(ControlConnection connection, ControlMessage message) {
        // 请求方必须已完成 HELLO 握手（peerId 已设置），否则视为非法请求
        UUID requesterId = connection.peerId();
        if (requesterId == null) {
            sendError(connection, message, "尚未完成握手");
            return;
        }

        UUID targetId;
        try {
            targetId = UUID.fromString(JsonUtil.getString(message.json(), "targetClientId", ""));
        } catch (IllegalArgumentException e) {
            sendError(connection, message, "targetClientId 非法");
            return;
        }
        if (targetId.equals(requesterId)) {
            sendError(connection, message, "不能与自己建立 P2P");
            return;
        }

        ControlConnection targetConn = HelloHandler.connectionOf(targetId);
        if (targetConn == null || !targetConn.isOpen()) {
            sendError(connection, message, "目标客户端不在线: " + targetId);
            return;
        }

        // 会话号标识这一次打洞尝试：双方 P2P_RESULT 回报与中转降级都用它关联
        UUID sessionId = UUID.randomUUID();

        // 向请求方发目标方的 endpoint，向目标方发请求方的 endpoint；
        // 请求方 initiator=true（由它先发探测包）
        connection.send(exchangeMessage(sessionId, targetId, HelloHandler.natOf(targetId), true));
        targetConn.send(exchangeMessage(sessionId, requesterId, HelloHandler.natOf(requesterId), false));

        LOGGER.info("P2P 撮合: session={} 请求方={} → 目标={}", sessionId, requesterId, targetId);
    }

    /**
     * 组装 P2P_ENDPOINT_EXCHANGE：告诉收信方"对方"是谁、公网 endpoint 在哪、
     * NAT 类型如何（收信方据此判断打洞策略，见 NatType.punchLikely）。
     */
    private static ControlMessage exchangeMessage(UUID sessionId, UUID peerClientId,
                                                  NatInfo peerNat, boolean initiator) {
        JsonObject json = new JsonObject();
        json.addProperty("sessionId", sessionId.toString());
        json.addProperty("peerClientId", peerClientId.toString());
        json.addProperty("ip", peerNat.publicIp());
        json.addProperty("port", peerNat.publicPort());
        json.addProperty("natType", peerNat.type().name());
        json.addProperty("initiator", initiator);
        return ControlMessage.of(MessageType.P2P_ENDPOINT_EXCHANGE, json);
    }

    /**
     * P2P_RESULT：打洞结果回报。
     * <p>
     * Phase 1 仅记录（观测打洞成功率、排查 NAT 问题）；Phase 3 的合并状态机
     * 将消费该结果：成功 → 继续预同步，失败 → 双方降级 RELAY_FORWARD 中转。
     */
    private static void handleResult(ControlConnection connection, ControlMessage message) {
        String sessionId = JsonUtil.getString(message.json(), "sessionId", "?");
        boolean success = JsonUtil.getBoolean(message.json(), "success", false);
        LOGGER.info("P2P 打洞结果: session={} 客户端={} success={}",
                sessionId, connection.peerId(), success);
        // TODO Phase 3: 通知合并状态机；失败时若双方 NAT 均不可打洞可主动指示走中转
    }

    /** 统一 ERROR 应答（reply 复制 _rid，让请求方的 request() future 异常完成而非超时）。 */
    private static void sendError(ControlConnection connection, ControlMessage message, String reason) {
        JsonObject json = new JsonObject();
        json.addProperty("code", "P2P_BROKER");
        json.addProperty("message", reason);
        connection.send(message.reply(MessageType.ERROR, json, null));
        LOGGER.warn("P2P 撮合失败({}): {}", connection.peerId(), reason);
    }
}
