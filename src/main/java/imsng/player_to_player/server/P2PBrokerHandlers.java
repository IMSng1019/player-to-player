package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.p2p.NatInfo;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
 *       <b>Phase 2 中转降级</b>（规范"打洞不成功的情况下由中转服务器负责中转"）：
 *       任一方回报失败且中转可用（{@link HelloHandler#appendRelayInfo} 同一套端点
 *       口径）时，向<b>双方</b>下发 {@link MessageType#P2P_USE_RELAY}，双方各自
 *       与中转端建立同 sessionId 的 RELAY_FORWARD 会话；中转不可用则只记日志
 *       （规范：服务端选择不中转时，打洞不成功则放弃）。</li>
 * </ul>
 * <p>
 * 会话跟踪表按 sessionId 记录撮合双方，供结果回报关联；条目在"双方均成功 /
 * 已指示中转 / 超龄"时移除（超龄由后续请求顺带惰性清理，避免额外定时器）。
 * <p>
 * 线程模型：全部在 Netty 事件循环上完成（纯内存查表 + 发消息，无阻塞）。
 */
public final class P2PBrokerHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/p2p-broker");

    /** 打洞会话条目的最长存活（毫秒）：超龄视为流程已死，惰性清理。 */
    private static final long SESSION_MAX_AGE_MILLIS = 120_000;

    /** 一次打洞撮合的跟踪状态。 */
    private record PunchSession(UUID requester, UUID target, long createdAtMillis,
                                Map<UUID, Boolean> results, AtomicBoolean relayInstructed) {
        PunchSession(UUID requester, UUID target) {
            this(requester, target, System.currentTimeMillis(),
                    new ConcurrentHashMap<>(), new AtomicBoolean(false));
        }
    }

    /** 活跃打洞会话：sessionId → 跟踪状态。 */
    private static final Map<UUID, PunchSession> SESSIONS = new ConcurrentHashMap<>();

    private P2PBrokerHandlers() {
    }

    /** 把打洞协助处理器挂到控制服务器的分发表上。 */
    public static void register(HandlerRegistry reg, GlobalConfig config) {
        reg.on(MessageType.P2P_CONNECT_REQUEST, P2PBrokerHandlers::handleConnectRequest);
        reg.on(MessageType.P2P_RESULT, (conn, msg) -> handleResult(conn, msg, config));
    }

    /** P2P_CONNECT_REQUEST：撮合双方，交换公网 endpoint。 */
    private static void handleConnectRequest(ControlConnection connection, ControlMessage message) {
        purgeStaleSessions();

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
        SESSIONS.put(sessionId, new PunchSession(requesterId, targetId));

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
     * P2P_RESULT：打洞结果回报 → 中转降级决策（Phase 2）。
     * <ul>
     *   <li>双方均成功 → 会话完结（上层已在直连通道上开工）；</li>
     *   <li>任一方失败 → 立即（不等对方回报：失败方等不来直连）向双方下发
     *       P2P_USE_RELAY；AtomicBoolean 保证两份失败回报只触发一次指示；</li>
     *   <li>中转不可用 → 记日志放弃（规范允许）。Phase 3 合并状态机在此基础上
     *       消费"最终没有任何传输可用"的失败态。</li>
     * </ul>
     */
    private static void handleResult(ControlConnection connection, ControlMessage message,
                                     GlobalConfig config) {
        String rawSession = JsonUtil.getString(message.json(), "sessionId", "?");
        boolean success = JsonUtil.getBoolean(message.json(), "success", false);
        UUID reporter = connection.peerId();
        LOGGER.info("P2P 打洞结果: session={} 客户端={} success={}", rawSession, reporter, success);

        UUID sessionId;
        try {
            sessionId = UUID.fromString(rawSession);
        } catch (IllegalArgumentException e) {
            return; // 非本服生成的会话号：忽略
        }
        PunchSession session = SESSIONS.get(sessionId);
        if (session == null || reporter == null
                || !(reporter.equals(session.requester()) || reporter.equals(session.target()))) {
            return; // 未知会话 / 非当事方回报：忽略（入站不可信）
        }
        session.results().put(reporter, success);

        if (success) {
            Boolean peerResult = session.results().get(
                    reporter.equals(session.requester()) ? session.target() : session.requester());
            if (peerResult != null && peerResult) {
                SESSIONS.remove(sessionId); // 双方均成功：会话完结
            }
            return;
        }

        // ---- 失败 → 中转降级（只触发一次）----
        if (!session.relayInstructed().compareAndSet(false, true)) {
            return;
        }
        SESSIONS.remove(sessionId);

        JsonObject relayInfo = new JsonObject();
        HelloHandler.appendRelayInfo(config, relayInfo); // 与 HELLO_ACK 完全同口径
        if (JsonUtil.getInt(relayInfo, "relayPort", 0) <= 0) {
            LOGGER.warn("P2P 打洞失败且无中转可用，会话放弃: session={}", sessionId);
            return;
        }
        sendUseRelay(session.requester(), session.target(), sessionId, relayInfo);
        sendUseRelay(session.target(), session.requester(), sessionId, relayInfo);
        LOGGER.info("P2P 会话 {} 降级中转: {} ↔ {}", sessionId, session.requester(), session.target());
    }

    /** 向一方下发 P2P_USE_RELAY（对端字段是"另一方"）。 */
    private static void sendUseRelay(UUID recipient, UUID peer, UUID sessionId, JsonObject relayInfo) {
        ControlConnection conn = HelloHandler.connectionOf(recipient);
        if (conn == null || !conn.isOpen()) {
            LOGGER.warn("P2P_USE_RELAY 无法送达（已离线）: {}", recipient);
            return;
        }
        JsonObject json = new JsonObject();
        json.addProperty("sessionId", sessionId.toString());
        json.addProperty("peerClientId", peer.toString());
        json.addProperty("relayAddress", JsonUtil.getString(relayInfo, "relayAddress", ""));
        json.addProperty("relayPort", JsonUtil.getInt(relayInfo, "relayPort", 0));
        conn.send(ControlMessage.of(MessageType.P2P_USE_RELAY, json));
    }

    /** 惰性清理超龄会话（打洞全程秒级，两分钟未完结即视为死会话）。 */
    private static void purgeStaleSessions() {
        long now = System.currentTimeMillis();
        SESSIONS.entrySet().removeIf(e ->
                now - e.getValue().createdAtMillis() > SESSION_MAX_AGE_MILLIS);
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
