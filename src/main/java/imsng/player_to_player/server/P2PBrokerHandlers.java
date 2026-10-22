package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.p2p.NatInfo;
import imsng.player_to_player.p2p.P2PKeyGate;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
 *       否则生成 sessionId，<b>等双方公钥指纹就绪</b>（必要时先按需索要，见下）
 *       再向<b>双方</b>各发一条 P2P_ENDPOINT_EXCHANGE，内容是<b>对方</b>的 NAT 观测
 *       ip/port/natType 与公钥指纹；请求方 initiator=true
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
 * <b>公钥指纹绑定（Phase 2 收口，消除 UDP 中间人）</b>：
 * <ul>
 *   <li>客户端在控制会话建立后主动上报 {@link MessageType#P2P_KEY_REPORT}
 *       （指纹 = 其 X25519 公钥的 SHA-256 hex，见 {@code p2p.KeyFingerprint}）；
 *       本类按 clientId 登记进 {@link P2PKeyGate#SERVER} —— <b>身份以 HELLO 锚定的
 *       连接身份为准</b>，json 里的 clientId 只作日志对照（否则谁都能替别人登记指纹）；</li>
 *   <li>撮合时双方指纹都已就绪 → 立即下发 endpoint，并在各自的消息里带上
 *       <b>对方</b>的 {@code peerKeyFingerprint}；任一方缺失 → 先发
 *       {@link MessageType#P2P_KEY_REQUEST} 按需索要，等上报到达再下发
 *       （"未就绪 → 索取 → 下发"这段纯逻辑在 {@link P2PKeyGate} 里，可脱离 Netty 单测）；</li>
 *   <li>超时兜底 {@value #KEY_WAIT_TIMEOUT_MILLIS} 毫秒：到点仍未就绪就按"无指纹"降级下发并记
 *       warn —— 可用性优先，绝不永久卡住撮合（旧客户端/降级场景仍能打通，只是没有认证）。</li>
 * </ul>
 * <p>
 * 线程模型：全部在 Netty 事件循环上完成（纯内存查表 + 发消息，无阻塞）；
 * 唯一例外是超时兜底，由一个 {@code ThreadPools.scheduler()} 的一次性任务触发
 * （任务体只做"取出超时项并下发"的轻量操作）。
 */
public final class P2PBrokerHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/p2p-broker");

    /** 打洞会话条目的最长存活（毫秒）：超龄视为流程已死，惰性清理。 */
    private static final long SESSION_MAX_AGE_MILLIS = 120_000;

    /**
     * 撮合时等待双方指纹上报的上限（毫秒）：到点按"无指纹"降级下发，绝不永久卡住撮合。
     * <p>
     * 取值理由：指纹上报是"控制连接上的一次往返 + 客户端一次密钥对生成"，正常在毫秒级；
     * 3 秒已覆盖极端网络抖动，又不会让打洞失败判定（上层 5 秒打洞超时 + 中转降级）
     * 明显变慢。超时只是"少一层认证"，不影响连通性。
     */
    private static final long KEY_WAIT_TIMEOUT_MILLIS = 3_000;

    /** 服务端侧指纹表 + 待定撮合门控（纯逻辑类）；身份生命周期由 HelloHandler 挂钩清理。 */
    private static final P2PKeyGate KEY_GATE = P2PKeyGate.SERVER;

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
        // 公钥指纹上报（Phase 2 收口）：登记指纹，并把因此齐备的撮合立即下发
        reg.on(MessageType.P2P_KEY_REPORT, P2PBrokerHandlers::handleKeyReport);
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
        // v6：先回传本次撮合号，再开始下发端点。调用方据此区分同一 peer 的
        // 游戏预连接与预同步，防止两个 ReliableChannel 同时认领同一 transport。
        JsonObject accepted = new JsonObject();
        accepted.addProperty("sessionId", sessionId.toString());
        connection.send(message.reply(MessageType.P2P_CONNECT_REQUEST, accepted, null));

        // ---- 指纹门控：双方指纹齐备才下发；缺失先索要，超时降级（见类 Javadoc）----
        // 先冲刷一次超时项：定时器万一被调度线程挤住，新请求也能把旧撮合推进下去
        long now = System.currentTimeMillis();
        deliverExpired(now);
        P2PKeyGate.Pending pending = new P2PKeyGate.Pending(
                sessionId, requesterId, targetId, now + KEY_WAIT_TIMEOUT_MILLIS);
        List<UUID> missingFingerprints = KEY_GATE.open(pending);
        if (missingFingerprints.isEmpty()) {
            // 双方指纹已就绪：立即下发。complete() 成功才算"认领"本次撮合 ——
            // 并发的指纹上报回调可能已先把它领走（那它就已经下发过了），此处不得重复
            P2PKeyGate.Pending claimed = KEY_GATE.complete(sessionId);
            if (claimed != null) {
                deliverExchange(claimed, false);
            }
        } else {
            for (UUID clientId : missingFingerprints) {
                sendKeyRequest(clientId);
            }
            // 竞态兜底：指纹可能恰好在本线程"读指纹表"与"发出索要"之间到达；
            // 那次上报的回调若跑在待定撮合登记之前就会漏掉本次，这里再领一次补齐。
            // takeReady 是"取出即移除"，与上面的 complete 互斥，不会重复下发。
            for (P2PKeyGate.Pending ready : KEY_GATE.takeReady()) {
                deliverExchange(ready, false);
            }
            // 超时兜底：客户端不支持指纹上报 / 上报帧丢失 / 上报来自已断连连接时，
            // 撮合不能在"等指纹"里永久停住 —— 3 秒后按无指纹降级下发（记 warn）
            scheduleKeyTimeout(sessionId);
        }

        LOGGER.info("P2P 撮合: session={} 请求方={} → 目标={}（待补指纹 {} 方）",
                sessionId, requesterId, targetId, missingFingerprints.size());
    }

    /**
     * 组装 P2P_ENDPOINT_EXCHANGE：告诉收信方"对方"是谁、公网 endpoint 在哪、
     * NAT 类型如何（收信方据此判断打洞策略，见 NatType.punchLikely）。
     *
     * @param peerKeyFingerprint <b>对方</b>公钥的 SHA-256 指纹（64 位 hex）；
     *                           null = 未登记（旧客户端/超时降级）→ 不写字段，
     *                           接收方按"无指纹"继续握手并记 warn（向后兼容）
     */
    private static ControlMessage exchangeMessage(UUID sessionId, UUID peerClientId,
                                                  NatInfo peerNat, boolean initiator,
                                                  String peerKeyFingerprint) {
        JsonObject json = new JsonObject();
        json.addProperty("sessionId", sessionId.toString());
        json.addProperty("peerClientId", peerClientId.toString());
        json.addProperty("ip", peerNat.publicIp());
        json.addProperty("port", peerNat.publicPort());
        json.addProperty("natType", peerNat.type().name());
        json.addProperty("initiator", initiator);
        if (peerKeyFingerprint != null) {
            // 字段存在即为权威：客户端打洞时据此校验对端公钥，不匹配即握手失败
            json.addProperty("peerKeyFingerprint", peerKeyFingerprint);
        }
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
        sendUseRelay(session.requester(), session.target(), sessionId, relayInfo, true);
        sendUseRelay(session.target(), session.requester(), sessionId, relayInfo, false);
        LOGGER.info("P2P 会话 {} 降级中转: {} ↔ {}", sessionId, session.requester(), session.target());
    }

    /** 向一方下发 P2P_USE_RELAY（对端字段是"另一方"；initiator 与直连撮合同口径）。 */
    private static void sendUseRelay(UUID recipient, UUID peer, UUID sessionId,
                                     JsonObject relayInfo, boolean initiator) {
        ControlConnection conn = HelloHandler.connectionOf(recipient);
        if (conn == null || !conn.isOpen()) {
            LOGGER.warn("P2P_USE_RELAY 无法送达（已离线）: {}", recipient);
            return;
        }
        JsonObject json = new JsonObject();
        json.addProperty("sessionId", sessionId.toString());
        json.addProperty("peerClientId", peer.toString());
        json.addProperty("initiator", initiator);
        json.addProperty("relayAddress", JsonUtil.getString(relayInfo, "relayAddress", ""));
        json.addProperty("relayPort", JsonUtil.getInt(relayInfo, "relayPort", 0));
        conn.send(ControlMessage.of(MessageType.P2P_USE_RELAY, json));
    }

    // ------------------------------------------------------------ 公钥指纹门控

    /**
     * P2P_KEY_REPORT：登记客户端公钥指纹，并把"因此齐备"的待定撮合立即下发。
     * <p>
     * <b>身份只认 HELLO 锚定的连接身份</b>（{@link ControlConnection#peerId()}）：
     * json 里的 clientId 仅作日志对照，与连接身份不符一律拒绝 —— 否则任何人都能
     * 替别人登记指纹，整个指纹绑定就失去意义。
     */
    private static void handleKeyReport(ControlConnection connection, ControlMessage message) {
        UUID clientId = connection.peerId();
        if (clientId == null) {
            LOGGER.warn("未完成握手的连接上报 P2P 公钥指纹，忽略: {}", connection.remoteAddress());
            return;
        }
        String claimed = JsonUtil.getString(message.json(), "clientId", "");
        if (!claimed.isEmpty() && !claimed.equals(clientId.toString())) {
            // 只记长度、不回显客户端可控的长串（防日志注入/刷屏）
            LOGGER.warn("P2P 公钥指纹上报身份不符（连接身份 {}，声称 clientId 长度 {}），拒绝",
                    clientId, claimed.length());
            return;
        }
        String rawFingerprint = JsonUtil.getString(message.json(), "fingerprint", "");
        if (!KEY_GATE.report(clientId, rawFingerprint)) {
            LOGGER.warn("P2P 公钥指纹非法（应为 64 位 hex，实收长度 {}），忽略: client={}",
                    rawFingerprint.length(), clientId);
            return;
        }
        LOGGER.info("客户端 {} 上报 P2P 公钥指纹 {}", clientId, KEY_GATE.fingerprintOf(clientId));
        // 指纹齐备的撮合立即下发（takeReady 取出即移除，与超时定时器互斥，不会重复）
        for (P2PKeyGate.Pending ready : KEY_GATE.takeReady()) {
            deliverExchange(ready, false);
        }
    }

    /** 向缺失指纹的一方索要：{@link MessageType#P2P_KEY_REQUEST} 无字段，客户端以 P2P_KEY_REPORT 回应。 */
    private static void sendKeyRequest(UUID clientId) {
        ControlConnection conn = HelloHandler.connectionOf(clientId);
        if (conn == null || !conn.isOpen()) {
            LOGGER.warn("P2P_KEY_REQUEST 无法送达（已离线）: {}", clientId);
            return;
        }
        conn.send(ControlMessage.of(MessageType.P2P_KEY_REQUEST));
        LOGGER.debug("撮合指纹未就绪，已向客户端 {} 索要公钥指纹", clientId);
    }

    /**
     * 一次性超时兜底：{@value #KEY_WAIT_TIMEOUT_MILLIS} 毫秒后冲刷到点仍未齐备的撮合。
     * <p>
     * 不保存 {@code ScheduledFuture} 也不取消：任务体只做一次"领取超时项"的原子操作，
     * 撮合若已正常下发则什么都领不到（幂等），省掉一份需要维护的定时器句柄。
     */
    private static void scheduleKeyTimeout(UUID sessionId) {
        ThreadPools.scheduler().schedule(() -> {
            try {
                deliverExpired(System.currentTimeMillis());
            } catch (Exception e) {
                // 定时任务必须自吞异常：抛出去会让调度线程上的后续任务静默失效
                LOGGER.warn("撮合 {} 的指纹超时兜底异常", sessionId, e);
            }
        }, KEY_WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * 冲刷所有已过指纹等待截止时刻的待定撮合：按"缺失指纹 = 不下发该字段"降级下发。
     * <p>
     * 触发路径有两条且都是幂等的：本次撮合自己的定时任务，以及任意新的
     * P2P_CONNECT_REQUEST（防止调度线程被挤住时撮合无限期悬挂）。
     */
    private static void deliverExpired(long nowMillis) {
        for (P2PKeyGate.Pending expired : KEY_GATE.takeExpired(nowMillis)) {
            LOGGER.warn("撮合指纹等待超时 {}ms，按无指纹降级下发: session={} 请求方={} 目标={}",
                    KEY_WAIT_TIMEOUT_MILLIS, expired.sessionId(),
                    expired.requester(), expired.target());
            deliverExchange(expired, true);
        }
    }

    /**
     * 向撮合双方各下发一条 endpoint（各自携带<b>对方</b>的指纹）。
     * <p>
     * 调用前必须已"认领"该撮合（{@link P2PKeyGate#takeReady()} /
     * {@link P2PKeyGate#takeExpired(long)} / {@link P2PKeyGate#complete(UUID)} 之一），
     * 取出即移除的语义保证每个撮合只下发一次。
     *
     * @param degraded true = 指纹等待超时后的降级下发（仅用于日志定性）
     */
    private static void deliverExchange(P2PKeyGate.Pending pending, boolean degraded) {
        UUID sessionId = pending.sessionId();
        ControlConnection requesterConn = HelloHandler.connectionOf(pending.requester());
        ControlConnection targetConn = HelloHandler.connectionOf(pending.target());
        boolean requesterOnline = requesterConn != null && requesterConn.isOpen();
        boolean targetOnline = targetConn != null && targetConn.isOpen();
        if (!requesterOnline || !targetOnline) {
            // 等待指纹期间一方掉线：撮合不可能开始，收回结果跟踪条目即可（对端会自行超时）
            LOGGER.warn("撮合 {} 下发取消：一方已离线（请求方在线={} 目标在线={}）",
                    sessionId, requesterOnline, targetOnline);
            SESSIONS.remove(sessionId);
            return;
        }
        String requesterFingerprint = KEY_GATE.fingerprintOf(pending.requester());
        String targetFingerprint = KEY_GATE.fingerprintOf(pending.target());
        // 向请求方发目标方的 endpoint，向目标方发请求方的 endpoint；
        // 请求方 initiator=true（由它先发探测包）
        requesterConn.send(exchangeMessage(sessionId, pending.target(),
                HelloHandler.natOf(pending.target()), true, targetFingerprint));
        targetConn.send(exchangeMessage(sessionId, pending.requester(),
                HelloHandler.natOf(pending.requester()), false, requesterFingerprint));
        if (requesterFingerprint == null || targetFingerprint == null) {
            LOGGER.warn("撮合 {} 下发时仍有指纹缺失（请求方指纹={} 目标指纹={}），缺失侧握手不做公钥认证",
                    sessionId, requesterFingerprint != null, targetFingerprint != null);
        }
        LOGGER.info("P2P 撮合下发: session={} 请求方={} 目标={} 降级={}",
                sessionId, pending.requester(), pending.target(), degraded);
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
