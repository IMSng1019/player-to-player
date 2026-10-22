package imsng.player_to_player.p2p;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 客户端侧 P2P 会话管理（DESIGN.md 第 7 节"打洞流程"，规范"预连接"）。
 * <p>
 * 处理服务端/中转端下发的 {@link MessageType#P2P_ENDPOINT_EXCHANGE}：
 * <ol>
 *   <li>解析对端 endpoint / NAT 类型 / initiator 标记（入站字段按不可信校验）；</li>
 *   <li>用双方 NAT 类型做 {@link NatType#punchLikely} 预判 —— 明显无望
 *       （UDP 阻断 / 双对称）直接回报失败，省掉 5 秒白等，服务端可立刻
 *       安排中转降级；</li>
 *   <li>否则在 {@code ThreadPools.io()} 上执行 {@link HolePuncher#punch}
 *       （优先复用 NAT 探测遗留的 socket 命中已有映射，其次按
 *       {@code NodeContext.natInfo().localPort} 新建）；</li>
 *   <li>结果以 {@link MessageType#P2P_RESULT}（sessionId, success）回报服务端；</li>
 *   <li>成功的通道存入静态会话表，上层（副客户端隧道 / Phase 3 预同步）
 *       经 {@link #addListener 会话监听器} 或 {@link #get} 领取。</li>
 * </ol>
 * <b>中转降级（Phase 2）</b>：打洞失败时服务端撮合器向双方下发
 * {@link MessageType#P2P_USE_RELAY}，本类经 {@link RelayConnector} 建立
 * {@link RelayClient} 中转会话（同 sessionId）——上层拿到的传输对直连/中转透明。
 * <p>
 * <b>公钥指纹绑定（Phase 2 收口，消除 UDP 中间人）</b>：
 * <ol>
 *   <li>本类持有<b>本次控制会话唯一的</b> X25519 密钥对（惰性生成，见
 *       {@link #sessionKeyPair()}），世界会话拆除时弃用；</li>
 *   <li>会话建立（{@link #register}）后<b>主动上报</b>其公钥指纹，收到服务端
 *       {@link MessageType#P2P_KEY_REQUEST} 时再报一次 —— 覆盖"撮合时对端指纹尚未
 *       就绪"的竞态（服务端另有 3 秒超时降级兜底，不会卡住撮合）；</li>
 *   <li>打洞复用<b>同一把</b>密钥对，因此"上报的指纹"与"UDP KEY 包里的公钥"必然同源；
 *       服务端把对端指纹随 {@code P2P_ENDPOINT_EXCHANGE.peerKeyFingerprint} 下发，
 *       {@link HolePuncher} 据此拒绝对端被替换的公钥。</li>
 * </ol>
 * <b>残留信任模型</b>：指纹经服务端控制信道（明文 TCP，身份由 HELLO 锚定）传递，
 * 信任锚是游戏服本身 —— 游戏服被攻陷时中间人依旧可能（服务器在信任边界内的固有前提），
 * 但 UDP 打洞路径上的旁路攻击者已被排除。字段缺失/非法（旧服务端、服务端降级）时
 * 沿用旧行为并记 warn，保证向后兼容。
 */
public final class P2PSessions {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/p2p-sessions");

    /** sessionId 长度上限（服务端生成的 UUID 级别字符串；防恶意超长串占内存）。 */
    private static final int MAX_SESSION_ID_CHARS = 128;

    /** 活跃会话表：sessionId → 传输（P2PChannel 直连或 RelayClient 中转）。 */
    private static final Map<String, P2PTransport> SESSIONS = new ConcurrentHashMap<>();

    /** sessionId → 对端 clientId（EXCHANGE/USE_RELAY 均携带；监听器回调用）。 */
    private static final Map<String, UUID> SESSION_PEERS = new ConcurrentHashMap<>();
    /** 取消的撮合号短期保留，迟到打洞/中转结果不得复活资源。 */
    private static final Map<String, Long> CANCELLED = new ConcurrentHashMap<>();
    private static final java.util.concurrent.atomic.AtomicLong GENERATION =
            new java.util.concurrent.atomic.AtomicLong();

    /** 会话就绪监听器（主客户端组宿主、副客户端加入器挂接）。 */
    private static final List<SessionListener> LISTENERS = new CopyOnWriteArrayList<>();

    /**
     * 本次控制会话的 X25519 密钥对（惰性生成、会话内复用；世界会话拆除时弃用）。
     * <p>
     * 为什么必须由本类持有而不是让 {@link HolePuncher} 每次自建：上报给服务端的
     * 指纹必须与打洞 KEY 包里发出的公钥严格同源，否则对端指纹校验必然失败
     * （安全问题会被伪装成连通性问题）。服务端的指纹表按 clientId 只存一份，
     * 与"一把密钥对 ↔ 一个控制会话"的粒度对齐（见类 Javadoc）。
     */
    private static final AtomicReference<KeyPair> SESSION_KEY_PAIR = new AtomicReference<>();

    /**
     * 会话就绪回调：直连打洞成功或中转会话建立时触发（io/Netty 线程，不得阻塞）。
     *
     * @apiNote {@code initiator} = 本端是否是这次撮合的发起方（P2P_CONNECT_REQUEST
     * 的请求方）。Phase 3 起主客户端可能同时"被动接待副客户端"（GroupHost）与
     * "主动向另一主客户端发起预同步"（MergeClient）——两类监听器靠该标记分流，
     * 否则被动接待方会误抢本端主动发起的会话并与发起方争抢通道所有权。
     */
    @FunctionalInterface
    public interface SessionListener {
        void onSessionReady(String sessionId, UUID peerClientId, P2PTransport transport,
                            boolean initiator);
    }

    private P2PSessions() {
    }

    /**
     * 发起并准确领取一次撮合（专用阻塞线程调用）。监听先于请求；服务端回复
     * sessionId 后只接收该会话。失败时留下取消标记，迟到传输立即关闭。
     */
    public static P2PTransport connect(ControlConnection conn, UUID peer) throws Exception {
        PendingSession pending = new PendingSession(peer);
        SessionListener listener = pending::ready;
        addListener(listener);
        String sessionId = null;
        boolean transferred = false;
        try {
            JsonObject request = new JsonObject();
            request.addProperty("targetClientId", peer.toString());
            var replyFuture = conn.request(ControlMessage.of(MessageType.P2P_CONNECT_REQUEST, request));
            ControlMessage reply = replyFuture.get(15, java.util.concurrent.TimeUnit.SECONDS);
            if (reply.type() != MessageType.P2P_CONNECT_REQUEST)
                throw new java.io.IOException("撮合请求被拒绝");
            sessionId = UUID.fromString(JsonUtil.getString(reply.json(), "sessionId", "")).toString();
            pending.bind(sessionId);
            pending.future().get(30, java.util.concurrent.TimeUnit.SECONDS);
            if (!conn.isOpen()) throw new java.io.IOException("控制会话已关闭");
            P2PTransport transport = pending.take();
            transferred = true;
            return transport;
        } finally {
            removeListener(listener);
            if (!transferred) {
                if (sessionId != null) {
                    CANCELLED.put(sessionId, System.nanoTime());
                    P2PTransport transport = remove(sessionId);
                    if (transport != null) transport.close();
                }
                pending.cancel();
            }
        }
    }

    /**
     * 注册 P2P 撮合相关处理器（P2P_ENDPOINT_EXCHANGE / P2P_USE_RELAY / P2P_KEY_REQUEST），
     * 并<b>主动上报</b>本方公钥指纹。
     * <p>
     * 主动上报的时机与理由：本方法由 WorldSession 在 HELLO 收到 HELLO_ACK 之后调用，
     * 此刻控制连接的身份已被服务端锚定，指纹上报不会被误挂到别的身份上；
     * 早早上报可让服务端撮合时直接下发 endpoint，省掉一次"索要→等待"往返。
     * 而 {@code P2P_KEY_REQUEST} 处理器用于兜底：撮合时若本端指纹仍未就绪
     * （例如本类刚注册、上报恰在途），服务端会按需索要。
     *
     * @param reg        处理器注册表（ControlClient）
     * @param serverConn 与服务端/中转端的控制连接（P2P_RESULT 与指纹上报从这里发出）
     */
    public static void register(HandlerRegistry reg, ControlConnection serverConn) {
        reg.on(MessageType.P2P_ENDPOINT_EXCHANGE, (conn, msg) -> handleEndpointExchange(serverConn, msg));
        reg.on(MessageType.P2P_USE_RELAY, (conn, msg) -> handleUseRelay(msg));
        reg.on(MessageType.P2P_KEY_REQUEST, (conn, msg) -> reportOwnFingerprint(serverConn));
        reportOwnFingerprint(serverConn);
    }

    /** 挂接会话就绪监听器（重复挂接自负；回调线程见 {@link SessionListener}）。 */
    public static void addListener(SessionListener listener) {
        LISTENERS.add(listener);
    }

    /** 摘除会话就绪监听器。 */
    public static void removeListener(SessionListener listener) {
        LISTENERS.remove(listener);
    }

    /** 按 sessionId 取活跃传输；不存在或已关闭返回 null（已关闭的顺手清出表）。 */
    public static P2PTransport get(String sessionId) {
        P2PTransport transport = SESSIONS.get(sessionId);
        if (transport != null && !transport.isOpen()) {
            SESSIONS.remove(sessionId, transport);
            return null;
        }
        return transport;
    }

    /** 移除并返回会话（上层接管所有权或会话终结时调用；不自动 close）。 */
    public static P2PTransport remove(String sessionId) {
        return SESSIONS.remove(sessionId);
    }

    /** 存入会话（打洞成功内部调用；中转降级建立后 RelayClient 的持有方也可存入）。 */
    public static void put(String sessionId, P2PTransport transport) {
        long now = System.nanoTime();
        CANCELLED.entrySet().removeIf(e -> now - e.getValue() > 120_000_000_000L);
        if (CANCELLED.containsKey(sessionId)) {
            transport.close();
            return;
        }
        P2PTransport previous = SESSIONS.put(sessionId, transport);
        if (previous != null && previous != transport) {
            previous.close(); // 同会话被替换：旧传输关闭防泄漏
        }
    }

    /**
     * 关闭全部活跃会话并清空会话表（WorldSession.onLeave 调用）。
     * <p>
     * 为什么需要整体关闭入口：会话表是静态的，离开世界时若只关控制连接，
     * 打洞成功的 UDP socket、阻塞 receive 的 io 线程、keepalive 定时任务
     * 都会残留并继续向旧对端发包，多次进出世界持续累积泄漏。
     * <p>
     * 逐个 close 各自 try/catch —— 单个传输关闭抛异常不能阻断其余会话的回收。
     * P2PChannel.close / RelayClient.close 均以 AtomicBoolean CAS 保证幂等，
     * 与其他路径（get 顺手清理、put 替换旧值、RelayClient 断连自摘）重复关闭安全；
     * ConcurrentHashMap 迭代期间容忍并发移除（RelayClient.close 会自摘出表）。
     */
    public static void closeAll() {
        GENERATION.incrementAndGet();
        int count = 0;
        for (Map.Entry<String, P2PTransport> entry : SESSIONS.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.warn("会话 {} 关闭失败（继续关闭其余会话）", entry.getKey(), e);
            }
            count++;
        }
        SESSIONS.clear();
        SESSION_PEERS.clear();
        CANCELLED.clear();
        LISTENERS.clear();
        // 会话密钥对随世界会话弃用：重进世界换新密钥（register 时会重新上报指纹）。
        // 服务端侧的同名指纹也会在新 HELLO 与断连清理路径被移除 —— 两侧生命周期一致，
        // 不会出现"服务端留着旧指纹、客户端已换新密钥"的错配（那会把安全问题
        // 伪装成对端指纹校验失败，白白降级到中转）。
        SESSION_KEY_PAIR.set(null);
        if (count > 0) {
            LOGGER.info("已关闭全部 P2P 会话: {} 个", count);
        }
    }

    /** 查询会话的对端 clientId；未知返回 null。 */
    public static UUID peerOf(String sessionId) {
        return SESSION_PEERS.get(sessionId);
    }

    /** 会话就绪：登记对端并通知全部监听器（监听器异常互不影响）。 */
    private static void fireSessionReady(String sessionId, UUID peerClientId,
                                         P2PTransport transport, boolean initiator) {
        if (!transport.isOpen()) return;
        if (peerClientId != null) {
            SESSION_PEERS.put(sessionId, peerClientId);
        }
        for (SessionListener listener : LISTENERS) {
            try {
                listener.onSessionReady(sessionId, peerClientId, transport, initiator);
            } catch (Exception e) {
                LOGGER.error("会话监听器异常: session={}", sessionId, e);
            }
        }
    }

    // ------------------------------------------------------------ 消息处理

    /**
     * 处理 P2P_ENDPOINT_EXCHANGE（Netty 事件循环线程 —— 只做解析与预判，
     * 打洞阻塞逻辑丢给 IO 线程）。
     */
    private static void handleEndpointExchange(ControlConnection serverConn, ControlMessage msg) {
        JsonObject json = msg.json();
        String sessionId = JsonUtil.getString(json, "sessionId", "");
        String peerIp = JsonUtil.getString(json, "ip", "");
        int peerPort = JsonUtil.getInt(json, "port", 0);
        boolean initiator = JsonUtil.getBoolean(json, "initiator", false);
        UUID peerClientId = parseUuid(JsonUtil.getString(json, "peerClientId", ""));

        // 入站字段校验（不可信数据）：非法直接回报失败，不让后续逻辑带病跑
        if (sessionId.isEmpty() || sessionId.length() > MAX_SESSION_ID_CHARS) {
            LOGGER.warn("P2P_ENDPOINT_EXCHANGE 缺失/非法 sessionId，忽略");
            return; // 连 sessionId 都没有，无法回报，只能丢弃
        }
        if (peerIp.isEmpty() || peerPort <= 0 || peerPort > 0xFFFF) {
            LOGGER.warn("会话 {} 对端 endpoint 非法: {}:{}", sessionId, peerIp, peerPort);
            reportResult(serverConn, sessionId, false);
            return;
        }
        NatType peerNat;
        try {
            peerNat = NatType.valueOf(JsonUtil.getString(json, "natType", NatType.UNKNOWN.name()));
        } catch (IllegalArgumentException e) {
            peerNat = NatType.UNKNOWN; // 容忍未知枚举（跨版本兼容）
        }
        // 对端公钥指纹（Phase 2 收口）：服务端撮合时随本消息下发，打洞阶段据此拒绝
        // 对端被替换的公钥。字段缺失 = 服务端降级或旧服务端（HolePuncher 会记 warn）；
        // 字段存在但非法 = 入站数据不可信，明确记 warn 后同样按"无指纹"降级。
        String rawPeerFingerprint = JsonUtil.getString(json, "peerKeyFingerprint", "");
        String peerFingerprint = KeyFingerprint.normalize(rawPeerFingerprint);
        if (peerFingerprint == null && !rawPeerFingerprint.isBlank()) {
            // 只记长度、不回显对端可控的长串（防日志注入/刷屏）
            LOGGER.warn("会话 {} 服务端下发的对端指纹非法（长度 {}），本次握手按无指纹降级",
                    sessionId, rawPeerFingerprint.length());
        }

        // NAT 预判：明显打不通就不浪费 5 秒，直接失败让服务端走中转
        NatInfo localNat = NodeContext.get().natInfo();
        NatType localType = localNat != null ? localNat.type() : NatType.UNKNOWN;
        if (!localType.punchLikely(peerNat)) {
            LOGGER.info("会话 {} NAT 预判打洞无望（本机 {} / 对端 {}），直接回报失败",
                    sessionId, localType, peerNat);
            reportResult(serverConn, sessionId, false);
            return;
        }

        // 打洞是阻塞流程（最长约 5 秒），转入 IO 线程执行
        final NatType peerNatFinal = peerNat;
        long generation = GENERATION.get();
        ThreadPools.io().execute(() -> {
            boolean success = false;
            DatagramSocket socket = null;
            try {
                InetSocketAddress peerEndpoint =
                        new InetSocketAddress(InetAddress.getByName(peerIp), peerPort);
                socket = obtainSocket(localNat);
                // 复用本会话密钥对：保证 KEY 包里发出的公钥与已上报给服务端的指纹同源
                P2PChannel channel = HolePuncher.punch(socket, peerEndpoint, sessionId, initiator,
                        sessionKeyPair(), peerFingerprint);
                if (generation != GENERATION.get()) {
                    channel.close();
                    return;
                }
                put(sessionId, channel); // 通道已接管 socket 所有权
                success = true;
                // 会话就绪通知：组宿主/加入器/合并端据此启动隧道或预同步（Phase 2/3）
                fireSessionReady(sessionId, peerClientId, channel, initiator);
            } catch (UnknownHostException e) {
                LOGGER.warn("会话 {} 对端地址解析失败: {}", sessionId, peerIp);
            } catch (SocketException e) {
                LOGGER.warn("会话 {} 创建打洞 socket 失败: {}", sessionId, e.toString());
            } catch (Exception e) {
                // 打洞超时/密钥协商失败等：失败属打洞常态（对端 NAT 严格），info 级即可
                LOGGER.info("会话 {} 打洞失败（本机 {} / 对端 {}）: {}",
                        sessionId, localType, peerNatFinal, e.toString());
            } finally {
                if (!success && socket != null) {
                    socket.close(); // 失败路径回收 socket；成功后归通道管理
                }
            }
            reportResult(serverConn, sessionId, success);
        });
    }

    /**
     * 获取打洞 socket：优先复用 NAT 探测遗留的 socket（NAT 上已有该端口的
     * 映射，NatInfo 上报的 publicPort 才对得上）；已被领取/关闭则按探测时
     * 的本地端口重建，端口被占再退随机端口（此时对锥形 NAT 仍可打通，
     * 只是服务端下发给对端的端口预测不准，成功率下降）。
     */
    private static DatagramSocket obtainSocket(NatInfo localNat) throws SocketException {
        DatagramSocket probe = NatDetector.takeProbeSocket().orElse(null);
        if (probe != null) {
            return probe;
        }
        int preferredPort = localNat != null ? localNat.localPort() : 0;
        if (preferredPort > 0 && preferredPort <= 0xFFFF) {
            try {
                return new DatagramSocket(preferredPort);
            } catch (SocketException e) {
                LOGGER.warn("打洞端口 {} 绑定失败({})，改用随机端口", preferredPort, e.getMessage());
            }
        }
        return new DatagramSocket(0);
    }

    /**
     * 处理 P2P_USE_RELAY（Phase 2 中转降级；Netty 事件循环 —— 建连阻塞逻辑转 io 线程）。
     * <p>
     * 服务端在打洞会话至少一方回报失败且中转可用时向<b>双方</b>下发本消息。
     * 双方各自经 {@link RelayConnector} 的共享中转连接建立 {@link RelayClient}
     * 会话（同 sessionId），随后与直连路径一样触发会话就绪通知。
     * relayAddress 空串 = 服务端兼任中转（RelayConnector 已在 WorldSession
     * 握手后按 HELLO_ACK 配置好端点，这里的字段仅作覆盖）。
     */
    private static void handleUseRelay(ControlMessage msg) {
        long generation = GENERATION.get();
        JsonObject json = msg.json();
        String sessionId = JsonUtil.getString(json, "sessionId", "");
        UUID peerClientId = parseUuid(JsonUtil.getString(json, "peerClientId", ""));
        // initiator 标记随中转指示下发（撮合的请求方 = 发起方），与直连路径口径一致
        boolean initiator = JsonUtil.getBoolean(json, "initiator", false);
        if (sessionId.isEmpty() || sessionId.length() > MAX_SESSION_ID_CHARS || peerClientId == null) {
            LOGGER.warn("P2P_USE_RELAY 字段非法，忽略");
            return;
        }
        String relayAddress = JsonUtil.getString(json, "relayAddress", "");
        int relayPort = JsonUtil.getInt(json, "relayPort", 0);
        if (!relayAddress.isEmpty() && relayPort > 0 && relayPort <= 0xFFFF) {
            RelayConnector.configure(relayAddress, relayPort); // 服务端指示的端点优先
        }
        ThreadPools.io().execute(() -> {
            try {
                ControlConnection relayConn = RelayConnector.getOrConnect();
                UUID selfId = NodeContext.get().clientId();
                RelayClient transport = new RelayClient(relayConn, selfId, peerClientId, sessionId);
                if (generation != GENERATION.get()) {
                    transport.close();
                    return;
                }
                put(sessionId, transport);
                fireSessionReady(sessionId, peerClientId, transport, initiator);
                LOGGER.info("会话 {} 已降级为中转传输 (对端 {})", sessionId, peerClientId);
            } catch (Exception e) {
                LOGGER.warn("会话 {} 中转降级失败: {}", sessionId, e.toString());
            }
        });
    }

    // ------------------------------------------------------------ 密钥对与指纹上报

    /**
     * 取本次控制会话的 X25519 密钥对（首次调用生成，之后复用同一个实例）。
     * <p>
     * 并发安全：CAS 竞争失败方返回已发布的那一把 —— 保证"上报指纹"与"打洞发包"
     * 观察到的必定是同一份密钥，绝不出现两把密钥各自被当成会话密钥的情况。
     * 包级可见：单测可直接验证"上报指纹与打洞公钥同源"。
     *
     * @throws GeneralSecurityException JDK 不支持 X25519（理论不可达）
     */
    static KeyPair sessionKeyPair() throws GeneralSecurityException {
        KeyPair existing = SESSION_KEY_PAIR.get();
        if (existing != null) {
            return existing;
        }
        KeyPair fresh = SessionCrypto.generateKeyPair();
        if (SESSION_KEY_PAIR.compareAndSet(null, fresh)) {
            return fresh;
        }
        return SESSION_KEY_PAIR.get();
    }

    /**
     * 上报本方公钥指纹（{@link MessageType#P2P_KEY_REPORT}）。幂等：会话建立时主动上报一次，
     * 收到 {@link MessageType#P2P_KEY_REQUEST} 时可重复上报（服务端按 clientId 覆盖登记）。
     * <p>
     * json 里的 clientId 只作服务端日志对照：服务端身份一律以 HELLO 锚定的连接身份为准，
     * 因此即便有人伪造本端的 clientId 上报，也改不了别人名下的指纹。
     */
    private static void reportOwnFingerprint(ControlConnection serverConn) {
        if (serverConn == null) {
            return;
        }
        String fingerprint;
        try {
            fingerprint = KeyFingerprint.of(sessionKeyPair());
        } catch (GeneralSecurityException e) {
            LOGGER.error("生成 P2P 会话密钥对失败，指纹无法上报（本会话打洞将退化为无认证握手）", e);
            return;
        }
        JsonObject json = new JsonObject();
        UUID selfId = NodeContext.get().clientId();
        json.addProperty("clientId", selfId != null ? selfId.toString() : "");
        json.addProperty("fingerprint", fingerprint);
        try {
            serverConn.send(ControlMessage.of(MessageType.P2P_KEY_REPORT, json));
            LOGGER.info("已上报 P2P 公钥指纹: {}", fingerprint);
        } catch (Exception e) {
            LOGGER.warn("P2P 公钥指纹上报失败（服务端将按需索要）", e);
        }
    }

    /** 防御性 UUID 解析：null / 格式非法返回 null 而非抛异常。 */
    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** 回报打洞结果（P2P_RESULT），服务端据此决定是否启用中转降级。 */
    private static void reportResult(ControlConnection serverConn, String sessionId, boolean success) {
        JsonObject json = new JsonObject();
        json.addProperty("sessionId", sessionId);
        json.addProperty("success", success);
        try {
            serverConn.send(ControlMessage.of(MessageType.P2P_RESULT, json));
        } catch (Exception e) {
            LOGGER.warn("会话 {} P2P_RESULT 回报失败", sessionId, e);
        }
    }
}
