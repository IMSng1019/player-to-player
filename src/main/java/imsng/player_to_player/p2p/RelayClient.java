package imsng.player_to_player.p2p;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 中转降级传输（DESIGN.md 第 7 节"失败降级"，规范"中转服务端：若打洞不成功
 * 则充当中转"；{@link P2PTransport} 的中转实现）。
 * <p>
 * 打洞失败且服务端允许中转时使用：数据不走 UDP 直连，而是包成
 * {@link MessageType#RELAY_FORWARD} 帧经与中转端的 TCP 控制连接转发。
 * 构造时先向中转端发 {@link MessageType#RELAY_REGISTER}（clientId）建立
 * 可被寻址的会话，之后：
 * <ul>
 *   <li>{@link #send}：载荷作二进制附件包成 RELAY_FORWARD
 *       （json: targetClientId, fromClientId, sessionId）发给中转端；</li>
 *   <li>接收：中转端转发来的 RELAY_FORWARD 由静态分发器按 sessionId 路由到
 *       对应实例，binary 原文交给 receiver。</li>
 * </ul>
 * <b>注册模型</b>：HandlerRegistry 同一消息类型只允许注册一个处理器，而同一
 * 中转连接上可能并存多条中转会话 —— 故 RELAY_FORWARD 处理器只由
 * {@link #registerDispatcher} 注册<b>一次</b>（客户端引导阶段对中转连接调用），
 * 实例在构造时挂入静态会话表、close 时摘除。
 * <p>
 * <b>加密说明（Phase 1 取舍）</b>：中转路径载荷对中转端明文可见（中转端本就
 * 是受信基础设施，与 MC 原版"服务器可见一切流量"一致）。Phase 2 预同步走
 * 中转时可在上层先经 {@link SessionCrypto} 加密再 send，实现端到端保密。
 * <p>
 * <b>线程模型</b>：send 任意线程可调（Netty 出站自带线程安全）；receiver
 * 回调在 Netty 事件循环线程上执行，禁止阻塞，重活转交 ThreadPools。
 */
public final class RelayClient implements P2PTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/relay-client");

    /**
     * 单帧载荷上限：与 {@link P2PChannel#MAX_PAYLOAD_BYTES} 一致，保证上层
     * 无论拿到直连还是中转传输，行为约束相同（透明降级）。
     */
    public static final int MAX_PAYLOAD_BYTES = P2PChannel.MAX_PAYLOAD_BYTES;

    /** 本连接上的活跃中转会话：sessionId → 实例（静态分发器按此路由）。 */
    private static final Map<String, RelayClient> ACTIVE = new ConcurrentHashMap<>();

    /** 与中转端的控制连接（共享，不归本类关闭）。 */
    private final ControlConnection relayConn;
    /** 本机 clientId（RELAY_FORWARD.fromClientId）。 */
    private final UUID selfClientId;
    /** 对端 clientId（RELAY_FORWARD.targetClientId）。 */
    private final UUID targetClientId;
    /** 会话 ID（与打洞会话同源，服务端分配）。 */
    private final String sessionId;

    private final AtomicReference<Consumer<byte[]>> receiver = new AtomicReference<>();
    private final AtomicBoolean open = new AtomicBoolean(true);

    /**
     * 建立中转会话：登记到静态会话表并向中转端发 RELAY_REGISTER。
     * <p>
     * 注意：使用前须确保 {@link #registerDispatcher} 已对该中转连接的
     * 注册表调用过（客户端引导负责），否则收不到入站数据。
     *
     * @param relayConn      已连接中转端的控制连接
     * @param selfClientId   本机 clientId
     * @param targetClientId 对端 clientId
     * @param sessionId      服务端分配的会话 ID
     */
    public RelayClient(ControlConnection relayConn, UUID selfClientId,
                       UUID targetClientId, String sessionId) {
        this.relayConn = relayConn;
        this.selfClientId = selfClientId;
        this.targetClientId = targetClientId;
        this.sessionId = sessionId;

        RelayClient previous = ACTIVE.put(sessionId, this);
        if (previous != null && previous != this) {
            previous.close(); // 同会话重建：旧实例关闭防悬挂
        }
        // 在中转端登记自己，使 targetClientId 指向本机的 RELAY_FORWARD 可被寻址
        JsonObject reg = new JsonObject();
        reg.addProperty("clientId", selfClientId.toString());
        relayConn.send(ControlMessage.of(MessageType.RELAY_REGISTER, reg));
        LOGGER.info("中转会话建立: session={}, self={}, target={}", sessionId, selfClientId, targetClientId);
    }

    /**
     * 在中转连接的处理器注册表上挂 RELAY_FORWARD 分发器（每条连接<b>只调一次</b>，
     * 由客户端引导在建立中转连接后调用；重复注册会被 HandlerRegistry 拒绝）。
     */
    public static void registerDispatcher(HandlerRegistry reg) {
        reg.on(MessageType.RELAY_FORWARD, (conn, msg) -> dispatch(msg));
    }

    // ------------------------------------------------------------ P2PTransport

    @Override
    public void send(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data 不得为 null");
        }
        if (data.length > MAX_PAYLOAD_BYTES) {
            // 与 P2PChannel 相同的上限（透明降级）；分片留 Phase 2
            throw new IllegalArgumentException(
                    "中转单帧载荷上限 " + MAX_PAYLOAD_BYTES + " 字节，实际 " + data.length);
        }
        if (!isOpen()) {
            LOGGER.debug("中转会话 {} 已关闭，丢弃出站帧", sessionId);
            return;
        }
        JsonObject json = new JsonObject();
        json.addProperty("targetClientId", targetClientId.toString());
        json.addProperty("fromClientId", selfClientId.toString());
        json.addProperty("sessionId", sessionId);
        relayConn.send(ControlMessage.of(MessageType.RELAY_FORWARD, json, data));
    }

    @Override
    public void setReceiver(Consumer<byte[]> receiver) {
        this.receiver.set(receiver);
    }

    @Override
    public boolean isOpen() {
        return open.get() && relayConn.isOpen();
    }

    /** 关闭中转会话：摘出会话表（不关闭共享的中转控制连接）。幂等。 */
    @Override
    public void close() {
        if (!open.compareAndSet(true, false)) {
            return;
        }
        ACTIVE.remove(sessionId, this);
        LOGGER.info("中转会话关闭: session={}", sessionId);
    }

    // ------------------------------------------------------------ 入站分发

    /** RELAY_FORWARD 入站分发（Netty 事件循环线程）：按 sessionId 路由到实例。 */
    private static void dispatch(ControlMessage msg) {
        JsonObject json = msg.json();
        String sessionId = JsonUtil.getString(json, "sessionId", "");
        if (sessionId.isEmpty()) {
            return; // 畸形帧：静默丢弃（入站按不可信处理）
        }
        RelayClient client = ACTIVE.get(sessionId);
        if (client == null || !client.open.get()) {
            LOGGER.debug("收到无归属中转帧: session={}（会话不存在或已关闭）", sessionId);
            return;
        }
        // 来源校验：fromClientId 必须是本会话的对端（防同中转上其他客户端串数据；
        // 伪造 clientId 的防护依赖中转端 Phase 2 校验注册身份）
        String from = JsonUtil.getString(json, "fromClientId", "");
        if (!client.targetClientId.toString().equals(from)) {
            LOGGER.debug("中转帧来源不符: session={}, from={}", sessionId, from);
            return;
        }
        Consumer<byte[]> cb = client.receiver.get();
        if (cb != null) {
            try {
                // 回调在 Netty 事件循环上执行；契约要求回调不阻塞
                cb.accept(msg.binary());
            } catch (Exception e) {
                LOGGER.error("中转会话 {} receiver 回调异常", sessionId, e);
            }
        }
    }
}
