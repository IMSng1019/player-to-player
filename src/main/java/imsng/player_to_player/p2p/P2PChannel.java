package imsng.player_to_player.p2p;

import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 加密 P2P UDP 通道（DESIGN.md 第 7 节"加密通道"；{@link P2PTransport} 直连实现）。
 * <p>
 * 由 {@link HolePuncher} 打洞成功后创建：持有打洞用的 UDP socket、锁定的对端
 * 地址与协商好的 {@link SessionCrypto} 会话。职责：
 * <ul>
 *   <li><b>发送</b>：明文加 1 字节帧类型前缀（DATA/KEEPALIVE）后经
 *       SessionCrypto 加密发出；载荷上限 {@value #MAX_PAYLOAD_BYTES} 字节，
 *       超限抛 IllegalArgumentException —— UDP 单报文受 IP 分片与 MTU 制约，
 *       更大数据的<b>应用层分片重组留 Phase 2</b>（预同步大块数据需要）；</li>
 *   <li><b>接收</b>：专属循环跑在 {@code ThreadPools.io()} 上阻塞 receive，
 *       {@link #close()} 关 socket 使其以异常退出；解密失败/重放/畸形包
 *       静默丢弃（入站按不可信处理）；</li>
 *   <li><b>keepalive</b>：{@value #KEEPALIVE_INTERVAL_SECONDS} 秒无业务发送则
 *       发一帧加密 keepalive，维持双方 NAT 映射不老化（家用 NAT 的 UDP
 *       映射常见 30~60 秒超时）；</li>
 *   <li><b>握手收尾</b>：responder 侧对迟到的 KEY 重发包回应自己的 KEY
 *       （见 {@link HolePuncher} 丢包补偿说明）。</li>
 * </ul>
 * 仅过滤"来源地址 == 锁定对端"的报文；伪造源地址的报文过不了 GCM 认证，
 * 该过滤只是省掉无谓解密。
 */
public final class P2PChannel implements P2PTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/p2p-channel");

    /** 单帧明文载荷上限（字节）：留出 GCM tag/seq/IP+UDP 头后仍在 64KB 数据报限内。 */
    public static final int MAX_PAYLOAD_BYTES = 60_000;
    /** keepalive 触发阈值：距上次发送超过该秒数则发 keepalive。 */
    private static final int KEEPALIVE_INTERVAL_SECONDS = 15;
    /** 接收缓冲（最大 UDP 载荷）。 */
    private static final int RECV_BUF_BYTES = 65_535;

    /** 明文帧类型：业务数据。 */
    private static final byte FRAME_DATA = 0;
    /** 明文帧类型：keepalive（解密后直接丢弃，不上抛 receiver）。 */
    private static final byte FRAME_KEEPALIVE = 1;

    private final DatagramSocket socket;
    private final InetSocketAddress peer;
    private final SessionCrypto crypto;
    private final String sessionId;
    /** responder 的 KEY 握手包（对迟到的 KEY 重发再次应答）；initiator 侧为 null。 */
    private final byte[] handshakeReply;

    private final AtomicReference<Consumer<byte[]>> receiver = new AtomicReference<>();
    private final AtomicBoolean open = new AtomicBoolean(true);
    /** 最近一次成功发送的时间戳（keepalive 判据）。 */
    private final AtomicLong lastSendAt = new AtomicLong(System.currentTimeMillis());

    private volatile Future<?> receiveLoop;
    private volatile ScheduledFuture<?> keepaliveTask;

    /**
     * @param socket         打洞用的 UDP socket（所有权转移给通道，close 时关闭）
     * @param peer           打洞锁定的对端地址
     * @param crypto         已协商的会话加密
     * @param sessionId      会话 ID（日志用）
     * @param handshakeReply responder 传入自己的 KEY 包用于握手丢包补偿；initiator 传 null
     */
    P2PChannel(DatagramSocket socket, InetSocketAddress peer, SessionCrypto crypto,
               String sessionId, byte[] handshakeReply) {
        this.socket = socket;
        this.peer = peer;
        this.crypto = crypto;
        this.sessionId = sessionId;
        this.handshakeReply = handshakeReply;
    }

    /** 启动接收循环与 keepalive 定时器（HolePuncher 创建后立即调用，只调一次）。 */
    void start() {
        try {
            socket.setSoTimeout(0); // 打洞阶段设过短超时，接收循环恢复为无限阻塞
        } catch (IOException e) {
            LOGGER.warn("会话 {} 设置 socket 超时失败", sessionId, e);
        }
        receiveLoop = ThreadPools.io().submit(this::receiveLoop);
        // 定时器只做轻量判断与一次 UDP 发送，符合 scheduler() 短任务约束
        keepaliveTask = ThreadPools.scheduler().scheduleWithFixedDelay(
                this::maybeKeepalive, KEEPALIVE_INTERVAL_SECONDS, 1, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------ P2PTransport

    @Override
    public void send(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data 不得为 null");
        }
        if (data.length > MAX_PAYLOAD_BYTES) {
            // 分片重组留 Phase 2：上层暂时自行控制块大小
            throw new IllegalArgumentException(
                    "P2P 单帧载荷上限 " + MAX_PAYLOAD_BYTES + " 字节，实际 " + data.length);
        }
        byte[] framed = new byte[data.length + 1];
        framed[0] = FRAME_DATA;
        System.arraycopy(data, 0, framed, 1, data.length);
        sendFrame(framed);
    }

    @Override
    public void setReceiver(Consumer<byte[]> receiver) {
        this.receiver.set(receiver);
    }

    @Override
    public boolean isOpen() {
        return open.get() && !socket.isClosed();
    }

    @Override
    public void close() {
        if (!open.compareAndSet(true, false)) {
            return; // 幂等
        }
        if (keepaliveTask != null) {
            keepaliveTask.cancel(false);
        }
        socket.close(); // 令阻塞中的 receive 以 SocketException 退出
        if (receiveLoop != null) {
            receiveLoop.cancel(true);
        }
        LOGGER.info("P2P 通道关闭: 会话 {}, 对端 {}", sessionId, peer);
    }

    /** 对端地址（调试/日志用）。 */
    public InetSocketAddress peer() {
        return peer;
    }

    // ------------------------------------------------------------ 内部

    /** 加密并发送一帧（含帧类型前缀的明文）。 */
    private void sendFrame(byte[] framedPlaintext) {
        if (!isOpen()) {
            LOGGER.debug("会话 {} 已关闭，丢弃出站帧", sessionId);
            return;
        }
        try {
            byte[] cipher = crypto.encrypt(framedPlaintext);
            socket.send(new DatagramPacket(cipher, cipher.length, peer));
            lastSendAt.set(System.currentTimeMillis());
        } catch (GeneralSecurityException e) {
            // 加密失败属编程/JVM 环境错误，不该发生
            LOGGER.error("会话 {} 加密失败", sessionId, e);
        } catch (IOException e) {
            LOGGER.debug("会话 {} UDP 发送失败: {}", sessionId, e.toString());
        }
    }

    /** keepalive 定时判断：距上次发送超阈值则发一帧空 keepalive。 */
    private void maybeKeepalive() {
        if (!isOpen()) {
            return;
        }
        if (System.currentTimeMillis() - lastSendAt.get() >= KEEPALIVE_INTERVAL_SECONDS * 1000L) {
            sendFrame(new byte[]{FRAME_KEEPALIVE});
        }
    }

    /** 接收循环（IO 线程）：阻塞收包 → 过滤来源 → 解密 → 分发。 */
    private void receiveLoop() {
        byte[] buf = new byte[RECV_BUF_BYTES];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        while (open.get()) {
            try {
                // 复用 DatagramPacket 前必须重置 length（receive 会把它缩为上次收包长度）
                packet.setLength(buf.length);
                socket.receive(packet);
            } catch (IOException e) {
                if (open.get()) {
                    // 非主动关闭导致的 socket 错误：通道判死
                    LOGGER.warn("会话 {} 接收循环异常退出: {}", sessionId, e.toString());
                    close();
                }
                return;
            }
            // 只处理锁定对端的报文（其他来源的报文反正过不了 GCM，这里省掉解密开销）
            if (!peer.equals(packet.getSocketAddress())) {
                continue;
            }
            byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
            // 握手丢包补偿：对端（initiator）没收到我们的 KEY 会继续重发 KEY，
            // 这里识别明文握手包并重新应答，而不是当密文解密失败丢弃
            if (isHandshakePacket(data)) {
                if (handshakeReply != null) {
                    try {
                        socket.send(new DatagramPacket(handshakeReply, handshakeReply.length, peer));
                    } catch (IOException e) {
                        LOGGER.debug("会话 {} 握手补偿应答失败: {}", sessionId, e.toString());
                    }
                }
                continue;
            }
            byte[] plaintext;
            try {
                plaintext = crypto.decrypt(data);
            } catch (GeneralSecurityException e) {
                // 篡改/重放/畸形帧：静默丢弃（记 debug 便于排查，不给攻击者放大面）
                LOGGER.debug("会话 {} 丢弃无效帧({}B): {}", sessionId, data.length, e.getMessage());
                continue;
            }
            if (plaintext.length == 0) {
                continue;
            }
            byte frameType = plaintext[0];
            if (frameType == FRAME_KEEPALIVE) {
                continue; // keepalive 只为保活 NAT 映射，不上抛
            }
            if (frameType != FRAME_DATA) {
                continue; // 未知帧类型：容忍（未来扩展）
            }
            Consumer<byte[]> cb = receiver.get();
            if (cb != null) {
                byte[] payload = Arrays.copyOfRange(plaintext, 1, plaintext.length);
                try {
                    // 回调在本 IO 线程执行；契约要求回调不阻塞（见 P2PTransport Javadoc）
                    cb.accept(payload);
                } catch (Exception e) {
                    LOGGER.error("会话 {} receiver 回调异常", sessionId, e);
                }
            }
        }
    }

    /** 判断是否为明文握手包（"P2P1|" 前缀；加密帧首 8 字节是 seq，实际不可能撞上）。 */
    private static boolean isHandshakePacket(byte[] data) {
        if (data.length < HolePuncher.MAGIC.length) {
            return false;
        }
        for (int i = 0; i < HolePuncher.MAGIC.length; i++) {
            if (data[i] != HolePuncher.MAGIC[i]) {
                return false;
            }
        }
        return true;
    }
}
