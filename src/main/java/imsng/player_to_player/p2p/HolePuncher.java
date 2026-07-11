package imsng.player_to_player.p2p;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Arrays;

/**
 * UDP 打洞执行器（DESIGN.md 第 7 节"打洞流程"）。
 * <p>
 * 双方从服务端/中转端收到 {@code P2P_ENDPOINT_EXCHANGE}（内含对方公网
 * endpoint 与 sessionId）后各自调用 {@link #punch}，同时向对方 endpoint
 * 发送明文探测包，在双方 NAT 上打出映射：
 * <pre>
 * 探测阶段（指数退避 200ms→400→800→1600ms 封顶，总超时 5 秒）：
 *   A ──"P2P1|PUNCH|sid"──→ B      收到 PUNCH 回 "P2P1|ACK|sid"
 *   任一方收到对方任意有效包（PUNCH/ACK/KEY）即确认打洞已通
 * 密钥交换阶段（由 initiator=true 的一方发起）：
 *   I ──"P2P1|KEY|sid|"+X.509公钥──→ R    R 收到后立即回自己的 KEY 包
 *   双方拿到对方公钥后各自 SessionCrypto.establish，进入加密通道
 * 丢包补偿：I 的 KEY 按退避重发直到收到 R 的 KEY；R 进入通道后由
 *   {@link P2PChannel} 对迟到的 KEY 重发包再次回应自己的 KEY（见 handshakeReply）
 * </pre>
 * <b>对端地址锁定</b>：以服务端下发的 endpoint 为初始目标，但以实际收到
 * 有效包的来源地址为准更新（NAT 可能改写源端口，对称 NAT 下尤甚），
 * 通道最终锁定该观测地址。
 * <p>
 * <b>中间人风险（Phase 1 已知取舍）</b>：公钥经 UDP 明文交换、未做身份认证，
 * 路径攻击者理论上可 MITM。Phase 2 计划经服务端 TCP 控制信道（身份由 HELLO
 * 锚定）交换公钥指纹比对后再启用通道。
 * <p>
 * <b>线程模型</b>：{@link #punch} 全程阻塞收发（最长 5 秒），必须在
 * {@code ThreadPools.io()} 上调用（P2PSessions 负责调度）。
 */
public final class HolePuncher {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/punch");

    /** 打洞 + 密钥交换总超时（毫秒）。 */
    private static final long TOTAL_TIMEOUT_MILLIS = 5_000;
    /** 探测重发初始间隔（毫秒），指数退避。 */
    private static final long INITIAL_BACKOFF_MILLIS = 200;
    /** 探测重发间隔上限（毫秒）。 */
    private static final long MAX_BACKOFF_MILLIS = 1_600;
    /** 握手包长度上限（X25519 公钥 X.509 编码仅 44 字节，1KB 足够且防滥用）。 */
    private static final int MAX_HANDSHAKE_PACKET = 1_024;
    /** 公钥字段长度上限（防畸形包让 KeyFactory 解析大块垃圾）。 */
    private static final int MAX_KEY_BYTES = 256;

    /** 握手协议魔数前缀（明文 ASCII，版本 1）。 */
    static final byte[] MAGIC = "P2P1|".getBytes(StandardCharsets.US_ASCII);

    private HolePuncher() {
    }

    /**
     * 执行打洞 + 密钥交换，成功返回已启动的加密通道（通道接管 socket 所有权）。
     *
     * @param socket       本地 UDP socket（优先复用 NAT 探测时的 socket 以命中
     *                     已有映射）；成功后归通道管理，失败由调用方关闭
     * @param peerEndpoint 服务端下发的对方公网 endpoint
     * @param sessionId    服务端分配的会话 ID（写入所有握手包，防串session）
     * @param initiator    是否由本方发起密钥交换（服务端指定，双方必须相反）
     * @return 加密 P2P 通道
     * @throws IOException 超时 / socket 错误 / 密钥协商失败
     */
    public static P2PChannel punch(DatagramSocket socket, InetSocketAddress peerEndpoint,
                                   String sessionId, boolean initiator) throws IOException {
        // 每会话新生成 X25519 临时密钥对（前向保密）
        KeyPair keyPair;
        byte[] ownKeyPacket;
        try {
            keyPair = SessionCrypto.generateKeyPair();
            ownKeyPacket = keyPacket(sessionId, SessionCrypto.encodePublicKey(keyPair));
        } catch (GeneralSecurityException e) {
            throw new IOException("生成 X25519 密钥对失败", e);
        }
        byte[] punchPacket = textPacket("PUNCH", sessionId);
        byte[] ackPacket = textPacket("ACK", sessionId);

        long deadline = System.currentTimeMillis() + TOTAL_TIMEOUT_MILLIS;
        long nextSendAt = 0;
        long interval = INITIAL_BACKOFF_MILLIS;
        boolean peerSeen = false;     // 是否已收到对方任意有效包（打洞已通）
        byte[] peerKeyDer = null;     // 对方公钥（拿到即握手完成）
        InetSocketAddress actualPeer = peerEndpoint; // 实际观测的对端地址（可能与下发值不同）

        int originalTimeout = socket.getSoTimeout();
        try {
            DatagramPacket recv = new DatagramPacket(new byte[MAX_HANDSHAKE_PACKET], MAX_HANDSHAKE_PACKET);
            while (peerKeyDer == null) {
                long now = System.currentTimeMillis();
                if (now >= deadline) {
                    break; // 总超时
                }
                // 按退避周期发送：未通前发 PUNCH；打通后 initiator 发 KEY，responder 静默等 KEY
                if (now >= nextSendAt) {
                    if (!peerSeen) {
                        sendTo(socket, punchPacket, actualPeer);
                    } else if (initiator) {
                        sendTo(socket, ownKeyPacket, actualPeer);
                    }
                    nextSendAt = now + interval;
                    interval = Math.min(interval * 2, MAX_BACKOFF_MILLIS);
                }
                // 在"下次发送"与"总期限"之间的窗口内等待入站包
                int wait = (int) Math.max(1, Math.min(nextSendAt, deadline) - now);
                socket.setSoTimeout(wait);
                try {
                    // 复用 DatagramPacket 前必须重置 length，否则 receive 只填充
                    // 上一个包的长度，后续更长的包会被截断
                    recv.setLength(MAX_HANDSHAKE_PACKET);
                    socket.receive(recv);
                } catch (SocketTimeoutException e) {
                    continue; // 窗口耗尽，回到发送判断
                }
                Packet packet = parse(recv.getData(), recv.getLength());
                if (packet == null || !sessionId.equals(packet.sessionId())) {
                    continue; // 畸形包/其他会话的包：按不可信数据静默丢弃
                }
                // 对端地址修正：以有效包实际来源为准（NAT 改写源端口的情况）
                SocketAddress source = recv.getSocketAddress();
                if (source instanceof InetSocketAddress inet && !inet.equals(actualPeer)) {
                    LOGGER.debug("会话 {} 对端地址修正: {} -> {}", sessionId, actualPeer, inet);
                    actualPeer = inet;
                }
                if (!peerSeen) {
                    // 首次收到对方包：打洞已通；重置退避让 initiator 立即发 KEY
                    peerSeen = true;
                    nextSendAt = 0;
                    interval = INITIAL_BACKOFF_MILLIS;
                    LOGGER.debug("会话 {} 打洞探测已通（对端 {}）", sessionId, actualPeer);
                }
                switch (packet.cmd()) {
                    case "PUNCH" ->
                        // 回 ACK 让对方也确认打通
                        sendTo(socket, ackPacket, actualPeer);
                    case "ACK" -> {
                        // 仅用于确认打通，peerSeen 已置位，无需其他动作
                    }
                    case "KEY" -> {
                        byte[] der = packet.payload();
                        if (der == null || der.length == 0 || der.length > MAX_KEY_BYTES) {
                            continue; // 畸形公钥字段
                        }
                        peerKeyDer = der;
                        if (!initiator) {
                            // responder 收到 initiator 公钥后立即回自己的公钥
                            sendTo(socket, ownKeyPacket, actualPeer);
                        }
                    }
                    default -> {
                        // 未知命令：容忍（未来版本扩展），丢弃
                    }
                }
            }
        } finally {
            // 恢复超时设置（成功路径上 P2PChannel.start 会再设为阻塞）
            try {
                socket.setSoTimeout(originalTimeout);
            } catch (IOException ignored) {
            }
        }

        if (peerKeyDer == null) {
            throw new IOException("打洞超时（" + TOTAL_TIMEOUT_MILLIS + "ms）: 会话 " + sessionId
                    + ", 对端 " + peerEndpoint);
        }

        // 双方公钥齐备：协商会话密钥并启动加密通道
        SessionCrypto crypto;
        try {
            crypto = SessionCrypto.establish(keyPair, peerKeyDer, sessionId, initiator);
        } catch (GeneralSecurityException e) {
            throw new IOException("会话密钥协商失败: " + sessionId, e);
        }
        // responder 把自己的 KEY 包交给通道作 handshakeReply：对 initiator 迟到的
        // KEY 重发再次应答，补偿"responder 的 KEY 回包丢失"的情况；
        // initiator 侧无需应答（responder 不会主动重发 KEY）。
        P2PChannel channel = new P2PChannel(socket, actualPeer, crypto, sessionId,
                initiator ? null : ownKeyPacket);
        channel.start();
        LOGGER.info("P2P 打洞成功: 会话 {}, 对端 {}, initiator={}", sessionId, actualPeer, initiator);
        return channel;
    }

    // ------------------------------------------------------------ 包构造与解析

    /** 构造纯文本握手包 "P2P1|<cmd>|<sessionId>"。 */
    static byte[] textPacket(String cmd, String sessionId) {
        return ("P2P1|" + cmd + "|" + sessionId).getBytes(StandardCharsets.UTF_8);
    }

    /** 构造公钥交换包 "P2P1|KEY|<sessionId>|" + X.509 公钥字节。 */
    static byte[] keyPacket(String sessionId, byte[] publicKeyDer) {
        byte[] head = ("P2P1|KEY|" + sessionId + "|").getBytes(StandardCharsets.UTF_8);
        byte[] out = Arrays.copyOf(head, head.length + publicKeyDer.length);
        System.arraycopy(publicKeyDer, 0, out, head.length, publicKeyDer.length);
        return out;
    }

    /** 已解析的握手包（payload 仅 KEY 命令携带）。 */
    record Packet(String cmd, String sessionId, byte[] payload) {
    }

    /**
     * 解析握手包；任何畸形（缺魔数/缺分隔符/超长）返回 null 而不抛异常
     * （入站 UDP 一律按不可信处理）。
     */
    static Packet parse(byte[] data, int length) {
        if (length < MAGIC.length + 2 || length > MAX_HANDSHAKE_PACKET) {
            return null;
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (data[i] != MAGIC[i]) {
                return null;
            }
        }
        // cmd 段：魔数后到下一个 '|'
        int cmdEnd = indexOf(data, MAGIC.length, length, (byte) '|');
        if (cmdEnd < 0) {
            return null;
        }
        String cmd = new String(data, MAGIC.length, cmdEnd - MAGIC.length, StandardCharsets.UTF_8);
        int sidStart = cmdEnd + 1;
        if ("KEY".equals(cmd)) {
            // KEY 包：sessionId 后还有一个 '|' 与二进制公钥
            int sidEnd = indexOf(data, sidStart, length, (byte) '|');
            if (sidEnd < 0) {
                return null;
            }
            String sid = new String(data, sidStart, sidEnd - sidStart, StandardCharsets.UTF_8);
            byte[] payload = Arrays.copyOfRange(data, sidEnd + 1, length);
            return new Packet(cmd, sid, payload);
        }
        String sid = new String(data, sidStart, length - sidStart, StandardCharsets.UTF_8);
        return new Packet(cmd, sid, null);
    }

    /** 在 data[from, to) 中查找字节 target，返回下标或 -1。 */
    private static int indexOf(byte[] data, int from, int to, byte target) {
        for (int i = from; i < to; i++) {
            if (data[i] == target) {
                return i;
            }
        }
        return -1;
    }

    /** 发送一个数据报（IOException 降为日志：打洞期间对端不可达属常态）。 */
    private static void sendTo(DatagramSocket socket, byte[] data, InetSocketAddress target) {
        try {
            socket.send(new DatagramPacket(data, data.length, target));
        } catch (IOException e) {
            // Windows 上对不可达地址发包可能立刻抛 ICMP 相关异常，打洞中属正常现象
            LOGGER.debug("打洞探测包发送失败 -> {}: {}", target, e.toString());
        }
    }
}
