package imsng.player_to_player.p2p;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Optional;

/**
 * 极简 STUN 客户端（RFC 5389 Binding Request，DESIGN.md 第 7 节"NAT 检测"）。
 * <p>
 * 只实现 NAT 探测所需的最小子集：
 * <ul>
 *   <li>发送 Binding Request（消息类型 0x0001，magic cookie 0x2112A442，
 *       SecureRandom 生成的 12 字节事务 ID）；</li>
 *   <li>解析响应中的 XOR-MAPPED-ADDRESS(0x0020) 属性，缺失时兜底解析
 *       经典 MAPPED-ADDRESS(0x0001)（兼容极旧的 RFC 3489 服务器）；</li>
 *   <li>不做 MESSAGE-INTEGRITY / FINGERPRINT 校验（Binding 探测无凭据，
 *       公共服务器也不下发），事务 ID 匹配即认为响应有效。</li>
 * </ul>
 * <b>仅支持 IPv4</b>：本模组打洞面向家庭宽带场景，规范亦只要求 ipv4 中转地址；
 * 响应中地址族为 IPv6(0x02) 的属性直接跳过。
 * <p>
 * <b>线程模型</b>：{@link #query} 内部阻塞收发（超时 3 秒、重发 2 次），
 * 必须在 {@code ThreadPools.io()} 上调用，严禁 MC 主线程/Netty 事件循环调用。
 */
public final class StunClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/stun");

    /** STUN Binding Request 消息类型。 */
    private static final int TYPE_BINDING_REQUEST = 0x0001;
    /** STUN Binding Success Response 消息类型。 */
    private static final int TYPE_BINDING_SUCCESS = 0x0101;
    /** RFC 5389 固定 magic cookie。 */
    private static final int MAGIC_COOKIE = 0x2112A442;
    /** 属性：MAPPED-ADDRESS（RFC 3489 经典格式，兜底用）。 */
    private static final int ATTR_MAPPED_ADDRESS = 0x0001;
    /** 属性：XOR-MAPPED-ADDRESS（RFC 5389 推荐，优先用）。 */
    private static final int ATTR_XOR_MAPPED_ADDRESS = 0x0020;
    /** 地址族：IPv4。 */
    private static final int FAMILY_IPV4 = 0x01;

    /** 单次等待响应超时（毫秒）。 */
    private static final int RECEIVE_TIMEOUT_MILLIS = 3_000;
    /** 首发之外的重发次数（UDP 可能丢包，共尝试 1 + RETRIES 次）。 */
    private static final int RETRIES = 2;
    /** 接收缓冲：STUN 探测响应很小，512 字节绰绰有余。 */
    private static final int RECV_BUF_BYTES = 512;

    private static final SecureRandom RANDOM = new SecureRandom();

    private StunClient() {
    }

    /**
     * 用给定 socket 向指定 STUN 服务器做一次 Binding 查询。
     * <p>
     * 会临时修改 socket 的 soTimeout，返回前恢复原值 —— 调用方（NatDetector）
     * 在同一 socket 上串行做多次查询，且随后打洞时还要复用该 socket。
     *
     * @param socket 已绑定的 UDP socket（不关闭，由调用方管理生命周期）
     * @param server STUN 服务器地址（须已解析；未解析地址由调用方过滤）
     * @return 服务器观测到的本机公网映射地址；超时/解析失败返回 empty
     */
    public static Optional<InetSocketAddress> query(DatagramSocket socket, InetSocketAddress server) {
        // 事务 ID：SecureRandom 12 字节（RFC 5389 要求密码学随机，防响应欺骗）
        byte[] transactionId = new byte[12];
        RANDOM.nextBytes(transactionId);
        byte[] request = buildBindingRequest(transactionId);

        int originalTimeout;
        try {
            originalTimeout = socket.getSoTimeout();
        } catch (IOException e) {
            return Optional.empty(); // socket 已关闭
        }

        try {
            socket.setSoTimeout(RECEIVE_TIMEOUT_MILLIS);
            for (int attempt = 0; attempt <= RETRIES; attempt++) {
                try {
                    socket.send(new DatagramPacket(request, request.length, server));
                    // 一个超时窗口内可能收到无关包（比如迟到的上一台服务器的响应），
                    // 循环收包直到匹配事务 ID 或窗口耗尽
                    long deadline = System.currentTimeMillis() + RECEIVE_TIMEOUT_MILLIS;
                    while (true) {
                        long remain = deadline - System.currentTimeMillis();
                        if (remain <= 0) {
                            break; // 本次尝试超时，进入重发
                        }
                        socket.setSoTimeout((int) remain);
                        DatagramPacket recv = new DatagramPacket(new byte[RECV_BUF_BYTES], RECV_BUF_BYTES);
                        socket.receive(recv);
                        Optional<InetSocketAddress> mapped = parseResponse(
                                Arrays.copyOf(recv.getData(), recv.getLength()), transactionId);
                        if (mapped.isPresent()) {
                            return mapped;
                        }
                        // 不匹配：继续在剩余窗口内等
                    }
                } catch (SocketTimeoutException e) {
                    // 超时进入下一次重发
                } catch (IOException e) {
                    // 网络错误（ICMP 端口不可达等）：该服务器视为不可用
                    LOGGER.debug("STUN 查询 {} 失败: {}", server, e.toString());
                    return Optional.empty();
                }
            }
            return Optional.empty();
        } catch (IOException e) {
            // setSoTimeout 失败（socket 被并发关闭）
            return Optional.empty();
        } finally {
            try {
                socket.setSoTimeout(originalTimeout);
            } catch (IOException ignored) {
                // socket 已被并发关闭，无需恢复
            }
        }
    }

    /** 构造 Binding Request 帧：20 字节头，无属性。 */
    private static byte[] buildBindingRequest(byte[] transactionId) {
        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.putShort((short) TYPE_BINDING_REQUEST);
        buf.putShort((short) 0); // 消息长度：无属性为 0
        buf.putInt(MAGIC_COOKIE);
        buf.put(transactionId);
        return buf.array();
    }

    /**
     * 解析 Binding 响应：校验类型/长度/cookie/事务 ID，遍历属性提取映射地址。
     * XOR-MAPPED-ADDRESS 优先；只在其缺失时接受 MAPPED-ADDRESS。
     * 入站数据按不可信处理：任何越界/畸形都返回 empty 而不抛异常。
     */
    private static Optional<InetSocketAddress> parseResponse(byte[] data, byte[] expectTransactionId) {
        if (data.length < 20) {
            return Optional.empty();
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        int type = buf.getShort() & 0xFFFF;
        int msgLen = buf.getShort() & 0xFFFF;
        int cookie = buf.getInt();
        byte[] txId = new byte[12];
        buf.get(txId);
        if (type != TYPE_BINDING_SUCCESS || cookie != MAGIC_COOKIE
                || !Arrays.equals(txId, expectTransactionId)) {
            return Optional.empty();
        }
        // 属性区实际可用长度取声明值与真实剩余的较小者（防畸形长度越界）
        int attrEnd = Math.min(20 + msgLen, data.length);

        InetSocketAddress xorMapped = null;
        InetSocketAddress plainMapped = null;
        int pos = 20;
        // 逐属性扫描；属性按 4 字节对齐（RFC 5389 15 节）
        while (pos + 4 <= attrEnd) {
            int attrType = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
            int attrLen = ((data[pos + 2] & 0xFF) << 8) | (data[pos + 3] & 0xFF);
            int valueStart = pos + 4;
            if (valueStart + attrLen > attrEnd) {
                break; // 畸形属性长度，终止解析
            }
            if (attrType == ATTR_XOR_MAPPED_ADDRESS) {
                xorMapped = parseAddressAttr(data, valueStart, attrLen, true);
            } else if (attrType == ATTR_MAPPED_ADDRESS && plainMapped == null) {
                plainMapped = parseAddressAttr(data, valueStart, attrLen, false);
            }
            pos = valueStart + attrLen;
            if ((attrLen & 3) != 0) {
                pos += 4 - (attrLen & 3); // 补齐到 4 字节边界
            }
        }
        return Optional.ofNullable(xorMapped != null ? xorMapped : plainMapped);
    }

    /**
     * 解析 (XOR-)MAPPED-ADDRESS 属性值：
     * <pre>
     * byte  0      保留
     * byte  1      地址族（0x01=IPv4，0x02=IPv6）
     * uint16 2-3   端口（XOR 版与 cookie 高 16 位异或）
     * byte[] 4-    地址（XOR 版与 cookie 异或）
     * </pre>
     * 仅接受 IPv4；IPv6 返回 null（本模组打洞只做 IPv4，见类 Javadoc）。
     */
    private static InetSocketAddress parseAddressAttr(byte[] data, int start, int len, boolean xor) {
        if (len < 8) {
            return null; // IPv4 属性值固定 8 字节
        }
        int family = data[start + 1] & 0xFF;
        if (family != FAMILY_IPV4) {
            return null;
        }
        int port = ((data[start + 2] & 0xFF) << 8) | (data[start + 3] & 0xFF);
        byte[] addr = Arrays.copyOfRange(data, start + 4, start + 8);
        if (xor) {
            port ^= MAGIC_COOKIE >>> 16;
            addr[0] ^= (byte) (MAGIC_COOKIE >>> 24);
            addr[1] ^= (byte) (MAGIC_COOKIE >>> 16);
            addr[2] ^= (byte) (MAGIC_COOKIE >>> 8);
            addr[3] ^= (byte) MAGIC_COOKIE;
        }
        try {
            InetAddress ip = InetAddress.getByAddress(addr);
            if (!(ip instanceof Inet4Address)) {
                return null;
            }
            return new InetSocketAddress(ip, port);
        } catch (IOException e) {
            return null; // getByAddress 对 4 字节输入不会失败，防御性兜底
        }
    }
}
