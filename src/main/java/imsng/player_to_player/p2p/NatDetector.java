package imsng.player_to_player.p2p;

import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * NAT 类型探测（规范"玩家加载模组：检查玩家的路由器的nat类型以及是否可以p2p链接"，
 * DESIGN.md 第 7 节）。
 * <p>
 * 依次向多个公共 STUN 服务器发 Binding Request，按观测结果分类：
 * <ul>
 *   <li>全部无响应 → {@link NatType#UDP_BLOCKED}（UDP 出站被防火墙阻断）；</li>
 *   <li>映射地址 == 本机绑定地址（公网 IP 是本机网卡地址且端口一致）
 *       → {@link NatType#OPEN_INTERNET}；</li>
 *   <li>不同服务器观测到不同映射端口 → {@link NatType#SYMMETRIC}
 *       （对不同目的地分配不同映射，打洞需端口预测）；</li>
 *   <li>其余 → 保守判为 {@link NatType#PORT_RESTRICTED_CONE}。</li>
 * </ul>
 * <b>分类为近似值</b>：完整区分 FULL_CONE / RESTRICTED_CONE / PORT_RESTRICTED_CONE
 * 需要 RFC 3489 的 CHANGE-REQUEST（换 IP/端口回包）支持，公共 STUN 服务器普遍
 * 已不提供该能力，故锥形一律按最严格的 PORT_RESTRICTED_CONE 处理 ——
 * 打洞策略上这是安全方向的保守假设（双方都按需要先向对方发包来打开映射来执行）。
 * <p>
 * 探测完成后 socket <b>保持打开</b>并寄存于本类静态槽位，供 {@link P2PSessions}
 * 打洞时复用（复用同一 socket 才能命中探测时建立的 NAT 映射，NatInfo 里的
 * publicPort 才有意义）；不复用则在下次探测时被替换关闭。
 */
public final class NatDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/nat");

    /**
     * 公共 STUN 服务器列表（DESIGN.md：公共 STUN + 中转端内置 STUN，
     * 后者 Phase 2 接入）。DNS 解析失败的条目直接跳过。
     */
    private static final String[][] STUN_SERVERS = {
            {"stun.l.google.com", "19302"},
            {"stun.cloudflare.com", "3478"},
            {"stun.miwifi.com", "3478"},
    };

    /** 探测后寄存的 socket（供打洞复用）；探测失败/UDP 阻断时为 null。 */
    private static final AtomicReference<DatagramSocket> PROBE_SOCKET = new AtomicReference<>();

    private NatDetector() {
    }

    /**
     * 异步探测 NAT 类型（在 {@link ThreadPools#io()} 上执行，探测总耗时
     * 最长约 3 服务器 × 3 次 × 3 秒，不阻塞调用线程）。
     *
     * @param preferredLocalUdpPort 期望绑定的本地 UDP 端口（config.p2pUdpPort）；
     *                              被占用或非法时退回 0 由系统随机分配
     * @return 探测结果（永不异常完成；内部错误一律折算为 UNKNOWN/UDP_BLOCKED）
     */
    public static CompletableFuture<NatInfo> detect(int preferredLocalUdpPort) {
        return CompletableFuture.supplyAsync(() -> detectBlocking(preferredLocalUdpPort), ThreadPools.io());
    }

    /**
     * 领取探测时的 socket（所有权转移给调用方，槽位清空）。
     * P2PSessions 打洞时优先用它 —— NAT 上已有该端口的映射记录。
     *
     * @return 探测 socket；未探测/已被领取/已关闭返回 empty
     */
    public static Optional<DatagramSocket> takeProbeSocket() {
        DatagramSocket socket = PROBE_SOCKET.getAndSet(null);
        if (socket != null && !socket.isClosed()) {
            return Optional.of(socket);
        }
        return Optional.empty();
    }

    /** 同步探测主体（IO 线程上执行）。 */
    private static NatInfo detectBlocking(int preferredLocalUdpPort) {
        DatagramSocket socket = bindSocket(preferredLocalUdpPort);
        if (socket == null) {
            // 连本地 socket 都建不了（极端环境），视为 UDP 不可用
            return new NatInfo(NatType.UDP_BLOCKED, "", 0, 0);
        }
        int localPort = socket.getLocalPort();

        // 逐台服务器查询，记录每台观测到的映射地址
        List<InetSocketAddress> observations = new ArrayList<>();
        for (String[] entry : STUN_SERVERS) {
            InetSocketAddress server;
            try {
                // 显式解析，未知主机（离线/DNS 污染）直接跳过该服务器
                server = new InetSocketAddress(InetAddress.getByName(entry[0]), Integer.parseInt(entry[1]));
            } catch (UnknownHostException e) {
                LOGGER.debug("STUN 服务器 DNS 解析失败，跳过: {}", entry[0]);
                continue;
            }
            StunClient.query(socket, server).ifPresent(mapped -> {
                LOGGER.debug("STUN {} 观测映射: {}", entry[0], mapped);
                observations.add(mapped);
            });
        }

        if (observations.isEmpty()) {
            // 所有服务器都无响应：UDP 出站被阻断（或所有服务器恰好都挂了，保守判死）
            socket.close();
            LOGGER.info("NAT 探测: 无任何 STUN 响应，判定 UDP_BLOCKED");
            return new NatInfo(NatType.UDP_BLOCKED, "", 0, localPort);
        }

        NatType type = classify(observations, localPort);
        InetSocketAddress first = observations.get(0);
        NatInfo info = new NatInfo(type, first.getAddress().getHostAddress(), first.getPort(), localPort);
        LOGGER.info("NAT 探测完成: type={}, public={}:{}, localPort={}",
                type, info.publicIp(), info.publicPort(), localPort);

        // 寄存 socket 供打洞复用；替换掉上一次探测遗留的 socket（若有）
        DatagramSocket previous = PROBE_SOCKET.getAndSet(socket);
        if (previous != null) {
            previous.close();
        }
        return info;
    }

    /** 绑定本地 UDP socket：首选端口失败（占用/无权限）退 0 随机端口。 */
    private static DatagramSocket bindSocket(int preferredPort) {
        if (preferredPort > 0 && preferredPort <= 0xFFFF) {
            try {
                return new DatagramSocket(preferredPort);
            } catch (SocketException e) {
                LOGGER.warn("P2P UDP 端口 {} 绑定失败({})，改用随机端口", preferredPort, e.getMessage());
            }
        }
        try {
            return new DatagramSocket(0);
        } catch (SocketException e) {
            LOGGER.error("无法创建 UDP socket", e);
            return null;
        }
    }

    /** 按观测集合分类 NAT 类型（近似分类，见类 Javadoc）。 */
    private static NatType classify(List<InetSocketAddress> observations, int localPort) {
        // 不同服务器看到不同映射端口 → 对称型
        Set<Integer> ports = new HashSet<>();
        Set<String> ips = new HashSet<>();
        for (InetSocketAddress obs : observations) {
            ports.add(obs.getPort());
            ips.add(obs.getAddress().getHostAddress());
        }
        if (ports.size() > 1 || ips.size() > 1) {
            return NatType.SYMMETRIC;
        }
        // 映射地址与本机地址一致（映射 IP 是本机网卡地址且端口未变）→ 公网直连
        InetSocketAddress mapped = observations.get(0);
        if (mapped.getPort() == localPort && isLocalAddress(mapped.getAddress())) {
            return NatType.OPEN_INTERNET;
        }
        // 锥形细分需要 CHANGE-REQUEST，保守按端口受限锥形
        return NatType.PORT_RESTRICTED_CONE;
    }

    /** 判断给定 IP 是否为本机某网卡地址（OPEN_INTERNET 判据之一）。 */
    private static boolean isLocalAddress(InetAddress address) {
        try {
            return NetworkInterface.getByInetAddress(address) != null;
        } catch (SocketException e) {
            return false;
        }
    }
}
