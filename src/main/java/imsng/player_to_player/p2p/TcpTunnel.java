package imsng.player_to_player.p2p;

import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP ↔ 可靠 P2P 流 的双向隧道桥（Phase 2，规范"副客户端经 P2P 隧道加入主客户端"）。
 * <p>
 * 隧道里跑的是 MC 原版游戏协议的原始字节 —— 本类完全不理解也不修改协议内容，
 * 因此对绝大多数模组的自定义包天然兼容（规范"使用相同环境 可以兼容绝大多数模组"）。
 * <pre>
 *   副客户端 MC ──TCP──> 127.0.0.1:随机口 ═╗
 *                                          ║ ReliableChannel（P2P 直连或中转）
 *   主客户端集成服务端 <──TCP── 127.0.0.1:LAN口 ═╝
 * </pre>
 * <ul>
 *   <li><b>主客户端侧</b>：{@link #bridgeToLocalPort}——对每条已建立的可靠通道，
 *       连接本机集成服务端的 LAN 端口，双向泵送字节；</li>
 *   <li><b>副客户端侧</b>：{@link #listenAndBridge}——在 127.0.0.1 上开一个随机
 *       监听口（只回环、backlog=1，杜绝其他进程/主机搭车），等待本机 MC 客户端
 *       连入后双向泵送。</li>
 * </ul>
 * 泵线程跑在 {@link ThreadPools#io()}（无界缓存池，阻塞 IO 允许）；任一方向
 * EOF/异常即整体拆除（socket 与可靠通道一起关闭），与 TCP 连接语义一致。
 * <p>
 * 隧道建立前双方各写/读一条 {@code DataOutputStream.writeUTF} 的 JSON 头
 * （见 {@link #writeHeader}/{@link #readHeader}），用于表明会话用途
 * （Phase 2 只有 tunnel_join；Phase 3 预同步会复用可靠通道跑其他用途）。
 */
public final class TcpTunnel {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/tunnel");

    /** 泵缓冲大小。 */
    private static final int PUMP_BUFFER_BYTES = 16 * 1024;

    /** 副客户端侧等待本机 MC 客户端连入监听口的超时（毫秒）。 */
    private static final int ACCEPT_TIMEOUT_MILLIS = 60_000;

    private TcpTunnel() {
    }

    // ------------------------------------------------------------ 会话头

    /** 写入一条隧道会话头（长度前缀 UTF 字符串，内容为 JSON）。 */
    public static void writeHeader(ReliableChannel channel, String json) throws IOException {
        DataOutputStream out = new DataOutputStream(channel.outputStream());
        out.writeUTF(json);
        out.flush();
    }

    /** 读取一条隧道会话头（阻塞；调用方应在 io 线程执行）。 */
    public static String readHeader(ReliableChannel channel) throws IOException {
        return new DataInputStream(channel.inputStream()).readUTF();
    }

    // ------------------------------------------------------- 主客户端侧桥接

    /**
     * 把可靠通道桥接到本机指定 TCP 端口（主客户端侧：目标为集成服务端 LAN 口）。
     * 立即返回；连接与两个方向的泵均在 io 线程上进行。
     *
     * @param channel 已完成会话头交换的可靠通道（所有权移交本桥）
     * @param port    本机目标端口（127.0.0.1）
     * @param label   日志标签
     */
    public static void bridgeToLocalPort(ReliableChannel channel, int port, String label) {
        ThreadPools.io().execute(() -> {
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 10_000);
                socket.setTcpNoDelay(true);
                startPumps(channel, socket, label);
            } catch (IOException e) {
                LOGGER.warn("隧道 {} 连接本机端口 {} 失败: {}", label, port, e.toString());
                closeQuietly(channel, socket);
            }
        });
    }

    // ------------------------------------------------------- 副客户端侧桥接

    /**
     * 在 127.0.0.1 上开随机监听口并桥接可靠通道（副客户端侧）。
     * 同步返回监听端口（供 MC 客户端 ConnectScreen 连接），接受与泵送异步进行。
     *
     * @param channel 已完成会话头交换的可靠通道（所有权移交本桥）
     * @param label   日志标签
     * @return 本机监听端口
     * @throws IOException 监听口创建失败
     */
    public static int listenAndBridge(ReliableChannel channel, String label) throws IOException {
        // backlog=1 + 仅回环绑定：只接受本机 MC 客户端的那一条连接
        ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        server.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
        int port = server.getLocalPort();
        ThreadPools.io().execute(() -> {
            Socket socket = null;
            try {
                socket = server.accept();
                socket.setTcpNoDelay(true);
                startPumps(channel, socket, label);
            } catch (IOException e) {
                LOGGER.warn("隧道 {} 等待本机 MC 客户端连入失败: {}", label, e.toString());
                closeQuietly(channel, socket);
            } finally {
                // 只服务一条连接：接受成功与否都不再需要监听口
                try {
                    server.close();
                } catch (IOException ignored) {
                }
            }
        });
        LOGGER.info("隧道 {} 已在 127.0.0.1:{} 监听本机 MC 客户端", label, port);
        return port;
    }

    // ---------------------------------------------------------------- 泵送

    /** 启动两个方向的泵；任一方向终止即整体拆除。 */
    private static void startPumps(ReliableChannel channel, Socket socket, String label) {
        AtomicBoolean closed = new AtomicBoolean(false);
        Runnable teardown = () -> {
            if (closed.compareAndSet(false, true)) {
                closeQuietly(channel, socket);
                LOGGER.info("隧道 {} 已拆除", label);
            }
        };
        try {
            InputStream socketIn = socket.getInputStream();
            OutputStream socketOut = socket.getOutputStream();
            ThreadPools.io().execute(() -> pump(socketIn, channel.outputStream(),
                    label + " tcp→p2p", teardown));
            ThreadPools.io().execute(() -> pump(channel.inputStream(), socketOut,
                    label + " p2p→tcp", teardown));
            LOGGER.info("隧道 {} 已建立（{} ↔ 可靠通道）", label, socket.getRemoteSocketAddress());
        } catch (IOException e) {
            LOGGER.warn("隧道 {} 泵启动失败: {}", label, e.toString());
            teardown.run();
        }
    }

    /** 单向字节泵（io 线程，阻塞）：EOF/异常触发整体拆除。 */
    private static void pump(InputStream in, OutputStream out, String label, Runnable teardown) {
        byte[] buffer = new byte[PUMP_BUFFER_BYTES];
        try {
            int n;
            while ((n = in.read(buffer)) >= 0) {
                if (n > 0) {
                    out.write(buffer, 0, n);
                    out.flush();
                }
            }
            LOGGER.debug("泵 {} 读到 EOF，正常结束", label);
        } catch (IOException e) {
            LOGGER.debug("泵 {} 终止: {}", label, e.toString());
        } finally {
            teardown.run();
        }
    }

    /** 静默关闭通道与 socket（幂等）。 */
    private static void closeQuietly(ReliableChannel channel, Socket socket) {
        try {
            channel.close();
        } catch (Exception ignored) {
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
