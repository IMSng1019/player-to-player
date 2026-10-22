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
 * 泵线程跑在 {@link ThreadPools#startBlocking}（专用阻塞线程，不得进有界 io 池）；任一方向
 * EOF/异常即整体拆除（socket 与可靠通道一起关闭），与 TCP 连接语义一致。
 * <p>
 * 隧道建立前双方各写/读一条 {@code DataOutputStream.writeUTF} 的 JSON 头
 * （见 {@link #writeHeader}/{@link #readHeader}），用于表明会话用途
 * （Phase 2 只有 tunnel_join；Phase 3 预同步会复用可靠通道跑其他用途）。
 */
public final class TcpTunnel {

    /** 终止回调的兜底执行：回调异常不得影响隧道拆除本身。 */
    static final class Terminations {
        private Terminations() {
        }

        static void run(Runnable onTerminated) {
            if (onTerminated == null) {
                return;
            }
            try {
                onTerminated.run();
            } catch (RuntimeException e) {
                LOGGER.warn("隧道终止回调异常（忽略）", e);
            }
        }
    }


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

    /** 准备握手的阻塞读取有总期限，不能永久占住接待资源。 */
    public static String readHeader(ReliableChannel channel, long timeoutSeconds) throws IOException {
        var timeout = ThreadPools.scheduler().schedule(channel::abort, timeoutSeconds,
                java.util.concurrent.TimeUnit.SECONDS);
        try { return readHeader(channel); }
        finally { timeout.cancel(false); }
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
        bridgeToLocalPort(channel, port, label, null);
    }

    /**
     * 同 {@link #bridgeToLocalPort(ReliableChannel, int, String)}，另外在隧道<b>终止时
     * 恰好调用一次</b> {@code onTerminated}（无论连接失败还是任一方向泵结束）。
     * <p>
     * 调用方（{@code GroupHost}）用它把"在册会话名额"与隧道的真实生命周期绑定：
     * 名额若在接待方法返回时就归还，对端只要不断发起会话就能无界地堆起隧道线程。
     *
     * @param onTerminated 终止回调（可为 null）；必须是幂等且不阻塞的小动作
     */
    public static void bridgeToLocalPort(ReliableChannel channel, int port, String label,
                                         Runnable onTerminated) {
        // 连接 + 两个方向的泵都是阻塞长驻任务，必须用专用线程（有界 io 池承载不了
        // "整局游戏都占着"的任务，见 ThreadPools#startBlocking）
        ThreadPools.startBlocking("tunnel-bridge", () -> {
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 10_000);
                socket.setTcpNoDelay(true);
                startPumps(channel, socket, label, onTerminated);
            } catch (IOException e) {
                LOGGER.warn("隧道 {} 连接本机端口 {} 失败: {}", label, port, e.toString());
                closeQuietly(channel, socket);
                Terminations.run(onTerminated);
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
        return listenPrepared(channel, label, () -> { }).port();
    }

    @FunctionalInterface
    public interface Activation { void run() throws IOException; }

    /** 可立即取消的监听器；准备世界切换失败时不必等原先的 60 秒 accept 超时。 */
    public record Listener(ServerSocket socket, ReliableChannel channel) implements AutoCloseable {
        public int port() { return socket.getLocalPort(); }
        @Override public void close() {
            try { socket.close(); } catch (IOException ignored) { }
            channel.abort();
        }
    }

    /** 本机 MC 真正连接后才激活远端 LAN，避免预连接等待期间触发 MC 登录超时。 */
    public static Listener listenPrepared(ReliableChannel channel, String label,
                                          Activation activation) throws IOException {
        // backlog=1 + 仅回环绑定：只接受本机 MC 客户端的那一条连接
        ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        server.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
        int port = server.getLocalPort();
        ThreadPools.startBlocking("tunnel-accept", () -> {
            Socket socket = null;
            try {
                socket = server.accept();
                socket.setTcpNoDelay(true);
                activation.run();
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
        return new Listener(server, channel);
    }

    /** 准备端已收到激活后连接 LAN，并在任何游戏字节之前发送确认。 */
    static void activateAndBridge(ReliableChannel channel, int port, String label,
                                  String activationReply, Runnable terminated) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 10_000);
            socket.setTcpNoDelay(true);
            writeHeader(channel, activationReply);
            startPumps(channel, socket, label, terminated);
        } catch (IOException error) {
            closeQuietly(channel, socket);
            throw error;
        }
    }

    // ---------------------------------------------------------------- 泵送

    /** 启动两个方向的泵；任一方向终止即整体拆除。 */
    private static void startPumps(ReliableChannel channel, Socket socket, String label) {
        startPumps(channel, socket, label, null);
    }

    private static void startPumps(ReliableChannel channel, Socket socket, String label,
                                   Runnable onTerminated) {
        AtomicBoolean closed = new AtomicBoolean(false);
        Runnable teardown = () -> {
            if (closed.compareAndSet(false, true)) {
                closeQuietly(channel, socket);
                LOGGER.info("隧道 {} 已拆除", label);
                Terminations.run(onTerminated);
            }
        };
        try {
            InputStream socketIn = socket.getInputStream();
            OutputStream socketOut = socket.getOutputStream();
            ThreadPools.startBlocking("tunnel-pump", () -> pump(socketIn, channel.outputStream(),
                    label + " tcp→p2p", teardown));
            ThreadPools.startBlocking("tunnel-pump", () -> pump(channel.inputStream(), socketOut,
                    label + " p2p→tcp", teardown));
            LOGGER.info("隧道 {} 已建立（{} ↔ 可靠通道）", label, socket.getRemoteSocketAddress());
        } catch (IOException e) {
            LOGGER.warn("隧道 {} 泵启动失败: {}", label, e.toString());
            teardown.run();
        }
    }

    /** 单向字节泵（专用阻塞线程）：EOF/异常触发整体拆除。 */
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
