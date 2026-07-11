package imsng.player_to_player.netproto;

import imsng.player_to_player.util.ThreadPools;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 控制协议客户端连接器（{@link ControlServer} 的客户端侧对偶；DESIGN.md 第 3 节）。
 * <p>
 * 客户端加入世界时用它连接服务端控制端口（HELLO 握手、环境同步、区块申请……），
 * 打洞降级时用它连接中转端口（RELAY_REGISTER / RELAY_FORWARD）。
 * 用法：构造 → {@link #on} 注册处理器 → {@link #connect} 拿 {@link ControlConnection}。
 * <p>
 * 与 ControlServer 不同，本类<b>允许 connect 之后继续注册处理器</b>：
 * 分发是对处理器表的实时查表（ConcurrentHashMap），而部分子系统
 * （如 {@code P2PSessions.register} 需要连接本身作参数）只能在连接建立后挂接 ——
 * 服务端主动推送的消息类型（P2P_ENDPOINT_EXCHANGE 等）必然晚于对应注册到达，语义安全。
 * <p>
 * 线程模型：connect 是阻塞调用（同步 TCP 建连），只能在 IO/后台线程执行，
 * 严禁 MC 主线程 / Netty 事件循环。
 */
public final class ControlClient implements HandlerRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/netproto");

    private final String host;
    private final int port;
    /** 消息类型 → 处理器（分发时实时查表，允许运行期追加）。 */
    private final Map<MessageType, MessageHandler> handlers = new ConcurrentHashMap<>();
    /** 断连监听器（会话清理用）。 */
    private final List<Consumer<ControlConnection>> disconnectListeners = new CopyOnWriteArrayList<>();

    private volatile EventLoopGroup group;
    private volatile NettyControlConnection connection;

    public ControlClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /** {@inheritDoc} 允许 connect 前后任意时机调用（见类 Javadoc）。 */
    @Override
    public void on(MessageType type, MessageHandler handler) {
        MessageHandler previous = handlers.putIfAbsent(type, handler);
        if (previous != null) {
            throw new IllegalStateException("消息类型 " + type + " 已注册过处理器");
        }
    }

    /** 注册断连监听（连接关闭时回调）。 */
    public void onDisconnect(Consumer<ControlConnection> listener) {
        disconnectListeners.add(listener);
    }

    /**
     * 同步建立连接（阻塞，必须在 IO/后台线程调用）。幂等：已连接则返回现有连接。
     *
     * @return 已就绪的控制连接
     * @throws IOException 建连失败（拒绝连接/超时/域名解析失败）
     */
    public synchronized ControlConnection connect() throws IOException {
        NettyControlConnection existing = connection;
        if (existing != null && existing.isOpen()) {
            return existing;
        }
        EventLoopGroup worker = new NioEventLoopGroup(1, ThreadPools.namedFactory("p2p-netty-client"));
        // requireHandshake=false：本端是发起方，对端（服务端/中转端）身份已知，
        // 且客户端侧连接从不 setPeerId，若门控会误拒服务端推送（P2P_ENDPOINT_EXCHANGE 等）
        NettyControlConnection conn = new NettyControlConnection(handlers, disconnectListeners, false);
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(worker)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 管线与 ControlServer 完全对称（帧切分 → 编解码 → 心跳 → 连接体）
                            ch.pipeline()
                                    .addLast("frame", new LengthFieldBasedFrameDecoder(
                                            Protocol.MAX_FRAME_BYTES, 0, 4, 0, 4))
                                    .addLast("decoder", new ControlCodec.Decoder())
                                    .addLast("encoder", new ControlCodec.Encoder())
                                    .addLast("idle", new IdleStateHandler(
                                            Protocol.HEARTBEAT_TIMEOUT_SECONDS,
                                            Protocol.HEARTBEAT_INTERVAL_SECONDS, 0))
                                    .addLast("connection", conn);
                        }
                    });
            bootstrap.connect(host, port).syncUninterruptibly();
            group = worker;
            connection = conn;
            LOGGER.info("控制连接已建立: {}:{}", host, port);
            return conn;
        } catch (Exception e) {
            worker.shutdownGracefully();
            throw new IOException("连接控制端口失败: " + host + ":" + port, e);
        }
    }

    /** 当前连接；未连接或已断开返回 null。 */
    public ControlConnection connection() {
        NettyControlConnection conn = connection;
        return conn != null && conn.isOpen() ? conn : null;
    }

    /** 关闭连接并释放事件循环（幂等）。 */
    public synchronized void close() {
        NettyControlConnection conn = connection;
        connection = null;
        if (conn != null) {
            conn.close();
        }
        EventLoopGroup worker = group;
        group = null;
        if (worker != null) {
            worker.shutdownGracefully();
        }
    }
}
