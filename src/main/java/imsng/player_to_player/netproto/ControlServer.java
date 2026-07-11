package imsng.player_to_player.netproto;

import imsng.player_to_player.util.ThreadPools;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 控制协议服务器（DESIGN.md 第 3 节）：在 MC 端口之外监听一个独立 TCP 端口，
 * 用 MC 自带的 Netty 收发 {@link ControlMessage} 帧。
 * <p>
 * 三种使用场景共用本类：
 * <ul>
 *   <li>服务端控制面（默认 25580，{@code server.P2PServerService}）；</li>
 *   <li>中转面（默认 25581，{@code proxy.RelayCore}，独立中转端与服务端兼任中转皆同）。</li>
 * </ul>
 * 用法：构造 → {@link #on} 注册处理器 → {@link #start}；不再接受
 * start 之后的注册（处理器表以只读方式发布给各连接，避免运行期并发注册的可见性问题）。
 * <p>
 * 管线：LengthFieldBasedFrameDecoder（按 totalLen 切帧防拆包）→ 解码器 →
 * IdleStateHandler（心跳）→ 编码器 → {@link NettyControlConnection}（每连接一实例）。
 * <p>
 * 线程模型：boss 1 + worker N 的 Nio 事件循环（守护线程，统一 p2p-netty 命名）。
 */
public final class ControlServer implements HandlerRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/netproto");

    private final int port;
    /** 消息类型 → 处理器；start 前注册完毕，运行期只读。 */
    private final Map<MessageType, MessageHandler> handlers = new ConcurrentHashMap<>();
    /** 断连监听器（P2PServerService/RelayCore 清理状态用）。 */
    private final List<Consumer<ControlConnection>> disconnectListeners = new CopyOnWriteArrayList<>();

    private volatile EventLoopGroup bossGroup;
    private volatile EventLoopGroup workerGroup;
    private volatile Channel serverChannel;
    /** 已接受的子连接集合（channel 关闭时 ChannelGroup 自动移除）：stop() 用于同步关闭全部连接。 */
    private final ChannelGroup childChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private volatile boolean started;

    /**
     * @param port 监听端口（服务端控制面 {@code GlobalConfig.controlPort}，
     *             中转面 {@code GlobalConfig.relayPort}）
     */
    public ControlServer(int port) {
        this.port = port;
    }

    /** {@inheritDoc} 必须在 {@link #start} 之前调用。 */
    @Override
    public void on(MessageType type, MessageHandler handler) {
        if (started) {
            throw new IllegalStateException("ControlServer 已启动，不再接受处理器注册: " + type);
        }
        MessageHandler previous = handlers.putIfAbsent(type, handler);
        if (previous != null) {
            // 同类型重复注册视为编程错误（HandlerRegistry 契约），尽早暴露路由冲突
            throw new IllegalStateException("消息类型 " + type + " 已注册过处理器");
        }
    }

    /** 注册断连监听（连接关闭时回调，用于清理该连接关联的登记状态）。 */
    public void onDisconnect(Consumer<ControlConnection> listener) {
        disconnectListeners.add(listener);
    }

    /** 启动监听（同步绑定端口；失败抛 IllegalStateException 让上层快速失败）。幂等。 */
    public synchronized void start() {
        if (serverChannel != null) {
            return;
        }
        started = true;
        EventLoopGroup boss = new NioEventLoopGroup(1, ThreadPools.namedFactory("p2p-netty-boss"));
        EventLoopGroup worker = new NioEventLoopGroup(0, ThreadPools.namedFactory("p2p-netty-worker"));
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 收集子连接：stop() 时先同步关掉它们，防止优雅停机静默期
                            // 事件循环仍在处理排队帧、在最终持久化之后又改注册表
                            childChannels.add(ch);
                            ch.pipeline()
                                    // 按 totalLen 前缀切帧（int32 长度字段，剥掉），超限断开防攻击
                                    .addLast("frame", new LengthFieldBasedFrameDecoder(
                                            Protocol.MAX_FRAME_BYTES, 0, 4, 0, 4))
                                    .addLast("decoder", new ControlCodec.Decoder())
                                    .addLast("encoder", new ControlCodec.Encoder())
                                    // 心跳：读空闲超时判死，写空闲达到间隔发 PING
                                    .addLast("idle", new IdleStateHandler(
                                            Protocol.HEARTBEAT_TIMEOUT_SECONDS,
                                            Protocol.HEARTBEAT_INTERVAL_SECONDS, 0))
                                    // requireHandshake=true：接受的连接身份未知，未握手只放行白名单类型
                                    .addLast("connection", new NettyControlConnection(
                                            handlers, disconnectListeners, true));
                        }
                    });
            serverChannel = bootstrap.bind(port).syncUninterruptibly().channel();
            bossGroup = boss;
            workerGroup = worker;
            LOGGER.info("控制服务器已监听端口 {}", port);
        } catch (Exception e) {
            // 绑定失败（端口占用等）：回收事件循环并向上抛，绝不留半启动状态
            boss.shutdownGracefully();
            worker.shutdownGracefully();
            started = false;
            throw new IllegalStateException("控制端口 " + port + " 监听失败", e);
        }
    }

    /**
     * 停止监听并<b>同步关闭全部已接受连接</b>，之后才优雅关闭事件循环（幂等）。
     * 返回时所有连接已关闭、不再有入站帧进入处理器 —— 调用方（如
     * {@code P2PServerService}）可安全执行最终持久化，不会有事后的注册表变更。
     */
    public synchronized void stop() {
        Channel ch = serverChannel;
        serverChannel = null;
        if (ch != null) {
            ch.close().syncUninterruptibly();
        }
        // 先同步关掉全部子连接：仅靠 shutdownGracefully 的静默期收尾时，
        // 事件循环仍可能处理排队帧，导致最终 persist 之后又有状态变更。
        // await 而非 sync：个别连接关闭失败不应中断整个停机流程
        childChannels.close().awaitUninterruptibly();
        EventLoopGroup boss = bossGroup;
        EventLoopGroup worker = workerGroup;
        bossGroup = null;
        workerGroup = null;
        if (boss != null) {
            boss.shutdownGracefully();
        }
        if (worker != null) {
            worker.shutdownGracefully();
        }
        if (ch != null) {
            LOGGER.info("控制服务器已停止 (端口 {})", port);
        }
    }
}
