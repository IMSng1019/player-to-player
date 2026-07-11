package imsng.player_to_player.netproto;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.ThreadPools;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * {@link ControlConnection} 的 Netty 实现（一条 TCP 控制连接）。
 * <p>
 * 本类同时充当管线末端的入站处理器（{@link SimpleChannelInboundHandler}）：
 * 每条连接一个实例，由 {@link ControlServer}（将来也可由客户端侧连接器）在
 * initChannel 时挂到管线尾部。职责：
 * <ul>
 *   <li>心跳：写空闲 {@link Protocol#HEARTBEAT_INTERVAL_SECONDS} 秒发 PING，
 *       读空闲 {@link Protocol#HEARTBEAT_TIMEOUT_SECONDS} 秒判死断开；
 *       收到 PING 自动应答 PONG（携带 _rid 原样回带）；</li>
 *   <li>请求-响应：{@link #request} 自动填写自增 {@code _rid} 并登记 pending 表，
 *       入站帧若命中 pending 表则完成对应 future（不再走处理器分发）；
 *       超时由 {@link ThreadPools#scheduler()} 兜底以 TimeoutException 完成；</li>
 *   <li>分发：其余帧按 {@link MessageType} 查处理器表路由；无处理器且带 _rid 时
 *       应答 ERROR（让对端 request() 快速失败而非傻等超时）。</li>
 * </ul>
 * 线程模型：入站回调全部在本连接的 Netty 事件循环上执行；send/request 任意线程可调
 * （Netty 出站自带线程安全）。
 */
final class NettyControlConnection extends SimpleChannelInboundHandler<ControlMessage>
        implements ControlConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/netproto");

    /**
     * 握手前（{@link #peerId()} 尚未设置）允许进入处理器分发的消息类型白名单：
     * HELLO 是握手本身；RELAY_REGISTER 是中转端设置 peerId 的入口，必须放行；
     * PING/PONG/ERROR 属连接管理帧不携带业务语义。其余类型（区块申请、环境
     * 下载等）一律拒绝，防止未认证连接伪造 groupId 申请区块或下载环境文件。
     */
    private static final Set<MessageType> PRE_AUTH_TYPES = EnumSet.of(
            MessageType.HELLO, MessageType.PING, MessageType.PONG,
            MessageType.ERROR, MessageType.RELAY_REGISTER);

    /** 消息类型 → 处理器（由 ControlServer/客户端连接器共享持有，本类只读）。 */
    private final Map<MessageType, MessageHandler> handlers;
    /** 断连监听（连接关闭时逐个通知；异常互不影响）。 */
    private final Iterable<Consumer<ControlConnection>> disconnectListeners;
    /**
     * 是否启用握手门控：服务端/中转端<b>接受</b>的入站连接为 true（对端身份未知，
     * 须先握手）；客户端<b>主动发起</b>的连接为 false —— 客户端从不给自己的连接
     * setPeerId，且对端是它主动连接的服务端/中转端（身份已知），若也门控则
     * P2P_ENDPOINT_EXCHANGE / RELAY_FORWARD 等服务端推送会被误拒。
     */
    private final boolean requireHandshake;

    /** 底层通道：handlerAdded 时填充（channelActive 再次赋值兜底）。 */
    private volatile Channel channel;
    /** 对端身份（HELLO/RELAY_REGISTER 处理器设置）。 */
    private volatile UUID peerId;

    /** request() 的 _rid 发号器（每连接独立，无需全局唯一）。 */
    private final AtomicLong ridCounter = new AtomicLong();
    /** 在途请求：_rid → future（应答/超时/断连三选一完成）。 */
    private final Map<Long, CompletableFuture<ControlMessage>> pending = new ConcurrentHashMap<>();

    /**
     * @param requireHandshake 是否启用握手门控（服务端/中转端接受的连接传 true，
     *                         客户端主动发起的连接传 false，见字段注释）
     */
    NettyControlConnection(Map<MessageType, MessageHandler> handlers,
                           Iterable<Consumer<ControlConnection>> disconnectListeners,
                           boolean requireHandshake) {
        this.handlers = handlers;
        this.disconnectListeners = disconnectListeners;
        this.requireHandshake = requireHandshake;
    }

    // ------------------------------------------------------- ControlConnection

    @Override
    public void send(ControlMessage message) {
        Channel ch = channel;
        if (ch == null || !ch.isOpen()) {
            LOGGER.debug("连接已关闭，丢弃出站帧: {}", message.type());
            return;
        }
        ch.writeAndFlush(message);
    }

    @Override
    public CompletableFuture<ControlMessage> request(ControlMessage message) {
        long rid = ridCounter.incrementAndGet();
        message.json().addProperty(Protocol.RID_FIELD, rid);
        CompletableFuture<ControlMessage> future = new CompletableFuture<>();
        pending.put(rid, future);
        // 超时兜底：到点仍在 pending 表中则以 TimeoutException 完成
        ThreadPools.scheduler().schedule(() -> {
            CompletableFuture<ControlMessage> timedOut = pending.remove(rid);
            if (timedOut != null) {
                timedOut.completeExceptionally(
                        new TimeoutException("请求超时: " + message.type() + " _rid=" + rid));
            }
        }, Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        send(message);
        return future;
    }

    @Override
    public SocketAddress remoteAddress() {
        Channel ch = channel;
        return ch != null ? ch.remoteAddress() : null;
    }

    @Override
    public UUID peerId() {
        return peerId;
    }

    @Override
    public void setPeerId(UUID peerId) {
        this.peerId = peerId;
    }

    @Override
    public boolean isOpen() {
        Channel ch = channel;
        return ch != null && ch.isOpen();
    }

    @Override
    public void close() {
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
    }

    // ------------------------------------------------------------ Netty 回调

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        // 提前到 handlerAdded 赋值：Netty fulfillConnectPromise 先 trySuccess 再
        // fireChannelActive，若只在 channelActive 赋值，调用方从 connect future 唤醒后
        // 立刻 send/request 时 channel 可能还是 null 导致静默丢帧；此时 ctx.channel() 已可用
        channel = ctx.channel();
        super.handlerAdded(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channel = ctx.channel();
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ControlMessage msg) {
        // 1. 心跳：PING 自动回 PONG；PONG 静默吞掉（IdleStateHandler 已因该读活动复位计时）
        if (msg.type() == MessageType.PING) {
            send(msg.reply(MessageType.PONG, null, null));
            return;
        }
        if (msg.type() == MessageType.PONG) {
            return;
        }
        // 2. 请求-响应匹配：命中在途请求则完成 future，不再进处理器分发
        //   （在途请求由本端发起，应答放行不受握手门控约束）
        if (msg.hasRid()) {
            CompletableFuture<ControlMessage> future = pending.remove(msg.rid());
            if (future != null) {
                future.complete(msg);
                return;
            }
        }
        // 3. 握手门控（仅服务端/中转端接受的连接）：未认证（peerId 未设置）时只放行
        //    白名单类型，其余一律回 ERROR 不进处理器分发 —— 否则未握手连接可伪造
        //    groupId 申请区块、下载环境文件（各业务处理器天然受保护，无需逐个加守卫）
        if (requireHandshake && peerId == null && !PRE_AUTH_TYPES.contains(msg.type())) {
            LOGGER.warn("未握手连接 {} 发来 {}，已拒绝", remoteAddress(), msg.type());
            JsonObject err = new JsonObject();
            err.addProperty("code", "not_authenticated");
            err.addProperty("message", "须先完成 HELLO 握手: " + msg.type());
            // reply 会把 _rid 原样回带，让对端 request() 快速失败而非傻等超时
            send(msg.reply(MessageType.ERROR, err, null));
            return;
        }
        // 4. 按类型分发；处理器异常不得打崩事件循环（记日志继续服务其他帧）
        MessageHandler handler = handlers.get(msg.type());
        if (handler == null) {
            LOGGER.warn("消息类型 {} 无处理器（来自 {}）", msg.type(), remoteAddress());
            if (msg.hasRid()) {
                // 带 _rid 说明对端在 request() 等待：回 ERROR 让它快速失败
                JsonObject err = new JsonObject();
                err.addProperty("code", "no_handler");
                err.addProperty("message", "本端未注册该消息类型的处理器: " + msg.type());
                send(msg.reply(MessageType.ERROR, err, null));
            }
            return;
        }
        try {
            handler.handle(this, msg);
        } catch (Exception e) {
            LOGGER.error("处理消息 {} 时异常（来自 {}）", msg.type(), remoteAddress(), e);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 在途请求全部以断连异常完成（不能让调用方等到超时）
        IOException closed = new IOException("连接已断开: " + remoteAddress());
        pending.values().forEach(f -> f.completeExceptionally(closed));
        pending.clear();
        for (Consumer<ControlConnection> listener : disconnectListeners) {
            try {
                listener.accept(this);
            } catch (Exception e) {
                LOGGER.error("断连监听器异常", e);
            }
        }
        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idle) {
            if (idle.state() == IdleState.WRITER_IDLE) {
                // 写空闲达到心跳间隔：主动 PING 保活
                send(ControlMessage.of(MessageType.PING));
            } else if (idle.state() == IdleState.READER_IDLE) {
                // 读空闲达到心跳超时：判定连接死亡
                LOGGER.warn("连接 {} 心跳超时（{}s 无入站），断开", remoteAddress(),
                        Protocol.HEARTBEAT_TIMEOUT_SECONDS);
                ctx.close();
            }
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 入站数据不可信：解码/处理异常记日志并断开，不向上抛
        LOGGER.warn("连接 {} 异常，断开: {}", remoteAddress(), cause.toString());
        ctx.close();
    }
}
