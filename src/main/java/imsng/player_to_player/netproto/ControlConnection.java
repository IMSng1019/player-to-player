package imsng.player_to_player.netproto;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * 一条控制协议连接的抽象（服务端视角的"某个客户端"，或客户端视角的"服务端/中转端"）。
 * <p>
 * 实现位于 netproto 包内（基于 Netty Channel）。消息处理器（{@link MessageHandler}）
 * 只面向本接口编程，不接触 Netty 细节 —— 这样 Phase 2 的 P2P 加密通道也可以
 * 实现本接口，让上层逻辑对"经服务端"还是"点对点"透明。
 */
public interface ControlConnection {

    /** 异步发送一条消息（fire-and-forget，进入 Netty 出站队列即返回）。 */
    void send(ControlMessage message);

    /**
     * 请求-响应模式发送：自动在 JSON 中写入自增 {@code _rid}，
     * 返回的 future 在收到携带相同 _rid 的应答帧时完成；
     * 超过 {@link Protocol#REQUEST_TIMEOUT_MILLIS} 未应答则以 TimeoutException 异常完成。
     * <p>
     * 注意：不得在 MC 主线程上 join()/get() 阻塞等待，用 thenAccept 链式处理
     * 或在后台线程等待。
     */
    CompletableFuture<ControlMessage> request(ControlMessage message);

    /** 对端网络地址（日志与打洞 endpoint 交换用）。 */
    SocketAddress remoteAddress();

    /**
     * 对端身份（HELLO 握手完成后由握手处理器设置；握手前为 null）。
     * 服务端用它把连接与玩家/组关联起来。
     */
    UUID peerId();

    /** 设置对端身份（仅握手处理器调用）。 */
    void setPeerId(UUID peerId);

    /** 连接是否仍然可用。 */
    boolean isOpen();

    /** 关闭连接（幂等）。 */
    void close();
}
