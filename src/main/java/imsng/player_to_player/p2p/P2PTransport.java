package imsng.player_to_player.p2p;

import java.util.function.Consumer;

/**
 * P2P 数据传输抽象（规范"跨端合作"与 DESIGN.md 第 7 节）。
 * <p>
 * 上层（预同步数据流、区块数据、副客户端游戏包隧道等，Phase 2+）只面向本接口编程，
 * 对底下走的是加密 UDP 直连（{@link P2PChannel}）还是中转端 TCP 转发
 * （{@link RelayClient}）完全透明 —— 打洞成功走直连，失败降级中转，
 * 上层逻辑不需要感知差异。
 * <p>
 * <b>线程模型</b>：{@code send} 可在任意线程调用；{@code receiver} 回调在
 * 传输实现自己的接收线程（{@code ThreadPools.io()} 或 Netty 事件循环）上执行，
 * 回调内不得做阻塞操作，重活转交 ThreadPools，MC 世界交互回主线程。
 */
public interface P2PTransport {

    /**
     * 发送一块数据到对端（fire-and-forget；UDP 实现不保证到达与有序，
     * 可靠性由上层协议按需实现）。
     *
     * @param data 载荷；实现可对长度设上限（超限抛 IllegalArgumentException）
     */
    void send(byte[] data);

    /**
     * 设置接收回调（收到对端一块完整载荷时触发；解密/解包后的原文）。
     * 后设置的回调覆盖先前的；传 null 表示丢弃入站数据。
     */
    void setReceiver(Consumer<byte[]> receiver);

    /** 传输是否仍然可用。 */
    boolean isOpen();

    /** 关闭传输并释放底层资源（幂等）。 */
    void close();
}
