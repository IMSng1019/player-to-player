package imsng.player_to_player.netproto;

/**
 * 控制协议消息处理器。
 * <p>
 * 各子系统（环境同步、区块注册表、算力、打洞协助、日志……）实现本接口并
 * 按 {@link MessageType} 注册到 ControlServer / ControlClient 的分发表；
 * 网络层收到帧后按类型路由到对应处理器。
 * <p>
 * <b>线程模型</b>：handle 在 Netty 事件循环线程上被调用 ——
 * 只做轻量解析与应答；任何阻塞操作（磁盘 IO、哈希计算）必须转交
 * {@code ThreadPools.io()}，任何 MC 世界交互必须回主线程执行。
 */
@FunctionalInterface
public interface MessageHandler {

    /**
     * 处理一条入站消息。
     *
     * @param connection 消息来源连接（应答直接向它 send）
     * @param message    入站消息
     */
    void handle(ControlConnection connection, ControlMessage message);
}
