package imsng.player_to_player.netproto;

/**
 * 消息处理器注册表：按消息类型注册 {@link MessageHandler}。
 * <p>
 * ControlServer 与 ControlClient 均实现本接口；各子系统（环境同步、区块注册表、
 * 算力、打洞协助、日志……）通过统一的 {@code register(HandlerRegistry, ...)}
 * 静态方法把自己的处理器挂上来，实现网络层与业务层解耦。
 */
public interface HandlerRegistry {

    /**
     * 注册某类型消息的处理器；同一类型重复注册视为编程错误，
     * 实现应抛出 IllegalStateException（尽早暴露路由冲突）。
     */
    void on(MessageType type, MessageHandler handler);
}
