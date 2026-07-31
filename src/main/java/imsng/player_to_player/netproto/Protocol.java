package imsng.player_to_player.netproto;

/**
 * 控制协议常量（版本、帧限制、心跳参数）。
 * <p>
 * 控制协议运行在独立 TCP 端口上（区别于 MC 游戏端口），
 * 服务端默认 25580、中转端默认 25581，帧格式见 {@link ControlMessage}。
 */
public final class Protocol {

    /**
     * 协议版本：HELLO 握手时双方校验，不一致直接拒绝。
     * <p>
     * v4 将退出事务改为增量提交：客户端不再为干净区块发送 preserveExisting 消息，
     * 而是在 EXIT_COMMIT.writtenChunks 中一次性声明实际上传集合；普通 CHUNK_RELEASE
     * 同时增加稳定 releaseId 与回执确认消息，保证正 ACK 丢失、连接超时和服务端重启后
     * 能重放第一次成功结果。v3 服务端无法解释这些语义，因此必须拒绝新旧节点混连。
     */
    public static final int VERSION = 4;

    /** 单帧最大长度（16 MB）：超限视为恶意/损坏连接，立即断开。环境文件按块传输不会超过它。 */
    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    /** 心跳发送间隔（秒）。 */
    public static final int HEARTBEAT_INTERVAL_SECONDS = 30;

    /** 心跳超时（秒）：超过该时长无任何入站消息则判定连接死亡。 */
    public static final int HEARTBEAT_TIMEOUT_SECONDS = 90;

    /** request() 请求-响应默认超时（毫秒）。 */
    public static final long REQUEST_TIMEOUT_MILLIS = 15_000;

    /** JSON 中请求-响应关联字段名（由 ControlConnection 自动填写/匹配）。 */
    public static final String RID_FIELD = "_rid";

    private Protocol() {
    }
}
