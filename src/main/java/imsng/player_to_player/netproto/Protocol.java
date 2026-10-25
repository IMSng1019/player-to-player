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
     * v5 将运行期区块上传和退出尾部上传改为批次提交：客户端不再为干净区块发送
     * preserveExisting 消息，
     * 而是在 EXIT_COMMIT.writtenChunks 中一次性声明实际上传集合；普通 CHUNK_RELEASE
     * 同时增加稳定 releaseId 与回执确认消息，保证正 ACK 丢失、连接超时和服务端重启后
     * 能重放第一次成功结果。退出最终确认同时改为复合 WAL 单次 force；任何非 v5
     * 节点都无法解释这些语义，因此必须拒绝新旧节点混连。
     */
    // v6 增加撮合 sessionId 应答、预建隧道握手及合并准备/提交屏障。
    // v5 无法遵守新时序，因此服务端、中转端与客户端统一升级。
    // v7 将分离改为 prepare→旧区域卸载及耐久释放→commit，旧端的直接改挂不再兼容。
    // v8 增加登录前只读环境会话、固定版本安装与独立计算进程；所有节点统一升级。
    // v9 将 level.dat 拆为权威字段快照/版本补丁，旧端无法遵守元数据接管屏障。
    public static final int VERSION = 9;

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

    /**
     * {@code _rid} 号段起点 —— <b>接受端</b>（服务端/中转端接受的连接）。
     * <p>
     * 同一条连接上两端都会发起 request（客户端上送指令、服务端下发转发执行），
     * 而应答匹配是"命中在途表即完成 future 并不再分发"——共用一个自增号段会互吞。
     * 故按连接角色分段：接受端低位、发起端高位。详见
     * {@code NettyControlConnection#ridBase} 的类内注释。
     */
    public static final long RID_BASE_ACCEPTOR = 1L;

    /** {@code _rid} 号段起点 —— <b>发起端</b>（客户端主动拨出的连接）。 */
    public static final long RID_BASE_DIALER = 1L << 40;

    private Protocol() {
    }
}
