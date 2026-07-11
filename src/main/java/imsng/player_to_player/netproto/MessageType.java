package imsng.player_to_player.netproto;

import java.util.HashMap;
import java.util.Map;

/**
 * 控制协议消息类型总表。
 * <p>
 * 编号分区（便于扩展与阅读）：
 * <pre>
 *   1-9    连接管理（握手/心跳/错误）
 *   10-19  环境文件同步与 mod 分发
 *   20-29  区块注册表（申请/释放/数据/探测）
 *   30-39  算力与角色
 *   40-49  P2P 打洞协助
 *   50-59  中转（relay）
 *   60-69  日志
 *   70-79  指令与聊天路由（Phase 4）
 *   80-89  合并与分离（Phase 3）
 *   90-99  特殊加载：末影珍珠/传送门（Phase 4）
 * </pre>
 * 新增类型只增不改号，保证协议向后兼容。
 */
public enum MessageType {

    // ---------------------------------------------------------- 连接管理 1-9
    /** C→S 握手：协议版本、玩家 UUID/名字、模式、算力摘要、NAT 信息。 */
    HELLO(1),
    /** S→C 握手应答：clientId 确认、服务端全局环境哈希、控制面配置摘要。 */
    HELLO_ACK(2),
    /** 双向心跳请求。 */
    PING(3),
    /** 双向心跳应答。 */
    PONG(4),
    /** 通用错误应答（json: code, message）。 */
    ERROR(5),

    // ------------------------------------------------ 环境同步与 mod 分发 10-19
    /** C→S 请求环境清单。 */
    ENV_MANIFEST_REQUEST(10),
    /** S→C 环境清单（相对路径 → sha256/大小 + 全局哈希）。 */
    ENV_MANIFEST(11),
    /** C→S 请求某个环境文件的某一块（json: path, offset）。 */
    ENV_FILE_REQUEST(12),
    /** S→C 环境文件数据块（json: path, offset, total, last；binary: 文件内容）。 */
    ENV_FILE_DATA(13),
    /** C→S 请求按前缀过滤后的 mod 清单（json: role）。 */
    MOD_LIST_REQUEST(14),
    /** S→C mod 清单（文件名 + 前缀解析结果 + sha256）。 */
    MOD_LIST(15),

    // ---------------------------------------------------------- 区块注册 20-29
    /** C→S 申请加载区块（json: dimension, x, z, groupId）。服务端检查本块+四邻。 */
    CHUNK_CLAIM_REQUEST(20),
    /** S→C 申请结果（granted / 拒绝原因：占用组、阻塞区块；hasServerData 指示走服务端数据还是种子生成）。 */
    CHUNK_CLAIM_RESPONSE(21),
    /** C→S 卸载区块（json: dimension, x, z, groupId；binary: 最终区块数据，Phase 2 起携带）。 */
    CHUNK_RELEASE(22),
    /** S→C 卸载确认。 */
    CHUNK_RELEASE_ACK(23),
    /** C→S 区块数据增量上传（主客户端实时同步修改；Phase 2）。 */
    CHUNK_DATA_UPLOAD(24),
    /** C→S 请求服务端已有区块数据（之前被加载过的区块）。 */
    CHUNK_DATA_REQUEST(25),
    /** S→C 区块数据（binary: 区块 NBT）。 */
    CHUNK_DATA(26),
    /**
     * C→S 区块占用<b>只读探测</b>（Phase 4；json: dimension, x, z, groupId）：
     * 与 {@link #CHUNK_CLAIM_REQUEST} 同一套"目标 + 四邻"检查，但<b>不登记任何占用</b>。
     * 传送门预检（玩家站进传送门时提前检查对面维度是否被其他组占用，被占则提前
     * 触发预连接/合并）与末影珍珠交接判定（珍珠即将飞入的区块归属查询）共用。
     */
    CHUNK_PROBE_REQUEST(27),
    /** S→C 探测结果（json: blocked, blockingChunk, blockingGroup；空闲时 blocked=false）。 */
    CHUNK_PROBE_RESPONSE(28),

    // ---------------------------------------------------------- 算力角色 30-39
    /** C→S 算力上报（json: ComputeScore 序列化）。 */
    COMPUTE_REPORT(30),
    /** S→C 角色指派（json: role=primary/secondary, groupId, primaryClientId）。 */
    ROLE_ASSIGN(31),
    /** C→S 玩家位置更新（玩家表；json: uuid, dimension, x, y, z）。 */
    PLAYER_POS_UPDATE(32),
    /**
     * C→S 角色指派申请（Phase 2）：客户端环境同步完成、准备进入世界时发出。
     * 服务端按玩家存档位置查区块注册表/组表决定主副角色，以 {@link #ROLE_ASSIGN}
     * 作为应答（携带 _rid）。
     */
    ROLE_REQUEST(33),
    /**
     * C→S 组世界就绪通告（Phase 4）：主客户端的集成服务端完成 LAN 发布、可以
     * 接待副客户端隧道时发出（json: groupId）。服务端据此冲刷组分离时暂存的
     * 副客户端重定向指派（新主的世界没就绪前推送重定向必然失败）。
     */
    GROUP_WORLD_READY(34),

    // ---------------------------------------------------------- 打洞协助 40-49
    /** C→S 请求与目标组的主客户端建立 P2P（json: targetGroupId 或 targetClientId）。 */
    P2P_CONNECT_REQUEST(40),
    /** S→C 向双方下发对方公网 endpoint（json: peerClientId, ip, port, natType, sessionId）。 */
    P2P_ENDPOINT_EXCHANGE(41),
    /** C→S 打洞结果回报（json: sessionId, success），失败时服务端决定是否启用中转。 */
    P2P_RESULT(42),
    /**
     * S→C 中转降级指示（Phase 2）：某次打洞会话至少一方回报失败且中转可用时，
     * 服务端向<b>双方</b>下发本消息（json: sessionId, peerClientId, relayAddress,
     * relayPort；relayAddress 空串 = 复用服务端主机地址）。双方各自与中转端建立
     * {@code RelayClient} 会话（同 sessionId），上层拿到的传输对直连/中转透明。
     */
    P2P_USE_RELAY(43),

    // -------------------------------------------------------------- 中转 50-59
    /** C→Relay 在中转端注册自己（json: clientId），建立可被寻址的中转会话。 */
    RELAY_REGISTER(50),
    /** 双向：经中转端转发的数据（json: targetClientId, sessionId；binary: 载荷原文）。 */
    RELAY_FORWARD(51),

    // -------------------------------------------------------------- 日志 60-69
    /** C→S 主客户端日志上传（json: playerName, append；binary: 日志文本）。 */
    LOG_UPLOAD(60),

    // ------------------------------------------------ 指令与聊天路由 70-79（Phase 4）
    /**
     * 指令逐级传递（规范"服务器的指令处理"：副→主→中转→服务端→其他主→其他副）。
     * 三种用法靠 JSON 字段区分，应答一律 reply 同类型（_rid 关联）：
     * <ul>
     *   <li><b>上送</b> C→S（json: playerUuid, playerName, command）：主客户端把本组
     *       集成服务端执行失败的玩家指令上送（副客户端的指令经隧道在主客户端的
     *       集成服务端执行，失败信号在主客户端捕获 —— "副→主"一级是原版语义）；</li>
     *   <li><b>转发执行</b> S→C（json: execute=true, command, sourceName,
     *       permissionLevel）：服务端把指令分发给其他组的主客户端尝试执行，
     *       应答 {executed, message}；</li>
     *   <li><b>上送应答</b> S→C reply（json: handled, message, action?）：handled=true 且
     *       action="teleport" 时附 dimension/x/y/z —— 服务端按玩家表解析出的跨组 tp，
     *       由发起方主客户端在本地执行传送（随后的移动由加载门控/分离监视接管）。</li>
     * </ul>
     */
    COMMAND_RELAY(70),
    /**
     * 聊天/系统消息全网分发（规范"服务器的信息处理"）。
     * <ul>
     *   <li>C→S（json: playerUuid, playerName, text）：主客户端上送本组聊天
     *       （含副客户端 —— 它们的聊天经隧道天然汇聚到主客户端的集成服务端）；</li>
     *   <li>S→C（json: playerName, text [, privateTo]）：服务端向其他组的主客户端
     *       推送，主客户端在本组集成服务端内广播（副客户端经原版链路看到）；
     *       privateTo 非空 = 跨组私聊（/msg），只投递给该玩家。</li>
     * </ul>
     */
    CHAT_RELAY(71),

    // ---------------------------------------------------- 合并与分离 80-89（Phase 3）
    /**
     * C→S 合并申请（json: initiatorGroupId, targetGroupId, reason, blockingChunk）。
     * 主客户端的区块申请被拒（blockingGroup）或角色指派要求迁移时发出；
     * 服务端 {@code MergeCoordinator} 校验双方在线后开始撮合，
     * 以 {@link #MERGE_PLAN}（携带 _rid）应答。
     */
    MERGE_REQUEST(80),
    /**
     * S→C 合并计划（json: mergeId, newPrimaryClientId, oldPrimaryClientId,
     * groupAId, groupBId, members[]）：服务端按算力表选出新主客户端
     * （规范"算力分配"：单核分最高且剩余内存达标者），双方主客户端各收一份。
     * 收到后 A（让出方）与 B（接管方）开始预同步。
     */
    MERGE_PLAN(81),
    /**
     * C→S 合并阶段回报（json: mergeId, phase, success, detail）。
     * phase ∈ presync_started / snapshot_done / caught_up / switched / failed；
     * 服务端据此推进/回滚合并状态机（失败 → MERGE_ABORT 双方，各自恢复原状）。
     */
    MERGE_PROGRESS(82),
    /**
     * S→C 合并中止（json: mergeId, reason）：任一方失败/掉线时服务端向双方下发，
     * A 继续运行，B 丢弃影子实例（规范"若 B 追赶失败或断开，A 继续运行，
     * 服务端取消合并"）。
     */
    MERGE_ABORT(83),
    /**
     * S→C 合并完成确认（json: mergeId, groupId, primaryClientId, members[]）：
     * 服务端在收到 switched 回报后更新组表/注册表归属，向新组全体成员广播
     * 最终成员视图；副客户端据此把隧道重定向到新主客户端。
     */
    MERGE_COMMIT(84),
    /**
     * C→S 分离申请（json: groupId, departingClientId, chunks[]）：
     * 主客户端检测到某副客户端渲染区域与组内其他成员无交集持续达标（规范"分离"）
     * 后发出；服务端把 chunks[] 的占用从原组迁移到新组（departingClient 自立门户），
     * 以 {@link #SPLIT_ACK} 应答。
     */
    SPLIT_REQUEST(85),
    /** S→C 分离确认（json: granted, newGroupId, migratedChunks）。 */
    SPLIT_ACK(86),
    /**
     * C→S 玩家数据上行（json: playerUuid, playerName；binary: gzip 玩家 NBT）。
     * 分离前主客户端上传离组玩家的最终数据；合并/退出时亦可用（Phase 3/4 玩家数据
     * 上行的收口）。服务端写入世界 playerdata/&lt;uuid&gt;.dat ——
     * {@code RoleAssignHandler} 的存档定位与环境同步天然消费同一份数据。
     */
    PLAYER_DATA_UPLOAD(87),
    /** C→S 请求玩家数据（json: playerUuid）；本人或其组主客户端可请求。 */
    PLAYER_DATA_REQUEST(88),
    /** S→C 玩家数据（json: exists；binary: gzip 玩家 NBT）。 */
    PLAYER_DATA(89),

    // ---------------------------------------------------- 特殊加载 90-99（Phase 4）
    /**
     * 末影珍珠交接（规范"末影珍珠"特殊加载）。双向复用：
     * <ul>
     *   <li>C→S（json: targetGroupId, dimension, throwerUuid, throwerName；
     *       binary: gzip 珍珠 NBT）：珍珠即将飞入被其他组占用/缓冲的区块时，
     *       抛出者所在组把珍珠实体整体交给目标组演算（避免为一颗珍珠触发合并 ——
     *       规范"该加载方案可以使合并次数大量减少"）；</li>
     *   <li>S→C（同 json + binary）：服务端转发给目标组主客户端，其在本组
     *       集成服务端内重生成珍珠实体继续飞行。</li>
     * </ul>
     */
    PEARL_HANDOFF(90),
    /**
     * 末影珍珠落点回报（json: throwerUuid, dimension, x, y, z）。
     * 落点组主客户端→S→抛出者所在组主客户端：规范"当末影珍珠落地后则看做一个
     * 小型的 tp 指令"——抛出者的主客户端把玩家传送到落点（含跨维度 + 原版 5 点
     * 摔落伤害），随后的合并/分离由加载门控与分离监视自然接管。
     */
    PEARL_LANDED(91);

    private final int id;

    private static final Map<Integer, MessageType> BY_ID = new HashMap<>();

    static {
        for (MessageType type : values()) {
            MessageType previous = BY_ID.put(type.id, type);
            if (previous != null) {
                throw new IllegalStateException("消息类型编号冲突: " + type + " 与 " + previous);
            }
        }
    }

    MessageType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    /** 按编号解析；未知编号返回 null（上层应答 ERROR 并记日志，不抛异常防止恶意帧打崩连接）。 */
    public static MessageType fromId(int id) {
        return BY_ID.get(id);
    }
}
