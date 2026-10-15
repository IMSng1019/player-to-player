package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.env.EnvironmentSnapshot;
import imsng.player_to_player.env.EnvironmentSnapshotManager;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageHandler;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.p2p.NatInfo;
import imsng.player_to_player.p2p.P2PKeyGate;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * HELLO 握手处理器（服务端）。
 * <p>
 * 对应规范"玩家加入世界"事件的服务端侧起点：客户端建立控制连接后的第一条消息
 * 必须是 HELLO（version, clientId, playerName, mode, compute, nat），本处理器：
 * <ol>
 *   <li>校验协议版本 {@link Protocol#VERSION}，不符则回 accepted=false 的 HELLO_ACK 并关闭连接
 *       （避免新旧节点混连产生隐性协议错误）；</li>
 *   <li>把 clientId 写入连接（{@link ControlConnection#setPeerId}），并登记
 *       clientId → 连接 的静态并发映射，供 {@link P2PBrokerHandlers} 查找打洞双方、
 *       供 {@link P2PServerService} 断连清理；</li>
 *   <li>解析随 HELLO 上报的算力评分（存 {@link ComputeTable}，规范：玩家加入世界时
 *       "玩家向服务端给出算力能力"）与 NAT 探测信息（存静态 NAT 表，
 *       P2P_ENDPOINT_EXCHANGE 时转发给打洞对端）；</li>
 *   <li>应答 HELLO_ACK：服务端全局环境哈希（规范：服务端的哈希值在服务端启动时计算，
 *       启动扫描未完成时 envReady=false，客户端应稍后重试环境比对）、世界名、
 *       中转地址与端口、主客户端内存门槛。</li>
 * </ol>
 * <p>
 * <b>P2P 公钥指纹的生命周期由本类锚定</b>（Phase 2 收口）：指纹表按 clientId 存放在
 * {@link P2PKeyGate#SERVER}，但"何时可信"完全跟随 HELLO 会话 —— 新 HELLO（可能是新进程、
 * 新密钥对）先清旧值、等客户端重新上报，客户端完全离线时再清一次；绝不把上一个会话的
 * 指纹当成新会话的既成事实（否则会表现为"对端指纹校验失败"，把安全问题伪装成连通性问题）。
 * <p>
 * 线程模型：handle 运行在 Netty 事件循环上，只做轻量 JSON 解析与应答，无阻塞操作。
 */
public final class HelloHandler implements MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/server");

    /**
     * clientId → 在线控制连接。静态并发映射：P2PBrokerHandlers 据此把
     * P2P_ENDPOINT_EXCHANGE 精确投递给打洞双方；P2PServerService 断连时按身份清理。
     */
    private static final Map<UUID, ControlConnection> ONLINE = new ConcurrentHashMap<>();

    /** clientId → HELLO 上报的 NAT 探测信息（打洞协助时转发给对端）。 */
    private static final Map<UUID, NatInfo> NAT_TABLE = new ConcurrentHashMap<>();

    /** 固定条带锁串行同一客户端的新 HELLO 发布与权威写任务。 */
    private static final Object[] SESSION_LOCKS = new Object[256];

    static {
        java.util.Arrays.setAll(SESSION_LOCKS, ignored -> new Object());
    }

    private final GlobalConfig config;
    private final ComputeTable computeTable;
    /** 不可变环境快照管理器：首轮快照未发布时 current() 返回 null。 */
    private final EnvironmentSnapshotManager snapshots;
    /** 服务端世界名（HELLO_ACK 下发，客户端据此确定 <IP>+<世界名> 世界文件夹）。 */
    private final String worldName;
    /** 玩家表：HELLO 是唯一的建行入口（玩家 UUID = clientId，见 ConnectionIdentity）。 */
    private final PlayerTable players;
    /**
     * 权限下发（HELLO 重连时按组成员强制重推，防客户端侧缓存丢失）。
     * <p>
     * 允许为 null：单测里没有真实 MinecraftServer 可查 ops 名单，传 null 即关闭
     * 该副作用（与 {@code RegistryHandlers.register} 的 writeback 同款"null = 无此能力"口径）。
     */
    private final PermissionRelay permissions;

    public HelloHandler(GlobalConfig config, ComputeTable computeTable,
                        EnvironmentSnapshotManager snapshots, String worldName,
                        PlayerTable players, PermissionRelay permissions) {
        this.config = config;
        this.computeTable = computeTable;
        this.snapshots = snapshots;
        this.worldName = worldName;
        this.players = players;
        this.permissions = permissions;
    }

    @Override
    public void handle(ControlConnection connection, ControlMessage message) {
        JsonObject json = message.json();

        // 1. 协议版本校验：不一致直接拒绝（入站数据不可信，字段缺失按 -1 处理）
        int version = JsonUtil.getInt(json, "version", -1);
        if (version != Protocol.VERSION) {
            reject(connection, message,
                    "协议版本不匹配: 服务端=" + Protocol.VERSION + ", 客户端=" + version);
            return;
        }

        // 2. 客户端身份：clientId 非法（伪造/损坏帧）同样拒绝
        UUID clientId;
        try {
            clientId = UUID.fromString(JsonUtil.getString(json, "clientId", ""));
        } catch (IllegalArgumentException e) {
            reject(connection, message, "clientId 非法");
            return;
        }
        String playerName = JsonUtil.getString(json, "playerName", "?");
        String mode = JsonUtil.getString(json, "mode", "client");

        // 3. 算力与 NAT：字段缺失容忍（旧版本客户端可能不带），有则登记
        ComputeScore score = null;
        if (json.has("compute") && json.get("compute").isJsonObject()) {
            score = ComputeScore.fromJson(json.getAsJsonObject("compute"));
        }
        NatInfo nat = json.has("nat") && json.get("nat").isJsonObject()
                ? NatInfo.fromJson(json.getAsJsonObject("nat"))
                : NatInfo.UNKNOWN;

        connection.setPeerId(clientId);
        ControlConnection previous;
        synchronized (sessionLock(clientId)) {
            // 新会话只有在已经开始的旧权威写完成后才发布；发布之后，旧排队任务
            // 会在 withCurrentSession 中看到连接身份不符并被拒绝。
            if (score != null) {
                computeTable.report(clientId, score);
            }
            NAT_TABLE.put(clientId, nat);
            // 身份声明与实际发布同临界区：客户端约定 clientId == 玩家档案 UUID，
            // 故这里既建立玩家表的第一行，也建立"本连接代表谁"的自洽判据
            //（ConnectionIdentity 的成员校验、PermissionRelay 的名单下发都用它）。
            ConnectionIdentity.declare(clientId, playerName,
                    JsonUtil.getInt(json, "commandRouting", 0) >= 1);
            players.declare(clientId, playerName);
            // P2P 公钥指纹必须随新会话重新上报：同一个 clientId 可能来自重启后的新进程
            // （密钥对已换），旧指纹若被当作"已就绪"，服务端会把过期指纹下发给对端，
            // 对端校验必然失败 —— 那是可用性事故，且会被误读成中间人告警。
            // 客户端在 HELLO_ACK 之后会主动重新上报（P2PSessions.register）。
            P2PKeyGate.SERVER.forget(clientId);
            previous = ONLINE.put(clientId, connection);
        }
        if (previous != null && previous != connection && previous.isOpen()) {
            LOGGER.warn("客户端 {} 重复握手，关闭旧连接 {}", clientId,
                    previous.remoteAddress());
            previous.close();
        }

        // 4. 应答 HELLO_ACK（reply 复制 _rid，使客户端 request() 的 future 完成）
        EnvironmentSnapshot current = snapshots.current();
        JsonObject ack = new JsonObject();
        ack.addProperty("accepted", true);
        // 环境扫描未完成时 envHash 为空串 + envReady=false：客户端应等待后重发 ENV_MANIFEST_REQUEST
        ack.addProperty("envHash", current != null ? current.snapshotId() : "");
        ack.addProperty("envReady", current != null);
        ack.addProperty("worldName", worldName);
        appendRelayInfo(config, ack);
        ack.addProperty("minFreeMemoryBytes", config.minFreeMemoryBytes);
        // 指令路由能力位（加性字段）：客户端只有在 ACK 里看到它才启用"全量上链"，
        // 否则保持老行为（本地执行 + 失败才上送）—— 混版本时双向都有安全网
        ack.addProperty("commandRouting", 1);
        // 重连客户端此刻还没注册 PERMISSION_RELAY 处理器：名单随 ACK 一起给，
        // 保证"本地权限位"不会因为丢帧而停在旧值（周期兜底随后仍会幂等重发）
        if (permissions != null) {
            permissions.appendToAck(ack, clientId);
        }
        connection.send(message.reply(MessageType.HELLO_ACK, ack, null));

        // 重连的客户端可能带着空权限名单：按其所在组强制重推一次（未入组则等待角色指派）
        if (permissions != null) {
            permissions.pushForClient(clientId);
        }

        LOGGER.info("客户端握手成功: {} ({}) mode={} nat={} 来自 {}",
                playerName, clientId, mode, nat.type(), connection.remoteAddress());
    }

    /**
     * 填写 HELLO_ACK 的中转字段。与客户端的约定语义：
     * <ul>
     *   <li>relayAddress 非空 → 独立中转服务端地址（规范：服务端可以指定中转服务器的
     *       ipv4 地址来作为中转服务端）；</li>
     *   <li>relayAddress 空串且 relayPort &gt; 0 → 服务端自己兼任中转，客户端连接
     *       与控制连接相同的主机（服务端无法可靠得知自己的对外地址，留空由客户端复用
     *       已知可达的服务端 IP）；</li>
     *   <li>relayPort == 0 → 无中转可用（规范：服务端选择不作为中转时，打洞不成功则不中转）。</li>
     * </ul>
     * 包内静态：P2PBrokerHandlers 组装 P2P_USE_RELAY 时复用同一套端点口径。
     */
    static void appendRelayInfo(GlobalConfig config, JsonObject ack) {
        String relayAddress = "";
        int relayPort = config.relayPort;
        String cfgAddr = config.relayServerAddress == null ? "" : config.relayServerAddress.trim();
        if (!cfgAddr.isEmpty()) {
            // 配置支持 "ip:端口" 与纯 "ip" 两种写法；仅当恰有一个冒号时拆分端口，
            // 避免把 IPv6 地址（多冒号）错误截断
            int colon = cfgAddr.lastIndexOf(':');
            if (colon > 0 && cfgAddr.indexOf(':') == colon && colon < cfgAddr.length() - 1) {
                try {
                    relayPort = Integer.parseInt(cfgAddr.substring(colon + 1));
                    relayAddress = cfgAddr.substring(0, colon);
                } catch (NumberFormatException e) {
                    relayAddress = cfgAddr; // 冒号后不是数字：整串当作主机名，端口用默认
                }
            } else {
                relayAddress = cfgAddr;
            }
        } else if (!config.serverActsAsRelay) {
            relayPort = 0; // 既无独立中转又不兼任：显式告知客户端没有中转可用
        }
        ack.addProperty("relayAddress", relayAddress);
        ack.addProperty("relayPort", relayPort);
    }

    /** 拒绝握手：回 accepted=false 的 HELLO_ACK，随后关闭连接。 */
    private void reject(ControlConnection connection, ControlMessage message, String reason) {
        JsonObject ack = new JsonObject();
        ack.addProperty("accepted", false);
        ack.addProperty("reason", reason);
        connection.send(message.reply(MessageType.HELLO_ACK, ack, null));
        // 延迟关闭：send 是异步入队，立即 close 可能把拒绝帧冲掉，
        // 客户端会只看到"连接被重置"而拿不到 reason
        ThreadPools.scheduler().schedule(connection::close, 500, TimeUnit.MILLISECONDS);
        LOGGER.warn("拒绝客户端握手({}): {}", connection.remoteAddress(), reason);
    }

    // ------------------------------------------------------------ 静态查询（包内）

    /** 查询某客户端的在线控制连接；不在线返回 null（P2PBrokerHandlers 用）。 */
    static ControlConnection connectionOf(UUID clientId) {
        return ONLINE.get(clientId);
    }

    /** 查询某客户端 HELLO 上报的 NAT 信息；未知返回 {@link NatInfo#UNKNOWN}。 */
    static NatInfo natOf(UUID clientId) {
        return NAT_TABLE.getOrDefault(clientId, NatInfo.UNKNOWN);
    }

    /**
     * 仅当连接仍是该 UUID 当前会话时执行动作；检查与动作和新 HELLO 发布原子互斥。
     */
    public static boolean withCurrentSession(ControlConnection connection,
                                             BooleanSupplier action) {
        UUID clientId = connection != null ? connection.peerId() : null;
        if (clientId == null || action == null) {
            return false;
        }
        synchronized (sessionLock(clientId)) {
            if (ONLINE.get(clientId) != connection) {
                return false;
            }
            return action.getAsBoolean();
        }
    }

    /**
     * 断连清理：仅当映射仍指向该连接实例时才移除 ——
     * 客户端快速重连时新连接已覆盖映射，此时旧连接的断连回调不得误删新连接。
     */
    static void unregister(UUID clientId, ControlConnection connection) {
        unregisterIfCurrent(clientId, connection);
    }

    /** 原子摘除当前会话；连接已被替换时返回 false 且不修改新会话状态。 */
    static boolean unregisterIfCurrent(UUID clientId,
                                       ControlConnection connection) {
        return unregisterIfCurrent(clientId, connection, () -> {
        });
    }

    /**
     * 原子摘除当前会话，并在允许替换 HELLO 发布前完成断连状态转换。
     * transition 必须是短小、非阻塞的内存操作；尤其不能在这里等待需要
     * {@link #withCurrentSession(ControlConnection, BooleanSupplier)} 的任务。
     */
    static boolean unregisterIfCurrent(UUID clientId,
                                       ControlConnection connection,
                                       Runnable transition) {
        if (clientId == null) {
            return false;
        }
        synchronized (sessionLock(clientId)) {
            if (ONLINE.get(clientId) != connection) {
                return false;
            }
            ONLINE.remove(clientId);
            if (!ONLINE.containsKey(clientId)) {
                // 该客户端已完全离线才清 NAT 信息与 P2P 公钥指纹
                //（重连会随新 HELLO + P2PSessions.register 重新上报）
                NAT_TABLE.remove(clientId);
                P2PKeyGate.SERVER.forget(clientId);
                // 身份声明同理：连接已被新会话替换时不得误清（新 HELLO 刚写入的值）
                ConnectionIdentity.forget(clientId);
            }
            if (transition != null) {
                transition.run();
            }
            return true;
        }
    }

    /** 服务停止时清空全部静态状态，保证 stop() → start() 幂等重启不残留旧连接。 */
    static void clearAll() {
        ONLINE.clear();
        NAT_TABLE.clear();
        ConnectionIdentity.clearAll();
        // 撮合指纹表与待定撮合同属"按会话存活的静态身份数据"：服务停止一并清空，
        // 避免重启后拿着上一个进程的指纹撮合（服务已停，超时定时器也不会再触发）。
        P2PKeyGate.SERVER.clear();
    }

    private static Object sessionLock(UUID clientId) {
        return SESSION_LOCKS[(clientId.hashCode() & Integer.MAX_VALUE)
                % SESSION_LOCKS.length];
    }
}
