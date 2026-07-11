package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.env.EnvironmentManifest;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageHandler;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.p2p.NatInfo;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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

    private final GlobalConfig config;
    private final ComputeTable computeTable;
    /** 环境清单提供者：启动时由 IO 线程异步扫描填充，未完成时 get() 返回 null。 */
    private final Supplier<EnvironmentManifest> manifest;
    /** 服务端世界名（HELLO_ACK 下发，客户端据此确定 <IP>+<世界名> 世界文件夹）。 */
    private final String worldName;

    public HelloHandler(GlobalConfig config, ComputeTable computeTable,
                        Supplier<EnvironmentManifest> manifest, String worldName) {
        this.config = config;
        this.computeTable = computeTable;
        this.manifest = manifest;
        this.worldName = worldName;
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

        connection.setPeerId(clientId);

        // 同一 clientId 重复握手视为"客户端重连"：登记新连接并关掉旧连接，
        // 避免旧的半死连接占用映射导致打洞消息投递到死连接上
        ControlConnection previous = ONLINE.put(clientId, connection);
        if (previous != null && previous != connection && previous.isOpen()) {
            LOGGER.warn("客户端 {} 重复握手，关闭旧连接 {}", clientId, previous.remoteAddress());
            previous.close();
        }

        // 3. 算力与 NAT：字段缺失容忍（旧版本客户端可能不带），有则登记
        if (json.has("compute") && json.get("compute").isJsonObject()) {
            computeTable.report(clientId, ComputeScore.fromJson(json.getAsJsonObject("compute")));
        }
        NatInfo nat = json.has("nat") && json.get("nat").isJsonObject()
                ? NatInfo.fromJson(json.getAsJsonObject("nat"))
                : NatInfo.UNKNOWN;
        NAT_TABLE.put(clientId, nat);

        // 4. 应答 HELLO_ACK（reply 复制 _rid，使客户端 request() 的 future 完成）
        EnvironmentManifest current = manifest.get();
        JsonObject ack = new JsonObject();
        ack.addProperty("accepted", true);
        // 环境扫描未完成时 envHash 为空串 + envReady=false：客户端应等待后重发 ENV_MANIFEST_REQUEST
        ack.addProperty("envHash", current != null ? current.globalHash() : "");
        ack.addProperty("envReady", current != null);
        ack.addProperty("worldName", worldName);
        appendRelayInfo(config, ack);
        ack.addProperty("minFreeMemoryBytes", config.minFreeMemoryBytes);
        connection.send(message.reply(MessageType.HELLO_ACK, ack, null));

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
     * 断连清理：仅当映射仍指向该连接实例时才移除 ——
     * 客户端快速重连时新连接已覆盖映射，此时旧连接的断连回调不得误删新连接。
     */
    static void unregister(UUID clientId, ControlConnection connection) {
        ONLINE.remove(clientId, connection);
        if (!ONLINE.containsKey(clientId)) {
            // 该客户端已完全离线才清 NAT 信息（重连会随新 HELLO 重新上报）
            NAT_TABLE.remove(clientId);
        }
    }

    /** 服务停止时清空全部静态状态，保证 stop() → start() 幂等重启不残留旧连接。 */
    static void clearAll() {
        ONLINE.clear();
        NAT_TABLE.clear();
    }
}
