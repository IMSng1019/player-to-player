package imsng.player_to_player.proxy;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.ControlServer;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 中转核心（relay）：P2P 打洞失败时的降级数据通路（DESIGN.md 第 7 节"失败降级"）。
 * <p>
 * 规范"中转服务端"：辅助 p2p 打洞，若打洞不成功则充当中转。本类是纯转发面 ——
 * 内部起一个独立的 {@link ControlServer}（默认端口 25581），只认两种消息：
 * <ul>
 *   <li>{@code RELAY_REGISTER}（json: clientId）：客户端在中转端登记自己，
 *       建立 clientId → 连接 的可寻址映射；</li>
 *   <li>{@code RELAY_FORWARD}（json: targetClientId, fromClientId, sessionId；
 *       binary: 载荷原文）：查目标映射，<b>填入 fromClientId</b>（以中转端观测的
 *       登记身份为准，客户端自报的该字段不可信）后原样转发；
 *       目标不在线回 ERROR。载荷对中转端完全不透明（P2P 加密通道的密文），
 *       中转端不解析也无法解析。</li>
 * </ul>
 * <p>
 * <b>两种部署形态复用</b>（规范要求）：
 * ① {@code proxy_server} 模式 —— {@link ProxyServerService} 独立进程运行本类；
 * ② 服务端兼任中转 —— {@code server.P2PServerService} 在
 * {@code serverActsAsRelay=true} 且未指定外部中转地址时并列启动本类。
 * 两种形态行为完全一致，客户端无感知。
 * <p>
 * 线程模型：全部逻辑在 Netty 事件循环上完成（查表 + 转发，零阻塞、零拷贝载荷）。
 */
public final class RelayCore {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/relay");

    private final int port;
    /** clientId → 中转登记连接。转发目的地查询表，断连即清。 */
    private final Map<UUID, ControlConnection> registered = new ConcurrentHashMap<>();

    /** 内部控制服务器；start() 时创建，stop() 后置 null，保证幂等重启。 */
    private volatile ControlServer server;

    /**
     * 额外处理器挂接钩子（start 前设置，Phase 2 中转端环境分发用）：
     * 中转端把 EnvSyncServerHandlers 挂到同一个 ControlServer 上 ——
     * 客户端经 RELAY_REGISTER 登记身份（过未鉴权白名单门）后即可在
     * 同一连接上发起 ENV_MANIFEST_REQUEST / ENV_FILE_REQUEST。
     */
    private volatile java.util.function.Consumer<ControlServer> extraHandlers;

    /**
     * @param port 中转监听端口（{@code GlobalConfig.relayPort}，默认 25581）
     */
    public RelayCore(int port) {
        this.port = port;
    }

    /** 设置额外处理器挂接钩子（必须在 {@link #start} 之前调用才生效）。 */
    public void setExtraHandlers(java.util.function.Consumer<ControlServer> hook) {
        this.extraHandlers = hook;
    }

    /** 启动中转监听（幂等：已启动则忽略）。 */
    public synchronized void start() {
        if (server != null) {
            return;
        }
        ControlServer relay = new ControlServer(port);
        relay.on(MessageType.RELAY_REGISTER, this::handleRegister);
        relay.on(MessageType.RELAY_FORWARD, this::handleForward);
        // 额外处理器（Phase 2：中转端环境分发把 ENV_* 处理器挂进来）
        java.util.function.Consumer<ControlServer> hook = extraHandlers;
        if (hook != null) {
            hook.accept(relay);
        }
        // 断连清映射：仅当映射仍指向该连接时移除（防快速重连后误删新连接）
        relay.onDisconnect(conn -> {
            UUID id = conn.peerId();
            if (id != null && registered.remove(id, conn)) {
                LOGGER.info("中转客户端离线: {}", id);
            }
        });
        relay.start();
        server = relay;
        LOGGER.info("中转服务已启动，端口 {}", port);
    }

    /** 停止中转监听并清空登记表（幂等：未启动/已停止则忽略）。 */
    public synchronized void stop() {
        ControlServer relay = server;
        server = null;
        registered.clear();
        if (relay != null) {
            relay.stop();
            LOGGER.info("中转服务已停止");
        }
    }

    /** RELAY_REGISTER：登记 clientId → 连接，使该客户端可被其他端经中转寻址。 */
    private void handleRegister(ControlConnection connection, ControlMessage message) {
        UUID clientId;
        try {
            clientId = UUID.fromString(JsonUtil.getString(message.json(), "clientId", ""));
        } catch (IllegalArgumentException e) {
            sendError(connection, message, "clientId 非法");
            return;
        }
        // peerId 记在连接上：断连回调据此定位映射项
        connection.setPeerId(clientId);
        ControlConnection previous = registered.put(clientId, connection);
        if (previous != null && previous != connection && previous.isOpen()) {
            // 重复登记视为重连，关闭旧连接防止转发落到死连接上
            LOGGER.warn("中转客户端 {} 重复登记，关闭旧连接", clientId);
            previous.close();
        }
        LOGGER.info("中转客户端登记: {} 来自 {}", clientId, connection.remoteAddress());
    }

    /** RELAY_FORWARD：填 fromClientId 后把帧原样转发给目标客户端。 */
    private void handleForward(ControlConnection connection, ControlMessage message) {
        UUID fromId = connection.peerId();
        if (fromId == null) {
            // 未登记就转发：拒绝（无法告知接收方来源身份，也无法防伪造）
            sendError(connection, message, "转发前须先 RELAY_REGISTER");
            return;
        }
        UUID targetId;
        try {
            targetId = UUID.fromString(JsonUtil.getString(message.json(), "targetClientId", ""));
        } catch (IllegalArgumentException e) {
            sendError(connection, message, "targetClientId 非法");
            return;
        }
        ControlConnection target = registered.get(targetId);
        if (target == null || !target.isOpen()) {
            sendError(connection, message, "目标客户端未在中转端登记或已离线: " + targetId);
            return;
        }

        // 重建 JSON：fromClientId 以登记身份覆盖（发送方自报值不可信）；
        // 二进制载荷零拷贝原样透传（中转端不理解也不需要理解密文内容）
        JsonObject out = message.json().deepCopy();
        out.remove(Protocol.RID_FIELD); // _rid 是发送方与中转端的关联号，不得泄漏给目标端造成错误匹配
        out.addProperty("fromClientId", fromId.toString());
        target.send(ControlMessage.of(MessageType.RELAY_FORWARD, out, message.binary()));
    }

    /** 统一 ERROR 应答。 */
    private static void sendError(ControlConnection connection, ControlMessage message, String reason) {
        JsonObject json = new JsonObject();
        json.addProperty("code", "RELAY");
        json.addProperty("message", reason);
        connection.send(message.reply(MessageType.ERROR, json, null));
        LOGGER.warn("中转失败({}): {}", connection.remoteAddress(), reason);
    }
}
