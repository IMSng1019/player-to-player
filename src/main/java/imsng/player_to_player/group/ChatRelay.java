package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import net.fabricmc.fabric.api.message.v1.ServerMessageEvents;
import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 聊天全网分发（主客户端侧，Phase 4；规范"服务器的信息处理：服务器和客户端的
 * 消息需要让所有客户端看到"）。
 * <p>
 * 链路（与指令同链，见 {@code server.CommandChatHandlers}）：
 * <pre>
 *   副客户端聊天 ──隧道(原版)──→ 主客户端集成服务端
 *     └─ CHAT_MESSAGE 事件（本类捕获）──CHAT_RELAY──→ 物理服务端
 *          └─ 广播给其他组的主客户端 ──本类入站──→ 各组集成服务端系统消息广播
 *               └─ 原版链路 ──→ 各组副客户端
 * </pre>
 * <b>工程取舍</b>：跨组重放用系统消息（{@code <名字> 内容} 格式），不是签名聊天 ——
 * 1.19+ 的聊天签名与玩家会话绑定，跨服务端无法复现签名（所有跨服聊天桥的通行
 * 做法）；显示效果与原版聊天一致。回环防护：系统消息不会再触发 CHAT_MESSAGE
 * 事件（该事件只对玩家签名聊天发火），链路天然无环。
 * <p>
 * 线程模型：出站在服务器主线程组装 JSON、Netty 异步发送（微秒级不阻塞 tick）；
 * 入站在 Netty 线程收帧，广播转服务器主线程执行。
 */
public final class ChatRelay {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/chat");

    /** Fabric 事件全局只注册一次（事件体内按接管状态门控）。 */
    private static volatile boolean hooked;

    private ChatRelay() {
    }

    /**
     * 安装聊天捕获钩子（模组初始化时调用一次）。
     * 只捕获被接管集成服务端上的玩家聊天；物理服务端/单人游戏零行为差异。
     */
    public static void installHooks() {
        if (hooked) {
            return;
        }
        hooked = true;
        ServerMessageEvents.CHAT_MESSAGE.register((message, sender, params) -> {
            if (!GroupRuntime.isManagedLevel(sender.serverLevel())) {
                return;
            }
            ControlConnection conn = GroupRuntime.conn();
            if (conn == null || !conn.isOpen()) {
                return;
            }
            // signedContent = 玩家输入的原文（不带 <名字> 装饰，接收端统一格式化）
            JsonObject json = new JsonObject();
            json.addProperty("playerUuid", sender.getUUID().toString());
            json.addProperty("playerName", sender.getGameProfile().getName());
            json.addProperty("text", message.signedContent());
            conn.send(ControlMessage.of(MessageType.CHAT_RELAY, json));
        });
        LOGGER.info("跨组聊天捕获钩子已注册");
    }

    /**
     * 注册入站 CHAT_RELAY 处理器（世界会话建立时挂到控制连接上）。
     * 两种载荷：privateTo 非空 = 跨组私聊（只投递给目标玩家）；否则组内广播。
     */
    public static void register(HandlerRegistry reg) {
        reg.on(MessageType.CHAT_RELAY, (conn, msg) -> {
            MinecraftServer server = GroupRuntime.server();
            if (server == null) {
                return; // 本端当前没有被接管的集成服务端（如刚切换世界的空窗）
            }
            String name = JsonUtil.getString(msg.json(), "playerName", "?");
            String text = JsonUtil.getString(msg.json(), "text", "");
            if (text.isBlank() || text.length() > 1024) {
                return; // 入站不可信：空/超长丢弃
            }
            String privateTo = JsonUtil.getString(msg.json(), "privateTo", "");
            server.execute(() -> {
                if (privateTo.isEmpty()) {
                    // 跨组公屏：与原版聊天同格式的系统消息，组内全员（含副客户端）可见
                    server.getPlayerList().broadcastSystemMessage(
                            Component.literal("<" + name + "> " + text), false);
                    return;
                }
                // 跨组私聊（/msg 的投递半程）：只给目标玩家
                try {
                    ServerPlayer target =
                            server.getPlayerList().getPlayer(UUID.fromString(privateTo));
                    if (target != null) {
                        target.sendSystemMessage(
                                Component.literal(name + " 悄悄地对你说: " + text));
                    }
                } catch (IllegalArgumentException ignored) {
                    // privateTo 非法 UUID：丢弃（入站不可信）
                }
            });
        });
    }
}
