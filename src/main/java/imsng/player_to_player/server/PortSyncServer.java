package imsng.player_to_player.server;

import imsng.player_to_player.netproto.PortSyncPacket;
import net.fabricmc.fabric.api.networking.v1.PacketSender;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayNetworking;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.network.ServerGamePacketListenerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务端侧端口同步（MC 自定义通道 {@code player_to_player:server_ports}）。
 * <p>
 * <b>触发时机</b>：{@link ServerPlayConnectionEvents#JOIN}，即玩家完成登录、
 * 进入 play 状态时。端口包排在 JoinGame 之后入队，但 Fabric 的 legacy 接收器
 * 可直接在客户端 Netty 线程执行，因此不能假设客户端 render 线程已触发 JOIN。
 * 客户端在 play INIT 建立连接归属，并保留同连接在 JOIN 前收到的端口快照。
 * <p>
 * 这里<b>不</b>要求客户端先发送频道注册包：Fabric 的
 * {@code ServerPlayNetworking.send} 不检查 {@code canSend}，客户端按包 ID 直接
 * 在自己的接收器表里分发。这样即使客户端→服务端的 {@code minecraft:register}
 * 被代理过滤，端口同步仍然可用；旧客户端/无本模组的客户端会忽略未知频道并照常
 * 进入游戏。
 * <p>
 * 服务端另外登记了一个空的 C2S 接收器：频道清单（服务端→客户端的
 * {@code minecraft:register}）是客户端判断"服务端是否支持端口同步"的依据。
 * 旧版服务端不会声明该频道，客户端在清单到达时立即回退；若旧服务端完全
 * 没有 play 接收器（清单根本不发），客户端按基础超时兜底。
 * <p>
 * <b>线程模型</b>：事件在 MC 服务端主线程触发；发送只是入队，不阻塞。
 */
public final class PortSyncServer {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/server");

    /**
     * 当前已发布的两个端口；null = 服务未运行（Service 停止后清空，
     * 不再向任何新连接下发过期端口）。
     */
    private static volatile Ports published;

    /** 接收器/JOIN 事件只注册一次（模组生命周期内；Service 重启不重复注册）。 */
    private static boolean registered;

    private PortSyncServer() {
    }

    /**
     * 注册 MC 通道与 JOIN 回调（模组初始化时调用一次；幂等）。
     */
    public static synchronized void register() {
        if (registered) {
            return;
        }
        registered = true;
        // 只有 S→C 语义。这个空接收器的作用是让 Fabric 把频道写进服务端的
        // 频道清单，客户端据此判断服务端是否支持端口同步。
        ServerPlayNetworking.registerGlobalReceiver(PortSyncPacket.TYPE,
                (packet, player, sender) -> {
                    // 本通道只有 S→C 语义：客户端发来的任何内容都忽略。
                });
        ServerPlayConnectionEvents.JOIN.register(PortSyncServer::onPlayerJoin);
    }

    /** 玩家加入：把当前端口快照发给该玩家。 */
    private static void onPlayerJoin(ServerGamePacketListenerImpl handler,
                                     PacketSender sender,
                                     MinecraftServer server) {
        Ports ports = published;
        if (ports == null) {
            return; // P2PServerService 未运行（启动失败/已停止）：客户端回退本地配置
        }
        ServerPlayer player = handler.getPlayer();
        if (player == null) {
            return;
        }
        try {
            ServerPlayNetworking.send(player,
                    new PortSyncPacket(ports.controlPort(), ports.relayPort()));
            LOGGER.info("已向 {} 同步服务端端口: controlPort={}, relayPort={}",
                    player.getGameProfile().getName(),
                    ports.controlPort(), ports.relayPort());
        } catch (Exception e) {
            // 连接恰在 JOIN 与发送之间断开：只影响该玩家本次加入，不能打断主线程
            LOGGER.debug("端口同步发送失败（连接可能已断开）: {}",
                    player.getGameProfile().getName(), e);
        }
    }

    /**
     * 发布端口快照（Service 完成控制/中转端口绑定后调用）。
     *
     * @param controlPort 控制面实际绑定端口（&gt; 0）
     * @param relayPort   服务端本机中转端口；未兼任中转/使用外部中转时传 0
     */
    public static void publish(int controlPort, int relayPort) {
        published = new Ports(controlPort, relayPort);
    }

    /** 清空快照（Service 停止时调用；之后的新连接不再收到端口）。 */
    public static void clear() {
        published = null;
    }

    /**
     * 已发布的本机实际中转端口；未运行/未兼任中转返回 0。
     * <p>
     * {@code config.relayPort = 0}（系统分配临时端口）时，HELLO_ACK 与
     * P2P_USE_RELAY 仍需真实的绑定端口，故由 {@code HelloHandler.appendRelayInfo}
     * 用它补齐，保证与 MC 通道下发的端口完全同口径。
     */
    public static int publishedRelayPort() {
        Ports ports = published;
        return ports != null ? ports.relayPort() : 0;
    }

    /** 端口快照。 */
    public record Ports(int controlPort, int relayPort) {
    }
}
