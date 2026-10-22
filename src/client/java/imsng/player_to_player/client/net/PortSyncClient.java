package imsng.player_to_player.client.net;

import imsng.player_to_player.netproto.ConnectionPortSyncState;
import imsng.player_to_player.netproto.PortSyncPacket;
import net.fabricmc.fabric.api.client.networking.v1.C2SPlayChannelEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayConnectionEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayNetworking;
import net.minecraft.client.multiplayer.ClientPacketListener;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.ResourceLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 客户端端口同步：先按 play 监听器身份接收数据，再供物理服加入流程等待。
 * <p>
 * Fabric 1.20.4 的 legacy 通道回调在 Netty 线程执行，服务端把端口包排在登录包后，
 * 并不代表 render 线程已经处理完登录包。INIT 时建立连接状态，JOIN 时必须保留
 * 同一连接已经收到的快照；否则早包被丢弃，后续能力事件又让客户端白等十秒。
 * <p>
 * 状态机同时隔离快速切服时的旧包、旧频道事件和旧 DISCONNECT。快照只属于一条
 * play 连接，不能跨服复用；真正的物理服会话端口仍由 WorldSession 自行保存。
 */
public final class PortSyncClient {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/client_ports");
    private static final long BASE_WAIT_MILLIS = 1500L;
    private static final long SUPPORTED_WAIT_MILLIS = 10000L;
    private static final ConnectionPortSyncState STATE = new ConnectionPortSyncState();

    /** 物理服加入工作线程的票据；组世界 INIT 只取消旧等待，不冒充新的物理服加入。 */
    private static volatile ConnectionPortSyncState.Join pendingJoin;
    private static boolean registered;

    private PortSyncClient() {
    }

    /** 模组初始化时注册一次；INIT 必须早于网络线程收到 play 自定义包。 */
    public static synchronized void register() {
        if (registered) {
            return;
        }
        registered = true;
        ClientPlayConnectionEvents.INIT.register((handler, client) -> STATE.beginConnection(handler));
        ClientPlayConnectionEvents.DISCONNECT.register((handler, client) -> STATE.disconnect(handler));
        ClientPlayConnectionEvents.JOIN.register((handler, sender, client) -> {
            // Fabric 可能在 play 监听器构造阶段就消费配置阶段积累的频道清单，早于 INIT。
            // JOIN 后从完整集合补回能力；连接身份始终用事件参数，不在 INIT 查询全局玩家。
            if (ClientPlayNetworking.canSend(PortSyncPacket.ID)) {
                STATE.advertise(handler, true);
            }
        });
        // 保留携带 handler 的 legacy API，用真实来源连接做隔离；解析和状态更新都线程安全。
        ClientPlayNetworking.registerGlobalReceiver(PortSyncPacket.ID,
                (client, handler, buf, sender) -> onPorts(handler, buf));
        C2SPlayChannelEvents.REGISTER.register(
                (handler, sender, client, channels) -> onServerChannels(handler, channels));
    }

    private static void onPorts(ClientPacketListener handler, FriendlyByteBuf buf) {
        PortSyncPacket packet;
        try {
            packet = PortSyncPacket.TYPE.read(buf);
        } catch (RuntimeException error) {
            LOGGER.warn("丢弃无法解码的端口同步包: {}", error.toString());
            return;
        }
        int control = packet.controlPort();
        int relay = packet.relayPort();
        if (control < 1 || control > 65535 || relay < 0 || relay > 65535) {
            LOGGER.warn("丢弃服务端端口同步（端口非法）: controlPort={}, relayPort={}", control, relay);
            return;
        }
        if (STATE.receive(handler, control, relay)) {
            LOGGER.info("收到服务端端口同步: controlPort={}, relayPort={}", control, relay);
        } else {
            LOGGER.debug("忽略已过期连接的端口同步包");
        }
    }

    private static void onServerChannels(ClientPacketListener handler, List<ResourceLocation> channels) {
        STATE.advertise(handler, channels.contains(PortSyncPacket.ID));
    }

    /** 客户端主线程在物理服 JOIN 时调用，不重置该连接在 INIT 后已收到的数据。 */
    public static void beginJoin() {
        pendingJoin = STATE.beginJoin();
    }

    /** 仅阻塞加入工作线程；超时、断开、换连接和中断均由调用方回退或取消会话。 */
    public static Ports awaitForJoin() {
        ConnectionPortSyncState.Ports ports = STATE.await(pendingJoin, BASE_WAIT_MILLIS, SUPPORTED_WAIT_MILLIS);
        return ports == null ? null : new Ports(ports.controlPort(), ports.relayPort());
    }

    public record Ports(int controlPort, int relayPort) {
    }
}
