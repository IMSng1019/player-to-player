package imsng.player_to_player.client.net;

import imsng.player_to_player.netproto.PortSyncPacket;
import net.fabricmc.fabric.api.client.networking.v1.C2SPlayChannelEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayNetworking;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientPacketListener;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.ResourceLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 客户端侧端口同步（接收 {@link PortSyncPacket}，供 {@code WorldSession} 在
 * 建立控制连接前查询）。
 * <p>
 * <b>为什么需要等待</b>：客户端加入物理服时，控制面端口尚未知；服务端在 JOIN
 * 时主动推送端口包，但包在网络上晚于本端 JOIN 到达。因此 {@code WorldSession}
 * 的 join-flow（阻塞线程）必须在这里短暂等待，而 {@code onJoin} 本身仍立即返回，
 * 不阻塞渲染。
 * <p>
 * <b>代数语义</b>：每次加入物理服调用 {@link #beginJoin()} 开启新一代；只有本代
 * 收到的快照才会被 {@link #awaitForJoin()} 接受。服务端端口在两次加入之间可能被
 * 管理员改掉，绝不能把上一次连接的端口当成这一次的既成事实。集成服务端的 JOIN
 * （主客户端进入组世界 / 副客户端经隧道加入）不开启新代，因此不会影响仍在持续的
 * 物理服会话 —— 耐久退出重连使用会话层自己记录的端口。
 * <p>
 * <b>快速回退</b>：服务端在频道清单里声明了本通道 = 支持端口同步，等待上限放宽；
 * 清单明确没有本通道时立即返回 null。清单本身没到达（旧版服务端没有任何 play
 * 接收器 / 非 Fabric 服务端）时按基础超时兜底。无论哪种失败，调用方都回退本地
 * {@code controlPort}。
 * <p>
 * 线程模型：包处理器与频道事件都在 MC 客户端线程；{@link #awaitForJoin()} 在
 * join-flow 阻塞线程。全部状态由 {@link #LOCK} 保护。
 */
public final class PortSyncClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/client_ports");

    /**
     * 服务端未明确声明支持时的等待上限：覆盖端口包从服务端 JOIN 到本端的网络延迟。
     * 超过即按"旧版服务端"处理，回退本地配置。
     * <p>
     * 旧版服务端可能<b>完全不发</b> play 频道清单（Fabric 侧没有任何 play 接收器时
     * 注册包为空），此时只能靠这个基础超时兜底；取值兼顾慢速链路的正常往返。
     */
    private static final long BASE_WAIT_MILLIS = 1500L;

    /**
     * 服务端已声明支持时的等待上限：慢速链路（RTT 可达数秒）也必须给足时间，
     * 否则会误用本地配置端口连一个已改过端口的服务端。
     */
    private static final long SUPPORTED_WAIT_MILLIS = 10000L;

    private static final Object LOCK = new Object();

    /** 加入代数：beginJoin 自增；包快照按收到时的代数打标。 */
    private static long joinGeneration;

    /** 收到快照时的代数；-1 = 尚无可用于当前代的快照。 */
    private static long appliedGeneration = -1;

    private static int controlPort = -1;
    private static int relayPort = -1;

    /** 服务端是否声明可接收本通道；null = 尚未收到服务端的频道清单。 */
    private static Boolean serverAdvertised;

    /**
     * 本次加入的 play 连接。端口包/频道事件只认这条连接：快速切服或编排性断开时，
     * 旧连接排队中的任务可能晚于新加入执行，若不设防会把旧服务器的端口写进新会话。
     * 端口包用带 {@link ClientPacketListener} 参数的频道处理器接收，才能拿到
     * "包来自哪条连接"（类型化处理器只给 player，无法区分旧连接的迟到任务）。
     */
    private static ClientPacketListener joinConnection;

    /** 模组生命周期内只注册一次。 */
    private static boolean registered;

    private PortSyncClient() {
    }

    /** 注册接收器与频道能力事件（客户端初始化时调用一次）。 */
    public static synchronized void register() {
        if (registered) {
            return;
        }
        registered = true;
        // 用带 handler 参数的频道处理器（而非类型化处理器）：只有它能拿到
        // "包来自哪条 play 连接"，用于丢弃旧连接排队中的迟到包。
        ClientPlayNetworking.registerGlobalReceiver(PortSyncPacket.ID,
                (client, handler, buf, sender) -> onPorts(handler, buf));
        // 服务端的频道清单到达：判断它是否声明了本通道（服务端注册了空接收器）。
        C2SPlayChannelEvents.REGISTER.register(
                (handler, sender, client, channels) -> onServerChannels(handler, channels));
    }

    private static void onPorts(ClientPacketListener handler, FriendlyByteBuf buf) {
        PortSyncPacket packet;
        try {
            packet = PortSyncPacket.TYPE.read(buf);
        } catch (RuntimeException e) {
            LOGGER.warn("丢弃无法解码的端口同步包: {}", e.toString());
            return;
        }
        int control = packet.controlPort();
        int relay = packet.relayPort();
        if (control < 1 || control > 65535 || relay < 0 || relay > 65535) {
            // 入站数据不可信：非法端口直接丢弃，等待方按超时回退本地配置
            LOGGER.warn("丢弃服务端端口同步（端口非法）: controlPort={}, relayPort={}",
                    control, relay);
            return;
        }
        synchronized (LOCK) {
            if (joinConnection == null || handler != joinConnection) {
                return; // 旧连接/集成组世界的迟到包：不属于本次物理服加入
            }
            controlPort = control;
            relayPort = relay;
            appliedGeneration = joinGeneration;
            LOCK.notifyAll();
        }
        LOGGER.info("收到服务端端口同步: controlPort={}, relayPort={}", control, relay);
    }

    private static void onServerChannels(
            ClientPacketListener handler,
            List<ResourceLocation> channels) {
        boolean advertised = channels.contains(PortSyncPacket.ID);
        synchronized (LOCK) {
            if (joinConnection == null || handler != joinConnection) {
                return; // 旧连接/集成组世界的频道清单
            }
            if (advertised) {
                serverAdvertised = true;
            } else if (serverAdvertised == null) {
                // 该事件的列表可能是"增量"（动态注册只带新增频道），不能被后续
                // 增量事件覆盖成 false。首次事件通常是加入时的完整清单：没有本通道
                // 就按旧服务端处理，快速回退；之后只接受"包含本通道"的更新。
                serverAdvertised = false;
            }
            LOCK.notifyAll();
        }
        LOGGER.debug("服务端频道清单: 支持端口同步={}", advertised);
    }

    /**
     * 开启新一代加入（每次加入物理服、在 join-flow 启动前调用）。
     * <p>
     * 清空上一代的端口与能力判断：同一次客户端进程可能先后连不同服务器，
     * 或重连到端口已变更的同一服务器。
     */
    public static void beginJoin() {
        synchronized (LOCK) {
            joinGeneration++;
            appliedGeneration = -1;
            controlPort = -1;
            relayPort = -1;
            serverAdvertised = null;
            // 必须在客户端主线程调用（WorldSession.onJoin）：此刻玩家已登录完成，
            // getConnection() 返回本次加入的 play 监听器。
            joinConnection = Minecraft.getInstance().getConnection();
        }
    }

    /**
     * 等待当前这一代加入的端口快照。
     *
     * @return 端口快照；超时 / 服务端明确不支持 / 线程被中断时返回 null
     *         （调用方回退本地 {@code config.controlPort}）
     */
    public static Ports awaitForJoin() {
        long startNanos = System.nanoTime();
        synchronized (LOCK) {
            long generation = joinGeneration;
            long deadline = startNanos + TimeUnit.MILLISECONDS.toNanos(BASE_WAIT_MILLIS);
            boolean supportedDeadlineApplied = false;
            while (true) {
                if (appliedGeneration == generation && controlPort > 0) {
                    return new Ports(controlPort, relayPort);
                }
                if (Boolean.FALSE.equals(serverAdvertised)) {
                    return null; // 服务端明确没有本通道：不必等基础超时
                }
                if (!supportedDeadlineApplied && Boolean.TRUE.equals(serverAdvertised)) {
                    // 已确认服务端支持：把截止时间放宽到慢速链路上限（仍以本次加入为起点）
                    deadline = Math.max(deadline, startNanos
                            + TimeUnit.MILLISECONDS.toNanos(SUPPORTED_WAIT_MILLIS));
                    supportedDeadlineApplied = true;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                try {
                    // 轮询兜底（事件丢失时也能按时退出），正常由 notifyAll 立即唤醒
                    LOCK.wait(Math.max(1L, Math.min(50L,
                            TimeUnit.NANOSECONDS.toMillis(remaining))));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
    }

    /** 端口快照。 */
    public record Ports(int controlPort, int relayPort) {
    }
}
