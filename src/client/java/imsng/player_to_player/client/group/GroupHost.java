package imsng.player_to_player.client.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.GroupPublicationGate;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.LocalPermissions;
import imsng.player_to_player.group.ReplaceableRegistration;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.p2p.P2PSessions;
import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.p2p.TcpTunnel;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.client.Minecraft;
import net.minecraft.client.server.IntegratedServer;
import net.minecraft.server.MinecraftServer;
import net.minecraft.util.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 组宿主（主客户端，Phase 2/3）：集成服务端被接管后的对外接待面。
 * <ol>
 *   <li><b>LAN 发布</b>：{@code IntegratedServer.publishServer}（原版"对局域网开放"
 *       的同款入口，签名已 javap 核实）把集成服务端绑到本机随机 TCP 端口 ——
 *       副客户端的隧道最终桥接到这个端口；</li>
 *   <li><b>接待副客户端</b>：挂 {@link P2PSessions} 会话监听器，每当一条<b>入站</b>
 *       P2P 会话就绪（本端非发起方；发起方标记见 SessionListener Javadoc —— 本端
 *       主动发起的预同步会话由 MergeClient 认领，不得被接待面误抢），套
 *       {@link ReliableChannel} 读会话头：{@code op=tunnel_join} → 桥接到 LAN 端口；
 *       {@code op=presync}（Phase 3 合并接管方）→ 移交 {@link #presyncHandler}。</li>
 * </ol>
 * 会话监听器随 {@code P2PSessions.closeAll()}（世界会话拆除）一起清理；
 * LAN 发布随集成服务端停止自然失效。
 */
public final class GroupHost {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/group-host");

    /** 等待对端发来会话头的超时（毫秒）：超时视为死会话，关闭通道。 */
    private static final long HEADER_TIMEOUT_MILLIS = 15_000;

    /**
     * 同时在册的入站会话上限（含接待中与已建立隧道）。
     * <p>
     * 为什么需要硬上限：入站会话是<b>对端网络帧</b>驱动的（对端循环发 P2P_CONNECT_REQUEST
     * 即可让本端不断建立会话），而每条会话要占 2~5 条线程（接待 + 读会话头 + 隧道泵）。
     * 没有上限就等于把刚修掉的"线程无界 → native OOM"重新暴露给对端。
     * 组规模远小于此值；超过即拒绝并告警，属异常/攻击形态。
     */
    private static final int MAX_HOST_SESSIONS = 16;

    /** 入站会话名额（含隧道生命周期；隧道拆除时归还）。 */
    private static final java.util.concurrent.Semaphore HOST_SESSION_SLOTS =
            new java.util.concurrent.Semaphore(MAX_HOST_SESSIONS);

    /**
     * 跨服务器线程与客户端主线程的一次性发布门控。服务器启动回调只负责 arm，
     * 真正的 LAN 发布由客户端 tick 在本地玩家完成登录后消费。
     */
    private static final GroupPublicationGate<IntegratedServer> PUBLICATION_GATE =
            new GroupPublicationGate<>();

    /**
     * 固定的宿主入站会话监听器。使用固定引用后，主→副→主的角色切换可以显式注销
     * 旧监听器，避免同一条 P2P 传输被多个匿名监听器并发读取。
     */
    private static final P2PSessions.SessionListener HOST_SESSION_LISTENER =
            (sessionId, peerClientId, transport, initiator) -> {
                if (initiator) {
                    return;
                }
                // 名额上限：接待与隧道都要占线程，而对端可以不断发起会话，必须拒绝超限会话
                if (!HOST_SESSION_SLOTS.tryAcquire()) {
                    LOGGER.warn("入站 P2P 会话数已达上限 {}，拒绝会话 {}（对端 {}）——"
                                    + "防对端驱动的线程耗尽；组规模远小于该值，超过属实异常",
                            MAX_HOST_SESSIONS, sessionId, peerClientId);
                    transport.close();
                    return;
                }
                // 接待走专用小池（不是 io 池：接待最长阻塞 15 秒，放共享池会占满名额饿死
                // 区块上行；也不是每条会话一条线程：会话数由对端驱动）。
                try {
                    ThreadPools.hostSessionExecutor().execute(
                            () -> accept(sessionId, String.valueOf(peerClientId), transport));
                } catch (RuntimeException e) {
                    // 投递失败（执行器已关停/资源不足）必须归还名额，否则名额会永久泄漏，
                    // 泄漏满 MAX_HOST_SESSIONS 次后所有副客户端都再也进不来
                    HOST_SESSION_SLOTS.release();
                    transport.close();
                    LOGGER.warn("接待会话 {} 的任务投递失败，已关闭会话并归还名额: {}",
                            sessionId, e.toString());
                }
            };

    /** 宿主监听器的成对注册槽；reset 会可靠地从 P2PSessions 摘除当前实例。 */
    private static final ReplaceableRegistration<P2PSessions.SessionListener>
            HOST_LISTENER_REGISTRATION = new ReplaceableRegistration<>(
                    P2PSessions::addListener, P2PSessions::removeListener);

    /** LAN 端口（volatile：发布在主线程，隧道桥接在 io 线程读）。 */
    private static volatile int lanPort;

    /** 准备会话激活时重新检查，不能缓存停止前的端口。 */
    public static int currentPort() { return lanPort; }

    /**
     * 预同步会话处理器（Phase 3；MergeClient 挂接）：参数为会话头 JSON 与已就绪
     * 的可靠通道（所有权移交处理器）。null = 未挂接（op=presync 一律拒绝）。
     */
    @FunctionalInterface
    public interface PresyncHandler {
        void accept(JsonObject header, ReliableChannel channel, Runnable terminated);
    }
    private static volatile PresyncHandler presyncHandler;

    private GroupHost() {
    }

    /** 挂接预同步会话处理器（MergeClient 注册时调用；null 清除）。 */
    public static void setPresyncHandler(
            PresyncHandler handler) {
        presyncHandler = handler;
    }

    /**
     * 组宿主启动（{@link imsng.player_to_player.group.GroupRuntime#tryAttach} 的
     * 接管回调；服务器主线程，不得阻塞 —— LAN 发布转客户端主线程，接待转 io）。
     */
    public static void start(MinecraftServer server) {
        if (!(server instanceof IntegratedServer integrated)) {
            LOGGER.error("组宿主只能运行在集成服务端上（当前: {}）", server.getClass().getName());
            return;
        }

        // SERVER_STARTED 发生时，客户端玩家通常还没有完成本地登录。原版
        // publishServer 会先打开 TCP 监听，再访问 Minecraft.player 更新权限；此时
        // 直接调用会在端口已打开后抛 NPE，形成“显示局域网但模组不知道端口”的半发布状态。
        // 因此这里只登记实例，等待 onClientTick 观察到真实就绪条件后再发布。
        reset();
        // 必须先挂接入站监听器再开放发布门控。否则客户端 tick 可能抢先发布并发送
        // GROUP_WORLD_READY，服务端立即下发的副客户端会话会落在监听器尚未就绪的窗口内。
        HOST_LISTENER_REGISTRATION.replace(HOST_SESSION_LISTENER);
        PUBLICATION_GATE.arm(integrated);
        LOGGER.info("组宿主接待监听器已挂接，等待本地玩家与客户端连接就绪后发布 LAN");
    }

    /**
     * 客户端 tick 末尾的轻量就绪检查。
     *
     * <p>四个条件必须来自同一次 tick 快照：当前集成服务端仍是待发布实例、玩家对象
     * 已创建、客户端游戏连接已创建，并且 {@link GroupRuntime} 仍接管该服务器。
     * 门控消费后不会再次返回同一实例，因此本方法可每 tick 调用而不会重复发布。</p>
     */
    public static void onClientTick(Minecraft client) {
        IntegratedServer ready = PUBLICATION_GATE.takeIfReady(
                client.getSingleplayerServer(),
                client.player != null,
                client.getConnection() != null,
                GroupRuntime::isManagedServer);
        if (ready == null) {
            return;
        }
        publish(ready);
    }

    /**
     * 在客户端主线程发布集成服务端，并记录隧道实际使用的端口。
     *
     * <p>原版在 {@code publishServer} 内先写 {@code publishedPort}，后更新本地玩家权限。
     * 因而其他调用路径或旧异常路径可能留下“端口已监听但调用未正常返回”的状态。
     * 此时必须采用 {@link IntegratedServer#getPort()}，绝不能再开第二个监听器。</p>
     */
    private static void publish(IntegratedServer server) {
        if (!GroupRuntime.isManagedServer(server)) {
            LOGGER.info("集成服务端在 LAN 发布前已解除接管，本次发布取消");
            return;
        }

        int port;
        if (server.isPublished()) {
            port = server.getPort();
            LOGGER.warn("集成服务端已处于 LAN 发布状态，采用现有端口 {}，不重复监听", port);
        } else {
            int requestedPort = HttpUtil.getAvailablePort();
            try {
                // 游戏模式沿用世界默认；不开放作弊，组世界的权威演算不应被本地作弊指令干预。
                if (!server.publishServer(server.getDefaultGameType(), false, requestedPort)) {
                    LOGGER.error("集成服务端 LAN 发布失败（请求端口 {}），副客户端将无法加入",
                            requestedPort);
                    return;
                }
            } catch (RuntimeException e) {
                // publishServer 不是原子操作：异常可能发生在 TCP 已监听且 publishedPort
                // 已写入之后。若原版状态确认已发布，端口仍然可用，应继续完成模组侧登记。
                if (!server.isPublished()) {
                    LOGGER.error("集成服务端 LAN 发布异常，且未形成可用监听", e);
                    return;
                }
                LOGGER.warn("集成服务端 LAN 发布调用中途异常，但监听已建立；继续采用现有端口",
                        e);
            }
            port = server.getPort();
        }

        if (port <= 0) {
            LOGGER.error("集成服务端报告已发布但端口无效（{}），拒绝通告组世界就绪", port);
            return;
        }
        if (!GroupRuntime.isManagedServer(server)) {
            LOGGER.info("集成服务端在 LAN 发布完成时已解除接管，不再通告组世界就绪");
            return;
        }

        lanPort = port;
        // 组世界可用后立刻按服务端下发的 ops 名单落权：本地权限位决定 F3+F4 的游戏模式
        // 切换、指令补全树与未被拦截的执行路径（告示牌/命令方块/第三方程序化调用）。
        // publishServer 内部已按"未开作弊"把本地玩家权限置 0，这里按真实等级纠正。
        LocalPermissions.refreshScheduled(server);
        LOGGER.info("集成服务端已发布 LAN 端口 {}，等待副客户端经隧道加入", port);
        notifyGroupWorldReady();
    }

    /**
     * 向物理服务端通告本组 LAN 接待面已经可用。
     * 普通开组没有暂存重定向时，服务端把该消息作为空操作处理。
     */
    private static void notifyGroupWorldReady() {
        ControlConnection conn = GroupRuntime.conn();
        UUID groupId = GroupRuntime.activeGroupId();
        if (conn == null || !conn.isOpen() || groupId == null) {
            LOGGER.warn("LAN 已发布，但控制连接或组标识不可用，无法通告组世界就绪");
            return;
        }
        JsonObject ready = new JsonObject();
        ready.addProperty("groupId", groupId.toString());
        conn.send(ControlMessage.of(MessageType.GROUP_WORLD_READY, ready));
    }

    /**
     * 清除待发布实例和隧道端口登记；世界会话拆除及新宿主启动前均可幂等调用。
     * 实际 TCP 监听仍由集成服务端自身在停服时关闭。
     */
    public static void reset() {
        PUBLICATION_GATE.reset();
        // 先关闭端口可见性，再摘监听器；已提交到 IO 线程的旧回调也会读到 0 并拒绝桥接。
        lanPort = 0;
        HOST_LISTENER_REGISTRATION.clear();
        // reset 同时用于宿主启动/角色切换，不能清掉比集成服更早到达的权威名单。
        // 真正离开物理服务端的全量清理由 WorldSession 负责。
        LocalPermissions.resetRuntime();
    }

    /** 接待一条新入站 P2P 会话（专用接待池线程，可阻塞）。名额在入口处已取得。 */
    private static void accept(String sessionId, String peer,
                               imsng.player_to_player.p2p.P2PTransport transport) {
        ReliableChannel channel = new ReliableChannel(transport, "host:" + sessionId);
        // 隧道路径把名额归还交给"隧道终止回调"（隧道还活着就继续占名额）；
        // 其余路径在本方法返回时归还。二者只能有一个，否则上限形同虚设。
        boolean tunnelHandedOff = false;
        try {
            String headerJson = readHeaderWithTimeout(channel);
            JsonObject header = JsonUtil.parseObject(headerJson);
            String op = JsonUtil.getString(header, "op", "");
            switch (op) {
                case "tunnel_prepare" -> {
                    // 等待激活的长驻读取不能占接待池；名额仍随准备/隧道生命周期保留。
                    ThreadPools.startBlocking("tunnel-prepared", () ->
                            imsng.player_to_player.p2p.PreparedTunnel.accept(channel, header,
                                    () -> lanPort, HOST_SESSION_SLOTS::release));
                    tunnelHandedOff = true;
                }
                case "tunnel_join" -> {
                    int port = lanPort;
                    if (port <= 0) {
                        LOGGER.warn("LAN 端口尚未发布，无法接待副客户端 {}（会话 {} 关闭）",
                                peer, sessionId);
                        channel.close();
                        return;
                    }
                    LOGGER.info("副客户端 {} 经会话 {} 请求加入，桥接到 LAN 端口 {}",
                            JsonUtil.getString(header, "playerName", peer), sessionId, port);
                    // 名额随隧道生命周期持有：隧道拆除时归还（见 TcpTunnel 的终止回调）
                    TcpTunnel.bridgeToLocalPort(channel, port, "host:" + sessionId,
                            HOST_SESSION_SLOTS::release);
                    tunnelHandedOff = true;
                }
                case "presync" -> {
                    // Phase 3 合并：让出方 A 主动连到接管方 B 推送预同步流
                    var handler = presyncHandler;
                    if (handler == null) {
                        LOGGER.warn("收到预同步会话 {} 但本端无合并进行中，关闭", sessionId);
                        channel.close();
                        return;
                    }
                    handler.accept(header, channel, HOST_SESSION_SLOTS::release);
                    // 原主的预同步通道随后成为游戏隧道，名额覆盖完整生命周期。
                    tunnelHandedOff = true;
                }
                default -> {
                    LOGGER.warn("会话 {} 的用途未知（op={}），关闭", sessionId, op);
                    channel.close();
                }
            }
        } catch (Exception e) {
            LOGGER.warn("接待会话 {} 失败: {}", sessionId, e.toString());
            channel.close();
        } finally {
            if (!tunnelHandedOff) {
                HOST_SESSION_SLOTS.release();
            }
        }
    }

    /** 读会话头（带总超时：inputStream 无数据时最多阻塞到超时）。 */
    private static String readHeaderWithTimeout(ReliableChannel channel) throws Exception {
        // TcpTunnel.readHeader 阻塞在可靠通道输入流上；用 future 限时兜底 ——
        // 死会话（对端建完连接即消失）不该占住线程与通道资源。
        // 读取任务走专用线程而不是 io 池：超时兜底依赖"这个任务真的被调度到"，
        // 排在有界池队列尾部会让 15 秒超时变成常态失败。
        var future = new java.util.concurrent.CompletableFuture<String>();
        ThreadPools.startBlocking("host-header", () -> {
            try {
                future.complete(TcpTunnel.readHeader(channel));
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        try {
            return future.get(HEADER_TIMEOUT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            channel.close(); // 关通道使阻塞中的 readHeader 线程解除阻塞退出
            throw new java.io.IOException("等待会话头超时");
        }
    }
}
