package imsng.player_to_player.client.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.MergeTriggers;
import imsng.player_to_player.group.PresyncReceiver;
import imsng.player_to_player.group.PresyncSender;
import imsng.player_to_player.group.PresyncStore;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.p2p.P2PSessions;
import imsng.player_to_player.p2p.P2PTransport;
import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.p2p.TcpTunnel;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.client.Minecraft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 合并客户端（Phase 3，主客户端侧的合并编排；规范"预连接→预同步→合并"）。
 * <p>
 * 一次合并的双方角色由服务端 MERGE_PLAN 决出（算力分配）：
 * <ul>
 *   <li><b>让出方 A</b>（oldPrimary）：向 B 发起 P2P 预连接（P2P_CONNECT_REQUEST）
 *       → 会话头 {@code op=presync} → {@link PresyncSender}（快照/追赶/冻结切换）
 *       → 回报 switched → 让出：关停本地集成服务端、以副客户端身份经隧道
 *       重连 B（正是 {@link SecondaryJoiner} 的既有路径）；</li>
 *   <li><b>接管方 B</b>（newPrimary）：经 {@link GroupHost#setPresyncHandler} 收到
 *       预同步会话 → {@link PresyncReceiver}（快照/增量入 {@link PresyncStore}，
 *       玩家数据入本地存档）→ 集成服务端<b>持续运行不中断</b>，A 让出后其区块
 *       占用被服务端整组迁移到 B 名下，B 的加载门控随玩家视距推进逐块申请并
 *       优先消费暂存数据。</li>
 * </ul>
 * <b>触发面</b>（两条，殊途同归到 MERGE_REQUEST）：
 * <ol>
 *   <li>区块申请被拒（{@link MergeTriggers}，规范"需要加载的未知区块……被其他
 *       客户端加载"）—— 本类在世界会话建立时挂接消费者；</li>
 *   <li>服务端角色指派时发现更强者加入（Phase 4 收口；协议已就绪）。</li>
 * </ol>
 * 失败语义（规范"A 继续运行，服务端取消合并"）：任何一步失败只记日志 + 回报
 * failed，A 解冻继续运行，冷却期后重试；玩家全程无感知或只见短暂静止。
 * <p>
 * 线程模型：MERGE_PLAN/ABORT 处理器在 Netty 事件循环（只做状态判定），
 * 预同步与切换全程 io 线程；世界切换动作经 {@code minecraft.execute} 回主线程。
 */
public final class MergeClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/merge-client");

    /** 等待与对端主客户端的 P2P 会话就绪的上限（秒；打洞 + 中转降级余量）。 */
    private static final long CONNECT_TIMEOUT_SECONDS = 30;

    /** 触发去抖：同一阻塞组的两次 MERGE_REQUEST 之间的最小间隔（毫秒）。 */
    private static final long TRIGGER_DEBOUNCE_MILLIS = 15_000;

    /** 当前进行中的合并（同一时刻至多一场；volatile：多线程读，状态机串行写）。 */
    private static volatile ActiveMerge active;

    /** 上次触发时刻（去抖；io/Netty 线程读写，粗粒度即可）。 */
    private static volatile long lastTriggerAt;

    /** 一场合并的本端视角状态。 */
    private static final class ActiveMerge {
        final UUID mergeId;
        final UUID peerPrimary;
        final boolean yielding; // true = 本端是让出方 A
        final AtomicBoolean aborted = new AtomicBoolean(false);

        ActiveMerge(UUID mergeId, UUID peerPrimary, boolean yielding) {
            this.mergeId = mergeId;
            this.peerPrimary = peerPrimary;
            this.yielding = yielding;
        }
    }

    private MergeClient() {
    }

    /**
     * 挂接合并子系统（WorldSession 会话建立后调用；conn = 与物理服务端的控制连接）。
     * 注册 MERGE_PLAN / MERGE_ABORT / MERGE_COMMIT 处理器与区块拒绝触发消费者。
     */
    public static void register(imsng.player_to_player.netproto.ControlClient cc,
                                ControlConnection conn) {
        cc.on(MessageType.MERGE_PLAN, (c, msg) -> handlePlan(conn, msg));
        cc.on(MessageType.MERGE_ABORT, (c, msg) -> handleAbort(msg));
        cc.on(MessageType.MERGE_COMMIT, (c, msg) -> handleCommit(conn, msg));
        // 区块申请被拒 → 合并触发（规范"预连接"入口）；去抖后发 MERGE_REQUEST
        MergeTriggers.setConsumer((blockingGroup, blockedChunk) ->
                requestMerge(conn, blockingGroup, blockedChunk));
        LOGGER.info("合并客户端已挂接（触发面 + 计划处理器）");
    }

    /** 世界会话拆除时清理（WorldSession.teardownSession 调用）。 */
    public static void reset() {
        MergeTriggers.setConsumer(null);
        GroupHost.setPresyncHandler(null);
        PresyncStore.clear();
        active = null;
    }

    // ------------------------------------------------------------ 触发

    /** 区块拒绝触发 → MERGE_REQUEST（Netty/scheduler 线程；只做去抖与发送）。 */
    private static void requestMerge(ControlConnection conn, UUID blockingGroup,
                                     imsng.player_to_player.registry.ChunkKey blockedChunk) {
        if (NodeContext.get().clientRole() != ClientRole.PRIMARY || active != null) {
            return; // 只有主客户端发起；一次一场
        }
        long now = System.currentTimeMillis();
        if (now - lastTriggerAt < TRIGGER_DEBOUNCE_MILLIS) {
            return;
        }
        lastTriggerAt = now;
        JsonObject req = new JsonObject();
        req.addProperty("targetGroupId", blockingGroup.toString());
        req.addProperty("reason", "chunk_blocked");
        req.addProperty("blockingChunk", blockedChunk.asString());
        conn.request(ControlMessage.of(MessageType.MERGE_REQUEST, req))
                .whenComplete((resp, err) -> {
                    if (err != null || resp == null || resp.type() != MessageType.MERGE_PLAN) {
                        LOGGER.info("合并申请未获受理（冷却/忙碌/对方离线），继续按重试节奏申请区块: {}",
                                err != null ? err.toString() : (resp != null ? resp.json() : "?"));
                        return;
                    }
                    handlePlan(conn, resp); // reply 路径与广播路径共用一个入口（幂等）
                });
        LOGGER.info("已向服务端发起合并申请: 目标组 {}（阻塞区块 {}）",
                blockingGroup, blockedChunk.asString());
    }

    // ------------------------------------------------------------ 计划

    /** MERGE_PLAN：确定本端角色并启动对应流程（Netty 线程；重活转 io）。 */
    private static void handlePlan(ControlConnection conn, ControlMessage msg) {
        UUID mergeId = parseUuid(JsonUtil.getString(msg.json(), "mergeId", ""));
        UUID newPrimary = parseUuid(JsonUtil.getString(msg.json(), "newPrimaryClientId", ""));
        UUID oldPrimary = parseUuid(JsonUtil.getString(msg.json(), "oldPrimaryClientId", ""));
        UUID self = NodeContext.get().clientId();
        if (mergeId == null || newPrimary == null || oldPrimary == null || self == null) {
            return;
        }
        if (!self.equals(newPrimary) && !self.equals(oldPrimary)) {
            return; // 非当事方（异常投递）
        }
        synchronized (MergeClient.class) {
            ActiveMerge current = active;
            if (current != null) {
                if (current.mergeId.equals(mergeId)) {
                    return; // reply 与广播的重复投递：幂等
                }
                LOGGER.warn("已有合并 {} 进行中，忽略新计划 {}", current.mergeId, mergeId);
                return;
            }
            boolean yielding = self.equals(oldPrimary);
            active = new ActiveMerge(mergeId, yielding ? newPrimary : oldPrimary, yielding);
        }
        ActiveMerge merge = active;
        LOGGER.info("收到合并计划: mergeId={} 本端角色={}（对端主 {}）",
                mergeId, merge.yielding ? "让出方A" : "接管方B", merge.peerPrimary);
        if (merge.yielding) {
            ThreadPools.io().execute(() -> runYield(conn, merge));
        } else {
            armReceiver(conn, merge);
        }
    }

    // ------------------------------------------------------------ 让出方 A

    /** A 全流程（io 线程）：P2P 预连接 → 预同步 → switched → 降级重连。 */
    private static void runYield(ControlConnection conn, ActiveMerge merge) {
        Minecraft minecraft = Minecraft.getInstance();
        ReliableChannel channel = null;
        try {
            // ---- 1. 预连接：与 B 建 P2P（先监听后请求，同 SecondaryJoiner 时序）----
            P2PTransport transport = connectPeer(conn, merge.peerPrimary);
            channel = new ReliableChannel(transport, "presync:" + merge.mergeId);
            JsonObject header = new JsonObject();
            header.addProperty("op", "presync");
            header.addProperty("mergeId", merge.mergeId.toString());
            header.addProperty("clientId", NodeContext.get().clientId().toString());
            TcpTunnel.writeHeader(channel, header.toString());

            // ---- 2. 预同步（快照 + 追赶 + 冻结切换；进度实时回报服务端）----
            var server = GroupRuntime.server();
            var claims = GroupRuntime.claims();
            if (server == null || claims == null) {
                throw new IllegalStateException("本端集成服务端未在接管状态");
            }
            PresyncSender sender = new PresyncSender(channel, server, claims,
                    merge.mergeId.toString(),
                    phase -> reportProgress(conn, merge.mergeId, phase, true, ""));
            sender.run();
            // 预同步通道使命完成（TAIL 已被 B 确认），及时释放底层 P2P 资源
            channel.close();
            // 让出前的最终中止检查：MERGE_ABORT 可能在预同步期间到达
            //（handleAbort 已解冻），此处发现即放弃让出，A 保持原状继续运行。
            // 剩余竞态窗口收敛为一次网络传输时延（分布式协议的不可消除下界），
            // 服务端侧由 300s 会话超时兜底。
            if (merge.aborted.get()) {
                throw new IllegalStateException("合并已被服务端中止");
            }

            // ---- 3. 先完成本端状态降级，再回报 switched：MERGE_COMMIT 可能紧随
            //         switched 回报到达，handleCommit 依据"groupId 已是新组"跳过
            //         对本端的隧道重定向（runYield 自己负责重连）----
            claims.forgetAllLocal();
            GroupRuntime.suppressNextStopTeardown(); // 这次停止不是"离开世界"
            NodeContext.get().setClientRole(ClientRole.SECONDARY);
            NodeContext.get().setGroupId(merge.peerPrimary);
            active = null;
            reportProgress(conn, merge.mergeId, "switched", true, "");
            LOGGER.info("合并 {} 切换完成，本端开始让出（降级为副客户端）", merge.mergeId);

            // ---- 4. 让出：关停集成服务端并以副客户端身份经隧道重连 B ----
            UUID peer = merge.peerPrimary;
            minecraft.execute(() -> {
                // 编排性断开本地组世界（切换旗标保住控制连接），随后走副客户端
                // 加入路径 —— 与 Phase 2 的 secondary 指派完全同一条管线
                if (minecraft.level != null) {
                    WorldSwitcher.markSwitching();
                    minecraft.disconnect();
                }
                ThreadPools.io().execute(() -> SecondaryJoiner.join(minecraft, conn, peer));
            });
        } catch (Exception e) {
            LOGGER.error("合并 {} 让出流程失败（A 继续运行，服务端将取消合并）",
                    merge.mergeId, e);
            GroupRuntime.setTickFrozen(false); // 双保险（sender 内部失败路径已解冻）
            reportProgress(conn, merge.mergeId, "failed", false, e.toString());
            if (channel != null) {
                channel.close();
            }
            active = null;
        }
    }

    /** 与对端主客户端建立 P2P（打洞或中转降级；阻塞至就绪或超时）。 */
    private static P2PTransport connectPeer(ControlConnection conn, UUID peer) throws Exception {
        CompletableFuture<P2PTransport> ready = new CompletableFuture<>();
        P2PSessions.SessionListener listener = (sessionId, peerClientId, transport, initiator) -> {
            if (initiator && peer.equals(peerClientId)) {
                ready.complete(transport);
            }
        };
        P2PSessions.addListener(listener);
        try {
            JsonObject req = new JsonObject();
            req.addProperty("targetClientId", peer.toString());
            conn.send(ControlMessage.of(MessageType.P2P_CONNECT_REQUEST, req));
            return ready.get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            P2PSessions.removeListener(listener);
        }
    }

    // ------------------------------------------------------------ 接管方 B

    /** B：挂预同步会话处理器，等 A 连入（Netty 线程；接收循环在 io）。 */
    private static void armReceiver(ControlConnection conn, ActiveMerge merge) {
        GroupHost.setPresyncHandler((header, channel) -> {
            UUID mergeId = parseUuid(JsonUtil.getString(header, "mergeId", ""));
            if (!merge.mergeId.equals(mergeId)) {
                LOGGER.warn("预同步会话的 mergeId {} 与进行中的 {} 不符，拒绝", mergeId, merge.mergeId);
                channel.close();
                return;
            }
            ThreadPools.io().execute(() -> {
                try {
                    // 玩家数据写进 B 本地存档的 playerdata（A 组玩家重连后状态连续）
                    Path playerData = resolveLocalPlayerData();
                    PresyncReceiver receiver = new PresyncReceiver(channel, playerData,
                            merge.mergeId.toString(),
                            phase -> { /* 接收侧阶段由发送侧统一回报，避免双报干扰状态机 */ });
                    receiver.run();
                    channel.close(); // ACK_TAIL 已发出（可靠通道 close 会 linger 送达）
                    // 尾部已确认：B 侧回报 caught_up（switched 由 A 回报）
                    reportProgress(conn, merge.mergeId, "caught_up", true, "");
                    LOGGER.info("合并 {} 预同步接收完成（暂存 {} 个区块），等待服务端提交",
                            merge.mergeId, PresyncStore.size());
                } catch (Exception e) {
                    LOGGER.error("合并 {} 预同步接收失败", merge.mergeId, e);
                    PresyncStore.clear();
                    reportProgress(conn, merge.mergeId, "failed", false, e.toString());
                    channel.close();
                    active = null;
                } finally {
                    GroupHost.setPresyncHandler(null); // 一次性
                }
            });
        });
        LOGGER.info("合并 {}：本端为接管方，预同步接收器已就绪", merge.mergeId);
    }

    /** B 本地存档的 playerdata 目录（集成服务端运行中时从其世界路径解析）。 */
    private static Path resolveLocalPlayerData() {
        var server = GroupRuntime.server();
        if (server == null) {
            return null;
        }
        return server.getWorldPath(net.minecraft.world.level.storage.LevelResource.PLAYER_DATA_DIR);
    }

    // ------------------------------------------------------------ 中止与提交

    /** MERGE_ABORT：置中止旗标（预同步线程在阶段边界感知）并清理。 */
    private static void handleAbort(ControlMessage msg) {
        UUID mergeId = parseUuid(JsonUtil.getString(msg.json(), "mergeId", ""));
        ActiveMerge merge = active;
        if (merge == null || mergeId == null || !merge.mergeId.equals(mergeId)) {
            return;
        }
        merge.aborted.set(true);
        GroupRuntime.setTickFrozen(false); // A 继续运行（规范）
        PresyncStore.clear();              // B 丢弃已暂存数据
        GroupHost.setPresyncHandler(null);
        active = null;
        LOGGER.warn("合并 {} 已被服务端中止: {}", mergeId,
                JsonUtil.getString(msg.json(), "reason", "?"));
    }

    /**
     * MERGE_COMMIT：新组最终视图（规范"合并后……两个组客户端的所有副客户端对
     * 新的主客户端进行 p2p 连接"）。按本端与新组的关系分流：
     * <ul>
     *   <li>本端 = 新主 B：合并会话完结（占用已由服务端整组迁入）；</li>
     *   <li>本端 groupId 未变（B 组的副客户端 / 已自行重连的让出方 A）：只刷新视图；</li>
     *   <li>本端 groupId 变了（原 A 组的副客户端）：其到 A 的隧道即将随 A 关停
     *       而断开 —— 主动向新主发起 P2P 重定向（编排性断开旧隧道世界，走
     *       SecondaryJoiner 既有管线），玩家无需手动重进。</li>
     * </ul>
     */
    private static void handleCommit(ControlConnection conn, ControlMessage msg) {
        UUID groupId = parseUuid(JsonUtil.getString(msg.json(), "groupId", ""));
        UUID primary = parseUuid(JsonUtil.getString(msg.json(), "primaryClientId", ""));
        NodeContext ctx = NodeContext.get();
        UUID self = ctx.clientId();
        if (groupId == null || primary == null || self == null) {
            return;
        }
        UUID previousGroup = ctx.groupId();
        ctx.setGroupId(groupId);
        if (self.equals(primary)) {
            // 本端是合并后的主：合并会话完结（加载门控随视距推进逐块申请占用）
            ActiveMerge merge = active;
            if (merge != null && !merge.yielding) {
                active = null;
            }
            LOGGER.info("合并提交: 本端为新组 {} 的主客户端", groupId);
            return;
        }
        ctx.setClientRole(ClientRole.SECONDARY);
        if (previousGroup == null || groupId.equals(previousGroup)) {
            // B 组副客户端（隧道不受影响）或已自行重连的让出方 A：只刷新视图
            LOGGER.info("合并提交: 本端归入新组 {}（主客户端 {}，连接保持）", groupId, primary);
            return;
        }
        // 原 A 组的副客户端：主动重定向到新主（不等隧道断开再靠玩家手动重进）
        LOGGER.info("合并提交: 组迁移 {} → {}，向新主客户端 {} 发起 P2P 重定向",
                previousGroup, groupId, primary);
        Minecraft minecraft = Minecraft.getInstance();
        minecraft.execute(() -> {
            if (minecraft.level != null) {
                WorldSwitcher.markSwitching(); // 编排性断开：会话保持
                minecraft.disconnect();
            }
            ThreadPools.io().execute(() -> SecondaryJoiner.join(minecraft, conn, primary));
        });
    }

    // ------------------------------------------------------------ 工具

    /** 上报合并阶段（fire-and-forget；服务端状态机消费）。 */
    private static void reportProgress(ControlConnection conn, UUID mergeId, String phase,
                                       boolean success, String detail) {
        JsonObject json = new JsonObject();
        json.addProperty("mergeId", mergeId.toString());
        json.addProperty("phase", phase);
        json.addProperty("success", success);
        if (!detail.isEmpty()) {
            json.addProperty("detail", detail);
        }
        conn.send(ControlMessage.of(MessageType.MERGE_PROGRESS, json));
    }

    /** 防御性 UUID 解析。 */
    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
