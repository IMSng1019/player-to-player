package imsng.player_to_player.client.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.client.session.SwitchTelemetry;
import imsng.player_to_player.client.session.WorldSession;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.*;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.p2p.*;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import imsng.player_to_player.runtime.WorkerProcess;
import net.minecraft.client.Minecraft;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * v6 合并编排：旧组所有迁移成员先预建隧道，B 先挂接接收器；服务端确认齐备
 * 后才允许 A 预同步。A 确认尾部后仍保留旧角色，收到权威 COMMIT 才降级。
 * 连接准备失败不碰当前世界。实际切换继续完整执行原版断开、登录和模组握手。
 */
public final class MergeClient {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/merge-client");
    private static volatile ActiveMerge active;
    private static volatile long lastTriggerAt;

    private static final class ActiveMerge {
        final UUID id, target, source;
        final boolean yielding, receiving;
        final AtomicBoolean aborted = new AtomicBoolean();
        final AtomicBoolean started = new AtomicBoolean();
        final CompletableFuture<Void> committed = new CompletableFuture<>();
        final CompletableFuture<Void> drained = new CompletableFuture<>();
        volatile PreparedTunnel tunnel;
        volatile ReliableChannel sync;
        volatile boolean commitReceived;

        ActiveMerge(UUID id, UUID target, UUID source, UUID self) {
            this.id = id; this.target = target; this.source = source;
            yielding = source.equals(self);
            receiving = target.equals(self);
        }

        void cancel() {
            aborted.set(true);
            if (tunnel != null) tunnel.close();
            if (sync != null) sync.abort();
            if (yielding || receiving) WorkerProcess.cancelPresync();
            committed.completeExceptionally(new IllegalStateException("合并已取消"));
            drained.completeExceptionally(new IllegalStateException("合并已取消"));
        }
    }

    private MergeClient() { }

    public static void register(ControlClient client, ControlConnection conn) {
        client.on(MessageType.MERGE_PLAN, (c, msg) -> handlePlan(conn, msg));
        client.on(MessageType.MERGE_START, (c, msg) -> {
            ActiveMerge merge = matching(msg);
            if (merge != null && merge.yielding && merge.sync != null
                    && merge.started.compareAndSet(false, true))
                ThreadPools.startBlocking("merge-yield", () -> runYield(conn, merge));
        });
        client.on(MessageType.MERGE_ABORT, (c, msg) -> handleAbort(msg));
        client.on(MessageType.MERGE_COMMIT, (c, msg) -> handleCommit(conn, msg));
        client.on(MessageType.MERGE_DRAINED, (c, msg) -> {
            ActiveMerge merge = matching(msg);
            if (merge != null) merge.drained.complete(null);
        });
        MergeTriggers.setConsumer((blocking, key) -> requestMerge(conn, blocking, key));
    }

    public static synchronized void reset() {
        ActiveMerge previous = active;
        active = null;
        if (previous != null) previous.cancel();
        MergeTriggers.setConsumer(null);
        GroupHost.setPresyncHandler(null);
        WorkerProcess.cancelPresync();
        lastTriggerAt = 0;
    }

    private static synchronized void requestMerge(ControlConnection conn, UUID blocking,
                                                   imsng.player_to_player.registry.ChunkKey key) {
        if (NodeContext.get().clientRole() != ClientRole.PRIMARY || active != null) return;
        long now = System.nanoTime();
        if (lastTriggerAt != 0 && now - lastTriggerAt < 15_000_000_000L) return;
        lastTriggerAt = now;
        JsonObject request = new JsonObject();
        request.addProperty("targetGroupId", blocking.toString());
        request.addProperty("reason", "chunk_blocked");
        request.addProperty("blockingChunk", key.asString());
        conn.request(ControlMessage.of(MessageType.MERGE_REQUEST, request)).whenComplete((reply, error) -> {
            if (error == null && reply != null && reply.type() == MessageType.MERGE_PLAN)
                handlePlan(conn, reply);
        });
    }

    private static synchronized void handlePlan(ControlConnection conn, ControlMessage msg) {
        UUID id = uuid(msg.json(), "mergeId"), target = uuid(msg.json(), "newPrimaryClientId"),
                source = uuid(msg.json(), "oldPrimaryClientId"), self = NodeContext.get().clientId();
        if (id == null || target == null || source == null || self == null
                || !WorldSession.isCurrentConnection(conn) || active != null) return;
        ActiveMerge merge = new ActiveMerge(id, target, source, self);
        active = merge;
        SwitchTelemetry.begin("merge", id.toString());
        if (merge.receiving) {
            armReceiver(conn, merge);
            progress(conn, merge, "transport_ready", true);
        } else {
            ThreadPools.startBlocking("merge-prepare", () -> {
                try {
                    if (merge.yielding) {
                        prepareSource(conn, merge);
                        return;
                    }
                    PreparedTunnel tunnel = SecondaryJoiner.prepare(conn, target, id);
                    synchronized (MergeClient.class) {
                        if (active != merge || merge.aborted.get()) {
                            tunnel.close();
                            return;
                        }
                        merge.tunnel = tunnel;
                    }
                    progress(conn, merge, "transport_ready", true);
                } catch (Exception error) {
                    LOGGER.warn("合并 {} 隧道准备失败，保留当前世界连接", id, error);
                    progress(conn, merge, "failed", false);
                }
            });
        }
    }

    /** A 的单条传输先预同步、后游戏，避免第二条握手使用过期的 NAT 端点。 */
    private static void prepareSource(ControlConnection conn, ActiveMerge merge) throws Exception {
        var transport = P2PSessions.connect(conn, merge.target);
        var channel = new ReliableChannel(transport, "merge:" + merge.id);
        synchronized (MergeClient.class) {
            if (active != merge || merge.aborted.get() || !WorldSession.isCurrentConnection(conn)) {
                channel.abort();
                return;
            }
            merge.sync = channel;
        }
        JsonObject header = new JsonObject();
        header.addProperty("op", "presync");
        header.addProperty("mergeId", merge.id.toString());
        header.addProperty("clientId", NodeContext.get().clientId().toString());
        TcpTunnel.writeHeader(channel, header.toString());
        JsonObject ready = JsonUtil.parseObject(TcpTunnel.readHeader(channel, 15));
        if (!"presync_ready".equals(JsonUtil.getString(ready, "op", ""))
                || !merge.id.equals(uuid(ready, "mergeId")))
            throw new java.io.IOException("预同步准备确认不匹配");
        if (merge.aborted.get()) return;
        SwitchTelemetry.mark("transport_prepared");
        progress(conn, merge, "transport_ready", true);
    }

    private static void runYield(ControlConnection conn, ActiveMerge merge) {
        try {
            ReliableChannel channel = merge.sync;
            if (merge.aborted.get() || channel == null || !channel.isOpen())
                throw new IllegalStateException("预建传输已失效");
            if (!WorkerProcess.isManagedWorld() || merge.aborted.get())
                throw new IllegalStateException("原组运行时已失效");
            WorkerProcess.runPresync(channel, merge.id.toString(), true,
                    phase -> progress(conn, merge, phase, true));
            if (merge.aborted.get()) return;
            // 接收器已读完 TAIL_DONE，同一流随后只交给游戏隧道准备协议。
            PreparedTunnel tunnel = PreparedTunnel.prepare(channel, merge.id.toString());
            synchronized (MergeClient.class) {
                if (merge.aborted.get() || active != merge) {
                    tunnel.close();
                    return;
                }
                merge.tunnel = tunnel;
                merge.sync = null; // 所有权已移交，finally 不得关闭游戏流。
            }
            SwitchTelemetry.mark("tail_confirmed");
            progress(conn, merge, "switched", true);
            // 网络超时不能等同“服务端已取消”：提交结果未明确前保持冻结，防两端都写。
            merge.committed.join();
            merge.drained.join();
            // 独立子服不随Minecraft.disconnect停止。先消费编排断开旗标，再明确停掉已
            // 退休的旧写者；整个过程仍保留中心控制连接和预建目标隧道。
            WorldSwitcher.disconnectForSwitch(Minecraft.getInstance()).get();
            WorkerProcess.stopRetired();
            NodeContext.get().setGroupId(merge.target);
            NodeContext.get().setClientRole(ClientRole.SECONDARY);
            GroupHost.reset();
            SecondaryJoiner.activate(Minecraft.getInstance(), conn, merge.target, merge.tunnel,
                    () -> {
                        clearIfCurrent(merge);
                        progress(conn, merge, "redirected", true);
                    });
        } catch (Exception error) {
            LOGGER.error("合并 {} 让出流程失败（已提交={}）", merge.id, merge.commitReceived, error);
            // 提交后不恢复旧写者；提交前若已报告 switched，也等权威 ABORT 再解冻。
            if (!merge.commitReceived) progress(conn, merge, "failed", false);
        } finally {
            if (merge.sync != null) merge.sync.close();
        }
    }

    private static void armReceiver(ControlConnection conn, ActiveMerge merge) {
        GroupHost.setPresyncHandler((header, channel, terminated) -> {
            synchronized (MergeClient.class) {
                if (active != merge || !merge.id.equals(uuid(header, "mergeId")) || merge.aborted.get()
                        || !merge.source.equals(uuid(header, "_peerClientId"))
                        || !WorldSession.isCurrentConnection(conn)
                        || !merge.started.compareAndSet(false, true)) {
                    channel.abort();
                    terminated.run();
                    return;
                }
                merge.sync = channel;
                if (!WorkerProcess.isManagedWorld()) {
                    channel.abort();
                    terminated.run();
                    return;
                }
            }
            ThreadPools.startBlocking("presync-receive", () -> {
                boolean handedOff = false;
                try {
                    if (!WorkerProcess.isManagedWorld() || merge.aborted.get()) throw new IllegalStateException("接管组运行时已停止");
                    JsonObject ready = new JsonObject();
                    ready.addProperty("op", "presync_ready");
                    ready.addProperty("mergeId", merge.id.toString());
                    TcpTunnel.writeHeader(channel, ready.toString());
                    WorkerProcess.runPresync(channel, merge.id.toString(), false, phase -> { });
                    if (merge.aborted.get()) return;
                    progress(conn, merge, "caught_up", true);
                    JsonObject tunnelHeader = JsonUtil.parseObject(TcpTunnel.readHeader(channel, 15));
                    if (!"tunnel_prepare".equals(JsonUtil.getString(tunnelHeader, "op", ""))
                            || !merge.id.equals(uuid(tunnelHeader, "transactionId")))
                        throw new java.io.IOException("预同步后的隧道事务不匹配");
                    handedOff = true;
                    PreparedTunnel.accept(channel, tunnelHeader, GroupHost::currentPort, terminated,
                            socket -> WorkerProcess.authorizeGameSocket(socket, merge.source));
                } catch (Exception error) {
                    LOGGER.error("合并 {} 预同步接收失败", merge.id, error);
                    progress(conn, merge, "failed", false);
                } finally {
                    if (!handedOff) {
                        channel.close();
                        terminated.run();
                    }
                    if (active == merge) GroupHost.setPresyncHandler(null);
                }
            });
        });
    }

    private static synchronized void handleAbort(ControlMessage msg) {
        ActiveMerge merge = matching(msg);
        if (merge == null || merge.commitReceived) return;
        merge.cancel();
        if (merge.yielding) WorkerProcess.freeze(false);
        if (merge.receiving) WorkerProcess.cancelPresync();
        GroupHost.setPresyncHandler(null);
        active = null;
        SwitchTelemetry.finish("merge_aborted");
    }

    private static synchronized void handleCommit(ControlConnection conn, ControlMessage msg) {
        ActiveMerge merge = matching(msg);
        if (merge == null || merge.aborted.get() || merge.commitReceived) return;
        UUID group = uuid(msg.json(), "groupId"), primary = uuid(msg.json(), "primaryClientId");
        if (!merge.target.equals(group) || !merge.target.equals(primary)) return;
        merge.commitReceived = true;
        SwitchTelemetry.mark("ownership_committed");
        if (merge.yielding) {
            try {
                // COMMIT 才是写权限终止点。不能等成员排空，更不能等 SERVER_STOPPED
                // 才终结旧 claim：Saving worlds 正在依赖这些加载 future 收束。
                WorkerProcess.retireAfterTransfer(merge.source).whenComplete((ignored, error) -> {
                    if (error != null) {
                        merge.committed.completeExceptionally(error);
                        LOGGER.error("合并提交后旧计算进程退休失败，保持冻结", error);
                    } else merge.committed.complete(null);
                });
            } catch (RuntimeException retirementFailure) {
                merge.committed.completeExceptionally(retirementFailure);
                LOGGER.error("合并 {} 已提交，但旧运行时退休失败；保持冻结避免旧主继续写入",
                        merge.id, retirementFailure);
                return;
            }
        }
        if (!merge.yielding) merge.committed.complete(null);
        if (merge.receiving) {
            NodeContext.get().setGroupId(group);
            GroupHost.setPresyncHandler(null);
            WorkerProcess.wakeBlockedClaims(merge.source);
            // 原主 B 不重登录；已有的被拒区块申请由权威归属变更通知唤醒。
            SwitchTelemetry.finish("merge_received");
            clearIfCurrent(merge);
        } else if (!merge.yielding) {
            NodeContext.get().setGroupId(group);
            NodeContext.get().setClientRole(ClientRole.SECONDARY);
            ThreadPools.startBlocking("merge-activate", () -> {
                try {
                    SecondaryJoiner.activate(Minecraft.getInstance(), conn, primary, merge.tunnel, () -> {
                        clearIfCurrent(merge);
                        progress(conn, merge, "redirected", true);
                    });
                } catch (Exception error) {
                    LOGGER.error("合并 {} 已提交但成员重定向失败", merge.id, error);
                    SwitchTelemetry.finish("redirect_failed");
                }
            });
        }
    }

    private static synchronized void clearIfCurrent(ActiveMerge merge) {
        if (active == merge) active = null;
    }

    private static ActiveMerge matching(ControlMessage msg) {
        ActiveMerge merge = active;
        return merge != null && merge.id.equals(uuid(msg.json(), "mergeId")) ? merge : null;
    }

    private static UUID uuid(JsonObject json, String field) {
        try { return UUID.fromString(JsonUtil.getString(json, field, "")); }
        catch (IllegalArgumentException error) { return null; }
    }

    private static void progress(ControlConnection conn, ActiveMerge merge, String phase, boolean success) {
        if (!WorldSession.isCurrentConnection(conn)) return;
        JsonObject report = new JsonObject();
        report.addProperty("mergeId", merge.id.toString());
        report.addProperty("phase", phase);
        report.addProperty("success", success);
        conn.send(ControlMessage.of(MessageType.MERGE_PROGRESS, report));
    }
}
