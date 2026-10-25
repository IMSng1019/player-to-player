package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.GroupControlTasks;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.server.MinecraftServer;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * 计算进程中的权威元数据副本。版本确认是独立门控，不能使用合并的 tickFrozen 标志：
 * 合并回滚只解除自己的冻结，不能意外解除控制断线/元数据缺口造成的冻结。
 * NBT 解码在入站线程执行；原版状态应用只进入专用服务器控制队列。
 */
public final class MetadataClient {
    private static volatile MetadataSnapshot current;
    private static volatile boolean ready;
    private static volatile boolean connected;
    private static boolean runtimeApplied;
    private static long generation;
    private static MetadataVersionBarrier barrier;
    private static boolean retryScheduled;
    private static volatile boolean restartRequired;
    private static java.util.function.Consumer<java.util.Set<String>> restartListener = fields -> { };
    private static CompletableFuture<MetadataSnapshot> synchronizing;
    private record ReceivedSnapshot(MetadataSnapshot snapshot, long sampledAtMillis,
                                     boolean incremental, java.util.List<MetadataPatch> patches) { }
    private static final class StaleTermAcknowledgement extends IOException {
        StaleTermAcknowledgement() { super("元数据确认任期已变化，需要在同一屏障内重新获取快照"); }
    }

    private MetadataClient() { }
    public static MetadataSnapshot snapshot() { return current; }
    public static boolean isSimulationReady() { return ready && connected; }
    public static synchronized void setRestartListener(java.util.function.Consumer<java.util.Set<String>> listener) {
        restartListener = java.util.Objects.requireNonNull(listener);
    }

    /** 新计算实例必须先冻结，旧缓存即使版本看似一致也不能直接开放演算。 */
    public static synchronized void initialize(MetadataSnapshot snapshot) throws IOException {
        MetadataFiles.validateIdentity(snapshot);
        generation++;
        current = snapshot;
        ready = false;
        connected = true;
        runtimeApplied = false;
        barrier = new MetadataVersionBarrier(snapshot);
        retryScheduled = false;
        restartRequired = false;
        synchronizing = null;
    }

    public static synchronized void disconnected() {
        ready = false;
        connected = false;
        generation++;
        synchronizing = null;
    }

    /** 启动前允许空身份请求；拿到数据后必须核对生成身份，后续请求始终带明确世界 ID。 */
    public static CompletableFuture<MetadataSnapshot> requestSnapshot(ControlConnection connection, String worldId) {
        return requestSnapshotFrame(connection, worldId).thenApply(ReceivedSnapshot::snapshot);
    }

    private static CompletableFuture<ReceivedSnapshot> requestSnapshotFrame(ControlConnection connection, String worldId) {
        JsonObject json = new JsonObject(); json.addProperty("worldId", worldId == null ? "" : worldId);
        return connection.request(ControlMessage.of(MessageType.METADATA_SNAPSHOT_REQUEST, json)).thenApply(reply -> {
            try {
                return decodeSnapshot(reply, worldId);
            } catch (IOException | IllegalArgumentException error) { throw new CompletionException(error); }
        });
    }

    private static ReceivedSnapshot decodeSnapshot(ControlMessage reply, String worldId) throws IOException {
        if (reply.type() != MessageType.METADATA_SNAPSHOT)
            throw new IOException("服务端未返回世界元数据快照: " + reply.json());
        MetadataSnapshot result = MetadataWire.snapshotFromTag(MetadataWire.decode(reply.binary()));
        MetadataFiles.validateIdentity(result);
        if (worldId != null && !worldId.isBlank() && !worldId.equals(result.worldId()))
            throw new IOException("元数据快照来自不同世界");
        return new ReceivedSnapshot(result, JsonUtil.getLong(reply.json(), "sampledAtMillis", System.currentTimeMillis()), false, java.util.List.of());
    }

    private static CompletableFuture<ReceivedSnapshot> requestCatchupFrame(ControlConnection connection, MetadataSnapshot base) {
        JsonObject json = new JsonObject(); json.addProperty("worldId", base.worldId());
        json.addProperty("term", base.term()); json.addProperty("version", base.version());
        return connection.request(ControlMessage.of(MessageType.METADATA_CATCHUP, json)).thenCompose(reply -> {
            // 换任期不能拼接旧日志；窗口不足的服务端也直接回完整快照。
            if ("metadata_stale_term".equals(JsonUtil.getString(reply.json(), "code", "")))
                return requestSnapshotFrame(connection, base.worldId());
            try {
                if (reply.type() == MessageType.METADATA_SNAPSHOT)
                    return CompletableFuture.completedFuture(decodeSnapshot(reply, base.worldId()));
                var patches = MetadataCatchup.decode(base, reply);
                return CompletableFuture.completedFuture(new ReceivedSnapshot(base,
                        JsonUtil.getLong(reply.json(), "sampledAtMillis", System.currentTimeMillis()), true, patches));
            } catch (IOException | IllegalArgumentException failure) { return CompletableFuture.failedFuture(failure); }
        });
    }

    public static void register(HandlerRegistry registry) {
        registry.on(MessageType.METADATA_PATCH, (connection, message) -> {
            MinecraftServer server = GroupRuntime.server();
            if (server == null) return; // 启动期间的推送由 ready 后完整快照补齐。
            try {
                MetadataPatch patch = MetadataWire.patchFromTag(MetadataWire.decode(message.binary()));
                long sampledAt = JsonUtil.getLong(message.json(), "sampledAtMillis", System.currentTimeMillis());
                GroupControlTasks.executor(server).execute(() -> applyPatch(server, connection, patch, sampledAt));
            } catch (Exception error) { failAndSynchronize(server, connection); }
        });
        registry.on(MessageType.METADATA_SNAPSHOT, (connection, message) -> {
            MinecraftServer server = GroupRuntime.server();
            if (server != null) synchronize(server, connection);
        });
    }

    /**
     * 首次 ready、缺口补齐和接管屏障共用。返回值只有在原版对象应用成功且中心确认
     * 当前连接的版本后才完成，不能把已经下载到磁盘等同于运行对象已经生效。
     */
    public static synchronized CompletableFuture<MetadataSnapshot> synchronize(MinecraftServer server,
                                                                                ControlConnection connection) {
        if (server == null || current == null || !connection.isOpen() || restartRequired)
            return CompletableFuture.failedFuture(new IOException("元数据同步缺少当前受管世界或连接"));
        if (synchronizing != null && !synchronizing.isDone()) return synchronizing;
        ready = false;
        connected = true;
        long ticket = generation;
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(25);
        CompletableFuture<MetadataSnapshot> future = synchronizeAttempt(server, connection, ticket, deadline)
                .orTimeout(25, java.util.concurrent.TimeUnit.SECONDS);
        synchronizing = future;
        future.whenComplete((snapshot, error) -> {
            if (error != null) synchronized (MetadataClient.class) {
                if (ticket == generation) {
                    ready = false;
                    if (!requireRestart(error)) scheduleRetry(server, connection, ticket);
                }
            }
        });
        return future;
    }

    private static CompletableFuture<MetadataSnapshot> synchronizeAttempt(MinecraftServer server,
                                                                          ControlConnection connection, long ticket,
                                                                          long deadline) {
        MetadataSnapshot base;
        boolean useCatchup;
        synchronized (MetadataClient.class) {
            if (ticket != generation || !connected || !connection.isOpen() || restartRequired)
                return CompletableFuture.failedFuture(new IOException("元数据同步会话已取消"));
            if (System.nanoTime() >= deadline)
                return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("元数据版本屏障补齐超时"));
            base = current;
            useCatchup = runtimeApplied;
        }
        return (useCatchup ? requestCatchupFrame(connection, base) : requestSnapshotFrame(connection, base.worldId()))
                .thenApplyAsync(received -> {
                    synchronized (MetadataClient.class) {
                        if (ticket != generation || !connected || System.nanoTime() >= deadline)
                            throw new CompletionException(new IOException("元数据会话已过期或补齐超时"));
                        // 整批已经在入站线程验证连续性；运行对象在同一服务器控制任务内
                        // 依次前进。并发推送已提前应用的版本由installRuntime幂等跳过。
                        if (!received.incremental()) installRuntime(server, received.snapshot(), received.sampledAtMillis());
                        else {
                            MetadataSnapshot state = received.snapshot();
                            for (MetadataPatch patch : received.patches()) {
                                state = patch.applyTo(state);
                                var data = state.data();
                                long sampledAt = data.contains("__p2pSampledAtMillis", net.minecraft.nbt.Tag.TAG_LONG)
                                        ? data.getLong("__p2pSampledAtMillis") : received.sampledAtMillis();
                                installRuntime(server, state, sampledAt);
                            }
                        }
                        return current;
                    }
                }, GroupControlTasks.executor(server))
                .thenCompose(snapshot -> acknowledge(connection, snapshot).thenApply(ignored -> snapshot))
                .thenCompose(snapshot -> {
                    synchronized (MetadataClient.class) {
                        if (ticket != generation || !connected || !connection.isOpen()
                                || current.term() != snapshot.term() || System.nanoTime() >= deadline)
                            throw new CompletionException(new IOException("确认期间元数据任期或连接已变化"));
                        if (!barrier.allows(current, snapshot))
                            return synchronizeAttempt(server, connection, ticket, deadline);
                        ready = true;
                        // 返回真正已经ACK的版本；更新版本可能已经应用但自己的ACK还在路上。
                        return CompletableFuture.completedFuture(snapshot);
                    }
                }).exceptionallyCompose(error -> {
                    Throwable cause = unwrap(error);
                    if (!(cause instanceof StaleTermAcknowledgement)) return CompletableFuture.failedFuture(error);
                    // 角色指派/合并切换可在快照到达与ACK之间推进任期。这不是启动失败；
                    // 保持同一个RPC future等待最新屏障，只有明确的stale_term允许重试。
                    return CompletableFuture.runAsync(() -> { }, CompletableFuture.delayedExecutor(
                            50, java.util.concurrent.TimeUnit.MILLISECONDS, imsng.player_to_player.util.ThreadPools.io()))
                            .thenCompose(ignored -> synchronizeAttempt(server, connection, ticket, deadline));
                });
    }

    private static void installRuntime(MinecraftServer server, MetadataSnapshot incoming, long sampledAt) {
        MetadataSnapshot before = current;
        if (before == null || !before.worldId().equals(incoming.worldId()))
            throw new IllegalStateException("不能把不同世界元数据应用到运行实例");
        if (incoming.term() < before.term() || (incoming.term() == before.term() && incoming.version() < before.version()))
            return; // 请求期间已收到更高版本推送，禁止迟到响应回拨。
        if (!runtimeApplied || incoming.term() != before.term() || incoming.version() != before.version()) {
            MetadataRuntime.apply(server, incoming.data(), sampledAt);
            runtimeApplied = true;
        }
        current = incoming;
    }

    private static synchronized void applyPatch(MinecraftServer server, ControlConnection connection, MetadataPatch patch, long sampledAt) {
        MetadataSnapshot before = current;
        if (!connected || before == null || !before.worldId().equals(patch.worldId())) return;
        if (patch.term() < before.term()) return;
        barrier.observe(patch);
        if (patch.term() == before.term() && patch.version() <= before.version()) {
            acknowledge(connection, before).exceptionally(error -> { failAndSynchronize(server, connection); return null; });
            if (!ready) synchronize(server, connection);
            return;
        }
        if (patch.term() != before.term() || patch.baseVersion() != before.version()) {
            failAndSynchronize(server, connection);
            return;
        }
        try {
            installRuntime(server, patch.applyTo(before), sampledAt);
            acknowledge(connection, current).exceptionally(error -> { failAndSynchronize(server, connection); return null; });
            if (!ready) synchronize(server, connection);
        } catch (RuntimeException failed) {
            if (!requireRestart(failed)) failAndSynchronize(server, connection);
        }
    }

    private static CompletableFuture<Void> acknowledge(ControlConnection connection, MetadataSnapshot snapshot) {
        JsonObject json = new JsonObject(); json.addProperty("worldId", snapshot.worldId());
        json.addProperty("version", snapshot.version()); json.addProperty("term", snapshot.term());
        return connection.request(ControlMessage.of(MessageType.METADATA_ACK, json)).thenAccept(reply -> {
            if ("metadata_stale_term".equals(JsonUtil.getString(reply.json(), "code", "")))
                throw new CompletionException(new StaleTermAcknowledgement());
            if (reply.type() != MessageType.METADATA_ACK || !JsonUtil.getBoolean(reply.json(), "accepted", false))
                throw new CompletionException(new IOException("服务端拒绝元数据版本确认"));
        });
    }

    private static void failAndSynchronize(MinecraftServer server, ControlConnection connection) {
        ready = false;
        synchronize(server, connection).exceptionally(error -> {
            org.slf4j.LoggerFactory.getLogger("player_to_player/metadata").error("元数据补齐失败，保持模拟暂停", error);
            return null;
        });
    }

    /** 即便世界暂停昼夜/天气、后续不再有字段补丁，暂时失败也会主动重试补齐。 */
    private static synchronized void scheduleRetry(MinecraftServer server, ControlConnection connection, long ticket) {
        if (retryScheduled || !connected || ticket != generation || restartRequired) return;
        retryScheduled = true;
        imsng.player_to_player.util.ThreadPools.scheduler().schedule(() -> {
            synchronized (MetadataClient.class) {
                if (ticket != generation) return;
                retryScheduled = false;
                if (ready || restartRequired || !connected || !connection.isOpen() || !GroupRuntime.isManagedServer(server)) return;
            }
            synchronize(server, connection);
        }, 1, java.util.concurrent.TimeUnit.SECONDS);
    }

    /** 启动字段不能通过重试强行热应用；通知父进程进入已有的耐久退出/重新加入流程。 */
    private static synchronized boolean requireRestart(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (!(cause instanceof MetadataRestartRequiredException restart)) return false;
        ready = false;
        if (!restartRequired) {
            restartRequired = true;
            restartListener.accept(restart.fields());
        }
        return true;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof java.util.concurrent.CompletionException || cause instanceof java.util.concurrent.ExecutionException)
                && cause.getCause() != null) cause = cause.getCause();
        return cause;
    }
}
