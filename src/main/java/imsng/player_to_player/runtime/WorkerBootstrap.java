package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.*;
import imsng.player_to_player.core.*;
import imsng.player_to_player.group.*;
import imsng.player_to_player.metadata.*;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.fabricmc.loader.api.FabricLoader;
import net.fabricmc.api.EnvType;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 独立 Fabric 服务端的公共入口。物理 SERVER 环境在 P2P 拓扑中仍是 CLIENT，
 * 必须在常规 P2PBootstrap 启动中心服务前分流；不发远端 HELLO、不创建第二个中心服务。
 */
public final class WorkerBootstrap {
    private static volatile boolean worker;
    private static WorkerRpc rpc;
    private static ControlClient client;
    private static final Map<MessageType, MessageHandler> FORWARDED = new ConcurrentHashMap<>();
    private static final WorkerIdentityTable IDENTITIES = new WorkerIdentityTable();
    private static volatile PresyncReceiver receiver;
    private static volatile WorkerStream presyncStream;
    private static volatile String presyncId;
    private static volatile boolean stopping;
    private static volatile boolean retired;
    private static CompletableFuture<ControlMessage> stopFuture;
    private static volatile boolean remoteConnected = true;
    private static Path bundlePath;
    private static final CountDownLatch RELEASE = new CountDownLatch(1);
    private WorkerBootstrap() { }
    public static boolean isWorker() { return worker; }
    public static WorkerIdentityTable identities() { return IDENTITIES; }

    /** 返回 true 说明公共入口已由计算子进程完整接管，调用方必须直接 return。 */
    public static boolean initializeIfWorker() {
        String raw = System.getProperty("p2p.worker.session", "");
        if (raw.isBlank()) return false;
        if (FabricLoader.getInstance().getEnvironmentType() != EnvType.SERVER)
            throw new IllegalStateException("计算子进程必须使用Fabric服务端环境");
        try {
            Path session = Path.of(raw).toAbsolutePath().normalize();
            JsonObject spec = JsonUtil.parseObject(Files.readString(session, StandardCharsets.UTF_8));
            worker = true;
            bundlePath = session.resolveSibling("exit-" + spec.get("runtimeId").getAsString() + ".bin");
            NodeContext context = NodeContext.get();
            P2PPaths paths = new P2PPaths(FabricLoader.getInstance().getGameDir());
            paths.ensureBaseDirs();
            context.setPaths(paths);
            context.setConfig(GlobalConfig.loadOrCreate(paths.globalConfig(), NodeMode.CLIENT));
            context.setMode(NodeMode.CLIENT);
            context.setClientId(UUID.fromString(spec.get("clientId").getAsString()));
            context.setGroupId(UUID.fromString(spec.get("groupId").getAsString()));
            context.setPlayerName(spec.get("playerName").getAsString());
            context.setClientRole(ClientRole.PRIMARY);
            MetadataClient.initialize(MetadataWire.snapshotFromTag(MetadataWire.decode(
                    Base64.getDecoder().decode(spec.get("metadataSnapshot").getAsString()))));
            rpc = new WorkerRpc(WorkerBootstrap::handle, (connection, message) -> { });
            MetadataClient.setRestartListener(fields -> {
                JsonObject reason = new JsonObject();
                reason.addProperty("fields", String.join(", ", fields));
                rpc.emit("metadata_restart_required", reason, null);
            });
            client = new ControlClient("127.0.0.1", spec.get("ipcPort").getAsInt());
            rpc.register(client);
            client.onDisconnect(connection -> {
                rpc.disconnected();
                MetadataClient.disconnected();
                // 父进程死亡时先冻结唯一写者，不允许失联后继续生成/演算；再走原版保存停止。
                GroupRuntime.setTickFrozen(true);
                MinecraftServer server = GroupRuntime.server();
                if (server != null) server.halt(false);
                RELEASE.countDown();
            });
            ControlConnection ipc = client.connect();
            rpc.attach(ipc);
            JsonObject hello = new JsonObject();
            hello.addProperty("token", spec.get("token").getAsString());
            hello.addProperty("runtimeId", spec.get("runtimeId").getAsString());
            ControlMessage ack = ipc.request(ControlMessage.of(MessageType.HELLO, hello)).get(20, TimeUnit.SECONDS);
            if (ack.type() != MessageType.HELLO_ACK || !JsonUtil.getBoolean(ack.json(), "accepted", false))
                throw new IOException("父进程IPC身份验证失败");
            HandlerRegistry registry = (type, handler) -> {
                if (FORWARDED.putIfAbsent(type, handler) != null) throw new IllegalStateException("重复worker处理器");
            };
            CommandRelayClient.register(registry);
            CommandRelayClient.setServerRoutingSupported(JsonUtil.getBoolean(spec, "commandRoutingSupported", true));
            ChatRelay.register(registry);
            PearlHandoff.register(registry);
            LocalPermissions.register(registry);
            MetadataClient.register(registry);
            MetadataEvents.install(registry);
            // Loader已解析全部模组容器；在arm之前核对真正加载的服务端模组，失败不得
            // 启动任何组世界tick。目录哈希与运行容器校验是两个不同的完成条件。
            var manifest = imsng.player_to_player.env.EnvironmentManifest.fromJson(spec.getAsJsonObject("primaryManifest"));
            if (!manifest.globalHash().equals(JsonUtil.getString(spec, "manifestHash", "")))
                throw new IOException("子进程环境清单版本不一致");
            var loaded = imsng.player_to_player.env.FabricRuntimeMods.capture();
            var problems = imsng.player_to_player.env.ClientEnvironmentCompatibility.check(
                    FabricLoader.getInstance().getGameDir(), manifest, loaded.mods(), "server");
            if (!problems.isEmpty()) throw new IOException("实际服务端模组不匹配: " + String.join("; ", problems));
            registry.on(MessageType.CHUNK_OWNERSHIP_CHANGED, (connection, message) -> {
                var claims = GroupRuntime.claims();
                if (claims != null && message.json().has("changedGroups"))
                    for (var id : message.json().getAsJsonArray("changedGroups")) claims.wakeBlockedClaims(UUID.fromString(id.getAsString()));
            });
            GroupServerHooks.register();
            net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents.SERVER_STOPPED.register(server ->
                    rpc.emit("server_stopped", new JsonObject(), null));
            ChatRelay.installHooks();
            // 原版服务端线程停止后最终bundle仍在后台构建；保留一条非守护生命周期线程，
            // 直到父进程完成耐久提交并显式dispose，不能被JVM自然退出截断最终数据。
            Thread lifetime = new Thread(() -> {
                try {
                    RELEASE.await();
                    // 这是专用的计算JVM，所有世界保存和最终bundle已交付后明确退出，避免
                    // 第三方模组的非守护线程继续占用JAR/世界锁。父断连且仍有世界时不跳过保存。
                    if (GroupRuntime.server() == null) System.exit(0);
                } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }, "p2p-worker-lifetime");
            lifetime.setDaemon(false);
            lifetime.start();
            MergeTriggers.setConsumer((blocking, chunk) -> {
                JsonObject event = new JsonObject(); event.addProperty("blockingGroup", blocking.toString()); event.addProperty("chunk", chunk.asString());
                rpc.emit("merge_trigger", event, null);
            });
            GroupRuntime.setStopListener(() -> rpc.emit("stopped", new JsonObject(), null));
            GroupRuntime.setExitBundleListener(bundle -> {
                try {
                    WorkerExitBundleStore.write(bundlePath, bundle);
                    // 收到父进程已读取并接受的确认才算发布成功；现有退出发布器会在失败时
                    // 重试，同一本机文件可幂等恢复，不能把一条未确认事件当成尾部已交付。
                    rpc.request("exit_bundle", bundle.manifest().toJson()).get();
                } catch (Exception error) { throw new IllegalStateException("退出尾部文件写入或交付失败", error); }
            });
            GroupRuntime.arm(new GroupRuntime.GroupPlan(context.groupId(), new RemoteControl(),
                    JsonUtil.getString(spec, "worldIdentity", ""),
                    JsonUtil.getInt(spec, "claimRetrySeconds", 2), server -> {
                JsonObject ready = new JsonObject(); ready.addProperty("port", server.getPort());
                ready.addProperty("runtimeId", spec.get("runtimeId").getAsString());
                ready.addProperty("runtimeHash", spec.get("runtimeHash").getAsString());
                ready.addProperty("manifestHash", manifest.globalHash());
                rpc.emit("ready", ready, null);
            }));
            return true;
        } catch (Exception error) {
            throw new IllegalStateException("独立计算进程初始化失败；拒绝退回无门控服务器", error);
        }
    }

    private static CompletionStage<ControlMessage> handle(ControlMessage message) {
        String op = JsonUtil.getString(message.json(), "op", "");
        try {
            switch (op) {
                case "remote_push" -> {
                    ControlMessage forwarded = WorkerEnvelope.unwrap(message);
                    MessageHandler handler = FORWARDED.get(forwarded.type());
                    if (handler == null) throw new IllegalArgumentException("未支持的运行时推送: " + forwarded.type());
                    handler.handle(new RemoteControl(), forwarded);
                }
                case "authorize_socket" -> IDENTITIES.grant(message.json().get("port").getAsInt(),
                        UUID.fromString(message.json().get("playerId").getAsString()), message.json().get("playerName").getAsString());
                case "exit_begin" -> {
                    return GroupRuntime.beginExitDrain(UUID.fromString(message.json().get("shutdownId").getAsString()))
                            .thenApply(manifest -> ControlMessage.of(MessageType.WORKER_EVENT, manifest.toJson()));
                }
                case "exit_adopt" -> GroupRuntime.adoptExitManifest(ExitManifest.fromJson(message.json()));
                case "stop" -> {
                    return stopWithMetadata();
                }
                case "freeze" -> GroupRuntime.setTickFrozen(message.json().get("frozen").getAsBoolean());
                case "metadata_disconnect" -> { remoteConnected = false; MetadataClient.disconnected(); }
                case "metadata_sync" -> {
                    remoteConnected = true;
                    return MetadataClient.synchronize(GroupRuntime.server(), new RemoteControl()).thenApply(snapshot -> {
                        JsonObject result = new JsonObject(); result.addProperty("worldId", snapshot.worldId());
                        result.addProperty("version", snapshot.version()); result.addProperty("term", snapshot.term());
                        return ControlMessage.of(MessageType.WORKER_EVENT, result);
                    });
                }
                case "retire" -> {
                    GroupRuntime.retireAfterTransfer(UUID.fromString(message.json().get("groupId").getAsString()));
                    retired = true; // 已完成双向字段屏障并提交所有权，停服不能再以旧写者提交。
                }
                case "wake" -> { if (GroupRuntime.claims() != null) GroupRuntime.claims().wakeBlockedClaims(UUID.fromString(message.json().get("groupId").getAsString())); }
                case "split_cancel" -> SplitMonitor.requestCancel(UUID.fromString(message.json().get("splitId").getAsString()));
                case "presync_cancel" -> cancelPresync();
                case "dispose" -> RELEASE.countDown();
                case "presync_send", "presync_receive" -> { return startPresync(message, op.equals("presync_send")); }
                default -> throw new IllegalArgumentException("未知计算进程操作: " + op);
            }
            return CompletableFuture.completedFuture(WorkerRpc.ok());
        } catch (Throwable error) { return CompletableFuture.failedFuture(error); }
    }

    /** 正常退出先冻结并确认模拟字段尾部；失败保留进程及冻结，交给已有耐久退出流程重试。 */
    private static synchronized CompletionStage<ControlMessage> stopWithMetadata() {
        MinecraftServer server = GroupRuntime.server();
        if (server == null) return CompletableFuture.failedFuture(new IllegalStateException("计算世界尚未启动"));
        if (stopFuture != null && !stopFuture.isCompletedExceptionally()) return stopFuture;
        stopping = true;
        GroupRuntime.setTickFrozen(true);
        CompletableFuture<?> flush = retired ? CompletableFuture.completedFuture(null)
                : MetadataEvents.flushTransferState(server);
        stopFuture = flush.thenApply(ignored -> {
            // halt(false)只请求停止，真正的save/最终bundle仍由原版生命周期完成。
            server.halt(false);
            return WorkerRpc.ok();
        });
        stopFuture.whenComplete((ignored, failure) -> { if (failure != null) stopping = false; });
        return stopFuture;
    }

    private static synchronized CompletionStage<ControlMessage> startPresync(ControlMessage message, boolean sending) {
        if (presyncStream != null) return CompletableFuture.failedFuture(new IllegalStateException("已有预同步事务"));
        MinecraftServer server = GroupRuntime.server();
        if (server == null || stopping) return CompletableFuture.failedFuture(new IllegalStateException("计算世界未就绪"));
        String id = message.json().get("streamId").getAsString();
        WorkerStream stream = new WorkerStream(rpc, id);
        presyncStream = stream; presyncId = id;
        CompletableFuture<ControlMessage> future = new CompletableFuture<>();
        ThreadPools.startBlocking("worker-presync", () -> {
            try {
                // level.dat ACK 不能证明实体/内存状态可迁移。原版龙复活中间态等不能由现有
                // 快照链承接时，在冻结和传输之前拒绝本次交接，让原组保持运行。
                CompletableFuture.runAsync(() -> MetadataRuntime.assertTransferSafe(server),
                        GroupControlTasks.executor(server)).get();
                java.util.function.Consumer<String> progress = phase -> {
                    // 旧区块协议的终结标记只说明区块尾部到达，不能提前穿过新增字段屏障。
                    if ("caught_up".equals(phase) || "switched".equals(phase)) return;
                    JsonObject event = new JsonObject(); event.addProperty("streamId", id); event.addProperty("phase", phase);
                    rpc.emit("presync_progress", event, null);
                };
                if (sending) new PresyncSender(stream, server, GroupRuntime.claims(), id, progress).run();
                else {
                    receiver = new PresyncReceiver(stream, server.getWorldPath(LevelResource.PLAYER_DATA_DIR), id, progress);
                    receiver.run();
                }
                // 快照生成期间还可能进入不可迁移状态；源端此时已冻结，最终再检查一次。
                CompletableFuture.runAsync(() -> MetadataRuntime.assertTransferSafe(server),
                        GroupControlTasks.executor(server)).get();
                java.io.DataInputStream input = new java.io.DataInputStream(stream.inputStream());
                java.io.DataOutputStream output = new java.io.DataOutputStream(stream.outputStream());
                MetadataSnapshot applied;
                if (sending) {
                    MetadataSnapshot durable = MetadataEvents.flushTransferState(server).get(30, TimeUnit.SECONDS);
                    applied = MetadataClient.synchronize(server, new RemoteControl()).get(30, TimeUnit.SECONDS);
                    if (!MetadataTransferFence.of(durable).coveredBy(applied))
                        throw new IOException("源端未确认已耐久提交的模拟字段尾部");
                    MetadataTransferFence fence = MetadataTransferFence.of(applied);
                    fence.write(output, false);
                    MetadataTransferFence acknowledgement = MetadataTransferFence.read(input, true);
                    if (!fence.worldId().equals(acknowledgement.worldId()) || fence.term() != acknowledgement.term()
                            || acknowledgement.version() < fence.version())
                        throw new IOException("接收端没有确认本次模拟字段尾部版本");
                } else {
                    MetadataTransferFence required = MetadataTransferFence.read(input, false);
                    applied = MetadataClient.synchronize(server, new RemoteControl()).get(30, TimeUnit.SECONDS);
                    if (!required.coveredBy(applied)) throw new IOException("接收端元数据未达到源端冻结尾部屏障");
                    CompletableFuture.runAsync(() -> MetadataRuntime.assertTransferSafe(server),
                            GroupControlTasks.executor(server)).get();
                    MetadataTransferFence.of(applied).write(output, true);
                }
                JsonObject confirmed = new JsonObject();
                confirmed.addProperty("worldId", applied.worldId());
                confirmed.addProperty("metadataVersion", applied.version());
                confirmed.addProperty("metadataTerm", applied.term());
                // 父进程先安装这个证据再上报终结阶段，不能复用启动时的旧 metadataVersion。
                JsonObject terminal = confirmed.deepCopy();
                terminal.addProperty("streamId", id); terminal.addProperty("phase", "caught_up");
                rpc.emit("presync_progress", terminal, null);
                future.complete(ControlMessage.of(MessageType.WORKER_EVENT, confirmed));
            } catch (Throwable error) { future.completeExceptionally(error); }
            finally { synchronized (WorkerBootstrap.class) { if (presyncStream == stream) { presyncStream = null; presyncId = null; receiver = null; } } }
        });
        return future;
    }
    private static synchronized void cancelPresync() {
        if (receiver != null) receiver.cancel();
        if (presyncStream != null) presyncStream.abort();
        PresyncStore.clear();
    }

    /** 子进程的业务服务只认识此接口；真实远端连接及唯一玩家会话留在父进程。 */
    private static final class RemoteControl implements ControlConnection {
        @Override public void send(ControlMessage message) {
            rpc.request(WorkerEnvelope.wrap("remote_send", message));
        }
        @Override public CompletableFuture<ControlMessage> request(ControlMessage message) {
            return rpc.request(WorkerEnvelope.wrap("remote_request", message)).thenApply(WorkerEnvelope::unwrap);
        }
        @Override public SocketAddress remoteAddress() { return rpc.connection() == null ? null : rpc.connection().remoteAddress(); }
        @Override public UUID peerId() { return NodeContext.get().clientId(); }
        @Override public void setPeerId(UUID peer) { throw new UnsupportedOperationException(); }
        @Override public boolean isOpen() { return remoteConnected && rpc.connection() != null && rpc.connection().isOpen(); }
        @Override public void close() { if (rpc.connection() != null) rpc.connection().close(); }
    }
}
