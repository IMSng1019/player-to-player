package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.*;
import imsng.player_to_player.env.EnvironmentManifest;
import imsng.player_to_player.metadata.*;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * 玩家游戏进程中的独立计算服务端管理器。控制连接和 P2P 会话只在父进程保留一份，
 * 子进程的世界读写/权限/合并/退出通过认证的回环 IPC 执行，绝不回落到未接管的世界。
 */
public final class WorkerProcess {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/worker");
    private static final Set<MessageType> PUSH_TYPES = Set.of(MessageType.CHUNK_OWNERSHIP_CHANGED,
            MessageType.COMMAND_RELAY, MessageType.CHAT_RELAY, MessageType.PERMISSION_RELAY,
            MessageType.PEARL_HANDOFF, MessageType.PEARL_LANDED,
            MessageType.METADATA_PATCH, MessageType.METADATA_SNAPSHOT, MessageType.METADATA_EVENT);
    private static volatile Session current;
    private static volatile Consumer<ControlConnection> stopListener;
    private static volatile Consumer<ExitBundle> exitBundleListener;
    private static volatile java.util.function.BiConsumer<ControlConnection, String> metadataRestartListener;
    private static volatile JsonObject permissions;
    private static volatile ControlConnection registeredRemote;
    private static volatile java.util.function.Function<Path, CompletableFuture<Void>> stoppedFlush =
            path -> CompletableFuture.completedFuture(null);
    private static final Object MEMBER_LOCK = new Object();
    private static volatile Map<UUID, String> members = Map.of();
    private WorkerProcess() { }

    private static final class Session {
        final UUID id = UUID.randomUUID();
        final UUID group;
        final ControlConnection remote;
        final ControlServer listener = new ControlServer("127.0.0.1", 0);
        final CompletableFuture<Integer> ready = new CompletableFuture<>();
        final CompletableFuture<Void> stopped = new CompletableFuture<>();
        final CompletableFuture<Void> exitPrepared = new CompletableFuture<>();
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        final CompletableFuture<Void> launchFinished = new CompletableFuture<>();
        final java.util.concurrent.atomic.AtomicBoolean disposing = new java.util.concurrent.atomic.AtomicBoolean();
        final Map<String, ReliableChannel> streams = new ConcurrentHashMap<>();
        final Map<String, Consumer<String>> progress = new ConcurrentHashMap<>();
        final ExecutorService streamExecutor = Executors.newFixedThreadPool(2, ThreadPools.namedFactory("worker-stream"));
        final WorkerRpc rpc;
        final String token;
        final Path sessionFile, bundleFile;
        volatile Path workingDirectory;
        volatile Path saveDirectory;
        final Path worldFolder;
        volatile Process process;
        volatile boolean retired, exiting, closing, cancelled;
        volatile int gamePort;
        volatile String expectedRuntimeHash;
        volatile String expectedManifestHash;
        volatile long metadataVersion = -1;
        volatile long metadataTerm;
        volatile ExitManifest exitManifest;
        boolean bundleDelivered;
        Session(UUID group, ControlConnection remote, Path runtimeRoot) {
            this.group = group; this.remote = remote;
            this.worldFolder = runtimeRoot.getParent();
            byte[] secret = new byte[32]; new SecureRandom().nextBytes(secret); token = HexFormat.of().formatHex(secret);
            sessionFile = runtimeRoot.resolve("session-" + id + ".json");
            bundleFile = runtimeRoot.resolve("exit-" + id + ".bin");
            rpc = new WorkerRpc(message -> handle(this, message), (connection, message) -> event(this, message));
        }
    }

    /** 必须在首次 ROLE_REQUEST 前注册，权限提前到达时也缓存并在子服就绪前重放。 */
    public static void registerForwarders(ControlClient client, ControlConnection remote) {
        registeredRemote = remote;
        client.onDisconnect(connection -> {
            Session session = current;
            if (connection == remote && session != null && session.remote == remote && session.rpc.connection() != null)
                session.rpc.request("metadata_disconnect", null).exceptionally(error -> {
                    LOGGER.error("中心连接断开后的模拟暂停未获子进程确认", error); return null;
                });
        });
        synchronized (MEMBER_LOCK) { permissions = null; members = Map.of(); MEMBER_LOCK.notifyAll(); }
        for (MessageType type : PUSH_TYPES) client.on(type, (connection, message) -> {
            if (registeredRemote != remote) return;
            if (type == MessageType.PERMISSION_RELAY) cachePermissions(message.json());
            Session session = current;
            if (session != null && session.remote == remote && !session.retired && session.rpc.connection() != null) {
                session.rpc.request(WorkerEnvelope.wrap("remote_push", message))
                        .exceptionally(error -> { LOGGER.error("子服控制消息执行失败: {}", type, error); return null; });
            }
        });
    }
    private static void cachePermissions(JsonObject json) {
        Map<UUID, String> next = new HashMap<>();
        if (json.has("members")) for (var value : json.getAsJsonArray("members")) {
            JsonObject member = value.getAsJsonObject();
            next.put(UUID.fromString(member.get("uuid").getAsString()), member.get("name").getAsString());
        }
        synchronized (MEMBER_LOCK) { permissions = json.deepCopy(); members = Map.copyOf(next); MEMBER_LOCK.notifyAll(); }
    }
    public static void updatePermissions(JsonObject json) {
        cachePermissions(json);
        Session session = current;
        if (session != null && session.rpc.connection() != null)
            session.rpc.request(WorkerEnvelope.wrap("remote_push", ControlMessage.of(MessageType.PERMISSION_RELAY, json)));
    }
    public static void setStopListener(Runnable listener) { stopListener = connection -> listener.run(); }
    public static void setStopListener(Consumer<ControlConnection> listener) { stopListener = listener; }
    public static void setExitBundleListener(Consumer<ExitBundle> listener) { exitBundleListener = listener; }
    public static void setMetadataRestartListener(java.util.function.BiConsumer<ControlConnection, String> listener) {
        metadataRestartListener = listener;
    }
    public static void setStoppedFlush(java.util.function.Function<Path, CompletableFuture<Void>> flush) { stoppedFlush = flush; }
    public static Path currentDirectory() { Session session = current; return session == null ? null : session.workingDirectory; }
    public static Path currentWorldFolder() { Session session = current; return session == null ? null : session.worldFolder; }
    public static Path currentSaveDirectory() { Session session = current; return session == null ? null : session.saveDirectory; }
    public static boolean isRunning() { return current != null; }
    public static boolean isStarting() {
        Session session = current;
        return session != null && session.gamePort <= 0 && !session.closing;
    }
    public static boolean isManagedWorld() {
        Session session = current;
        return session != null && session.gamePort > 0 && !session.retired && !session.closing;
    }
    public static UUID activeGroupId() { Session session = current; return session == null ? null : session.group; }
    public static ControlConnection connection() { Session session = current; return session == null ? null : session.remote; }
    public static int currentPort() { Session session = current; return session == null || session.retired ? 0 : session.gamePort; }

    /** 后台调用，只有真实 SERVER_STARTED + 权限重放成功之后才返回实际接待端口。 */
    public static int start(Path worldFolder, Path environment, String worldPath, Path savesRoot,
                            String saveName, String identity, UUID group, ControlConnection remote,
                            Map<String, String> properties, EnvironmentManifest primaryManifest,
                            String runtimeHash) throws Exception {
        Path runtimeRoot = worldFolder.resolve("runtime");
        Session session = new Session(group, remote, runtimeRoot);
        session.saveDirectory = savesRoot.resolve(saveName).toAbsolutePath().normalize();
        synchronized (WorkerProcess.class) {
            if (current != null) throw new IllegalStateException("旧计算进程尚未结束，拒绝并发打开同一世界");
            current = session;
        }
        try {
            session.rpc.register(session.listener);
            session.listener.on(MessageType.HELLO, (connection, message) -> {
                boolean valid = connection.remoteAddress() instanceof InetSocketAddress inet
                        && inet.getAddress().isLoopbackAddress()
                        && session.id.toString().equals(JsonUtil.getString(message.json(), "runtimeId", ""))
                        && MessageDigest.isEqual(session.token.getBytes(StandardCharsets.US_ASCII),
                        JsonUtil.getString(message.json(), "token", "").getBytes(StandardCharsets.US_ASCII));
                synchronized (session) {
                    valid &= session.rpc.connection() == null && current == session && !session.closing;
                    if (valid) { connection.setPeerId(session.id); session.rpc.attach(connection); }
                }
                JsonObject ack = new JsonObject(); ack.addProperty("accepted", valid);
                connection.send(message.reply(MessageType.HELLO_ACK, ack, null));
                if (!valid) connection.close();
            });
            session.listener.onDisconnect(connection -> {
                if (session.rpc.connection() != connection) return;
                session.rpc.disconnected();
                IOException failure = new IOException("独立计算进程连接中断");
                session.ready.completeExceptionally(failure);
                if (!recoverExitBundle(session)) session.stopped.completeExceptionally(failure);
                if (!session.closing && !session.exiting && !session.retired) {
                    Consumer<ControlConnection> callback = stopListener;
                    if (callback != null) ThreadPools.io().execute(() -> callback.accept(session.remote));
                }
            });
            session.listener.start();
            requireStarting(session);
            int gamePort;
            try (ServerSocket reserve = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) { gamePort = reserve.getLocalPort(); }
            Map<String, String> localProperties = new HashMap<>(properties);
            // 安装之后、真正打开世界之前再获取一次，避免环境预检/角色选举期间的元数据变化
            // 被旧启动参数覆盖。Player 和本地模拟 Time 由安装器保留。
            MetadataSnapshot metadata = MetadataClient.requestSnapshot(remote, identity).get(20, TimeUnit.SECONDS);
            MetadataFiles.prepareSave(environment.resolve(worldPath), session.saveDirectory, metadata, System.currentTimeMillis());
            applyMetadataProperties(localProperties, metadata);
            localProperties.put("p2p.local-port", Integer.toString(gamePort));
            Path working = RuntimeInstallation.install(environment, runtimeRoot.resolve("primary"), worldPath, localProperties);
            requireStarting(session);
            session.workingDirectory = working;
            WorkerEnvironmentProof.verifyInstalled(working, savesRoot.resolve(saveName), worldPath, primaryManifest);
            if (runtimeHash == null || runtimeHash.isBlank()) throw new IOException("缺少环境运行版本");
            session.expectedRuntimeHash = runtimeHash;
            session.expectedManifestHash = primaryManifest.globalHash();
            JsonObject spec = new JsonObject();
            spec.addProperty("ipcPort", session.listener.boundPort()); spec.addProperty("token", session.token);
            spec.addProperty("runtimeId", session.id.toString()); spec.addProperty("clientId", NodeContext.get().clientId().toString());
            spec.addProperty("playerName", NodeContext.get().playerName()); spec.addProperty("groupId", group.toString());
            spec.addProperty("worldIdentity", identity); spec.addProperty("claimRetrySeconds", NodeContext.get().config().chunkClaimRetrySeconds);
            spec.addProperty("commandRoutingSupported", true);
            spec.addProperty("runtimeHash", runtimeHash);
            spec.addProperty("manifestHash", primaryManifest.globalHash());
            spec.add("primaryManifest", primaryManifest.toJson());
            spec.addProperty("metadataSnapshot", Base64.getEncoder().encodeToString(
                    MetadataWire.encode(MetadataWire.snapshotToTag(metadata))));
            JsonUtil.writeFileAtomic(session.sessionFile, spec);
            String executable = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win") ? "java.exe" : "java";
            Path java = Path.of(System.getProperty("java.home"), "bin", executable);
            List<String> command = RuntimeLaunchCommand.build(java, working, savesRoot, saveName, session.sessionFile);
            // ProcessBuilder 不经过 shell；环境中任何脚本/JVM 参数均不能借此执行。
            synchronized (session) {
                // 与dispose标记互斥：安装IO结束期间玩家可能已退出，不能在监听器被清理后
                // 又创建一个失去管理的JVM，也不能把取消前的READY当成新会话的启动完成。
                requireStarting(session);
                session.process = new ProcessBuilder(command).directory(working.toFile())
                        .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(runtimeRoot.resolve("worker-console.log").toFile())).start();
            }
            session.process.onExit().thenAccept(process -> {
                IOException error = new IOException("计算进程退出，exitCode=" + process.exitValue());
                session.ready.completeExceptionally(error);
                if (!session.stopped.isDone() && !recoverExitBundle(session)) session.stopped.completeExceptionally(error);
            });
            int port = session.ready.get(180, TimeUnit.SECONDS);
            requireStarting(session);
            if (port != gamePort) throw new IOException("计算进程报告了不匹配的游戏端口");
            JsonObject cached = permissions;
            if (cached != null) session.rpc.request(WorkerEnvelope.wrap("remote_push",
                    ControlMessage.of(MessageType.PERMISSION_RELAY, cached))).get(20, TimeUnit.SECONDS);
            // READY仅证明服务端已启动；还需通过运行时RPC补齐启动期间的变化并取得中心ACK。
            synchronizeMetadata().get(30, TimeUnit.SECONDS);
            synchronized (session) { requireStarting(session); session.gamePort = port; }
            LOGGER.info("独立计算服务端已就绪: runtime={} port={}", session.id, port);
            return port;
        } catch (Exception error) {
            session.launchFinished.complete(null);
            closeFailed(session);
            throw error;
        } finally { session.launchFinished.complete(null); }
    }

    /** 与 level.dat 重叠的原版启动参数始终取自本次快照，其余运行参数继续用环境预检值。 */
    static void applyMetadataProperties(Map<String, String> properties, MetadataSnapshot snapshot) {
        var data = snapshot.data();
        if (data.contains("Difficulty", net.minecraft.nbt.Tag.TAG_ANY_NUMERIC)) {
            properties.put("difficulty", switch (data.getByte("Difficulty")) {
                case 0 -> "peaceful"; case 1 -> "easy"; case 3 -> "hard"; default -> "normal";
            });
        }
        if (data.contains("GameType", net.minecraft.nbt.Tag.TAG_ANY_NUMERIC)) {
            properties.put("gamemode", switch (data.getInt("GameType")) {
                case 1 -> "creative"; case 2 -> "adventure"; case 3 -> "spectator"; default -> "survival";
            });
        }
        if (data.contains("hardcore")) properties.put("hardcore", Boolean.toString(data.getBoolean("hardcore")));
    }

    /** 运行中的接管屏障：只发送RPC，不触碰世界目录中的任何文件。 */
    public static CompletableFuture<ControlMessage> synchronizeMetadata() {
        Session session = requireSession();
        return session.rpc.request("metadata_sync", null).orTimeout(30, TimeUnit.SECONDS).thenApply(reply -> {
            if (current != session || session.closing) throw new CompletionException(new IOException("元数据确认所属计算进程已过期"));
            session.metadataVersion = reply.json().get("version").getAsLong();
            session.metadataTerm = reply.json().get("term").getAsLong();
            return reply;
        });
    }
    public static long metadataVersion() { Session session = current; return session == null ? -1 : session.metadataVersion; }
    public static long metadataTerm() { Session session = current; return session == null ? 0 : session.metadataTerm; }

    private static CompletionStage<ControlMessage> handle(Session session, ControlMessage message) {
        String op = JsonUtil.getString(message.json(), "op", "");
        try {
            if (current != session || session.closing) throw new IOException("计算进程代次已过期");
            switch (op) {
                case "remote_request" -> {
                    return session.remote.request(WorkerEnvelope.unwrap(message)).thenApply(reply -> WorkerEnvelope.wrap("remote_reply", reply));
                }
                case "remote_send" -> session.remote.send(WorkerEnvelope.unwrap(message));
                case "exit_bundle" -> {
                    return CompletableFuture.supplyAsync(() -> {
                        try { deliverExitBundle(session); return WorkerRpc.ok(); }
                        catch (IOException error) { throw new CompletionException(error); }
                    }, ThreadPools.io());
                }
                case "stream_read", "stream_write", "stream_abort" -> {
                    String id = message.json().get("streamId").getAsString();
                    ReliableChannel channel = session.streams.get(id);
                    if (channel == null) throw new IOException("预同步通道已移交或取消");
                    if (op.equals("stream_abort")) { channel.abort(); return CompletableFuture.completedFuture(WorkerRpc.ok()); }
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            if (op.equals("stream_write")) {
                                if (message.binary().length > 65536) throw new IOException("IPC流写入超限");
                                channel.outputStream().write(message.binary()); channel.outputStream().flush(); return WorkerRpc.ok();
                            }
                            int length = message.json().get("length").getAsInt();
                            if (length <= 0 || length > 65536) throw new IOException("IPC流读取超限");
                            byte[] bytes = new byte[length]; int n = channel.inputStream().read(bytes);
                            JsonObject result = new JsonObject(); result.addProperty("eof", n < 0);
                            return ControlMessage.of(MessageType.WORKER_EVENT, result, n < 0 ? null : Arrays.copyOf(bytes, n));
                        } catch (IOException error) { throw new CompletionException(error); }
                    }, session.streamExecutor);
                }
                default -> throw new IOException("未知父进程操作: " + op);
            }
            return CompletableFuture.completedFuture(WorkerRpc.ok());
        } catch (Throwable error) { return CompletableFuture.failedFuture(error); }
    }

    private static void event(Session session, ControlMessage message) {
        if (current != session) return;
        switch (JsonUtil.getString(message.json(), "op", "")) {
            case "ready" -> {
                if (!session.id.toString().equals(JsonUtil.getString(message.json(), "runtimeId", ""))
                        || !session.expectedRuntimeHash.equals(JsonUtil.getString(message.json(), "runtimeHash", ""))
                        || !session.expectedManifestHash.equals(JsonUtil.getString(message.json(), "manifestHash", "")))
                    session.ready.completeExceptionally(new IOException("计算进程启动证明与冻结环境版本不一致"));
                else session.ready.complete(message.json().get("port").getAsInt());
            }
            case "server_stopped" -> session.stopped.complete(null);
            case "metadata_restart_required" -> {
                var callback = metadataRestartListener;
                if (callback != null && !session.retired && !session.closing)
                    callback.accept(session.remote, JsonUtil.getString(message.json(), "fields", "启动设置"));
            }
            case "stopped" -> {
                Consumer<ControlConnection> callback = stopListener;
                if (!session.exiting && !session.retired && !session.closing && callback != null)
                    ThreadPools.io().execute(() -> callback.accept(session.remote));
            }
            case "merge_trigger" -> MergeTriggers.trigger(UUID.fromString(message.json().get("blockingGroup").getAsString()),
                    ChunkKey.parse(message.json().get("chunk").getAsString()));
            case "presync_progress" -> {
                if (message.json().has("metadataVersion")) {
                    session.metadataVersion = message.json().get("metadataVersion").getAsLong();
                    session.metadataTerm = message.json().get("metadataTerm").getAsLong();
                }
                Consumer<String> progress = session.progress.get(message.json().get("streamId").getAsString());
                if (progress != null) progress.accept(message.json().get("phase").getAsString());
            }
            default -> LOGGER.warn("未知计算进程事件: {}", JsonUtil.getString(message.json(), "op", ""));
        }
    }

    public static CompletableFuture<ExitManifest> beginExitDrain(UUID shutdown) {
        Session session = requireSession(); session.exiting = true;
        JsonObject json = new JsonObject(); json.addProperty("shutdownId", shutdown.toString());
        return session.rpc.request("exit_begin", json).thenApply(reply -> {
            ExitManifest manifest = ExitManifest.fromJson(reply.json()); session.exitManifest = manifest; return manifest;
        });
    }
    public static void adoptExitManifest(ExitManifest manifest) {
        Session session = requireSession(); session.rpc.request("exit_adopt", manifest.toJson()).join(); session.exitManifest = manifest;
    }
    public static CompletableFuture<Void> stopForExit() {
        Session session = requireSession();
        return session.rpc.request("stop", null).thenCompose(reply -> session.stopped)
                .thenCompose(ignored -> flushStopped(session));
    }
    public static void freeze(boolean frozen) {
        Session session = current;
        if (session == null) return;
        JsonObject json = new JsonObject(); json.addProperty("frozen", frozen);
        session.rpc.request("freeze", json).exceptionally(error -> { LOGGER.error("计算世界冻结操作失败", error); return null; });
    }
    /** 提交回调可异步等待此 ACK；收到之前绝不让出旧世界、也不宣布退休成功。 */
    public static CompletableFuture<Void> retireAfterTransfer(UUID group) {
        Session session = requireSession();
        JsonObject json = new JsonObject(); json.addProperty("groupId", group.toString());
        return session.rpc.request("retire", json).thenAccept(reply -> session.retired = true);
    }
    public static void wakeBlockedClaims(UUID group) {
        Session session = current; if (session == null) return;
        JsonObject json = new JsonObject(); json.addProperty("groupId", group.toString()); session.rpc.request("wake", json);
    }
    public static void cancelSplit(UUID split) {
        Session session = current; if (session == null) return;
        JsonObject json = new JsonObject(); json.addProperty("splitId", split.toString()); session.rpc.request("split_cancel", json);
    }
    public static void cancelPresync() { Session session = current; if (session != null) session.rpc.request("presync_cancel", null); }
    public static void runPresync(ReliableChannel channel, String id, boolean sending, Consumer<String> progress) throws Exception {
        Session session = requireSession();
        if (session.streams.putIfAbsent(id, channel) != null) throw new IllegalStateException("重复预同步通道");
        session.progress.put(id, progress);
        try {
            JsonObject json = new JsonObject(); json.addProperty("streamId", id);
            synchronizeMetadata().get();
            ControlMessage confirmed = session.rpc.request(sending ? "presync_send" : "presync_receive", json).get();
            session.metadataVersion = confirmed.json().get("metadataVersion").getAsLong();
            session.metadataTerm = confirmed.json().get("metadataTerm").getAsLong();
        } finally { session.streams.remove(id); session.progress.remove(id); }
    }

    /** 在首个MC字节前，以已认证P2P peer UUID绑定该TCP连接；禁止名字猜测回退。 */
    public static void authorizeGameSocket(Socket socket, UUID playerId) throws IOException {
        Session session = requireSession();
        if (!socket.getInetAddress().isLoopbackAddress() || socket.getPort() != session.gamePort)
            throw new IOException("游戏连接目标不属于当前计算进程");
        String name;
        if (playerId.equals(NodeContext.get().clientId())) name = NodeContext.get().playerName();
        else {
            // 中心服务的ROLE_ASSIGN/MERGE_COMMIT先于下一server tick的权限名单推送。
            // 已认证P2P身份可短暂等待权威名单，绝不猜测名字或把时序抖动当永久拒绝。
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            synchronized (MEMBER_LOCK) {
                while (true) {
                    if (current != session || session.retired || session.closing)
                        throw new IOException("等待授权期间计算进程已切换");
                    JsonObject cached = permissions;
                    boolean sameGroup = cached != null && session.group.toString().equals(JsonUtil.getString(cached, "groupId", ""));
                    name = sameGroup ? members.get(playerId) : null;
                    if (name != null) break;
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) throw new IOException("等待服务端组成员授权超时: " + playerId);
                    try { TimeUnit.NANOSECONDS.timedWait(MEMBER_LOCK, remaining); }
                    catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IOException("等待玩家授权被中断", error); }
                }
            }
        }
        JsonObject json = new JsonObject(); json.addProperty("port", socket.getLocalPort());
        json.addProperty("playerId", playerId.toString()); json.addProperty("playerName", name);
        try { session.rpc.request("authorize_socket", json).get(5, TimeUnit.SECONDS); }
        catch (Exception error) { throw new IOException("游戏连接身份登记失败", error); }
    }

    /** 房主也通过父进程桥接，确保和副客户端使用完全相同的权威UUID登录路径。 */
    public static int openLocalPlayerBridge() throws IOException {
        Session session = requireSession();
        ServerSocket listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        listener.setSoTimeout(60000);
        ThreadPools.startBlocking("worker-local-login", () -> {
            try (listener) {
                Socket game = listener.accept();
                Socket server = new Socket();
                try {
                    server.connect(new InetSocketAddress("127.0.0.1", session.gamePort), 10000);
                    authorizeGameSocket(server, NodeContext.get().clientId());
                    pumpSockets(game, server);
                } catch (Exception error) { game.close(); server.close(); throw error; }
            } catch (Exception error) { LOGGER.error("本地主玩家登录桥失败", error); }
        });
        return listener.getLocalPort();
    }
    private static void pumpSockets(Socket game, Socket server) throws IOException {
        game.setTcpNoDelay(true); server.setTcpNoDelay(true);
        Runnable close = () -> { try { game.close(); } catch (IOException ignored) { } try { server.close(); } catch (IOException ignored) { } };
        ThreadPools.startBlocking("worker-game-up", () -> pump(game, server, close));
        ThreadPools.startBlocking("worker-game-down", () -> pump(server, game, close));
    }
    private static void pump(Socket from, Socket to, Runnable close) {
        try { from.getInputStream().transferTo(to.getOutputStream()); } catch (IOException ignored) { } finally { close.run(); }
    }

    /** 合并退休后停止旧进程；已收到COMMIT，关闭旧子服不能触发世界会话拆除。 */
    public static void stopRetired() throws Exception {
        Session session = requireSession();
        if (!session.retired) throw new IllegalStateException("旧计算进程尚未退休");
        session.rpc.request("stop", null).get();
        session.stopped.get();
        flushStopped(session).get();
        dispose(session);
    }
    public static void closeSession() {
        Session session = current;
        if (session == null) return;
        boolean starting;
        synchronized (session) {
            starting = session.gamePort <= 0;
            if (starting) {
                session.cancelled = true;
                session.ready.completeExceptionally(new CancellationException("玩家已离开，计算进程启动取消"));
            }
        }
        ThreadPools.startBlocking("worker-session-close", () -> {
            try {
                if (starting) {
                    synchronized (session) { if (session.process != null && session.process.isAlive()) session.process.destroy(); }
                    return;
                }
                if (!session.stopped.isDone() && session.rpc.connection() != null) {
                    session.rpc.request("stop", null).get(); session.stopped.get();
                }
                if (session.workingDirectory != null) flushStopped(session).get();
            } catch (Exception error) { LOGGER.warn("计算进程停止异常，保存日志供恢复", error); }
            finally { dispose(session); }
        });
    }
    private static void dispose(Session session) {
        if (!session.disposing.compareAndSet(false, true)) { session.closed.join(); return; }
        synchronized (session) { session.closing = true; }
        try {
            // 安装可能仍在复制环境。即便尚无子JVM，也必须等本代安装任务退出，才允许
            // 新会话复用运行目录；否则旧安装的rename会覆盖新实例刚安装的文件。
            session.launchFinished.join();
            if (session.rpc.connection() != null) {
                try { session.rpc.request("dispose", null).get(5, TimeUnit.SECONDS); }
                catch (Exception error) { LOGGER.debug("子服释放确认未到达，将等待进程实际退出", error); }
            }
            session.listener.stop(); session.rpc.disconnected(); session.streamExecutor.shutdownNow();
            Process process = session.process;
            if (process != null && process.isAlive()) {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroy();
                    if (!process.waitFor(5, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        if (!process.waitFor(10, TimeUnit.SECONDS))
                            throw new IllegalStateException("旧计算进程无法退出，保留实例占用防止并发写世界");
                    }
                }
            }
            // 只有操作系统确认旧JVM退出后才能允许下次安装/启动，Windows JAR锁和世界锁
            // 都属于进程生命周期，SERVER_STOPPED 或 RPC 断开本身不足以证明资源已释放。
            synchronized (WorkerProcess.class) { if (current == session) current = null; }
            try { Files.deleteIfExists(session.sessionFile); } catch (IOException error) { LOGGER.warn("移除子进程会话文件失败", error); }
            session.closed.complete(null);
        } catch (Throwable failure) {
            session.closed.completeExceptionally(failure);
            LOGGER.error("计算进程清理失败，禁止复用尚未释放的运行目录", failure);
        }
    }
    private static CompletableFuture<Void> flushStopped(Session session) {
        try {
            return stoppedFlush.apply(session.workingDirectory).orTimeout(30, TimeUnit.SECONDS)
                    .exceptionally(error -> {
                        // 环境配置冲突/断网不允许覆盖服务端，也不把已经耐久的世界退出永久卡住；
                        // 完整运行目录保留，下次会话/人工解决可重试这些修改。
                        LOGGER.warn("停服环境变更未上传，已保留本地运行目录: {}", session.workingDirectory, error);
                        return null;
                    });
        } catch (RuntimeException error) {
            LOGGER.warn("停服环境变更检查失败，保留本地运行目录", error);
            return CompletableFuture.completedFuture(null);
        }
    }
    private static void deliverExitBundle(Session session) throws IOException {
        ExitBundle bundle = WorkerExitBundleStore.read(session.bundleFile);
        ExitManifest expected = session.exitManifest;
        if (expected == null || !expected.equals(bundle.manifest()))
            throw new IOException("子进程最终尾部与已确认的退出清单不一致");
        synchronized (session) {
            if (session.bundleDelivered) return;
            Consumer<ExitBundle> callback = exitBundleListener;
            if (callback == null) throw new IOException("退出协调器未订阅计算进程尾部");
            callback.accept(bundle);
            session.bundleDelivered = true;
        }
    }
    private static boolean recoverExitBundle(Session session) {
        if (!session.exiting || session.exitManifest == null || !Files.isRegularFile(session.bundleFile)) return false;
        // 文件只会在SERVER_STOPPED之后原子发布。回环事件丢失时可以恢复交付，
        // 但必须先完整解析和核对清单，不能仅凭文件名存在宣布停服/数据成功。
        ThreadPools.io().execute(() -> {
            try { deliverExitBundle(session); session.stopped.complete(null); }
            catch (Exception error) { session.stopped.completeExceptionally(error); LOGGER.error("退出尾部恢复失败", error); }
        });
        return true;
    }
    private static void closeFailed(Session session) {
        if (session.process != null && session.process.isAlive()) session.process.destroy();
        dispose(session);
    }
    private static Session requireSession() {
        Session session = current;
        if (session == null || session.closing) throw new IllegalStateException("没有可用的独立计算进程"); return session;
    }
    private static void requireStarting(Session session) throws IOException {
        if (current != session || session.cancelled || session.closing)
            throw new IOException("计算进程启动所属世界会话已取消");
    }
}
