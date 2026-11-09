package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.core.NodeMode;
import imsng.player_to_player.env.EnvironmentScanner;
import imsng.player_to_player.metadata.MetadataSnapshot;
import imsng.player_to_player.metadata.MetadataWire;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.tools.ToolProvider;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 真实 Fabric 计算子进程内的距离分离探针。测试玩家不连接图形端，只读写隔离测试存档；
 * 基础用例验证生产 tick 钩子发起准备，完整用例让玩家真实加载区块、传送、离组，
 * 等待最终玩家写回及旧 holder 卸载后提交。中心侧仅提供明确的协议应答，
 * 此测试不等价于公网重连、中心选主或新计算端启动。
 */
@EnabledIfSystemProperty(named = "p2p.worker.smoke", matches = "true")
@EnabledIfSystemProperty(named = "p2p.worker.developmentSpec", matches = ".+")
class SplitMonitorProcessSmokeTest {
    private record Scenario(int viewDistance, int simulationDistance, int initialX, int initialZ,
                            int destinationX, int destinationZ, boolean moveHost, int observationSeconds,
                            boolean holdBoundaryClaim) { }

    @Test
    void distantMembersReachSplitPreparationInARealWorker() throws Exception {
        runProbe(false, false);
    }

    @Test
    void teleportingMemberCommitsAfterFinalPlayerUploadAndRealChunkUnload() throws Exception {
        runProbe(true, true);
    }

    @Test
    void nearbySeparatedRenderAreasCommitWithTheObservedViewDistance() throws Exception {
        // 26.11.9现场：两人先在(32,34)，主端传送至原点附近，副端留在原位置；
        // 两方渲染范围已经分开，但旧规划器会把半径24的生成依赖矩形当作玩家需求。
        runProbe(true, true, new Scenario(10, 10, 32, 34, 0, -1, true, 35, true));
    }

    private void runProbe(boolean realChunks, boolean completeSplit) throws Exception {
        runProbe(realChunks, completeSplit, new Scenario(2, 2, 0, 0, 400, 0, false, realChunks ? 120 : 35, false));
    }

    private void runProbe(boolean realChunks, boolean completeSplit, Scenario scenario) throws Exception {
        Path root = Path.of("build", "split-probe", Long.toString(System.currentTimeMillis())).toAbsolutePath();
        Path environment = root.resolve("environment");
        Files.createDirectories(environment.resolve("mods"));
        createProbe(root, environment.resolve("mods/split-probe.jar"), realChunks, scenario);
        NodeContext context = NodeContext.get();
        P2PPaths paths = new P2PPaths(root.resolve("game"));
        paths.ensureBaseDirs();
        context.setPaths(paths);
        context.setConfig(GlobalConfig.loadOrCreate(paths.globalConfig(), NodeMode.CLIENT));
        context.setMode(NodeMode.CLIENT);
        context.setClientRole(ClientRole.PRIMARY);
        context.setClientId(UUID.randomUUID());
        context.setGroupId(context.clientId());
        context.setPlayerName("SplitHost");
        // 沿用既有真实 worker 冒烟的三维度原版元数据，避免探针另造不同世界配置。
        MetadataSnapshot metadata = WorkerProcessSmokeTest.metadataFixture();
        byte[] snapshot = MetadataWire.encode(MetadataWire.snapshotToTag(metadata));
        UUID sourceGroup = context.groupId();
        var chunkData = new java.util.concurrent.ConcurrentHashMap<ChunkKey, byte[]>();
        var playerData = new java.util.concurrent.ConcurrentHashMap<String, byte[]>();
        var registry = new ChunkRegistry(null, null);
        var collisions = new java.util.concurrent.CopyOnWriteArrayList<String>();
        var sourceReady = new CompletableFuture<JsonObject>();
        var reloadSource = new java.util.concurrent.atomic.AtomicBoolean();
        var checkingFollower = new java.util.concurrent.atomic.AtomicBoolean();
        var rejectedUploads = new java.util.concurrent.CopyOnWriteArrayList<String>();
        var boundaryClaimHeld = new java.util.concurrent.atomic.AtomicBoolean();
        var heldClaim = new java.util.concurrent.atomic.AtomicReference<CompletableFuture<ControlMessage>>();
        var heldReply = new java.util.concurrent.atomic.AtomicReference<ControlMessage>();
        var prematurePreparation = new java.util.concurrent.atomic.AtomicInteger();
        var prepareAt = new java.util.concurrent.atomic.AtomicLong();
        CompletableFuture<JsonObject> prepared = new CompletableFuture<>();
        CompletableFuture<JsonObject> committed = new CompletableFuture<>();
        ControlConnection remote = new ControlConnection() {
            public void send(ControlMessage message) { }
            public synchronized CompletableFuture<ControlMessage> request(ControlMessage message) {
                // 模拟中心和真实写入协调器一样，把所有权检查、最终写回、release放在同一
                // 线性化边界内。特别不能让释放后的迟到批次覆盖已确认的最终FULL快照。
                JsonObject response = new JsonObject();
                MessageType type;
                byte[] binary = null;
                switch (message.type()) {
                    case METADATA_SNAPSHOT_REQUEST, METADATA_CATCHUP -> {
                        type = MessageType.METADATA_SNAPSHOT; binary = snapshot;
                    }
                    case METADATA_ACK, METADATA_EVENT -> {
                        type = MessageType.METADATA_ACK; response.addProperty("accepted", true);
                    }
                    case CHUNK_CLAIM_REQUEST -> {
                        ChunkKey key = key(message);
                        UUID group = UUID.fromString(message.json().get("groupId").getAsString());
                        var result = registry.tryClaim(key, group);
                        type = MessageType.CHUNK_CLAIM_RESPONSE;
                        response.addProperty("granted", result.granted());
                        response.addProperty("hasServerData", chunkData.containsKey(key));
                        if (!result.granted()) {
                            response.addProperty("blockingGroup", result.blockingGroup().toString());
                            response.addProperty("blockingChunk", result.blockingChunk().asString());
                            if (checkingFollower.get()) collisions.add(key.asString() + " -> " + result.blockingChunk().asString());
                        }
                        if (scenario.holdBoundaryClaim() && !checkingFollower.get() && result.granted()
                                && key.x() == scenario.destinationX() + scenario.viewDistance() + 2
                                && key.z() == scenario.destinationZ() && boundaryClaimHeld.compareAndSet(false, true)) {
                            // 明确悬置普通FULL外边缘的一次申请超过十秒，不能靠随机慢生成碰运气。
                            // 无客户端同步登录依赖此边缘，服务器tick及距离计时仍可正常推进。
                            var delayed = new CompletableFuture<ControlMessage>();
                            ControlMessage answer = message.reply(type, response, null);
                            heldReply.set(answer); heldClaim.set(delayed);
                            CompletableFuture.delayedExecutor(14, TimeUnit.SECONDS).execute(() -> delayed.complete(answer));
                            return delayed;
                        }
                    }
                    case CHUNK_DATA_REQUEST -> {
                        type = MessageType.CHUNK_DATA;
                        binary = chunkData.get(key(message)); response.addProperty("exists", binary != null);
                    }
                    case PLAYER_DATA_UPLOAD -> {
                        playerData.put(message.json().get("playerUuid").getAsString(), message.binary().clone());
                        type = MessageType.PLAYER_DATA; response.addProperty("written", true);
                    }
                    case CHUNK_DATA_UPLOAD -> {
                        boolean writable = context.clientId().equals(registry.ownerOf(key(message)));
                        if (writable) chunkData.put(key(message), message.binary().clone());
                        else rejectedUploads.add("single:" + key(message).asString());
                        type = message.type(); response.addProperty("written", writable);
                    }
                    case CHUNK_DATA_BATCH -> {
                        var batch = ChunkDeltaBatch.decode(message.binary());
                        boolean writable = batch.deltas().stream().allMatch(delta -> context.clientId().equals(registry.ownerOf(delta.key())));
                        if (writable) for (var delta : batch.deltas()) chunkData.put(delta.key(), delta.payload());
                        else rejectedUploads.add("batch:" + batch.deltas().stream()
                                .filter(delta -> !context.clientId().equals(registry.ownerOf(delta.key())))
                                .map(delta -> delta.key().asString()).limit(10).toList());
                        type = message.type(); response.addProperty("written", writable);
                    }
                    case CHUNK_RELEASE -> {
                        ChunkKey key = key(message);
                        UUID group = UUID.fromString(message.json().get("groupId").getAsString());
                        boolean writable = group.equals(registry.ownerOf(key));
                        if (writable && message.binary() != null && message.binary().length > 0) chunkData.put(key, message.binary().clone());
                        boolean released = writable && registry.release(key, group);
                        type = MessageType.CHUNK_RELEASE_ACK;
                        response.addProperty("released", released); response.addProperty("finalDataWritten", released);
                    }
                    case CHUNK_RELEASE_RECEIPT_CONFIRM -> {
                        type = MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM; response.addProperty("accepted", true);
                    }
                    case SPLIT_REQUEST -> {
                        String phase = JsonUtil.getString(message.json(), "phase", "");
                        if (phase.equals("prepare")) {
                            prepareAt.compareAndSet(0, System.currentTimeMillis());
                            try {
                                Path runtime = WorkerProcess.currentDirectory();
                                if (Files.isRegularFile(runtime.resolve("split-probe-diagnostics.json")))
                                    Files.copy(runtime.resolve("split-probe-diagnostics.json"), root.resolve("before-prepare-runtime.json"),
                                            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                                writeStoredCoverage(root.resolve("before-prepare-store.json"), chunkData, scenario, rejectedUploads);
                            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
                            if (heldClaim.get() != null && !heldClaim.get().isDone()) {
                                prematurePreparation.incrementAndGet();
                                AssertionError failure = new AssertionError("普通FULL边缘尚未生成完成就发起整组卸载，恢复时会重新生成宽依赖圈");
                                prepared.completeExceptionally(failure);
                                return CompletableFuture.failedFuture(failure);
                            }
                            reloadSource.set(JsonUtil.getBoolean(message.json(), "reloadSource", false));
                            prepared.complete(message.json().deepCopy());
                        }
                        if (!completeSplit) {
                            type = MessageType.ERROR; response.addProperty("code", "probe_prepare_observed");
                        } else {
                            type = MessageType.SPLIT_ACK;
                            response.addProperty("granted", true);
                            response.addProperty("metadataWorldId", metadata.worldId());
                            response.addProperty("metadataTerm", metadata.term());
                            response.addProperty("metadataVersion", metadata.version());
                            response.addProperty("reloadSource", reloadSource.get());
                            response.addProperty("phase", phase.equals("commit") || phase.equals("source_ready") ? "committed" :
                                    phase.equals("cancel") ? "cancelled" : "prepared");
                            if (phase.equals("prepare")) {
                                try { Files.writeString(WorkerProcess.currentDirectory().resolve("split-probe-depart"), message.json().toString()); }
                                catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
                            } else if (phase.equals("commit")) {
                                committed.complete(message.json().deepCopy());
                            } else if (phase.equals("source_ready")) {
                                try { writeStoredCoverage(root.resolve("before-source-resume-store.json"), chunkData, scenario, rejectedUploads); }
                                catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
                                byte[] saved = playerData.get(sourceGroup.toString());
                                if (saved == null) return CompletableFuture.failedFuture(new AssertionError("源主最终玩家快照缺失"));
                                response.addProperty("sourceResumed", true);
                                JsonObject resume = new JsonObject();
                                resume.addProperty("playerData", java.util.Base64.getEncoder().encodeToString(saved));
                                try { Files.writeString(WorkerProcess.currentDirectory().resolve("split-probe-resume"), resume.toString()); }
                                catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
                                sourceReady.complete(message.json().deepCopy());
                            } else if (phase.equals("cancel")) {
                                committed.completeExceptionally(new AssertionError("源端取消了真实区块分离"));
                            }
                        }
                    }
                    default -> { type = MessageType.ERROR; response.addProperty("code", "probe_unhandled"); }
                }
                return CompletableFuture.completedFuture(message.reply(type, response, binary));
            }
            public SocketAddress remoteAddress() { return null; }
            public UUID peerId() { return context.clientId(); }
            public void setPeerId(UUID peer) { }
            public boolean isOpen() { return true; }
            public void close() { }
        };
        WorkerProcess.setStopListener(() -> { });
        WorkerProcess.setStoppedFlush(path -> CompletableFuture.completedFuture(null));
        var manifest = EnvironmentScanner.scan(environment, java.util.List.of());
        Path folder = root.resolve("session");
        Files.createDirectories(folder);
        try {
            WorkerProcess.start(folder, environment, "world", folder.resolve("data"), "world", metadata.worldId(),
                    context.groupId(), remote, Map.of("eula", "true", "view-distance", Integer.toString(scenario.viewDistance()),
                            "simulation-distance", Integer.toString(scenario.simulationDistance())),
                    manifest, manifest.globalHash());
            JsonObject fullReadiness = null;
            if (scenario.holdBoundaryClaim()) {
                // 冷世界生成和注入的网络延迟是准备前置条件，不应挤占十秒距离资格后的
                // 分离响应窗口。先等待真实普通FULL/保存链完成，再单独限定prepare推进时间。
                Path fullReady = WorkerProcess.currentDirectory().resolve("split-probe-full-ready.json");
                Path earlyPreparation = WorkerProcess.currentDirectory().resolve("split-probe-premature.json");
                long generationDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
                while (!Files.isRegularFile(fullReady) && System.nanoTime() < generationDeadline) {
                    if (prepared.isCompletedExceptionally()) prepared.get();
                    assertFalse(Files.isRegularFile(earlyPreparation), "普通FULL需求未就绪时不得开始卸载，见" + earlyPreparation);
                    Thread.sleep(20);
                }
                if (prepared.isCompletedExceptionally()) prepared.get();
                assertTrue(Files.isRegularFile(fullReady), "双方普通FULL及保存链必须在有界生成阶段内就绪");
                fullReadiness = JsonUtil.parseObject(Files.readString(fullReady));
            }
            JsonObject request = prepared.get(scenario.holdBoundaryClaim() ? 15 : scenario.observationSeconds(), TimeUnit.SECONDS);
            request.addProperty("prepareAt", prepareAt.get());
            if (fullReadiness != null) {
                request.addProperty("fullReadyAt", fullReadiness.get("fullReadyAt").getAsLong());
                request.addProperty("prepareAfterFullReadyMillis", prepareAt.get() - fullReadiness.get("fullReadyAt").getAsLong());
                request.add("fullReadiness", fullReadiness);
            }
            if (scenario.holdBoundaryClaim()) assertTrue(boundaryClaimHeld.get(), "必须实际覆盖慢FULL边缘申请");
            assertEquals(0, prematurePreparation.get(), "普通FULL需求未完成前不得发起重载交接");
            assertEquals("prepare", request.get("phase").getAsString());
            assertEquals(1, request.getAsJsonArray("departingClientIds").size());
            if (scenario.moveHost()) assertTrue(reloadSource.get(), "现场共享生成依赖岛必须走源组重载交接");
            if (completeSplit) {
                JsonObject commit = committed.get(75, TimeUnit.SECONDS);
                assertEquals(request.get("splitId"), commit.get("splitId"));
                if (reloadSource.get()) sourceReady.get(20, TimeUnit.SECONDS);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
                Path completed = WorkerProcess.currentDirectory().resolve("split-probe-committed");
                while (!Files.isRegularFile(completed) && System.nanoTime() < deadline) Thread.sleep(20);
                assertTrue(Files.isRegularFile(completed), "收到commit回执后原组必须解除冻结并保留主客户端");
                request.add("commit", commit);
                if (reloadSource.get()) {
                    assertTrue(playerData.containsKey(sourceGroup.toString()), "重载源主必须先有最终玩家快照");
                    UUID member = UUID.fromString(request.getAsJsonArray("departingClientIds").get(0).getAsString());
                    assertTrue(playerData.containsKey(member.toString()), "离组成员必须先有最终玩家快照");
                    request.addProperty("sourceReloaded", true);
                    request.addProperty("savedChunkCount", chunkData.size());
                    var retainedClaims = java.util.Set.copyOf(registry.claimsOf(sourceGroup));
                    assertFalse(retainedClaims.isEmpty(), "恢复后的源主必须重新加载自己的区块");
                    // 捕获仍在运行时的真实源组占用，停掉测试进程后重建同一静止占用背景。
                    // 新worker实际请求仍通过生产注册表四邻检查，不把仅发出commit当作成功。
                    closeWorker();
                    registry.releaseAll(sourceGroup);
                    for (ChunkKey claim : retainedClaims) assertTrue(registry.tryClaim(claim, sourceGroup).granted());
                    context.setClientId(member); context.setGroupId(member); context.setPlayerName("SplitMember");
                    JsonObject follower = new JsonObject();
                    follower.addProperty("playerData", java.util.Base64.getEncoder().encodeToString(playerData.get(member.toString())));
                    Files.writeString(environment.resolve("split-probe-follower.json"), follower.toString());
                    var followerManifest = EnvironmentScanner.scan(environment, java.util.List.of());
                    Path followerFolder = root.resolve("follower-session"); Files.createDirectories(followerFolder);
                    checkingFollower.set(true);
                    WorkerProcess.start(followerFolder, environment, "world", followerFolder.resolve("data"), "world",
                            metadata.worldId(), member, remote,
                            Map.of("eula", "true", "view-distance", Integer.toString(scenario.viewDistance()),
                                    "simulation-distance", Integer.toString(scenario.simulationDistance())),
                            followerManifest, followerManifest.globalHash());
                    Path followerReady = WorkerProcess.currentDirectory().resolve("split-probe-follower-ready");
                    long followerDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
                    while (!Files.isRegularFile(followerReady) && collisions.isEmpty() && System.nanoTime() < followerDeadline)
                        Thread.sleep(20);
                    assertTrue(collisions.isEmpty(), "分离后新组马上与源组占用相撞: " + collisions.stream().limit(12).toList());
                    assertTrue(Files.isRegularFile(followerReady), "离组新worker应从FULL存档独立加载，不能持续等待原组");
                    request.addProperty("followerLoadedWithoutClaimConflict", true);
                    request.addProperty("retainedClaims", retainedClaims.size());
                    request.addProperty("followerClaims", registry.claimsOf(member).size());
                }
            }
            request.addProperty("heldFullBoundaryClaim", boundaryClaimHeld.get());
            request.addProperty("prematurePreparations", prematurePreparation.get());
            request.addProperty("rejectedLateChunkWrites", rejectedUploads.size());
            Files.writeString(root.resolve("result.json"), request.toString(), StandardCharsets.UTF_8);
        } catch (Exception failure) {
            Path failedFolder = checkingFollower.get() ? root.resolve("follower-session") : folder;
            Path log = failedFolder.resolve("runtime/worker-console.log");
            String output = Files.exists(log) ? new String(Files.readAllBytes(log), java.nio.charset.Charset.forName(
                    System.getProperty("native.encoding", "UTF-8"))) : "无计算进程日志";
            Path diagnostics = failedFolder.resolve("runtime/primary/split-probe-diagnostics.json");
            String state = Files.isRegularFile(diagnostics) ? Files.readString(diagnostics) : "无运行状态快照";
            throw new AssertionError("距离分离探针失败，目录=" + root + "\n"
                    + output.substring(Math.max(0, output.length() - 18000)) + "\n运行状态：" + state, failure);
        } finally {
            if (heldClaim.get() != null && !heldClaim.get().isDone()) heldClaim.get().complete(heldReply.get());
            if (!collisions.isEmpty() && WorkerProcess.isRunning())
                WorkerProcess.retireAfterTransfer(context.groupId()).get(15, TimeUnit.SECONDS);
            closeWorker();
        }
    }

    private static ChunkKey key(ControlMessage message) {
        return new ChunkKey(message.json().get("dimension").getAsString(), message.json().get("x").getAsInt(),
                message.json().get("z").getAsInt());
    }

    private static void writeStoredCoverage(Path target, Map<ChunkKey, byte[]> stored, Scenario scenario,
                                           java.util.List<String> rejectedUploads) throws Exception {
        JsonObject report = new JsonObject();
        report.addProperty("storedChunks", stored.size());
        var rejected = new com.google.gson.JsonArray(); rejectedUploads.forEach(rejected::add);
        report.add("rejectedLateUploads", rejected);
        var positions = scenario.moveHost()
                ? new int[][]{{scenario.destinationX(), scenario.destinationZ()}, {scenario.initialX(), scenario.initialZ()}}
                : new int[][]{{scenario.initialX(), scenario.initialZ()}, {scenario.destinationX(), scenario.destinationZ()}};
        for (int index = 0; index < positions.length; index++) {
            int cx = positions[index][0], cz = positions[index][1], full = 0;
            var missing = new com.google.gson.JsonArray();
            int radius = scenario.viewDistance() + 2;
            for (int x = cx - radius; x <= cx + radius; x++) for (int z = cz - radius; z <= cz + radius; z++) {
                byte[] encoded = stored.get(new ChunkKey("minecraft:overworld", x, z));
                String status = "missing";
                if (encoded != null) status = net.minecraft.nbt.NbtIo.readCompressed(new java.io.ByteArrayInputStream(encoded),
                        net.minecraft.nbt.NbtAccounter.create(64L * 1024 * 1024)).getString("Status");
                if (status.equals("minecraft:full")) full++; else missing.add(x + "," + z + ":" + status);
            }
            JsonObject participant = new JsonObject(); participant.addProperty("center", cx + "," + cz);
            participant.addProperty("full", full); participant.add("notFull", missing);
            report.add(index == 0 ? "source" : "departing", participant);
        }
        Files.writeString(target, report.toString());
    }

    private static void closeWorker() throws Exception {
        WorkerProcess.closeSession();
        // closeSession异步停止子JVM；等待进程消失，避免Gradle先退出切断最终卸载的IPC。
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (WorkerProcess.isRunning() && System.nanoTime() < deadline) Thread.sleep(20);
        assertFalse(WorkerProcess.isRunning(), "测试计算子进程必须彻底退出后才结束用例");
    }

    private static void createProbe(Path root, Path jar, boolean realChunks, Scenario scenario) throws Exception {
        Path source = root.resolve("fixture-source/SplitProbe.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, """
                package splitfixture;
                public final class SplitProbe implements net.fabricmc.api.ModInitializer {
                    private static final boolean REAL_CHUNKS = %s, MOVE_HOST = %s;
                    private static final int INITIAL_X = %d, INITIAL_Z = %d, DESTINATION_X = %d, DESTINATION_Z = %d;
                    private static final int CLIENT_VIEW = %d;
                    private boolean installed;
                    private boolean moved;
                    private boolean removed;
                    private boolean sawFreeze;
                    private boolean sourceReload;
                    private boolean sourceResumed;
                    private boolean followerOnly;
                    private boolean fullReadyWritten;
                    private long created;
                    private long lastDiagnostics;
                    private com.google.gson.JsonObject coverageBeforeTick;
                    private int previousClaims = -1, stableClaimTicks;
                    private net.minecraft.server.level.ServerPlayer host;
                    private net.minecraft.server.level.ServerPlayer departing;
                    private final java.util.List<net.minecraft.server.level.ServerPlayer> probePlayers = new java.util.ArrayList<>();
                    private final java.util.List<io.netty.channel.embedded.EmbeddedChannel> probeChannels = new java.util.ArrayList<>();
                    public void onInitialize() {
                        net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents.START_SERVER_TICK.register(server -> {
                            if (installed && moved && !removed) {
                                coverageBeforeTick = ordinaryFullCoverage(server);
                                observeFullReady(server, coverageBeforeTick);
                            }
                        });
                        net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
                            server.getPlayerList().getPlayers().removeAll(probePlayers);
                            probeChannels.forEach(io.netty.channel.embedded.EmbeddedChannel::finishAndReleaseAll);
                        });
                        net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents.END_SERVER_TICK.register(server -> {
                            if (!imsng.player_to_player.metadata.MetadataClient.isSimulationReady()) return;
                            if (installed) {
                                var working = net.fabricmc.loader.api.FabricLoader.getInstance().getGameDir();
                                if (moved && !removed && !fullReadyWritten) observeFullReady(server, ordinaryFullCoverage(server));
                                if (System.currentTimeMillis() - lastDiagnostics >= 1000) {
                                    lastDiagnostics = System.currentTimeMillis();
                                    writeDiagnostics(server, working);
                                }
                                sawFreeze |= imsng.player_to_player.group.GroupRuntime.isTickFrozen();
                                if (!removed && java.nio.file.Files.isRegularFile(working.resolve("split-probe-depart"))) {
                                    removed = true;
                                    try {
                                        var coverage = new com.google.gson.JsonObject();
                                        if (coverageBeforeTick != null) coverage.add("beforeTick", coverageBeforeTick);
                                        var beforeDeparture = ordinaryFullCoverage(server);
                                        coverage.add("beforeDeparture", beforeDeparture);
                                        java.nio.file.Files.writeString(working.resolve("before-prepare-live.json"), coverage.toString());
                                        var request = imsng.player_to_player.util.JsonUtil.parseObject(
                                                java.nio.file.Files.readString(working.resolve("split-probe-depart")));
                                        sourceReload = imsng.player_to_player.util.JsonUtil.getBoolean(request, "reloadSource", false);
                                        if (sourceReload && !ordinaryFullReady(beforeDeparture)) {
                                            java.nio.file.Files.writeString(working.resolve("split-probe-premature.json"), coverage.toString());
                                            throw new IllegalStateException("整组重载在普通FULL或保存链未完成时开始卸载");
                                        }
                                        observeFullReady(server, beforeDeparture);
                                    } catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
                                    var leaving = sourceReload ? new java.util.ArrayList<>(server.getPlayerList().getPlayers())
                                            : java.util.List.of(departing);
                                    for (var player : leaving) {
                                        imsng.player_to_player.group.SplitMonitor.onPlayerDisconnect(server, player);
                                        server.getPlayerList().remove(player);
                                    }
                                    System.out.println("SPLIT_PROBE_DEPARTED claims=" +
                                            imsng.player_to_player.group.GroupRuntime.claims().trackedKeysSnapshot().size());
                                }
                                if (sourceReload && !sourceResumed && java.nio.file.Files.isRegularFile(working.resolve("split-probe-resume"))) {
                                    try {
                                        var context = imsng.player_to_player.core.NodeContext.get();
                                        host = createPlayer(server, new com.mojang.authlib.GameProfile(context.clientId(), "SplitHost"),
                                                savedPlayer(working.resolve("split-probe-resume")), INITIAL_X, INITIAL_Z, 37);
                                        verifyPlayer(host, DESTINATION_X, DESTINATION_Z, 37);
                                        sourceResumed = true;
                                        stableClaimTicks = 0; previousClaims = -1;
                                        System.out.println("SPLIT_PROBE_SOURCE_RESUMED freshPlayer=true");
                                    } catch (Exception failure) { throw new IllegalStateException(failure); }
                                }
                                int claims = imsng.player_to_player.group.GroupRuntime.claims().trackedKeysSnapshot().size();
                                stableClaimTicks = claims == previousClaims ? stableClaimTicks + 1 : 0;
                                previousClaims = claims;
                                if (removed && sawFreeze && !imsng.player_to_player.group.GroupRuntime.isTickFrozen()) {
                                    try {
                                        if (sourceReload && (!sourceResumed || stableClaimTicks < 30
                                                || server.overworld().getChunkSource().getChunkNow(DESTINATION_X, DESTINATION_Z) == null)) return;
                                        if (server.getPlayerList().getPlayers().size() != 1)
                                            throw new IllegalStateException("留组主客户端丢失");
                                        var request = imsng.player_to_player.util.JsonUtil.parseObject(
                                                java.nio.file.Files.readString(working.resolve("split-probe-depart")));
                                        var regions = new java.util.ArrayList<imsng.player_to_player.registry.SplitRegion>();
                                        for (var value : request.getAsJsonArray("regions"))
                                            regions.add(imsng.player_to_player.registry.SplitRegion.fromJson(value.getAsJsonObject()));
                                        var activeClaims = imsng.player_to_player.group.GroupRuntime.claims().trackedKeysSnapshot();
                                        if (!sourceReload && activeClaims.stream().anyMatch(key -> regions.stream().anyMatch(region -> region.protects(key))))
                                            throw new IllegalStateException("离组区域仍残留claim");
                                        if (sourceReload) {
                                            verifyPlayer(host, DESTINATION_X, DESTINATION_Z, 37);
                                            var memberFullDemand = new imsng.player_to_player.registry.SplitRegion("minecraft:overworld",
                                                    INITIAL_X - CLIENT_VIEW - 2, INITIAL_X + CLIENT_VIEW + 2,
                                                    INITIAL_Z - CLIENT_VIEW - 2, INITIAL_Z + CLIENT_VIEW + 2);
                                            if (activeClaims.stream().anyMatch(memberFullDemand::protects))
                                                throw new IllegalStateException("恢复源主仍占用离组玩家的渲染/FULL边缘");
                                        }
                                        java.nio.file.Files.writeString(working.resolve("split-probe-committed"), "committed");
                                    } catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
                                }
                                if (followerOnly && stableClaimTicks >= 40 && claims > 0
                                        && server.overworld().getChunkSource().getChunkNow(INITIAL_X, INITIAL_Z) != null) {
                                    try {
                                        verifyPlayer(host, INITIAL_X, INITIAL_Z, 23);
                                        java.nio.file.Files.writeString(working.resolve("split-probe-follower-ready"), "ready");
                                    } catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
                                }
                                if (REAL_CHUNKS && !moved && stableClaimTicks >= 10
                                        && server.overworld().getChunkSource().getChunkNow(INITIAL_X, INITIAL_Z) != null
                                        && System.currentTimeMillis() - created >= 3000) {
                                    moved = true;
                                    var mover = MOVE_HOST ? host : departing;
                                    mover.teleportTo(server.overworld(), DESTINATION_X * 16.0 + 8, 100,
                                            DESTINATION_Z * 16.0 + 8, 0, 0);
                                    // 图形客户端确认传送后，首个移动包会推进原版区块跟踪位置；
                                    // 此探针没有渲染端，因此显式执行相同的 ServerChunkCache.move 入口。
                                    server.overworld().getChunkSource().move(mover);
                                    System.out.println("SPLIT_PROBE_TELEPORTED claims=" +
                                            imsng.player_to_player.group.GroupRuntime.claims().trackedKeysSnapshot().size());
                                }
                                return;
                            }
                            installed = true;
                            try {
                                var context = imsng.player_to_player.core.NodeContext.get();
                                var memberId = java.util.UUID.randomUUID();
                                var working = net.fabricmc.loader.api.FabricLoader.getInstance().getGameDir();
                                followerOnly = java.nio.file.Files.isRegularFile(working.resolve("split-probe-follower.json"));
                                var permissions = new com.google.gson.JsonObject();
                                permissions.addProperty("groupId", context.groupId().toString());
                                var members = new com.google.gson.JsonArray();
                                var profiles = followerOnly
                                        ? java.util.List.of(new com.mojang.authlib.GameProfile(context.clientId(), "SplitMember"))
                                        : java.util.List.of(new com.mojang.authlib.GameProfile(context.clientId(), "SplitHost"),
                                            new com.mojang.authlib.GameProfile(memberId, "SplitMember"));
                                for (var profile : profiles) {
                                    var row = new com.google.gson.JsonObject();
                                    row.addProperty("uuid", profile.getId().toString()); row.addProperty("name", profile.getName());
                                    row.addProperty("level", 0); members.add(row);
                                    boolean initial = REAL_CHUNKS || profile.getId().equals(context.clientId());
                                    int chunkX = initial ? INITIAL_X : DESTINATION_X, chunkZ = initial ? INITIAL_Z : DESTINATION_Z;
                                    var saved = followerOnly ? savedPlayer(working.resolve("split-probe-follower.json")) : null;
                                    var player = createPlayer(server, profile, saved, chunkX, chunkZ,
                                            profile.getName().equals("SplitHost") ? 37 : 23);
                                    if (profile.getId().equals(memberId)) departing = player;
                                    else host = player;
                                }
                                if (followerOnly) moved = true;
                                permissions.add("members", members);
                                imsng.player_to_player.group.LocalPermissions.updateFromJson(permissions);
                                created = System.currentTimeMillis();
                                System.out.println("SPLIT_PROBE_READY players=" + server.getPlayerList().getPlayers().size());
                            } catch (Exception error) { throw new IllegalStateException(error); }
                        });
                    }
                    private net.minecraft.nbt.CompoundTag savedPlayer(java.nio.file.Path marker) throws Exception {
                        var value = imsng.player_to_player.util.JsonUtil.parseObject(java.nio.file.Files.readString(marker));
                        return imsng.player_to_player.group.PlayerStateNbt.decodeValidated(
                                java.util.Base64.getDecoder().decode(value.get("playerData").getAsString()));
                    }
                    private net.minecraft.server.level.ServerPlayer createPlayer(net.minecraft.server.MinecraftServer server,
                            com.mojang.authlib.GameProfile profile, net.minecraft.nbt.CompoundTag saved, int chunkX, int chunkZ, int diamonds) {
                        var information = new net.minecraft.server.level.ClientInformation("en_us", CLIENT_VIEW,
                                net.minecraft.world.entity.player.ChatVisiblity.FULL, true, 0,
                                net.minecraft.world.entity.HumanoidArm.RIGHT, false, false);
                        var player = new net.minecraft.server.level.ServerPlayer(server, server.overworld(), profile, information);
                        var connection = new net.minecraft.network.Connection(net.minecraft.network.protocol.PacketFlow.SERVERBOUND);
                        var channel = new io.netty.channel.embedded.EmbeddedChannel(connection);
                        probeChannels.add(channel);
                        channel.attr(net.minecraft.network.Connection.ATTRIBUTE_SERVERBOUND_PROTOCOL).set(
                                net.minecraft.network.ConnectionProtocol.PLAY.codec(net.minecraft.network.protocol.PacketFlow.SERVERBOUND));
                        channel.attr(net.minecraft.network.Connection.ATTRIBUTE_CLIENTBOUND_PROTOCOL).set(
                                net.minecraft.network.ConnectionProtocol.PLAY.codec(net.minecraft.network.protocol.PacketFlow.CLIENTBOUND));
                        player.connection = new net.minecraft.server.network.ServerGamePacketListenerImpl(server, connection,
                                player, new net.minecraft.server.network.CommonListenerCookie(profile, 0, information));
                        if (saved != null) player.load(saved);
                        else {
                            player.setPos(chunkX * 16.0 + 8, 100, chunkZ * 16.0 + 8);
                            player.getInventory().setItem(0, new net.minecraft.world.item.ItemStack(net.minecraft.world.item.Items.DIAMOND, diamonds));
                        }
                        server.getPlayerList().getPlayers().add(player); probePlayers.add(player);
                        if (REAL_CHUNKS) server.overworld().addNewPlayer(player);
                        else imsng.player_to_player.group.GroupRuntime.claims().claimWithRetry(
                                new imsng.player_to_player.registry.ChunkKey("minecraft:overworld", chunkX, chunkZ));
                        return player;
                    }
                    private void verifyPlayer(net.minecraft.server.level.ServerPlayer player, int chunkX, int chunkZ, int diamonds) {
                        if (player.chunkPosition().x != chunkX || player.chunkPosition().z != chunkZ)
                            throw new IllegalStateException("重建玩家位置未恢复: " + player.chunkPosition());
                        var item = player.getInventory().getItem(0);
                        if (!item.is(net.minecraft.world.item.Items.DIAMOND) || item.getCount() != diamonds)
                            throw new IllegalStateException("重建玩家背包哨兵未恢复");
                    }
                    private com.google.gson.JsonObject ordinaryFullCoverage(net.minecraft.server.MinecraftServer server) {
                        var result = new com.google.gson.JsonObject();
                        result.addProperty("tick", server.getTickCount());
                        int radius = server.getPlayerList().getViewDistance() + 2;
                        for (var player : server.getPlayerList().getPlayers()) {
                            var row = new com.google.gson.JsonObject();
                            int cx = player.chunkPosition().x, cz = player.chunkPosition().z, full = 0, saveFull = 0;
                            var missing = new com.google.gson.JsonArray();
                            var access = (imsng.player_to_player.mixin.ChunkMapSplitAccess) player.serverLevel().getChunkSource().chunkMap;
                            for (int x = cx - radius; x <= cx + radius; x++) for (int z = cz - radius; z <= cz + radius; z++) {
                                var holder = access.player_to_player$updatingChunks().get(net.minecraft.world.level.ChunkPos.asLong(x, z));
                                String reason = "missing_holder";
                                if (holder != null) {
                                    if (holder.getFullChunk() != null) full++;
                                    var saved = holder.getChunkToSave();
                                    if (!saved.isDone()) reason = "save_pending";
                                    else if (saved.isCompletedExceptionally() || saved.isCancelled()) reason = "save_failed";
                                    else {
                                        var chunk = saved.getNow(null);
                                        reason = chunk == null ? "no_chunk_to_save" : chunk.getStatus().toString();
                                        if (chunk != null && chunk.getStatus() == net.minecraft.world.level.chunk.ChunkStatus.FULL) saveFull++;
                                    }
                                    if (holder.getFullChunk() == null) reason += "/not_full_holder";
                                }
                                if (!reason.equals("minecraft:full")) missing.add(x + "," + z + ":" + reason);
                            }
                            row.addProperty("center", cx + "," + cz); row.addProperty("radius", radius);
                            row.addProperty("fullHolders", full); row.addProperty("fullChunkToSave", saveFull);
                            row.add("notReady", missing); result.add(player.getGameProfile().getName(), row);
                        }
                        return result;
                    }
                    private boolean ordinaryFullReady(com.google.gson.JsonObject coverage) {
                        int participants = 0;
                        for (var entry : coverage.entrySet()) {
                            if (!entry.getValue().isJsonObject()) continue;
                            participants++;
                            if (!entry.getValue().getAsJsonObject().getAsJsonArray("notReady").isEmpty()) return false;
                        }
                        return participants == 2;
                    }
                    private void observeFullReady(net.minecraft.server.MinecraftServer server, com.google.gson.JsonObject coverage) {
                        if (fullReadyWritten || followerOnly || !moved || !ordinaryFullReady(coverage)) return;
                        try {
                            var working = net.fabricmc.loader.api.FabricLoader.getInstance().getGameDir();
                            var ready = coverage.deepCopy();
                            ready.addProperty("fullReadyAt", System.currentTimeMillis());
                            java.nio.file.Files.writeString(working.resolve("split-probe-full-ready.tmp"), ready.toString());
                            java.nio.file.Files.move(working.resolve("split-probe-full-ready.tmp"),
                                    working.resolve("split-probe-full-ready.json"), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                            fullReadyWritten = true;
                        } catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
                    }
                    private void writeDiagnostics(net.minecraft.server.MinecraftServer server, java.nio.file.Path working) {
                        try {
                            var output = new com.google.gson.JsonObject();
                            output.addProperty("moved", moved);
                            output.addProperty("frozen", imsng.player_to_player.group.GroupRuntime.isTickFrozen());
                            output.addProperty("serverView", server.getPlayerList().getViewDistance());
                            output.add("ordinaryFullCoverage", ordinaryFullCoverage(server));
                            var players = new com.google.gson.JsonArray();
                            for (var player : server.getPlayerList().getPlayers()) {
                                var row = new com.google.gson.JsonObject();
                                row.addProperty("name", player.getGameProfile().getName());
                                row.addProperty("chunkX", player.chunkPosition().x); row.addProperty("chunkZ", player.chunkPosition().z);
                                row.addProperty("requestedView", player.requestedViewDistance());
                                row.addProperty("spectator", player.isSpectator());
                                int radius = server.getPlayerList().getViewDistance()
                                        + net.minecraft.world.level.chunk.ChunkStatus.maxDistance() + 2;
                                row.addProperty("expandedDemand", (player.chunkPosition().x - radius) + ".."
                                        + (player.chunkPosition().x + radius) + "," + (player.chunkPosition().z - radius)
                                        + ".." + (player.chunkPosition().z + radius));
                                players.add(row);
                            }
                            output.add("players", players);
                            var holders = new java.util.HashMap<imsng.player_to_player.registry.ChunkKey,
                                    net.minecraft.server.level.ChunkHolder>();
                            var ticketStats = new java.util.TreeMap<String, int[]>();
                            int pendingUnloads = 0;
                            for (var level : server.getAllLevels()) {
                                String dimension = level.dimension().location().toString();
                                var access = (imsng.player_to_player.mixin.ChunkMapSplitAccess) level.getChunkSource().chunkMap;
                                pendingUnloads += access.player_to_player$pendingUnloads().size();
                                for (var holder : access.player_to_player$updatingChunks().values())
                                    holders.put(new imsng.player_to_player.registry.ChunkKey(dimension,
                                            holder.getPos().x, holder.getPos().z), holder);
                                for (var entry : ((imsng.player_to_player.mixin.DistanceManagerAccess)
                                        level.getChunkSource().chunkMap.getDistanceManager()).player_to_player$tickets().long2ObjectEntrySet()) {
                                    var position = new net.minecraft.world.level.ChunkPos(entry.getLongKey());
                                    for (var ticket : entry.getValue()) {
                                        String key = dimension + "/" + ticket.getType() + "/level=" + ticket.getTicketLevel();
                                        var stats = ticketStats.computeIfAbsent(key,
                                                ignored -> new int[]{0, position.x, position.x, position.z, position.z});
                                        stats[0]++; stats[1] = Math.min(stats[1], position.x); stats[2] = Math.max(stats[2], position.x);
                                        stats[3] = Math.min(stats[3], position.z); stats[4] = Math.max(stats[4], position.z);
                                    }
                                }
                            }
                            var tickets = new com.google.gson.JsonObject();
                            ticketStats.forEach((key, value) -> tickets.addProperty(key, java.util.Arrays.toString(value)));
                            output.add("ticketsCountMinXMaxXMinZMaxZ", tickets);
                            output.addProperty("updatingHolders", holders.size()); output.addProperty("pendingUnloads", pendingUnloads);
                            var remaining = new java.util.HashSet<>(imsng.player_to_player.group.GroupRuntime.claims().trackedKeysSnapshot());
                            output.addProperty("trackedClaims", remaining.size());
                            var islands = new com.google.gson.JsonArray();
                            while (!remaining.isEmpty()) {
                                var seed = remaining.iterator().next(); remaining.remove(seed);
                                var queue = new java.util.ArrayDeque<imsng.player_to_player.registry.ChunkKey>(); queue.add(seed);
                                int count = 0, full = 0, proto = 0, missing = 0;
                                int minX = seed.x(), maxX = seed.x(), minZ = seed.z(), maxZ = seed.z();
                                var statuses = new java.util.TreeMap<String, Integer>();
                                while (!queue.isEmpty()) {
                                    var key = queue.removeFirst(); count++;
                                    minX = Math.min(minX, key.x()); maxX = Math.max(maxX, key.x());
                                    minZ = Math.min(minZ, key.z()); maxZ = Math.max(maxZ, key.z());
                                    var holder = holders.get(key);
                                    if (holder == null) missing++;
                                    else {
                                        if (holder.getFullChunk() != null) full++; else proto++;
                                        String status = String.valueOf(holder.getLastAvailableStatus()) + "/ticket=" + holder.getTicketLevel();
                                        statuses.merge(status, 1, Integer::sum);
                                    }
                                    for (int dx = -1; dx <= 1; dx++) for (int dz = -1; dz <= 1; dz++) {
                                        if (dx == 0 && dz == 0) continue;
                                        var neighbor = new imsng.player_to_player.registry.ChunkKey(key.dimension(), key.x() + dx, key.z() + dz);
                                        if (remaining.remove(neighbor)) queue.add(neighbor);
                                    }
                                }
                                var island = new com.google.gson.JsonObject();
                                island.addProperty("dimension", seed.dimension()); island.addProperty("count", count);
                                island.addProperty("bounds", minX + ".." + maxX + "," + minZ + ".." + maxZ);
                                island.addProperty("fullHolders", full); island.addProperty("protoOrPendingHolders", proto);
                                island.addProperty("claimsWithoutUpdatingHolder", missing);
                                var histogram = new com.google.gson.JsonObject(); statuses.forEach(histogram::addProperty);
                                island.add("statusAndTicketLevel", histogram); islands.add(island);
                            }
                            output.add("eightNeighborClaimIslands", islands);
                            java.nio.file.Files.writeString(working.resolve("split-probe-diagnostics.json"), output.toString());
                        } catch (Exception failure) { throw new IllegalStateException("分离探针状态采集失败", failure); }
                    }
                }
                """.formatted(realChunks, scenario.moveHost(), scenario.initialX(), scenario.initialZ(),
                        scenario.destinationX(), scenario.destinationZ(), scenario.viewDistance()), StandardCharsets.UTF_8);
        RuntimeLaunchCommand.DevelopmentSpec development = JsonUtil.readFile(
                Path.of(System.getProperty("p2p.worker.developmentSpec")), RuntimeLaunchCommand.DevelopmentSpec.class);
        Path classes = root.resolve("fixture-classes");
        Files.createDirectories(classes);
        assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null, "--release", "17", "-encoding", "UTF-8",
                "-classpath", development.classpath(), "-d", classes.toString(), source.toString()));
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry("fabric.mod.json"));
            output.write("""
                    {"schemaVersion":1,"id":"p2p_split_probe","version":"1.0.0","environment":"server",
                     "entrypoints":{"main":["splitfixture.SplitProbe"]},"depends":{"fabricloader":">=0.15.0"}}
                    """.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
            output.putNextEntry(new JarEntry("splitfixture/SplitProbe.class"));
            output.write(Files.readAllBytes(classes.resolve("splitfixture/SplitProbe.class")));
            output.closeEntry();
        }
    }
}
