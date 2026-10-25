package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.env.EnvironmentManifest;
import imsng.player_to_player.env.EnvironmentPreflightInfo;
import imsng.player_to_player.env.EnvironmentScanner;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.runtime.*;
import imsng.player_to_player.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.tools.ToolProvider;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 三个真实 Fabric/Minecraft JVM 的元数据冒烟。中心与两个 WorkerBootstrap 均运行生产
 * 代码；测试仅模拟图形客户端父进程的控制连接及 IPC 桥，并用 server-only 观测模组
 * 在服务器线程读取实际运行对象。不会把本机网络延迟描述为逐 tick 全网一致。
 */
@EnabledIfSystemProperty(named = "p2p.metadata.smoke", matches = "true")
class MetadataProcessSmokeTest {
    private final Path root = Path.of("build", "metadata-smoke", Long.toString(System.currentTimeMillis())).toAbsolutePath();
    private RuntimeLaunchCommand.DevelopmentSpec development;

    @Test
    void centerAndTwoComputeGroupsConvergeRecoverGapsAndFreezeOnDisconnect() throws Exception {
        Files.createDirectories(root);
        development = JsonUtil.readFile(Path.of(System.getProperty("p2p.worker.developmentSpec")),
                RuntimeLaunchCommand.DevelopmentSpec.class);
        Path fixture = createObserverFixture();
        int controlPort = freePort();
        Path centerRoot = root.resolve("center");
        Files.createDirectories(centerRoot.resolve("mods"));
        Files.copy(fixture, centerRoot.resolve("mods/metadata-observer.jar"));
        createScheduledFunction(centerRoot.resolve("world/datapacks/smoke"));
        Files.writeString(centerRoot.resolve("eula.txt"), "eula=true\n");
        Files.writeString(centerRoot.resolve("server.properties"), "server-ip=127.0.0.1\nserver-port=" + freePort()
                + "\nonline-mode=false\nview-distance=2\nsimulation-distance=2\nmax-tick-time=-1\nlevel-seed=817024\n");
        Files.createDirectories(centerRoot.resolve("player-to-player"));
        GlobalConfig config = new GlobalConfig();
        config.mode = "server"; config.controlPort = controlPort; config.serverActsAsRelay = false;
        config.geekbenchLookupEnabled = false;
        config.nonEnvironmentPaths.add("metadata-smoke-status.json");
        config.nonEnvironmentPaths.add("metadata-smoke-status.tmp");
        config.nonEnvironmentPaths.add("console.log");
        config.save(centerRoot.resolve("player-to-player/config.json"));

        Process center = launchCenter(centerRoot);
        WorkerNode first = null, second = null;
        JsonObject report = new JsonObject();
        try {
            waitUntil("中心服务端启动", Duration.ofSeconds(120), () -> {
                if (!center.isAlive()) throw new IllegalStateException("中心服务端提前退出: " + center.exitValue());
                return Files.isRegularFile(centerRoot.resolve("metadata-smoke-status.json"));
            });
            EnvironmentPreflightInfo environment = preflight(controlPort);
            first = new WorkerNode("first", controlPort, environment, fixture);
            second = new WorkerNode("second", controlPort, environment, fixture);
            first.start();
            second.start();
            WorkerNode a = first, b = second;
            assertNotEquals(a.id, b.id);
            a.sync(); b.sync();
            a.command("gamerule doDaylightCycle false");
            a.command("time set 1000");
            waitUntil("两组初始昼夜一致", Duration.ofSeconds(20), () -> a.dayTime() == 1000 && b.dayTime() == 1000);

            long started = System.nanoTime();
            ControlMessage add = a.commandRequest("time add 1000");
            assertAccepted(a.remote.request(add).get(15, TimeUnit.SECONDS));
            assertAccepted(a.remote.request(add).get(15, TimeUnit.SECONDS));
            waitUntil("非幂等指令仅执行一次并传播两组", Duration.ofSeconds(20), () -> a.dayTime() == 2000 && b.dayTime() == 2000);
            report.addProperty("twoGroupPropagationMillis", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            assertEquals(2000, a.snapshot().data().getLong("DayTime"));

            int beforeSnapshots = a.snapshotRequests.get();
            int beforeCatchup = a.catchupRequests.get();
            int beforeCatchupReplies = a.catchupReplies.get();
            int beforeFallbacks = a.catchupSnapshotFallbacks.get();
            a.dropNextPatch.set(true);
            b.command("time add 100");
            waitUntil("注入丢弃一个元数据补丁", Duration.ofSeconds(10), () -> !a.dropNextPatch.get());
            b.command("time add 100");
            waitUntil("版本缺口发起补齐请求并恢复", Duration.ofSeconds(20),
                    () -> a.catchupRequests.get() > beforeCatchup && a.catchupReplies.get() > beforeCatchupReplies
                            && a.dayTime() == 2200 && b.dayTime() == 2200 && a.ready() && b.ready());
            MetadataSnapshot gapState = a.snapshot();
            waitUntil("缺口补齐后两组确认同一任期和连续版本", Duration.ofSeconds(15),
                    () -> a.version() >= gapState.version() && a.version() == b.version()
                            && a.term() == gapState.term() && b.term() == gapState.term()
                            && a.ready() && b.ready());
            report.addProperty("gapSnapshotRequests", a.snapshotRequests.get() - beforeSnapshots);
            report.addProperty("gapCatchupRequests", a.catchupRequests.get() - beforeCatchup);
            report.addProperty("gapSnapshotFallbacks", a.catchupSnapshotFallbacks.get() - beforeFallbacks);

            // 两组建立时已经推进过任期，版本0不在当前连续补丁窗口内；单独验证服务端
            // 回退完整快照，而不是让一次成功的近距离补齐掩盖日志窗口不足的恢复路径。
            assertTrue(gapState.version() > 0);
            JsonObject oldHistory = new JsonObject();
            oldHistory.addProperty("worldId", gapState.worldId());
            oldHistory.addProperty("term", gapState.term());
            oldHistory.addProperty("version", 0L);
            ControlMessage fallback = a.remote.request(ControlMessage.of(MessageType.METADATA_CATCHUP, oldHistory))
                    .get(15, TimeUnit.SECONDS);
            assertEquals(MessageType.METADATA_SNAPSHOT, fallback.type(), fallback.json().toString());
            MetadataSnapshot fallbackState = MetadataWire.snapshotFromTag(MetadataWire.decode(fallback.binary()));
            assertEquals(gapState.worldId(), fallbackState.worldId());
            assertEquals(gapState.term(), fallbackState.term());
            assertTrue(fallbackState.version() >= gapState.version());
            assertEquals(2200, fallbackState.data().getLong("DayTime"));
            report.addProperty("oldHistorySnapshotFallback", true);

            a.command("bossbar add p2p:smoke \"Metadata smoke\"");
            waitUntil("Boss 血条在两组创建运行对象", Duration.ofSeconds(15),
                    () -> a.bossCount() == 1 && b.bossCount() == 1);
            b.command("bossbar remove p2p:smoke");
            a.sync(); b.sync();
            waitUntil("删除 Boss 血条后快照不会复活", Duration.ofSeconds(15),
                    () -> a.bossCount() == 0 && b.bossCount() == 0);
            report.addProperty("bossDeletionSurvivesSnapshot", true);

            a.command("worldborder set 100");
            a.command("worldborder set 200 4");
            waitUntil("两组开始同一边界渐变", Duration.ofSeconds(10),
                    () -> a.borderRemaining() > 0 && b.borderRemaining() > 0);
            long initialRemaining = a.borderRemaining();
            long borderSample = a.status().get("sample").getAsLong();
            waitUntil("渐变已经前进", Duration.ofSeconds(10),
                    () -> a.status().get("sample").getAsLong() >= borderSample + 5);
            a.sync(); b.sync();
            waitUntil("重发快照保留已消耗的边界渐变时间", Duration.ofSeconds(5),
                    () -> a.borderRemaining() < initialRemaining - 500 && b.borderRemaining() < initialRemaining - 500);
            report.addProperty("borderLerpSurvivesSnapshot", true);

            long scheduledAt = System.nanoTime();
            a.command("schedule function smoke:once 5t append");
            waitUntil("计划函数只在目标组运行一次并转交全局变更", Duration.ofSeconds(20),
                    () -> a.dayTime() == 2277 && b.dayTime() == 2277);
            long scheduledMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scheduledAt);
            report.addProperty("scheduledFunctionMillis", scheduledMillis);
            assertTrue(scheduledMillis < 5000, "计划函数不能在计算主线程等待完整请求超时: " + scheduledMillis);
            assertEquals(2277, a.snapshot().data().getLong("DayTime"));
            var pendingFunctions = a.snapshot().data().getCompound("__p2pSchedulePending");
            assertTrue(pendingFunctions.getAllKeys().stream().anyMatch(key ->
                    "completed".equals(pendingFunctions.getCompound(key).getString("status"))),
                    "计划函数成功回执必须持久化，不能仅凭最终字段变化推断执行完成");
            long scheduledSamples = a.status().get("sample").getAsLong();
            waitUntil("同步计划队列不会再次执行函数", Duration.ofSeconds(10),
                    () -> a.status().get("sample").getAsLong() >= scheduledSamples + 6);
            assertEquals(2277, a.dayTime()); assertEquals(2277, b.dayTime());
            report.addProperty("scheduledFunctionGlobalDelta", 77);

            MetadataSnapshot old = a.snapshot();
            a.remote.close();
            waitUntil("控制断线暂停计算", Duration.ofSeconds(10), () -> !a.ready());
            long frozenTime = a.gameTime();
            long initialSamples = a.status().get("sample").getAsLong();
            waitUntil("暂停期间控制循环仍存活", Duration.ofSeconds(10),
                    () -> a.status().get("sample").getAsLong() >= initialSamples + 6);
            assertEquals(frozenTime, a.gameTime(), "断线后实际组模拟时钟不能继续推进");
            a.reconnect();
            a.sync();
            waitUntil("重连补齐并重新确认任期", Duration.ofSeconds(20), () -> a.ready() && a.term() > old.term());
            ControlMessage stale = a.commandRequest("time add 9999");
            stale.json().addProperty("term", old.term());
            assertEquals(MessageType.ERROR, a.remote.request(stale).get(15, TimeUnit.SECONDS).type());
            assertEquals(2277, a.snapshot().data().getLong("DayTime"));
            report.addProperty("oldTerm", old.term()); report.addProperty("newTerm", a.term());
            report.addProperty("disconnectPausedGameTime", frozenTime);
            report.addProperty("result", "passed");
            Files.writeString(root.resolve("result.json"), report.toString(), StandardCharsets.UTF_8);
        } catch (Throwable failure) {
            report.addProperty("result", "failed"); report.addProperty("failure", failure.toString());
            Files.writeString(root.resolve("result.json"), report.toString(), StandardCharsets.UTF_8);
            throw new AssertionError("三进程元数据冒烟失败，日志目录: " + root + "\n中心尾日志:\n"
                    + tail(centerRoot.resolve("console.log")), failure);
        } finally {
            if (first != null) first.close();
            if (second != null) second.close();
            try { center.getOutputStream().write("stop\n".getBytes(StandardCharsets.UTF_8)); center.getOutputStream().flush(); }
            catch (IOException ignored) { }
            stopProcess(center);
        }
    }

    /** 每个实例对应一位父客户端；没有借用 WorkerProcess 的进程级单例，两个计算 JVM 真正并行运行。 */
    private final class WorkerNode implements AutoCloseable {
        final UUID id = UUID.randomUUID();
        final String name;
        final int controlPort;
        final EnvironmentPreflightInfo environment;
        final Path folder, env;
        final ControlServer ipc = new ControlServer("127.0.0.1", 0);
        final CompletableFuture<Void> started = new CompletableFuture<>();
        final AtomicInteger snapshotRequests = new AtomicInteger();
        final AtomicInteger catchupRequests = new AtomicInteger();
        final AtomicInteger catchupReplies = new AtomicInteger();
        final AtomicInteger catchupSnapshotFallbacks = new AtomicInteger();
        final AtomicBoolean dropNextPatch = new AtomicBoolean();
        final WorkerRpc rpc;
        volatile ControlConnection remote;
        ControlClient control;
        Process process;
        Path working;

        WorkerNode(String name, int controlPort, EnvironmentPreflightInfo environment, Path fixture) throws Exception {
            this.name = name; this.controlPort = controlPort; this.environment = environment;
            folder = root.resolve(name); env = folder.resolve("environment");
            Files.createDirectories(env.resolve("mods"));
            Files.copy(fixture, env.resolve("mods/metadata-observer.jar"));
            createScheduledFunction(env.resolve("world/datapacks/smoke"));
            rpc = new WorkerRpc(message -> {
                String op = JsonUtil.getString(message.json(), "op", "");
                if (op.equals("remote_request")) {
                    ControlMessage request = WorkerEnvelope.unwrap(message);
                    trace("worker_request", request);
                    if (request.type() == MessageType.METADATA_SNAPSHOT_REQUEST) snapshotRequests.incrementAndGet();
                    if (request.type() == MessageType.METADATA_CATCHUP) catchupRequests.incrementAndGet();
                    return remote.request(request).whenComplete((reply, failure) -> {
                        if (reply != null) {
                            trace("center_reply", reply);
                            if (request.type() == MessageType.METADATA_CATCHUP) {
                                if (reply.type() == MessageType.METADATA_CATCHUP || reply.type() == MessageType.METADATA_SNAPSHOT)
                                    catchupReplies.incrementAndGet();
                                if (reply.type() == MessageType.METADATA_SNAPSHOT) catchupSnapshotFallbacks.incrementAndGet();
                            }
                        }
                        else traceFailure(failure);
                    }).thenApply(reply -> WorkerEnvelope.wrap("remote_reply", reply));
                }
                if (op.equals("remote_send")) { var request = WorkerEnvelope.unwrap(message); trace("worker_send", request); remote.send(request); }
                return CompletableFuture.completedFuture(WorkerRpc.ok());
            }, (connection, message) -> {
                if ("ready".equals(JsonUtil.getString(message.json(), "op", ""))) started.complete(null);
            });
        }

        void reconnect() throws Exception {
            if (control != null) control.close();
            control = new ControlClient("127.0.0.1", controlPort);
            for (MessageType type : List.of(MessageType.METADATA_PATCH, MessageType.METADATA_SNAPSHOT,
                    MessageType.METADATA_EVENT, MessageType.PERMISSION_RELAY, MessageType.COMMAND_RELAY)) {
                control.on(type, (c, m) -> {
                    trace("center_push", m);
                    if (type == MessageType.METADATA_PATCH && dropNextPatch.compareAndSet(true, false)) return;
                    if (rpc.connection() != null) rpc.request(WorkerEnvelope.wrap("remote_push", m));
                });
            }
            control.onDisconnect(connection -> {
                if (connection == remote && rpc.connection() != null) rpc.request("metadata_disconnect", null);
            });
            remote = control.connect();
            JsonObject hello = new JsonObject();
            hello.addProperty("version", Protocol.VERSION); hello.addProperty("clientId", id.toString());
            hello.addProperty("playerName", name); hello.addProperty("mode", "client");
            hello.addProperty("runtimeHash", environment.runtimeHash()); hello.addProperty("commandRouting", 1);
            assertAccepted(remote.request(ControlMessage.of(MessageType.HELLO, hello)).get(15, TimeUnit.SECONDS));
            ControlMessage role = remote.request(ControlMessage.of(MessageType.ROLE_REQUEST)).get(20, TimeUnit.SECONDS);
            assertEquals(MessageType.ROLE_ASSIGN, role.type(), role.json().toString());
            assertEquals("primary", role.json().get("role").getAsString());
        }

        void start() throws Exception {
            reconnect();
            MetadataSnapshot snapshot = snapshot();
            EnvironmentManifest manifest = EnvironmentScanner.scan(env, List.of());
            int gamePort = freePort();
            working = RuntimeInstallation.install(env, folder.resolve("runtime"), "world",
                    Map.of("eula", "true", "p2p.local-port", Integer.toString(gamePort), "view-distance", "2", "simulation-distance", "2"));
            Path saves = folder.resolve("data");
            MetadataFiles.prepareSave(env.resolve("world"), saves.resolve("world"), snapshot, System.currentTimeMillis());
            String token = UUID.randomUUID().toString();
            UUID runtimeId = UUID.randomUUID();
            rpc.register(ipc);
            ipc.on(MessageType.HELLO, (connection, message) -> {
                boolean valid = token.equals(JsonUtil.getString(message.json(), "token", ""))
                        && runtimeId.toString().equals(JsonUtil.getString(message.json(), "runtimeId", ""));
                JsonObject ack = new JsonObject(); ack.addProperty("accepted", valid);
                if (valid) { connection.setPeerId(runtimeId); rpc.attach(connection); }
                connection.send(message.reply(MessageType.HELLO_ACK, ack, null));
            });
            ipc.start();
            JsonObject spec = new JsonObject();
            spec.addProperty("ipcPort", ipc.boundPort()); spec.addProperty("token", token);
            spec.addProperty("runtimeId", runtimeId.toString()); spec.addProperty("clientId", id.toString());
            spec.addProperty("playerName", name); spec.addProperty("groupId", id.toString());
            spec.addProperty("worldIdentity", snapshot.worldId()); spec.addProperty("claimRetrySeconds", 2);
            spec.addProperty("runtimeHash", environment.runtimeHash()); spec.addProperty("manifestHash", manifest.globalHash());
            spec.add("primaryManifest", manifest.toJson());
            spec.addProperty("metadataSnapshot", Base64.getEncoder().encodeToString(MetadataWire.encode(MetadataWire.snapshotToTag(snapshot))));
            Path session = folder.resolve("session.json"); JsonUtil.writeFileAtomic(session, spec);
            List<String> command = new ArrayList<>(RuntimeLaunchCommand.build(javaExecutable(), working, saves, "world", session));
            command.add(1, "-Xmx1G");
            process = new ProcessBuilder(command).directory(working.toFile()).redirectErrorStream(true)
                    .redirectOutput(folder.resolve("console.log").toFile()).start();
            process.onExit().thenAccept(exit -> started.completeExceptionally(new IOException("计算进程提前退出: " + exit.exitValue())));
            try { started.get(120, TimeUnit.SECONDS); sync(); }
            catch (Exception error) { throw new IOException("计算进程 " + name + " 启动失败:\n" + tail(folder.resolve("console.log")), error); }
        }

        void sync() throws Exception { rpc.request("metadata_sync", null).get(30, TimeUnit.SECONDS); }
        MetadataSnapshot snapshot() throws Exception { return MetadataClient.requestSnapshot(remote, "").get(15, TimeUnit.SECONDS); }
        ControlMessage commandRequest(String command) throws Exception {
            MetadataSnapshot snapshot = snapshot();
            JsonObject json = new JsonObject();
            json.addProperty("worldId", snapshot.worldId()); json.addProperty("baseVersion", snapshot.version());
            json.addProperty("term", snapshot.term()); json.addProperty("operationId", UUID.randomUUID().toString());
            json.addProperty("action", "command"); json.addProperty("command", command);
            json.addProperty("permission", 2); json.addProperty("source", "MetadataSmoke");
            json.addProperty("dimension", "minecraft:overworld");
            return ControlMessage.of(MessageType.METADATA_CHANGE, json);
        }
        void command(String command) throws Exception { assertAccepted(remote.request(commandRequest(command)).get(15, TimeUnit.SECONDS)); }
        JsonObject status() {
            try { return JsonUtil.parseObject(Files.readString(working.resolve("metadata-smoke-status.json"))); }
            catch (Exception ignored) { JsonObject empty = new JsonObject(); empty.addProperty("sample", -1); return empty; }
        }
        long dayTime() { return JsonUtil.getLong(status(), "dayTime", -1); }
        long gameTime() { return JsonUtil.getLong(status(), "gameTime", -1); }
        long term() { return JsonUtil.getLong(status(), "term", -1); }
        long version() { return JsonUtil.getLong(status(), "version", -1); }
        long bossCount() { return JsonUtil.getLong(status(), "bossCount", -1); }
        long borderRemaining() { return JsonUtil.getLong(status(), "borderRemaining", -1); }
        boolean ready() { return JsonUtil.getBoolean(status(), "ready", false); }
        synchronized void trace(String direction, ControlMessage message) {
            try { Files.writeString(folder.resolve("control-trace.log"), System.currentTimeMillis() + " " + direction
                    + " " + message.type() + " " + message.json() + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND); }
            catch (IOException failure) { throw new UncheckedIOException(failure); }
        }
        synchronized void traceFailure(Throwable failure) {
            try { Files.writeString(folder.resolve("control-trace.log"), System.currentTimeMillis() + " request_failed " + failure + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND); }
            catch (IOException problem) { throw new UncheckedIOException(problem); }
        }
        @Override public void close() {
            if (process != null && process.isAlive()) {
                try { rpc.request("stop", null).get(5, TimeUnit.SECONDS); rpc.request("dispose", null); }
                catch (Exception ignored) { }
                stopProcess(process);
            }
            if (control != null) control.close();
            ipc.stop();
        }
    }

    private Process launchCenter(Path working) throws IOException {
        List<String> args = new ArrayList<>(); args.add(javaExecutable().toString());
        args.addAll(development.jvmArguments()); args.add("-Xmx1G");
        args.add("-cp"); args.add(development.classpath()); args.add(development.mainClass());
        args.add("nogui");
        return new ProcessBuilder(args).directory(working.toFile()).redirectErrorStream(true)
                .redirectOutput(working.resolve("console.log").toFile()).start();
    }

    private EnvironmentPreflightInfo preflight(int port) throws Exception {
        ControlClient probe = new ControlClient("127.0.0.1", port);
        try {
            ControlConnection connection = probe.connect();
            JsonObject request = new JsonObject(); request.addProperty("version", Protocol.VERSION);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                var info = EnvironmentPreflightInfo.fromJson(connection.request(ControlMessage.of(MessageType.ENV_PREFLIGHT, request))
                        .get(10, TimeUnit.SECONDS).json());
                if (info.ready()) return info;
                Thread.sleep(100);
            }
            throw new IOException("中心环境快照未就绪");
        } finally { probe.close(); }
    }

    private Path createObserverFixture() throws Exception {
        Path source = root.resolve("fixture-source/MetadataObserver.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, """
                package metadatafixture;
                public final class MetadataObserver implements net.fabricmc.api.ModInitializer {
                    private long samples;
                    public void onInitialize() {
                        net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents.END_SERVER_TICK.register(server -> {
                            if (server.overworld() == null || ++samples % 5 != 0) return;
                            try {
                                var root = net.fabricmc.loader.api.FabricLoader.getInstance().getGameDir();
                                var json = new com.google.gson.JsonObject();
                                json.addProperty("sample", samples / 5);
                                json.addProperty("dayTime", server.overworld().getDayTime());
                                json.addProperty("gameTime", server.overworld().getGameTime());
                                json.addProperty("bossCount", server.getCustomBossEvents().getEvents().size());
                                json.addProperty("borderRemaining", server.overworld().getWorldBorder().getLerpRemainingTime());
                                json.addProperty("ready", imsng.player_to_player.metadata.MetadataClient.isSimulationReady());
                                var state = imsng.player_to_player.metadata.MetadataClient.snapshot();
                                if (state != null) { json.addProperty("version", state.version()); json.addProperty("term", state.term()); }
                                java.nio.file.Files.writeString(root.resolve("metadata-smoke-status.tmp"), json.toString());
                                java.nio.file.Files.move(root.resolve("metadata-smoke-status.tmp"), root.resolve("metadata-smoke-status.json"),
                                    java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                            } catch (Exception failure) { throw new IllegalStateException(failure); }
                        });
                    }
                }
                """, StandardCharsets.UTF_8);
        Path classes = root.resolve("fixture-classes"); Files.createDirectories(classes);
        int code = ToolProvider.getSystemJavaCompiler().run(null, null, null, "--release", "17", "-encoding", "UTF-8",
                "-classpath", development.classpath(), "-d", classes.toString(), source.toString());
        assertEquals(0, code, "真实运行观测模组编译失败");
        Path jarPath = root.resolve("metadata-observer.jar");
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath))) {
            jar.putNextEntry(new JarEntry("fabric.mod.json"));
            jar.write("""
                    {"schemaVersion":1,"id":"metadata_smoke_observer","version":"1.0.0","environment":"server",
                    "entrypoints":{"main":["metadatafixture.MetadataObserver"]},"depends":{"fabricloader":">=0.15.0"}}
                    """.getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
            jar.putNextEntry(new JarEntry("metadatafixture/MetadataObserver.class"));
            jar.write(Files.readAllBytes(classes.resolve("metadatafixture/MetadataObserver.class"))); jar.closeEntry();
        }
        return jarPath;
    }

    private static void createScheduledFunction(Path pack) throws IOException {
        Files.createDirectories(pack.resolve("data/smoke/functions"));
        Files.writeString(pack.resolve("pack.mcmeta"),
                "{\"pack\":{\"pack_format\":26,\"description\":\"Metadata schedule smoke\"}}", StandardCharsets.UTF_8);
        // @s 不存在是原版计划函数的服务端源语义，测试不依赖任何登录玩家实体。
        Files.writeString(pack.resolve("data/smoke/functions/once.mcfunction"),
                "time add 77\nexecute if entity @s run time add 9999\n", StandardCharsets.UTF_8);
    }

    private static void assertAccepted(ControlMessage message) {
        assertTrue(message.type() != MessageType.ERROR && JsonUtil.getBoolean(message.json(), "accepted", false), message.json().toString());
    }
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) { return socket.getLocalPort(); }
    }
    private static Path javaExecutable() { return Path.of(System.getProperty("java.home"), "bin", System.getProperty("os.name").contains("Windows") ? "java.exe" : "java"); }
    private static void waitUntil(String description, Duration timeout, BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < until) { if (condition.getAsBoolean()) return; Thread.sleep(50); }
        fail("等待超时: " + description);
    }
    private static void stopProcess(Process process) {
        try { if (!process.waitFor(20, TimeUnit.SECONDS)) { process.destroy(); if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); } }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); process.destroyForcibly(); }
    }
    private static String tail(Path log) {
        try {
            // 开发启动器可能按 Windows native.encoding 输出；坏字符不能掩盖真正的失败堆栈。
            String value = new String(Files.readAllBytes(log), java.nio.charset.Charset.forName(
                    System.getProperty("native.encoding", StandardCharsets.UTF_8.name())));
            return value.substring(Math.max(0, value.length() - 12000));
        }
        catch (IOException ignored) { return "尚无日志"; }
    }
}
