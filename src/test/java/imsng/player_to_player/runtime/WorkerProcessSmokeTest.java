package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.core.*;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.metadata.*;
import net.fabricmc.api.ModInitializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import javax.tools.ToolProvider;
import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 显式开启的真实独立Fabric服务端冒烟验收。无需图形客户端或公网中心服，可使用本次
 * 构建的开发描述，或只读现场环境的启动依赖与正式 remap JAR。两个模式均检查
 * server-only模组真实加载、同步配置真实读取、零出生点申请、身份桥接与持久化退出。
 */
@EnabledIfSystemProperty(named = "p2p.worker.smoke", matches = "true")
class WorkerProcessSmokeTest {
    @TempDir Path temporary;

    @Test void cancellingBeforeReadyNeverLeavesARunningWorkerOrPublishesAWorld() throws Exception {
        Path environment = temporary.resolve("cancel-environment"); Files.createDirectories(environment);
        Path folder = temporary.resolve("cancel-session"); Files.createDirectories(folder);
        NodeContext context = NodeContext.get();
        P2PPaths paths = new P2PPaths(temporary.resolve("cancel-game")); paths.ensureBaseDirs();
        context.setPaths(paths); context.setConfig(GlobalConfig.loadOrCreate(paths.globalConfig(), NodeMode.CLIENT));
        context.setMode(NodeMode.CLIENT); context.setClientRole(ClientRole.PRIMARY);
        context.setClientId(UUID.randomUUID()); context.setGroupId(context.clientId()); context.setPlayerName("CancelProbe");
        ControlConnection remote = new ControlConnection() {
            public void send(ControlMessage message) { }
            public CompletableFuture<ControlMessage> request(ControlMessage message) { return new CompletableFuture<>(); }
            public SocketAddress remoteAddress() { return null; }
            public UUID peerId() { return null; }
            public void setPeerId(UUID peer) { }
            public boolean isOpen() { return true; }
            public void close() { }
        };
        var manifest = new imsng.player_to_player.env.EnvironmentManifest(Map.of());
        var launched = CompletableFuture.supplyAsync(() -> {
            try { return WorkerProcess.start(folder, environment, "world", folder.resolve("data"), "world", "",
                    context.groupId(), remote, Map.of("eula", "true"), manifest, manifest.globalHash()); }
            catch (Exception error) { throw new CompletionException(error); }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!WorkerProcess.isStarting() && !launched.isDone() && System.nanoTime() < deadline) Thread.sleep(1);
        assertTrue(WorkerProcess.isStarting());
        WorkerProcess.closeSession();
        assertThrows(ExecutionException.class, () -> launched.get(40, TimeUnit.SECONDS));
        assertFalse(WorkerProcess.isRunning(), "取消完成前必须等待本代安装及子JVM清理，之后不得自动切入世界");
    }

    @Test void startsDedicatedFabricWithServerOnlyModAndStopsThroughIpc() throws Exception {
        Path environment = temporary.resolve("environment");
        Files.createDirectories(environment.resolve("mods"));
        Files.createDirectories(environment.resolve("config"));
        prepareProductionEnvironment(environment);
        Files.writeString(environment.resolve("config/worker-fixture.txt"), "authority-config-17", StandardCharsets.UTF_8);
        createServerOnlyFixture(environment.resolve("mods/worker-fixture.jar"));
        Path folder = temporary.resolve("world-session");
        Files.createDirectories(folder);
        NodeContext context = NodeContext.get();
        P2PPaths paths = new P2PPaths(temporary.resolve("game"));
        paths.ensureBaseDirs();
        context.setPaths(paths);
        context.setConfig(GlobalConfig.loadOrCreate(paths.globalConfig(), NodeMode.CLIENT));
        context.setMode(NodeMode.CLIENT);
        context.setClientRole(ClientRole.PRIMARY);
        context.setClientId(UUID.randomUUID());
        context.setGroupId(context.clientId());
        context.setPlayerName("WorkerProbe");
        AtomicInteger claims = new AtomicInteger();
        MetadataSnapshot metadata = metadataFixture();
        byte[] metadataBytes = MetadataWire.encode(MetadataWire.snapshotToTag(metadata));
        AtomicInteger metadataAcks = new AtomicInteger();
        ControlConnection remote = new ControlConnection() {
            public void send(ControlMessage message) { }
            public CompletableFuture<ControlMessage> request(ControlMessage message) {
                if (message.type() == MessageType.METADATA_SNAPSHOT_REQUEST) {
                    return CompletableFuture.completedFuture(message.reply(MessageType.METADATA_SNAPSHOT, new JsonObject(), metadataBytes));
                }
                if (message.type() == MessageType.METADATA_ACK) {
                    metadataAcks.incrementAndGet();
                    JsonObject ack = new JsonObject(); ack.addProperty("accepted", true);
                    return CompletableFuture.completedFuture(message.reply(MessageType.METADATA_ACK, ack, null));
                }
                if (message.type() == MessageType.CHUNK_CLAIM_REQUEST) claims.incrementAndGet();
                JsonObject error = new JsonObject(); error.addProperty("code", "SMOKE_NO_WORLD_DEMAND");
                return CompletableFuture.completedFuture(message.reply(MessageType.ERROR, error, null));
            }
            public SocketAddress remoteAddress() { return null; }
            public UUID peerId() { return context.clientId(); }
            public void setPeerId(UUID peer) { }
            public boolean isOpen() { return true; }
            public void close() { }
        };
        WorkerProcess.setStopListener(() -> { });
        WorkerProcess.setStoppedFlush(path -> CompletableFuture.completedFuture(null));
        var manifest = imsng.player_to_player.env.EnvironmentScanner.scan(environment, java.util.List.of());
        try {
            int port = WorkerProcess.start(folder, environment, "world", folder.resolve("data"),
                    "world", metadata.worldId(), context.groupId(), remote,
                    Map.of("view-distance", "2", "simulation-distance", "2", "eula", "true"),
                    manifest, manifest.globalHash());
            assertTrue(port > 0);
            assertTrue(WorkerProcess.isManagedWorld());
            assertTrue(metadataAcks.get() > 0, "子服启动后必须应用快照并完成元数据版本确认");
            assertEquals(metadata.version(), WorkerProcess.metadataVersion());
            assertEquals(metadata.term(), WorkerProcess.metadataTerm());
            assertEquals("SERVER|authority-config-17", Files.readString(
                    WorkerProcess.currentDirectory().resolve("worker-fixture-result.txt"), StandardCharsets.UTF_8));
            // 直接接子服即使伪造正确玩家名也必须拒绝；走父进程授权桥时，服务端返回
            // 的UUID必须为权威会话UUID，不能信任客户端hello里自报的UUID或离线算法。
            try (Socket unauthorized = new Socket("127.0.0.1", port)) {
                unauthorized.setSoTimeout(10000);
                loginHello(unauthorized, port, context.playerName(), context.clientId());
                assertEquals(0, readVarInt(new DataInputStream(new ByteArrayInputStream(readFrame(unauthorized)))));
            }
            int bridge = WorkerProcess.openLocalPlayerBridge();
            try (Socket authorized = new Socket("127.0.0.1", bridge)) {
                authorized.setSoTimeout(15000);
                loginHello(authorized, bridge, context.playerName(), UUID.randomUUID());
                assertEquals(context.clientId(), readLoginIdentity(authorized));
            }
            assertEquals(0, claims.get(), "没有玩家时子服不得申请原版出生点常加载区块");
            // 用已持久化的权威玩家文件驱动真正的child退出尾部发布，核对stop事件和
            // 最终数据是两个屏障，父进程收到/验证bundle后子服才得到发布确认。
            var player = new net.minecraft.nbt.CompoundTag();
            player.putString("Dimension", "minecraft:overworld");
            var position = new net.minecraft.nbt.ListTag();
            position.add(net.minecraft.nbt.DoubleTag.valueOf(1.5));
            position.add(net.minecraft.nbt.DoubleTag.valueOf(80));
            position.add(net.minecraft.nbt.DoubleTag.valueOf(-3.5));
            player.put("Pos", position); player.putLong("ProbeValue", 170024L);
            Path playerDir = WorkerProcess.currentSaveDirectory().resolve("playerdata"); Files.createDirectories(playerDir);
            Files.write(playerDir.resolve(context.clientId() + ".dat"), imsng.player_to_player.group.PlayerStateNbt.encode(player));
            var bundle = new CompletableFuture<imsng.player_to_player.group.ExitBundle>();
            WorkerProcess.setExitBundleListener(bundle::complete);
            var exit = WorkerProcess.beginExitDrain(UUID.randomUUID()).get(15, TimeUnit.SECONDS);
            WorkerProcess.adoptExitManifest(exit);
            WorkerProcess.stopForExit().get(45, TimeUnit.SECONDS);
            var finalBundle = bundle.get(30, TimeUnit.SECONDS);
            assertEquals(exit, finalBundle.manifest());
            assertEquals(170024L, imsng.player_to_player.group.PlayerStateNbt.decodeValidated(
                    finalBundle.playerData(context.clientId())).getLong("ProbeValue"));
        } catch (Exception failure) {
            Path log = folder.resolve("runtime/worker-console.log");
            // 开发启动器在 Windows 上使用 native.encoding；诊断读取不能以 UTF-8 解码失败
            // 覆盖真正的启动/登录错误。解码允许替代字符，原异常始终保留为 cause。
            String output = Files.exists(log) ? new String(Files.readAllBytes(log), java.nio.charset.Charset.forName(
                    System.getProperty("native.encoding", StandardCharsets.UTF_8.name()))) : "未生成子进程日志";
            throw new AssertionError("独立服务端冒烟失败：\n" + output.substring(Math.max(0, output.length() - 24000)), failure);
        } finally {
            WorkerProcess.closeSession();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (WorkerProcess.isRunning() && System.nanoTime() < deadline) Thread.sleep(20);
        }
        assertFalse(WorkerProcess.isRunning(), "子服停止后父进程必须归还IPC和当前实例");
    }

    /**
     * 正式冒烟不复用用户存档或运行目录：只读启动 JAR、游戏/Loader 依赖与 Fabric API。
     * 根目录的诊断工具、启动脚本、账号和配置均不复制；模组始终使用本次构建产物。
     * 临时副本后续仍经过 RuntimeInstallation、环境证明与真实 WorkerProcess 启动链。
     */
    private static void prepareProductionEnvironment(Path environment) throws Exception {
        String sourceProperty = System.getProperty("p2p.worker.productionEnvironment", "");
        if (sourceProperty.isBlank()) return;
        assertTrue(System.getProperty("p2p.worker.developmentSpec", "").isBlank(),
                "正式冒烟不能借助 Loom 开发入口绕过发行启动器");
        Path source = Path.of(sourceProperty).toAbsolutePath().normalize();
        assertTrue(Files.isDirectory(source, LinkOption.NOFOLLOW_LINKS), "正式环境源必须是本机普通目录");
        // 直接调用生产选择逻辑；保留现场原始文件名，使版本化/重命名入口得到真实验证。
        List<String> command = RuntimeLaunchCommand.build(Path.of("java"), source,
                environment.resolve("unused-saves"), "world", environment.resolve("unused-session.json"));
        Path launcher = Path.of(command.get(command.indexOf("-jar") + 1));
        Files.copy(launcher, environment.resolve(launcher.getFileName()));
        for (String directory : List.of("libraries", ".fabric/server", "versions")) {
            Path dependencies = source.resolve(directory);
            if (!Files.isDirectory(dependencies, LinkOption.NOFOLLOW_LINKS)) continue;
            try (var files = Files.walk(dependencies)) {
                for (Path file : files.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                        && path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar")).toList()) {
                    Path target = environment.resolve(source.relativize(file));
                    Files.createDirectories(target.getParent());
                    Files.copy(file, target);
                }
            }
        }
        boolean copiedApi = false;
        try (var mods = Files.list(source.resolve("mods"))) {
            for (Path mod : mods.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    && path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar")).toList()) {
                if (!imsng.player_to_player.env.ClientEnvironmentCompatibility.metadata(mod).id().equals("fabric-api")) continue;
                Files.copy(mod, environment.resolve("mods/fabric-api.jar"));
                copiedApi = true;
            }
        }
        assertTrue(copiedApi, "正式环境源需要已安装 Fabric API");
        Path builtMod = Path.of(System.getProperty("p2p.worker.productionMod"));
        assertTrue(Files.isRegularFile(builtMod), "正式冒烟需要本次 remap JAR");
        Files.copy(builtMod, environment.resolve("mods/player_to_player.jar"));
    }

    /** 不借用本机真实世界：fixture包含1.20.4三维度原版生成器与字段类型。 */
    private static MetadataSnapshot metadataFixture() throws Exception {
        var data = net.minecraft.nbt.TagParser.parseTag("""
                {DataVersion:3700,version:19133,Version:{Id:3700,Name:"1.20.4",Snapshot:0b,Series:"main"},
                 LevelName:"metadata-smoke",GameType:0,Difficulty:2b,hardcore:0b,allowCommands:1b,
                 initialized:1b,SpawnX:0,SpawnY:64,SpawnZ:0,SpawnAngle:0.0f,Time:40L,DayTime:6000L,
                 clearWeatherTime:0,rainTime:12000,raining:0b,thunderTime:12000,thundering:0b,
                 DataPacks:{Enabled:["vanilla","fabric","fabric-convention-tags-v1","fabric-gametest-api-v1"],Disabled:["bundle","trade_rebalance","update_1_21"]},
                 DragonFight:{DragonKilled:0b,PreviouslyKilled:0b,NeedsStateScanning:1b},
                 GameRules:{doDaylightCycle:"true",doWeatherCycle:"true"},
                 WorldGenSettings:{seed:42L,generate_features:1b,bonus_chest:0b,dimensions:{
                  "minecraft:overworld":{type:"minecraft:overworld",generator:{type:"minecraft:noise",settings:"minecraft:overworld",biome_source:{type:"minecraft:multi_noise",preset:"minecraft:overworld"}}},
                  "minecraft:the_nether":{type:"minecraft:the_nether",generator:{type:"minecraft:noise",settings:"minecraft:nether",biome_source:{type:"minecraft:multi_noise",preset:"minecraft:nether"}}},
                  "minecraft:the_end":{type:"minecraft:the_end",generator:{type:"minecraft:noise",settings:"minecraft:end",biome_source:{type:"minecraft:the_end"}}}
                 }}}
                """);
        if (!System.getProperty("p2p.worker.productionEnvironment", "").isBlank()) {
            // fabric-gametest-api 的开发数据包不在发行版 Fabric API 中，不能让测试世界依赖它。
            data.getCompound("DataPacks").getList("Enabled", 8)
                    .removeIf(pack -> pack.getAsString().equals("fabric-gametest-api-v1"));
        }
        var root = new net.minecraft.nbt.CompoundTag(); root.put("Data", data);
        return new MetadataSnapshot(imsng.player_to_player.world.WorldIdentity.ofLevelData(root).orElseThrow(), 4, 2, data);
    }

    private static void loginHello(Socket socket, int port, String name, UUID claimedId) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(bytes);
        writeVarInt(out, 0); writeVarInt(out, 765); writeString(out, "127.0.0.1"); out.writeShort(port); writeVarInt(out, 2);
        writeFrame(socket, bytes.toByteArray());
        bytes.reset(); writeVarInt(out, 0); writeString(out, name);
        out.writeLong(claimedId.getMostSignificantBits()); out.writeLong(claimedId.getLeastSignificantBits());
        writeFrame(socket, bytes.toByteArray());
    }
    private static UUID readLoginIdentity(Socket socket) throws IOException {
        boolean compression = false;
        for (int packets = 0; packets < 16; packets++) {
            byte[] frame = readFrame(socket);
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
            if (compression) {
                int expanded = readVarInt(input);
                input = expanded == 0 ? input : new DataInputStream(new java.util.zip.InflaterInputStream(input));
            }
            int id = readVarInt(input);
            if (id == 2) return new UUID(input.readLong(), input.readLong());
            if (id == 3) { readVarInt(input); compression = true; continue; }
            if (id == 4) {
                int query = readVarInt(input);
                ByteArrayOutputStream bytes = new ByteArrayOutputStream(); DataOutputStream reply = new DataOutputStream(bytes);
                if (compression) writeVarInt(reply, 0);
                writeVarInt(reply, 2); writeVarInt(reply, query); reply.writeBoolean(false); writeFrame(socket, bytes.toByteArray());
                continue;
            }
            throw new IOException("授权登录未收到预期profile，packetId=" + id);
        }
        throw new IOException("授权登录包数超限");
    }
    private static byte[] readFrame(Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream()); int size = readVarInt(in);
        if (size <= 0 || size > 1024 * 1024) throw new IOException("非法测试登录帧长度");
        byte[] bytes = in.readNBytes(size); if (bytes.length != size) throw new EOFException(); return bytes;
    }
    private static void writeFrame(Socket socket, byte[] bytes) throws IOException {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream()); writeVarInt(out, bytes.length); out.write(bytes); out.flush();
    }
    private static int readVarInt(DataInputStream input) throws IOException {
        int value = 0;
        for (int shift = 0; shift < 35; shift += 7) { int part = input.readUnsignedByte(); value |= (part & 127) << shift; if ((part & 128) == 0) return value; }
        throw new IOException("非法VarInt");
    }
    private static void writeVarInt(DataOutputStream output, int value) throws IOException {
        do { int part = value & 127; value >>>= 7; output.writeByte(value == 0 ? part : part | 128); } while (value != 0);
    }
    private static void writeString(DataOutputStream output, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8); writeVarInt(output, bytes.length); output.write(bytes);
    }

    private void createServerOnlyFixture(Path jarFile) throws Exception {
        Path source = temporary.resolve("fixture-source/WorkerProbe.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, """
                package p2pfixture;
                public final class WorkerProbe implements net.fabricmc.api.ModInitializer {
                    public void onInitialize() {
                        try {
                            var loader = net.fabricmc.loader.api.FabricLoader.getInstance();
                            var root = loader.getGameDir();
                            var config = java.nio.file.Files.readString(root.resolve("config/worker-fixture.txt"));
                            java.nio.file.Files.writeString(root.resolve("worker-fixture-result.txt"),
                                loader.getEnvironmentType().name() + "|" + config);
                        } catch (Exception error) { throw new IllegalStateException(error); }
                    }
                }
                """, StandardCharsets.UTF_8);
        Path classes = temporary.resolve("fixture-classes");
        Files.createDirectories(classes);
        Path loader = Path.of(ModInitializer.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        int result = ToolProvider.getSystemJavaCompiler().run(null, null, null,
                "--release", "17", "-classpath", loader.toString(), "-d", classes.toString(), source.toString());
        assertEquals(0, result, "server-only冒烟模组必须编译成功");
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarFile))) {
            jar.putNextEntry(new JarEntry("fabric.mod.json"));
            jar.write("""
                    {"schemaVersion":1,"id":"p2p_worker_fixture","version":"1.0.0","environment":"server",
                    "entrypoints":{"main":["p2pfixture.WorkerProbe"]},"depends":{"fabricloader":">=0.15.0"}}
                    """.getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
            jar.putNextEntry(new JarEntry("p2pfixture/WorkerProbe.class"));
            jar.write(Files.readAllBytes(classes.resolve("p2pfixture/WorkerProbe.class")));
            jar.closeEntry();
        }
    }
}
