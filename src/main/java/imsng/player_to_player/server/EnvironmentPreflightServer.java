package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.env.EnvironmentPreflightInfo;
import imsng.player_to_player.env.EnvironmentRuntimeIdentity;
import imsng.player_to_player.env.EnvironmentSnapshot;
import imsng.player_to_player.env.EnvironmentSnapshotManager;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import net.fabricmc.fabric.api.networking.v1.PacketByteBufs;
import net.fabricmc.fabric.api.networking.v1.ServerLoginConnectionEvents;
import net.fabricmc.fabric.api.networking.v1.ServerLoginNetworking;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.ResourceLocation;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntSupplier;

/**
 * 登录配置/注册表同步之前发布环境描述。客户端需下载时主动关闭该 MC 登录，
 * 文件下载另走只读控制连接，因此数 GB 环境也不占用原版 600 tick 登录等待窗口。
 */
public final class EnvironmentPreflightServer {
    public static final ResourceLocation CHANNEL = new ResourceLocation("player_to_player", "environment_preflight");
    private static final Set<String> GAME_SETTINGS = imsng.player_to_player.runtime.RuntimeInstallation.WORLD_SETTINGS;
    private static volatile EnvironmentPreflightServer published;
    private static boolean registered;
    private final EnvironmentSnapshotManager snapshots;
    private final String worldName;
    private final String worldPath;
    private final Map<String, String> worldProperties;
    private final IntSupplier controlPort;
    /** 同一主机的同名世界也必须区分；登录查询与控制握手绑定同一个服务实例。 */
    private final String serverInstanceId = java.util.UUID.randomUUID().toString();
    private volatile EnvironmentSnapshot cachedSnapshot;
    private volatile String cachedRuntimeHash = "";

    private EnvironmentPreflightServer(EnvironmentSnapshotManager snapshots, String worldName,
                                       String worldPath, Map<String, String> properties, IntSupplier controlPort) {
        this.snapshots = snapshots;
        this.worldName = worldName;
        this.worldPath = EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath);
        this.worldProperties = Map.copyOf(properties);
        this.controlPort = controlPort;
    }

    /** 只读取明确列出的游戏规则；端口、rcon、在线认证和管理凭据永不分发。 */
    public static Map<String, String> readGameSettings(Path serverRoot) {
        Properties source = new Properties();
        Path path = serverRoot.resolve("server.properties");
        if (Files.isRegularFile(path)) {
            try (Reader reader = Files.newBufferedReader(path)) { source.load(reader); }
            catch (IOException e) { throw new IllegalStateException("读取服务端游戏设置失败", e); }
        }
        Map<String, String> result = new TreeMap<>();
        for (String key : GAME_SETTINGS) {
            if (source.containsKey(key)) result.put(key, source.getProperty(key));
        }
        // 此入口只在已成功启动的专用服务端调用，中心已经完成 EULA 检查。
        result.put("eula", "true");
        return Map.copyOf(result);
    }

    public static synchronized void registerLogin() {
        if (registered) return;
        registered = true;
        ServerLoginNetworking.registerGlobalReceiver(CHANNEL, (server, handler, understood, buf, synchronizer, sender) -> {
            if (!understood) {
                handler.disconnect(Component.literal("此服务器需要支持环境预检的 Player to Player 模组。"));
                return;
            }
            boolean ready = buf.readBoolean();
            String runtimeHash = buf.readUtf(64);
            EnvironmentPreflightInfo info = currentInfo();
            if (!ready || info == null || !info.ready() || !info.runtimeHash().equals(runtimeHash)) {
                handler.disconnect(Component.literal("环境尚未准备完成，请等待同步后重新连接。"));
            }
        });
        ServerLoginConnectionEvents.QUERY_START.register((handler, server, sender, synchronizer) -> {
            EnvironmentPreflightInfo info = currentInfo();
            if (info == null) return;
            var buffer = PacketByteBufs.create();
            buffer.writeUtf(info.toJson().toString(), 32767);
            sender.sendPacket(CHANNEL, buffer);
        });
    }

    /** 注册独立只读握手；没有 clientId，更不会进入 ONLINE、玩家表或断线组清理。 */
    public static void install(HandlerRegistry handlers, EnvironmentSnapshotManager snapshots,
                               String worldName, String worldPath, Map<String, String> properties,
                               IntSupplier controlPort) {
        EnvironmentPreflightServer service = new EnvironmentPreflightServer(
                snapshots, worldName, worldPath, properties, controlPort);
        published = service;
        handlers.on(MessageType.ENV_PREFLIGHT, (connection, request) -> {
            if (connection.peerId() != null) {
                connection.send(request.reply(MessageType.ERROR, error("不能复用玩家会话进行预检"), null));
                return;
            }
            if (JsonUtil.getInt(request.json(), "version", -1) != Protocol.VERSION) {
                connection.send(request.reply(MessageType.ERROR, error("服务端与客户端协议版本不匹配"), null));
                return;
            }
            connection.restrictToEnvironment();
            EnvironmentPreflightInfo info = service.info();
            connection.send(request.reply(MessageType.ENV_PREFLIGHT, info.toJson(), null));
        });
    }

    public static EnvironmentPreflightInfo currentInfo() {
        EnvironmentPreflightServer service = published;
        return service == null || service.controlPort.getAsInt() < 1 ? null : service.info();
    }

    private synchronized EnvironmentPreflightInfo info() {
        EnvironmentSnapshot snapshot = snapshots.current();
        if (snapshot != cachedSnapshot) {
            cachedRuntimeHash = snapshot == null ? "" : EnvironmentRuntimeIdentity.hash(
                    snapshot.manifest(), worldPath, worldProperties);
            cachedSnapshot = snapshot;
        }
        return new EnvironmentPreflightInfo(controlPort.getAsInt(), PortSyncServer.publishedRelayPort(),
                worldName, worldPath, snapshot == null ? "" : snapshot.snapshotId(), cachedRuntimeHash,
                worldProperties, serverInstanceId);
    }

    public static void clear() { published = null; }
    private static JsonObject error(String reason) {
        JsonObject json = new JsonObject();
        json.addProperty("message", reason);
        return json;
    }
}
