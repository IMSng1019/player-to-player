package imsng.player_to_player.client.env;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import imsng.player_to_player.client.group.WorldSwitcher;
import imsng.player_to_player.client.mixin.ClientLoginAccess;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.ClientEnvironmentCompatibility;
import imsng.player_to_player.env.ClientRuntimeStartup;
import imsng.player_to_player.env.EnvironmentPreflightInfo;
import imsng.player_to_player.env.EnvironmentPreflightState;
import imsng.player_to_player.env.EnvSyncClient;
import imsng.player_to_player.netproto.ControlClient;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.server.EnvironmentPreflightServer;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.fabricmc.fabric.api.client.networking.v1.ClientLoginConnectionEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientLoginNetworking;
import net.fabricmc.fabric.api.networking.v1.PacketByteBufs;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.ConnectScreen;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.client.multiplayer.ServerData;
import net.minecraft.client.multiplayer.resolver.ServerAddress;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.network.chat.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * QUERY_START → 暂离登录 → 独立只读下载 → 实际加载检查 → 重连。
 * 查询由真实 MC 目标发出，非默认控制端口无需猜测；普通服务器没有该查询，照常进入。
 * 异步任务、下载成果和重连都携带 epoch，旧服务器迟到响应不得切走玩家的新界面。
 */
public final class EnvironmentPreflightClient {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/preflight");
    private static final EnvironmentPreflightState STATE = new EnvironmentPreflightState();
    private static final Map<ClientHandshakePacketListenerImpl, Request> LOGINS = new ConcurrentHashMap<>();
    private static volatile Request request;
    private static volatile Prepared prepared;
    private static CompletableFuture<ClientRuntimeProfile.Loaded> loaded;
    private static boolean resuming;
    private static boolean registered;

    public record Prepared(String address, String host, Path worldFolder,
                           EnvironmentPreflightInfo info, EnvSyncClient.PairResult environments) { }

    private static final class Request {
        final long epoch;
        final String key;
        final Screen parent;
        final ServerAddress address;
        final ServerData data;
        final boolean quickPlay;
        volatile boolean downloading;
        volatile ControlClient downloader;
        Request(long epoch, String key, Screen parent, ServerAddress address, ServerData data, boolean quickPlay) {
            this.epoch = epoch; this.key = key; this.parent = parent; this.address = address;
            this.data = data; this.quickPlay = quickPlay;
        }
    }
    private EnvironmentPreflightClient() { }

    public static synchronized void register() {
        if (registered) return;
        registered = true;
        loaded = CompletableFuture.supplyAsync(() -> {
            try { return ClientRuntimeProfile.captureLoadedMods(); }
            catch (Exception e) { throw new java.util.concurrent.CompletionException(e); }
        }, ThreadPools.io());
        ClientLoginConnectionEvents.INIT.register((handler, client) -> {
            Request active = request;
            ServerData data = ((ClientLoginAccess) handler).player_to_player$serverData();
            if (active != null && data == active.data && STATE.current(active.epoch)) LOGINS.put(handler, active);
        });
        ClientLoginConnectionEvents.DISCONNECT.register((handler, client) -> {
            Request closed = LOGINS.remove(handler);
            if (closed != null && !closed.downloading && request == closed) {
                STATE.cancel();
                prepared = null;
            }
        });
        ClientLoginNetworking.registerGlobalReceiver(EnvironmentPreflightServer.CHANNEL,
                (client, handler, buffer, callbacks) -> receive(client, handler, buffer));
    }

    public static void onConnect(Screen parent, Minecraft client, ServerAddress address, ServerData data, boolean quickPlay) {
        // 原版会忽略“连接界面中再次开始连接”的调用，不能提前让当前合法票据失效。
        if (resuming || client.screen instanceof ConnectScreen
                || (data != null && WorldSwitcher.isTunnelAddress(data.ip))) return;
        LOGINS.clear();
        closeDownloader(request);
        String key = EnvironmentPreflightState.addressKey(address.getHost(), address.getPort());
        long epoch = STATE.begin(key);
        prepared = null;
        request = new Request(epoch, key, parent, address, data, quickPlay);
    }

    private static CompletableFuture<FriendlyByteBuf> receive(Minecraft client,
            ClientHandshakePacketListenerImpl handler, FriendlyByteBuf buffer) {
        Request active = LOGINS.get(handler);
        if (active == null || !STATE.current(active.epoch)) return CompletableFuture.completedFuture(null);
        try {
            EnvironmentPreflightInfo info = EnvironmentPreflightInfo.fromJson(
                    JsonParser.parseString(buffer.readUtf(32767)).getAsJsonObject());
            if (info.ready() && STATE.matches(active.key, info.runtimeHash()) && prepared != null
                    && info.sameRuntime(prepared.info())) {
                var reply = PacketByteBufs.create();
                reply.writeBoolean(true);
                reply.writeUtf(info.runtimeHash(), 64);
                return CompletableFuture.completedFuture(reply);
            }
            if (!active.downloading) {
                active.downloading = true;
                var connection = ((ClientLoginAccess) handler).player_to_player$connection();
                String host = connection.getRemoteAddress() instanceof InetSocketAddress socket && socket.getAddress() != null
                        ? socket.getAddress().getHostAddress() : active.address.getHost();
                client.execute(() -> beginDownload(client, handler, active, info, host));
            }
        } catch (Exception bad) {
            active.downloading = true;
            client.execute(() -> {
                if (!STATE.current(active.epoch)) return;
                var connection = ((ClientLoginAccess) handler).player_to_player$connection();
                connection.disconnect(Component.literal("环境预检失败：" + bad.getMessage()));
                connection.handleDisconnection();
            });
        }
        // 不在登录中等待文件传输。服务端不会把 false 的预检连接放入配置/注册表阶段。
        var reply = PacketByteBufs.create();
        reply.writeBoolean(false);
        reply.writeUtf("", 64);
        return CompletableFuture.completedFuture(reply);
    }

    private static void beginDownload(Minecraft client, ClientHandshakePacketListenerImpl handler,
                                      Request active, EnvironmentPreflightInfo info, String host) {
        if (!STATE.current(active.epoch)) return;
        var connection = ((ClientLoginAccess) handler).player_to_player$connection();
        connection.disconnect(Component.literal("正在准备服务器环境"));
        connection.handleDisconnection();
        EnvironmentPreflightScreen screen = new EnvironmentPreflightScreen(() -> cancel(client, active));
        client.setScreen(screen);
        ThreadPools.startBlocking("environment-preflight", () -> synchronize(client, screen, active, info, host));
    }

    private static void synchronize(Minecraft client, EnvironmentPreflightScreen screen, Request active,
                                     EnvironmentPreflightInfo initial, String host) {
        ControlClient control = new ControlClient(host, initial.controlPort());
        active.downloader = control;
        try {
            if (!STATE.current(active.epoch)) return;
            ControlConnection connection;
            int actualPort = initial.controlPort();
            try {
                connection = control.connect();
            } catch (java.io.IOException refused) {
                int fallback = NodeContext.get().config().controlPort;
                if (!STATE.current(active.epoch) || fallback < 1 || fallback > 65535 || fallback == actualPort) throw refused;
                control.close();
                control = new ControlClient(host, fallback);
                active.downloader = control;
                connection = control.connect();
                actualPort = fallback;
            }
            EnvironmentPreflightInfo info = initial;
            // 首次快照构建可能需要较久；此等待已经不占原版登录连接。
            for (int attempt = 0; attempt < 300; attempt++) {
                if (!STATE.current(active.epoch)) return;
                JsonObject hello = new JsonObject();
                hello.addProperty("version", Protocol.VERSION);
                ControlMessage response = connection.request(ControlMessage.of(MessageType.ENV_PREFLIGHT, hello))
                        .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (response.type() == MessageType.ERROR) throw new IllegalStateException(
                        JsonUtil.getString(response.json(), "message", "服务端拒绝环境预检"));
                info = EnvironmentPreflightInfo.fromJson(response.json());
                if (!initial.sameServer(info) || (initial.ready() && info.ready() && !initial.sameRuntime(info)))
                    throw new IllegalStateException("控制端口与原登录服务器/环境版本不一致，请检查端口映射后重新连接。");
                if (info.ready()) break;
                Thread.sleep(1000);
            }
            if (!info.ready()) throw new IllegalStateException("服务端环境准备超时，请稍后重新连接。");
            info = info.withControlPort(actualPort);
            var context = NodeContext.get();
            Path worldFolder = context.paths().worldFolder(active.key, info.worldName()).toAbsolutePath().normalize();
            context.paths().ensureWorldDirs(worldFolder);
            Path primary = context.paths().environmentDir(worldFolder, ClientRole.PRIMARY);
            Path secondary = context.paths().environmentDir(worldFolder, ClientRole.SECONDARY);
            EnvSyncClient.PairResult pair = new EnvSyncClient(connection, context.config())
                    .syncPair(primary, secondary, info.snapshotId(),
                            () -> STATE.current(active.epoch) && request == active).join();
            if (!STATE.current(active.epoch)) return;
            ClientRuntimeProfile.Loaded current = loaded.get(60, TimeUnit.SECONDS);
            List<String> mismatches = ClientEnvironmentCompatibility.check(secondary, pair.secondaryManifest(), current.mods());
            if (!ClientRuntimeProfile.wasLaunchedFor(info.runtimeHash())
                    || !ClientRuntimeStartup.matches(info.runtimeHash(), pair.secondaryManifest(), info.worldPath())
                    || !mismatches.isEmpty()) {
                client.execute(() -> { if (STATE.current(active.epoch)) screen.status("正在安装独立的客户端游戏环境…"); });
                ClientRuntimeProfile.Installation installation = ClientRuntimeProfile.install(
                        worldFolder, secondary, pair.secondaryManifest(), info, current, active.key);
                String reason = mismatches.isEmpty() ? "环境已同步。启动配置需要在独立游戏实例中加载。"
                        : "环境已同步，但当前进程尚未加载所需环境：\n" + String.join("；", mismatches.stream().limit(3).toList());
                client.execute(() -> { if (STATE.current(active.epoch)) screen.requireRestart(reason, installation); });
                return;
            }
            EnvironmentPreflightInfo ready = info;
            client.execute(() -> {
                if (!STATE.ready(active.epoch, ready.runtimeHash())) return;
                prepared = new Prepared(active.key, host, worldFolder, ready, pair);
                active.downloading = false;
                resuming = true;
                try { ConnectScreen.startConnecting(active.parent, client, active.address, active.data, active.quickPlay); }
                finally { resuming = false; }
            });
        } catch (Exception failure) {
            LOGGER.warn("登录前环境准备失败", failure);
            client.execute(() -> {
                if (STATE.current(active.epoch)) screen.status("环境准备失败：" + rootMessage(failure) + "。请取消后重试。");
            });
        } finally {
            control.close();
            if (active.downloader == control) active.downloader = null;
        }
    }

    private static String rootMessage(Throwable error) {
        while (error.getCause() != null) error = error.getCause();
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

    private static void cancel(Minecraft client, Request active) {
        if (!STATE.current(active.epoch)) return;
        STATE.cancel();
        prepared = null;
        request = null;
        closeDownloader(active);
        client.setScreen(active.parent);
    }

    private static void closeDownloader(Request active) {
        if (active == null) return;
        ControlClient old = active.downloader;
        active.downloader = null;
        if (old != null) ThreadPools.io().execute(old::close);
    }

    public static Prepared prepared() { return prepared; }
    public static Prepared preparedFor(String rawAddress) {
        Prepared value = prepared;
        if (value == null || rawAddress == null) return null;
        ServerAddress address = ServerAddress.parseString(rawAddress);
        String key = EnvironmentPreflightState.addressKey(address.getHost(), address.getPort());
        return STATE.matches(key, value.info().runtimeHash()) ? value : null;
    }
    public static String activeRuntimeHash() { return prepared == null ? "" : prepared.info().runtimeHash(); }
    public static String worldPath() { return prepared == null ? "world" : prepared.info().worldPath(); }
}
