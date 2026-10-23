package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.env.EnvironmentRuntimeIdentity;
import imsng.player_to_player.env.EnvironmentSnapshotManager;
import imsng.player_to_player.env.EnvironmentUploadStore;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;
import java.util.List;
import java.util.HashSet;
import java.util.UUID;

/** 服务端环境版本通知与主客户端配置回传；所有磁盘工作在后台执行。 */
public final class EnvironmentUpdateServer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env-update");
    private final AutoCloseable subscription;
    private volatile boolean closed;
    private volatile String lastRuntimeHash = "";

    private EnvironmentUpdateServer(HandlerRegistry handlers, EnvironmentSnapshotManager snapshots,
                                    Path source, GroupTable groups, GlobalConfig config, String worldPath) {
        List<String> paths = config.environmentUploadPaths == null ? List.of()
                : List.copyOf(config.environmentUploadPaths);
        int maximum = Math.max(1, Math.min(8 * 1024 * 1024, config.environmentUploadMaxBytes));
        handlers.on(MessageType.ENV_CHANGED, (connection, request) -> {
            var info = EnvironmentPreflightServer.currentInfo();
            if (!closed && info != null) connection.send(request.reply(MessageType.ENV_CHANGED, info.toJson(), null));
        });
        handlers.on(MessageType.ENV_UPLOAD, (connection, request) -> ThreadPools.io().execute(() -> {
            JsonObject reply = new JsonObject();
            boolean current = HelloHandler.withCurrentSession(connection, () -> {
                UUID client = connection.peerId();
                UUID group = groups.groupOf(client);
                if (closed || group == null || !client.equals(groups.primaryOf(group))) {
                    reply.addProperty("status", "not_primary");
                    return true;
                }
                if ("policy".equals(JsonUtil.getString(request.json(), "action", ""))) {
                    JsonArray allowed = new JsonArray();
                    paths.forEach(allowed::add);
                    reply.add("allowedPaths", allowed);
                    reply.addProperty("maxBytes", maximum);
                    reply.addProperty("status", "policy");
                    return true;
                }
                String path = JsonUtil.getString(request.json(), "path", "");
                if (request.binary().length > maximum || !EnvironmentRuntimeIdentity.isRuntimePath(path, worldPath)) {
                    reply.addProperty("status", "forbidden");
                    return true;
                }
                boolean begun = false;
                boolean changed = false;
                try {
                    snapshots.beginSourceUpdate();
                    begun = true;
                    String result = EnvironmentUploadStore.apply(source, paths, path,
                            JsonUtil.getString(request.json(), "baseSha256", ""),
                            JsonUtil.getBoolean(request.json(), "delete", false) ? null : request.binary());
                    reply.addProperty("status", result);
                    changed = result.equals("applied");
                } catch (Exception failure) {
                    LOGGER.warn("环境配置更新失败: {} ({})", path, failure.toString());
                    reply.addProperty("status", "failed");
                    reply.addProperty("message", "服务端无法提交环境配置，请检查服务端日志。");
                } finally {
                    if (begun) snapshots.endSourceUpdate(changed);
                }
                return true;
            });
            if (!current) reply.addProperty("status", "stale_session");
            connection.send(request.reply(MessageType.ENV_UPLOAD_RESULT, reply, null));
        }));
        subscription = snapshots.onPublished(snapshot -> {
            var info = EnvironmentPreflightServer.currentInfo();
            if (closed || info == null || !info.ready() || info.runtimeHash().equals(lastRuntimeHash)) return;
            lastRuntimeHash = info.runtimeHash();
            HashSet<UUID> notified = new HashSet<>();
            for (UUID group : groups.primariesSnapshot().keySet()) {
                for (UUID client : groups.membersOf(group)) {
                    ControlConnection connection = HelloHandler.connectionOf(client);
                    if (notified.add(client) && connection != null && connection.isOpen()) {
                        connection.send(ControlMessage.of(MessageType.ENV_CHANGED, info.toJson()));
                    }
                }
            }
        });
    }

    public static EnvironmentUpdateServer register(HandlerRegistry handlers, EnvironmentSnapshotManager snapshots,
                                                   Path root, GroupTable groups, GlobalConfig config, String worldPath) {
        return new EnvironmentUpdateServer(handlers, snapshots, root, groups, config, worldPath);
    }

    @Override public void close() {
        closed = true;
        try { subscription.close(); }
        catch (Exception failure) { LOGGER.warn("环境更新监听关闭失败", failure); }
    }
}
