package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.PlayerStateNbt;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * 玩家数据面处理器（服务端，Phase 3；Phase 2 已知边界"玩家背包……不回传物理
 * 服务端"的收口）。
 * <ul>
 *   <li>{@link MessageType#PLAYER_DATA_UPLOAD}（json: playerUuid, uploadSequence；binary: gzip
 *       玩家 NBT）：合并让出方 / 分离原主上传玩家的最终数据，写入服务端存档
 *       {@code playerdata/<uuid>.dat} —— {@code RoleAssignHandler} 的存档定位、
 *       之后的环境同步都天然消费同一份文件，玩家跨组迁移后位置/背包连续；</li>
 *   <li>{@link MessageType#PLAYER_DATA_REQUEST} → {@link MessageType#PLAYER_DATA}：
 *       新主客户端拉取组成员的玩家数据（预同步/接管时初始化其集成服务端的
 *       playerdata）。</li>
 * </ul>
 * <b>授权口径</b>：上传方必须是该玩家当前所在组的主客户端，并在实际写盘时重新核验。
 * 请求读取仍允许本人或当前组主客户端，供进入世界前准备存档。
 * <p>
 * 线程模型：handler 在 Netty 事件循环；在线玩家快照必须在服务器主线程调用
 * {@link ServerPlayer#saveWithoutId}，离线磁盘读取与上传写盘转 {@link ThreadPools#io()}；
 * 写盘用"临时文件 + 原子移动"，防半写损坏玩家存档。
 */
public final class PlayerDataHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/player-data");

    private final MinecraftServer server;
    private final GroupTable groups;
    private final ExitTransactionHandlers exits;
    private final PlayerUploadOrder uploadOrder = new PlayerUploadOrder();

    private PlayerDataHandlers(MinecraftServer server, GroupTable groups,
                               ExitTransactionHandlers exits) {
        this.server = server;
        this.groups = groups;
        this.exits = exits;
    }

    /** 注册玩家数据面处理器。 */
    public static void register(HandlerRegistry reg, MinecraftServer server, GroupTable groups,
                                ExitTransactionHandlers exits) {
        PlayerDataHandlers handlers = new PlayerDataHandlers(server, groups, exits);
        reg.on(MessageType.PLAYER_DATA_UPLOAD, handlers::handleUpload);
        reg.on(MessageType.PLAYER_DATA_REQUEST, handlers::handleRequest);
    }

    // ------------------------------------------------------------ 上行

    private void handleUpload(ControlConnection conn, ControlMessage msg) {
        UUID peer = conn.peerId();
        UUID playerId = parseUuid(JsonUtil.getString(msg.json(), "playerUuid", ""));
        long sequence = JsonUtil.getLong(msg.json(), "uploadSequence", 0);
        if (peer == null || playerId == null || sequence <= 0) {
            conn.send(error(msg, "invalid_request", "playerUuid/uploadSequence 缺失、非法或未握手"));
            return;
        }
        if (!PlayerUploadOrder.isAuthorized(groups, peer, playerId)) {
            conn.send(error(msg, "not_authorized", "只有当前组主客户端可以上传玩家数据"));
            return;
        }
        byte[] gzipNbt = msg.binary();
        ThreadPools.io().execute(() -> {
            try {
                if (gzipNbt == null || gzipNbt.length == 0) {
                    conn.send(error(msg, "invalid_data", "玩家数据为空"));
                    return;
                }
                Path dataDir = server.getWorldPath(LevelResource.PLAYER_DATA_DIR);
                String digest = Sha256.hex(gzipNbt);
                if (exits == null || !exits.writeRealtimeIfAllowed(conn,
                        () -> PlayerDataFileStore.withPlayerWriteLock(playerId,
                                () -> uploadOrder.write(conn, playerId, sequence, digest,
                                        () -> PlayerUploadOrder.isAuthorized(groups, peer, playerId),
                                        () -> PlayerDataFileStore.write(dataDir, playerId, gzipNbt))))) {
                    conn.send(error(msg, "invalid_data",
                            "玩家上行已失去权限、序号冲突、写回失败或退出已冻结"));
                    return;
                }
                JsonObject out = new JsonObject();
                out.addProperty("written", true);
                out.addProperty("uploadSequence", sequence);
                conn.send(msg.reply(MessageType.PLAYER_DATA, out, null));
                LOGGER.info("玩家数据已确认: {} (上传方 {}, 序号 {}, {} 字节压缩)",
                        playerId, peer, sequence, gzipNbt.length);
            } catch (Exception e) {
                LOGGER.warn("玩家数据写回失败: {}", playerId, e);
                conn.send(error(msg, "write_failed", e.toString()));
            }
        });
    }

    // ------------------------------------------------------------ 下发

    private void handleRequest(ControlConnection conn, ControlMessage msg) {
        UUID peer = conn.peerId();
        UUID playerId = parseUuid(JsonUtil.getString(msg.json(), "playerUuid", ""));
        if (peer == null || playerId == null) {
            conn.send(error(msg, "invalid_request", "playerUuid 缺失或未握手"));
            return;
        }
        if (!authorized(peer, playerId)) {
            conn.send(error(msg, "not_authorized", "只能请求本人或本组成员的玩家数据"));
            return;
        }
        // 玩家此刻仍连接物理服时，磁盘文件可能落后于实时位置/背包；必须在
        // 服务器主线程直接序列化在线实体。只有离线时才读取原版 playerdata 文件。
        server.execute(() -> {
            ServerPlayer online = server.getPlayerList().getPlayer(playerId);
            if (online != null) {
                try {
                    byte[] gzip = PlayerStateNbt.encode(
                            online.saveWithoutId(new CompoundTag()));
                    sendPlayerData(conn, msg, playerId, gzip, "在线快照");
                } catch (Exception e) {
                    sendReadError(conn, msg, playerId, e);
                }
                return;
            }
            ThreadPools.io().execute(() -> sendDiskPlayerData(conn, msg, playerId));
        });
    }

    private void sendDiskPlayerData(ControlConnection conn, ControlMessage request,
                                    UUID playerId) {
        try {
            Path file = server.getWorldPath(LevelResource.PLAYER_DATA_DIR)
                    .resolve(playerId + ".dat");
            if (!Files.isRegularFile(file)) {
                JsonObject out = new JsonObject();
                out.addProperty("playerUuid", playerId.toString());
                out.addProperty("exists", false);
                conn.send(request.reply(MessageType.PLAYER_DATA, out, null));
                return;
            }
            sendPlayerData(conn, request, playerId, Files.readAllBytes(file), "磁盘兜底");
        } catch (Exception e) {
            sendReadError(conn, request, playerId, e);
        }
    }

    private void sendPlayerData(ControlConnection conn, ControlMessage request,
                                UUID playerId, byte[] gzip, String source) {
        JsonObject out = new JsonObject();
        out.addProperty("playerUuid", playerId.toString());
        out.addProperty("exists", true);
        conn.send(request.reply(MessageType.PLAYER_DATA, out, gzip));
        LOGGER.info("玩家数据已下发: {}（{}，{} 字节压缩）", playerId, source, gzip.length);
    }

    private void sendReadError(ControlConnection conn, ControlMessage request,
                               UUID playerId, Exception e) {
        LOGGER.warn("玩家数据下发失败: {}", playerId, e);
        conn.send(error(request, "read_failed", e.toString()));
    }

    // ------------------------------------------------------------ 工具

    /** 授权：本人，或该玩家当前所在组的主客户端。 */
    private boolean authorized(UUID peer, UUID playerId) {
        if (peer.equals(playerId)) {
            return true;
        }
        UUID group = groups.groupOf(playerId);
        return group != null && peer.equals(groups.primaryOf(group));
    }

    /** 防御性 UUID 解析。 */
    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** 构造保留 _rid 的 ERROR 应答。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
