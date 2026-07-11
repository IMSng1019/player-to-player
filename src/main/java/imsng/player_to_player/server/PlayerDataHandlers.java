package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * 玩家数据面处理器（服务端，Phase 3；Phase 2 已知边界"玩家背包……不回传物理
 * 服务端"的收口）。
 * <ul>
 *   <li>{@link MessageType#PLAYER_DATA_UPLOAD}（json: playerUuid；binary: gzip
 *       玩家 NBT）：合并让出方 / 分离原主上传玩家的最终数据，写入服务端存档
 *       {@code playerdata/<uuid>.dat} —— {@code RoleAssignHandler} 的存档定位、
 *       之后的环境同步都天然消费同一份文件，玩家跨组迁移后位置/背包连续；</li>
 *   <li>{@link MessageType#PLAYER_DATA_REQUEST} → {@link MessageType#PLAYER_DATA}：
 *       新主客户端拉取组成员的玩家数据（预同步/接管时初始化其集成服务端的
 *       playerdata）。</li>
 * </ul>
 * <b>授权口径</b>：上传/请求方必须是该玩家本人，或该玩家当前所在组的主客户端
 * （组表查询）——防任意客户端覆盖/窥探他人存档。
 * <p>
 * 线程模型：handler 在 Netty 事件循环，解压/磁盘 IO 一律转 {@link ThreadPools#io()}；
 * 写盘用"临时文件 + 原子移动"，防半写损坏玩家存档。
 */
public final class PlayerDataHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/player-data");

    /** 玩家 NBT 解压上限（正常玩家数据 KB 级；防解压炸弹）。 */
    private static final long MAX_PLAYER_NBT_BYTES = 16L * 1024 * 1024;

    private final MinecraftServer server;
    private final GroupTable groups;

    private PlayerDataHandlers(MinecraftServer server, GroupTable groups) {
        this.server = server;
        this.groups = groups;
    }

    /** 注册玩家数据面处理器。 */
    public static void register(HandlerRegistry reg, MinecraftServer server, GroupTable groups) {
        PlayerDataHandlers handlers = new PlayerDataHandlers(server, groups);
        reg.on(MessageType.PLAYER_DATA_UPLOAD, handlers::handleUpload);
        reg.on(MessageType.PLAYER_DATA_REQUEST, handlers::handleRequest);
    }

    // ------------------------------------------------------------ 上行

    private void handleUpload(ControlConnection conn, ControlMessage msg) {
        UUID peer = conn.peerId();
        UUID playerId = parseUuid(JsonUtil.getString(msg.json(), "playerUuid", ""));
        if (peer == null || playerId == null) {
            conn.send(error(msg, "invalid_request", "playerUuid 缺失或未握手"));
            return;
        }
        if (!authorized(peer, playerId)) {
            conn.send(error(msg, "not_authorized", "只能上传本人或本组成员的玩家数据"));
            return;
        }
        byte[] gzipNbt = msg.binary();
        ThreadPools.io().execute(() -> {
            try {
                if (gzipNbt == null || gzipNbt.length == 0) {
                    conn.send(error(msg, "invalid_data", "玩家数据为空"));
                    return;
                }
                // 解析校验（带上限）：确认是合法 NBT 才落盘，坏数据不碰存档
                CompoundTag tag = NbtIo.readCompressed(new ByteArrayInputStream(gzipNbt),
                        NbtAccounter.create(MAX_PLAYER_NBT_BYTES));
                Path dataDir = server.getWorldPath(LevelResource.PLAYER_DATA_DIR);
                Files.createDirectories(dataDir);
                // 原版同款格式（gzip NBT）：先写临时文件再原子移动，防半写损坏
                Path tmp = dataDir.resolve(playerId + ".dat.p2p-tmp");
                NbtIo.writeCompressed(tag, tmp);
                Files.move(tmp, dataDir.resolve(playerId + ".dat"),
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                JsonObject out = new JsonObject();
                out.addProperty("written", true);
                conn.send(msg.reply(MessageType.PLAYER_DATA, out, null));
                LOGGER.info("玩家数据已写回: {} (上传方 {}, {} 字节压缩)",
                        playerId, peer, gzipNbt.length);
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
        ThreadPools.io().execute(() -> {
            try {
                Path file = server.getWorldPath(LevelResource.PLAYER_DATA_DIR)
                        .resolve(playerId + ".dat");
                JsonObject out = new JsonObject();
                out.addProperty("playerUuid", playerId.toString());
                if (!Files.isRegularFile(file)) {
                    out.addProperty("exists", false);
                    conn.send(msg.reply(MessageType.PLAYER_DATA, out, null));
                    return;
                }
                // 重新压缩发送（文件本就是 gzip NBT，直接透传字节即可）
                ByteArrayOutputStream bos = new ByteArrayOutputStream(16 * 1024);
                bos.writeBytes(Files.readAllBytes(file));
                out.addProperty("exists", true);
                conn.send(msg.reply(MessageType.PLAYER_DATA, out, bos.toByteArray()));
            } catch (Exception e) {
                LOGGER.warn("玩家数据下发失败: {}", playerId, e);
                conn.send(error(msg, "read_failed", e.toString()));
            }
        });
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
