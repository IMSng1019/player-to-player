package imsng.player_to_player.registry;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 区块注册表子系统的控制协议消息处理器（服务端侧，DESIGN.md 第 5 节；
 * 协议语义见 {@code netproto/MessageType.java} 20-29 段注释）。
 * <ul>
 *   <li>{@code CHUNK_CLAIM_REQUEST}（json: dimension, x, z, groupId）→
 *       {@code CHUNK_CLAIM_RESPONSE}（granted / 拒绝原因：blockingChunk + blockingGroup；
 *       granted 时附 hasServerData 指示走服务端数据还是种子生成）；</li>
 *   <li>{@code CHUNK_RELEASE}（json: dimension, x, z, groupId）→ {@code CHUNK_RELEASE_ACK}
 *       （binary 携带的最终区块数据留 Phase 2 处理：MCA 单区块写回）；</li>
 *   <li>{@code PLAYER_POS_UPDATE}（json: uuid, dimension, x, y, z）：刷新玩家表，无应答。</li>
 * </ul>
 * <p>
 * 线程模型：claim/release 是纯内存操作（ConcurrentHashMap + 轻量锁），直接在
 * Netty 事件循环执行；hasServerData 的磁盘探测在授予路径上发生，转 io() 池后应答。
 */
public final class RegistryHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/registry");

    private RegistryHandlers() {
    }

    /**
     * 把区块注册表相关处理器挂到控制服务器的分发表上。
     *
     * @param reg      消息路由注册表（ControlServer）
     * @param registry 区块注册表
     * @param players  玩家表
     */
    public static void register(HandlerRegistry reg, ChunkRegistry registry, PlayerTable players) {
        reg.on(MessageType.CHUNK_CLAIM_REQUEST, (conn, msg) -> handleClaim(conn, msg, registry));
        reg.on(MessageType.CHUNK_RELEASE, (conn, msg) -> handleRelease(conn, msg, registry));
        reg.on(MessageType.PLAYER_POS_UPDATE, (conn, msg) -> handlePosUpdate(conn, msg, players));
    }

    // ------------------------------------------------------------ 区块申请

    private static void handleClaim(ControlConnection conn, ControlMessage msg, ChunkRegistry registry) {
        ChunkKey key = parseKey(msg.json());
        UUID groupId = parseUuid(JsonUtil.getString(msg.json(), "groupId", ""));
        if (key == null || groupId == null) {
            conn.send(error(msg, "invalid_request", "dimension/x/z/groupId 缺失或非法"));
            return;
        }
        // tryClaim 授予路径含 region 文件探测（磁盘 IO），整体转 io() 池执行后应答
        ThreadPools.io().execute(() -> {
            ChunkRegistry.ClaimResult result = registry.tryClaim(key, groupId);
            JsonObject out = new JsonObject();
            out.addProperty("granted", result.granted());
            if (result.granted()) {
                out.addProperty("hasServerData", result.hasServerData());
            } else {
                // 拒绝原因：占用组 + 阻塞区块（客户端据此对该组发起预连接）
                out.addProperty("blockingChunk", result.blockingChunk().asString());
                out.addProperty("blockingGroup", result.blockingGroup().toString());
            }
            conn.send(msg.reply(MessageType.CHUNK_CLAIM_RESPONSE, out, null));
            LOGGER.debug("区块申请: {} 组={} granted={}", key.asString(), groupId, result.granted());
        });
    }

    // ------------------------------------------------------------ 区块释放

    private static void handleRelease(ControlConnection conn, ControlMessage msg, ChunkRegistry registry) {
        ChunkKey key = parseKey(msg.json());
        UUID groupId = parseUuid(JsonUtil.getString(msg.json(), "groupId", ""));
        if (key == null || groupId == null) {
            conn.send(error(msg, "invalid_request", "dimension/x/z/groupId 缺失或非法"));
            return;
        }
        boolean released = registry.release(key, groupId);
        if (!released) {
            LOGGER.warn("区块释放未生效（未占用或占用者不符）: {} 组={}", key.asString(), groupId);
        }
        // TODO Phase 2: msg.binary() 携带最终区块数据 → MCA 单区块写回（服务端为写盘权威）
        JsonObject out = new JsonObject();
        out.addProperty("released", released);
        conn.send(msg.reply(MessageType.CHUNK_RELEASE_ACK, out, null));
    }

    // ------------------------------------------------------------ 玩家位置

    private static void handlePosUpdate(ControlConnection conn, ControlMessage msg, PlayerTable players) {
        UUID playerId = parseUuid(JsonUtil.getString(msg.json(), "uuid", ""));
        if (playerId == null) {
            return; // 无应答消息：非法数据静默丢弃并记日志即可
        }
        String dimension = JsonUtil.getString(msg.json(), "dimension", "minecraft:overworld");
        players.updatePosition(playerId, dimension,
                getDouble(msg.json(), "x"), getDouble(msg.json(), "y"), getDouble(msg.json(), "z"));
    }

    // ------------------------------------------------------------ 工具

    /** 从 JSON 解析 ChunkKey（dimension/x/z）；缺失/非法返回 null。 */
    private static ChunkKey parseKey(JsonObject json) {
        String dimension = JsonUtil.getString(json, "dimension", "");
        if (dimension.isEmpty() || !json.has("x") || !json.has("z")) {
            return null;
        }
        try {
            return new ChunkKey(dimension, json.get("x").getAsInt(), json.get("z").getAsInt());
        } catch (Exception e) {
            return null; // x/z 不是数字（入站数据不可信）
        }
    }

    /** 防御性 UUID 解析：null / 格式非法返回 null 而非抛异常。 */
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

    private static double getDouble(JsonObject obj, String key) {
        var el = obj.get(key);
        return el != null && el.isJsonPrimitive() ? el.getAsDouble() : 0.0;
    }

    /** 构造保留 _rid 的 ERROR 应答（json: code, message）。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
