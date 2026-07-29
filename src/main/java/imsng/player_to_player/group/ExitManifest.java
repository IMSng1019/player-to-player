package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 主客户端耐久退出事务的不可变清单。
 * <p>
 * 清单在本地世界仍运行时固定；从此禁止新增区块申请。物理服务端据此知道最终提交
 * 必须包含哪些玩家和区块，不能仅凭客户端一句“完成”就释放唯一写入所有权。
 */
public record ExitManifest(UUID shutdownId, UUID clientId, UUID groupId,
                           Set<UUID> playerIds, Set<ChunkKey> chunks) {

    public ExitManifest {
        Objects.requireNonNull(shutdownId, "shutdownId");
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(groupId, "groupId");
        playerIds = Set.copyOf(Objects.requireNonNull(playerIds, "playerIds"));
        chunks = Set.copyOf(Objects.requireNonNull(chunks, "chunks"));
        if (playerIds.isEmpty() || !playerIds.contains(clientId)) {
            throw new IllegalArgumentException("退出清单必须包含主客户端玩家");
        }
    }

    /** 网络与持久化共用的稳定 JSON 表示。 */
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("shutdownId", shutdownId.toString());
        json.addProperty("clientId", clientId.toString());
        json.addProperty("groupId", groupId.toString());
        JsonArray players = new JsonArray();
        playerIds.stream().map(UUID::toString).sorted().forEach(players::add);
        json.add("players", players);
        JsonArray chunkArray = new JsonArray();
        chunks.stream().map(ChunkKey::asString).sorted().forEach(chunkArray::add);
        json.add("chunks", chunkArray);
        return json;
    }

    /** 从不可信入站 JSON 解析清单；任何缺失或坏值抛 IllegalArgumentException。 */
    public static ExitManifest fromJson(JsonObject json) {
        try {
            UUID shutdownId = UUID.fromString(
                    JsonUtil.getString(json, "shutdownId", ""));
            UUID clientId = UUID.fromString(JsonUtil.getString(json, "clientId", ""));
            UUID groupId = UUID.fromString(JsonUtil.getString(json, "groupId", ""));
            Set<UUID> players = new HashSet<>();
            for (JsonElement element : requiredArray(json, "players")) {
                players.add(UUID.fromString(element.getAsString()));
            }
            Set<ChunkKey> chunks = new HashSet<>();
            for (JsonElement element : requiredArray(json, "chunks")) {
                chunks.add(ChunkKey.parse(element.getAsString()));
            }
            return new ExitManifest(shutdownId, clientId, groupId, players, chunks);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("退出事务清单格式非法", e);
        }
    }

    private static JsonArray requiredArray(JsonObject json, String name) {
        JsonElement element = json != null ? json.get(name) : null;
        if (element == null || !element.isJsonArray()) {
            throw new IllegalArgumentException("退出清单缺少数组字段: " + name);
        }
        return element.getAsJsonArray();
    }
}
