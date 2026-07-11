package imsng.player_to_player.registry;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 玩家表（服务端，DESIGN.md 第 5 节）：玩家 UUID → 名字/维度/坐标/组/端级别。
 * <p>
 * 规范出处："玩家表：玩家的坐标信息 玩家所在的组客户端 主客户端和副客户端的等级情况"。
 * Phase 1 只做登记与查询（HELLO 登记、PLAYER_POS_UPDATE 刷新、断连移除）；
 * 跨组 tp 指令与消息路由（Phase 4）将基于本表寻址。
 * <p>
 * 线程模型：全部操作基于 ConcurrentHashMap 单键原子读写，任意线程可调，无外部锁。
 */
public final class PlayerTable {

    /** 玩家表中的一行（不可变；字段更新以整行替换实现）。 */
    public record PlayerEntry(
            UUID playerId,
            String playerName,
            String dimension,
            double x, double y, double z,
            UUID groupId,
            String role) {

        /** 序列化为 JSON（状态查询/调试指令用）。 */
        public JsonObject toJson() {
            JsonObject obj = new JsonObject();
            obj.addProperty("playerId", playerId.toString());
            obj.addProperty("playerName", playerName);
            obj.addProperty("dimension", dimension);
            obj.addProperty("x", x);
            obj.addProperty("y", y);
            obj.addProperty("z", z);
            obj.addProperty("groupId", groupId != null ? groupId.toString() : "");
            obj.addProperty("role", role);
            return obj;
        }

        /** 从 JSON 反序列化（入站数据不可信：缺失字段取安全默认值）。 */
        public static PlayerEntry fromJson(UUID playerId, JsonObject obj) {
            UUID groupId = null;
            String rawGroup = JsonUtil.getString(obj, "groupId", "");
            if (!rawGroup.isEmpty()) {
                try {
                    groupId = UUID.fromString(rawGroup);
                } catch (IllegalArgumentException ignored) {
                    // 非法 groupId 按无组处理
                }
            }
            return new PlayerEntry(
                    playerId,
                    JsonUtil.getString(obj, "playerName", "?"),
                    JsonUtil.getString(obj, "dimension", "minecraft:overworld"),
                    getDouble(obj, "x"),
                    getDouble(obj, "y"),
                    getDouble(obj, "z"),
                    groupId,
                    JsonUtil.getString(obj, "role", "unassigned"));
        }

        private static double getDouble(JsonObject obj, String key) {
            var el = obj.get(key);
            return el != null && el.isJsonPrimitive() ? el.getAsDouble() : 0.0;
        }
    }

    /** 玩家 UUID → 表项。 */
    private final Map<UUID, PlayerEntry> players = new ConcurrentHashMap<>();

    /** 登记（或整行覆盖）玩家；参数为 null 时忽略（防御性）。 */
    public void put(PlayerEntry entry) {
        if (entry != null && entry.playerId() != null) {
            players.put(entry.playerId(), entry);
        }
    }

    /** 查询玩家；不在表中返回 null。 */
    public PlayerEntry get(UUID playerId) {
        return playerId == null ? null : players.get(playerId);
    }

    /** 移除玩家（断连清理）。 */
    public void remove(UUID playerId) {
        if (playerId != null) {
            players.remove(playerId);
        }
    }

    /** 更新玩家位置（PLAYER_POS_UPDATE；未登记的玩家忽略 —— 必须先经 HELLO 登记）。 */
    public void updatePosition(UUID playerId, String dimension, double x, double y, double z) {
        if (playerId == null) {
            return;
        }
        players.computeIfPresent(playerId, (id, old) -> new PlayerEntry(
                id, old.playerName(), dimension, x, y, z, old.groupId(), old.role()));
    }

    /** 更新玩家的组与端级别（角色指派/合并/分离时）。 */
    public void updateGroupRole(UUID playerId, UUID groupId, String role) {
        if (playerId == null) {
            return;
        }
        players.computeIfPresent(playerId, (id, old) -> new PlayerEntry(
                id, old.playerName(), old.dimension(), old.x(), old.y(), old.z(), groupId, role));
    }

    /** 当前表快照（防御性拷贝，状态展示/调试用）。 */
    public Map<UUID, PlayerEntry> snapshot() {
        return Map.copyOf(players);
    }

    /** 在线玩家数。 */
    public int size() {
        return players.size();
    }
}
