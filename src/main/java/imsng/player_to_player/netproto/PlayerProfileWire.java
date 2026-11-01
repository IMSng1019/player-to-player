package imsng.player_to_player.netproto;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mojang.authlib.GameProfile;
import com.mojang.authlib.properties.Property;
import imsng.player_to_player.util.JsonUtil;

import java.util.UUID;

/**
 * 保留原版认证资料中的 properties，尤其是 textures 的 Mojang 签名。
 * 只由物理服已登录 GameProfile 生成，再经控制连接与本机认证 IPC 下发；
 * 不接受玩家 HELLO 自报的皮肤。客户端仍使用原版签名校验和纹理下载机制。
 */
public final class PlayerProfileWire {
    private PlayerProfileWire() { }

    public static GameProfile copy(GameProfile source) {
        GameProfile copy = new GameProfile(source.getId(), source.getName());
        copy.getProperties().putAll(source.getProperties());
        return copy;
    }

    public static JsonArray properties(GameProfile profile) {
        JsonArray array = new JsonArray();
        for (Property property : profile.getProperties().values()) {
            JsonObject row = new JsonObject();
            row.addProperty("name", property.name());
            row.addProperty("value", property.value());
            if (property.signature() != null) row.addProperty("signature", property.signature());
            array.add(row);
        }
        return array;
    }

    /** 兼容没有 properties 的旧节点/离线玩家，且为入站集合和字段设置独立上界。 */
    public static GameProfile read(UUID playerId, String playerName, JsonElement raw) {
        GameProfile profile = new GameProfile(playerId, playerName);
        if (raw == null || !raw.isJsonArray()) return profile;
        if (raw.getAsJsonArray().size() > 16) throw new IllegalArgumentException("玩家属性数量过多");
        for (JsonElement value : raw.getAsJsonArray()) {
            if (!value.isJsonObject()) throw new IllegalArgumentException("玩家属性必须为对象");
            JsonObject row = value.getAsJsonObject();
            String name = JsonUtil.getString(row, "name", "");
            String data = JsonUtil.getString(row, "value", "");
            String signature = JsonUtil.getString(row, "signature", null);
            if (name.isEmpty() || name.length() > 128 || data.length() > 32768
                    || signature != null && signature.length() > 8192) {
                throw new IllegalArgumentException("玩家属性字段超出限制");
            }
            profile.getProperties().put(name, new Property(name, data, signature));
        }
        return profile;
    }
}
