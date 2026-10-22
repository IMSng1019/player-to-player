package imsng.player_to_player.registry;

import com.google.gson.JsonObject;

/**
 * 分离准备期间的加载边界。矩形覆盖真实占用岛及世代依赖；它只是预约范围，
 * 不是授权：新组仍须在旧组耐久释放后逐块申请。使用 long 校验避免网络输入溢出。
 */
public record SplitRegion(String dimension, int minX, int maxX, int minZ, int maxZ) {
    public SplitRegion {
        if (dimension == null || dimension.isBlank() || dimension.length() > 256
                || minX > maxX || minZ > maxZ
                || (long) maxX - minX > 1024 || (long) maxZ - minZ > 1024
                || ((long) maxX - minX + 1) * ((long) maxZ - minZ + 1) > 262_144) {
            throw new IllegalArgumentException("分离范围非法或过大");
        }
    }

    public boolean contains(ChunkKey key) {
        return key != null && dimension.equals(key.dimension())
                && key.x() >= minX && key.x() <= maxX
                && key.z() >= minZ && key.z() <= maxZ;
    }

    /** 预约也保护四邻边界，避免第三组趁旧区块逐步释放占住接管方必须的邻居。 */
    public boolean protects(ChunkKey key) {
        if (key == null || !dimension.equals(key.dimension())) return false;
        long dx = Math.max(0L, Math.max((long) minX - key.x(), (long) key.x() - maxX));
        long dz = Math.max(0L, Math.max((long) minZ - key.z(), (long) key.z() - maxZ));
        return dx + dz <= 1;
    }

    public boolean overlaps(SplitRegion other) {
        return dimension.equals(other.dimension)
                && (long) minX <= (long) other.maxX + 1 && (long) maxX + 1 >= other.minX
                && (long) minZ <= (long) other.maxZ + 1 && (long) maxZ + 1 >= other.minZ;
    }

    public JsonObject toJson() {
        JsonObject value = new JsonObject();
        value.addProperty("dimension", dimension);
        value.addProperty("minX", minX);
        value.addProperty("maxX", maxX);
        value.addProperty("minZ", minZ);
        value.addProperty("maxZ", maxZ);
        return value;
    }

    public static SplitRegion fromJson(JsonObject value) {
        return new SplitRegion(value.get("dimension").getAsString(),
                value.get("minX").getAsInt(), value.get("maxX").getAsInt(),
                value.get("minZ").getAsInt(), value.get("maxZ").getAsInt());
    }
}
