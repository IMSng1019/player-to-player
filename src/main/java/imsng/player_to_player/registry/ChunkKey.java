package imsng.player_to_player.registry;

import java.util.List;

/**
 * 区块的全局唯一键：维度 + 区块坐标。
 * <p>
 * 规范强调"注意不同维度都有区块"，因此维度标识（如 {@code minecraft:overworld}、
 * {@code minecraft:the_nether}）是键的一部分；跨维度（传送门）加载天然可表达。
 *
 * @param dimension 维度标识字符串（ResourceKey 的 location 全名）
 * @param x         区块 X 坐标（方块坐标 >> 4）
 * @param z         区块 Z 坐标
 */
public record ChunkKey(String dimension, int x, int z) {

    /**
     * 东西南北四个直接相邻区块（规范定义的"周围四个区块"，即申请时的缓冲检查范围；
     * 不含对角）。
     */
    public List<ChunkKey> neighbors4() {
        return List.of(
                new ChunkKey(dimension, x + 1, z),  // 东
                new ChunkKey(dimension, x - 1, z),  // 西
                new ChunkKey(dimension, x, z - 1),  // 北
                new ChunkKey(dimension, x, z + 1)); // 南
    }

    /** 序列化为 "维度;x;z" 形式（注册表持久化与网络传输用）。 */
    public String asString() {
        return dimension + ";" + x + ";" + z;
    }

    /** 从 {@link #asString()} 的输出解析；格式非法抛 IllegalArgumentException。 */
    public static ChunkKey parse(String text) {
        int last = text.lastIndexOf(';');
        int mid = text.lastIndexOf(';', last - 1);
        if (mid <= 0 || last <= mid) {
            throw new IllegalArgumentException("非法 ChunkKey: " + text);
        }
        return new ChunkKey(
                text.substring(0, mid),
                Integer.parseInt(text.substring(mid + 1, last)),
                Integer.parseInt(text.substring(last + 1)));
    }
}
