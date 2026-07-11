package imsng.player_to_player.registry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * 世界 region 文件探测器：判断某区块在服务端存档中是否已有数据
 * （CHUNK_CLAIM_RESPONSE 的 {@code hasServerData} 字段 —— 规范：有服务端数据的
 * 区块客户端向服务端请求区块数据，没有的由客户端按种子自行生成）。
 * <p>
 * Phase 1 采用轻量判定：仅检查对应维度的 region 目录下 {@code r.<rx>.<rz>.mca}
 * 文件是否存在且非空。不解析 MCA 头部的单区块偏移表 —— region 文件存在但恰好
 * 缺该区块的情况极少（32×32 区块一文件），误报的代价只是客户端多发一次
 * CHUNK_DATA_REQUEST（Phase 2 服务端按实际内容应答），可接受。
 * <p>
 * 维度 → region 目录的映射按原版存档布局：
 * <pre>
 *   minecraft:overworld   →  &lt;world&gt;/region
 *   minecraft:the_nether  →  &lt;world&gt;/DIM-1/region
 *   minecraft:the_end     →  &lt;world&gt;/DIM1/region
 *   其他（数据包维度）     →  &lt;world&gt;/dimensions/&lt;命名空间&gt;/&lt;路径&gt;/region
 * </pre>
 * 线程安全：无状态（worldRoot 不可变），任意线程可调。
 */
public final class RegionFileProbe {

    /** 世界存档根目录（{@code MinecraftServer.getWorldPath(LevelResource.ROOT)}）。 */
    private final Path worldRoot;

    public RegionFileProbe(Path worldRoot) {
        this.worldRoot = worldRoot;
    }

    /** 该区块在服务端存档中是否已有数据（region 文件存在且非空）。 */
    public boolean hasServerData(ChunkKey key) {
        Path regionDir = regionDirOf(key.dimension());
        if (regionDir == null) {
            return false;
        }
        // region 坐标 = 区块坐标 >> 5（每 region 文件 32×32 区块）
        Path regionFile = regionDir.resolve("r." + (key.x() >> 5) + "." + (key.z() >> 5) + ".mca");
        try {
            return Files.isRegularFile(regionFile) && Files.size(regionFile) > 0;
        } catch (Exception e) {
            // 读取异常（权限/瞬时删除）按"无数据"处理：客户端会走种子生成，行为安全
            return false;
        }
    }

    /** 按维度标识解析 region 目录；标识畸形（缺冒号等）返回 null。 */
    private Path regionDirOf(String dimension) {
        String dim = dimension == null ? "" : dimension.trim().toLowerCase(Locale.ROOT);
        switch (dim) {
            case "minecraft:overworld":
                return worldRoot.resolve("region");
            case "minecraft:the_nether":
                return worldRoot.resolve("DIM-1").resolve("region");
            case "minecraft:the_end":
                return worldRoot.resolve("DIM1").resolve("region");
            default: {
                int colon = dim.indexOf(':');
                if (colon <= 0 || colon >= dim.length() - 1) {
                    return null; // 畸形维度标识（入站数据不可信）
                }
                // 数据包自定义维度：<world>/dimensions/<namespace>/<path>/region
                return worldRoot.resolve("dimensions")
                        .resolve(dim.substring(0, colon))
                        .resolve(dim.substring(colon + 1))
                        .resolve("region");
            }
        }
    }
}
