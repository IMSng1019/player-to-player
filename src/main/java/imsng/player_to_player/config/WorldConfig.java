package imsng.player_to_player.config;

import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * 世界配置文件（世界文件夹下的 {@code config.json}）。
 * <p>
 * 规范：对于不同的世界（不同服务器 + 不同世界名），每个世界在
 * {@code player-to-player/<IP>+<世界名>/} 下有独立的环境文件、数据文件与本配置。
 * 客户端每次加入世界时按 "IP+世界名" 重新选定世界文件夹（见 {@link P2PPaths#worldFolder}）。
 */
public final class WorldConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/config");

    /** 该世界所属服务端 IP（世界文件夹命名成分之一）。 */
    public String serverIp = "";

    /** 世界名称（世界文件夹命名成分之一）。 */
    public String worldName = "";

    /** 服务端下发的全局环境哈希（上次同步成功时记录，加速下次加入时的校验）。 */
    public String lastEnvironmentHash = "";

    /** 最近一次加入该世界的时间戳（毫秒）。 */
    public long lastJoinedEpochMillis = 0L;

    /** 最近一次在该世界担任的角色（primary / secondary），供诊断与快速恢复。 */
    public String lastRole = "";

    /** 读取世界配置；不存在则按参数创建。 */
    public static WorldConfig loadOrCreate(Path path, String serverIp, String worldName) {
        try {
            WorldConfig loaded = JsonUtil.readFile(path, WorldConfig.class);
            if (loaded != null) {
                return loaded;
            }
        } catch (Exception e) {
            LOGGER.warn("世界配置读取失败，重建: {}", path, e);
        }
        WorldConfig config = new WorldConfig();
        config.serverIp = serverIp;
        config.worldName = worldName;
        config.save(path);
        return config;
    }

    /** 落盘（原子写）。 */
    public void save(Path path) {
        try {
            JsonUtil.writeFileAtomic(path, this);
        } catch (IOException e) {
            LOGGER.error("世界配置保存失败: {}", path, e);
        }
    }
}
