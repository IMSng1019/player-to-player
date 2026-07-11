package imsng.player_to_player.config;

import imsng.player_to_player.core.ClientRole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * player-to-player 目录结构统一管理（见 DESIGN.md 第 2 节）。
 *
 * <pre>
 * &lt;游戏目录 / 服务端根目录&gt;/player-to-player/
 *   config.json                # 总配置
 *   registry/                  # [服务端] 区块注册表持久化
 *   logs/                      # [服务端] 收集的主客户端日志（&lt;玩家名&gt;-latest.log）
 *   &lt;IP&gt;+&lt;世界名&gt;/            # [客户端] 世界文件夹
 *     config.json              # 世界配置
 *     primary/environment/     # 主客户端角色环境文件
 *     primary/data/            # 主客户端角色数据文件
 *     secondary/environment/   # 副客户端角色环境文件
 *     secondary/data/          # 副客户端角色数据文件
 * </pre>
 *
 * 所有路径解析集中在本类，其他子系统一律不得自行拼接目录字符串。
 */
public final class P2PPaths {

    /** 根目录名（规范固定为 player-to-player）。 */
    public static final String ROOT_DIR_NAME = "player-to-player";

    private final Path root;

    /**
     * @param gameDir 游戏目录（客户端）或服务端根目录，
     *                即 {@code FabricLoader.getInstance().getGameDir()}
     */
    public P2PPaths(Path gameDir) {
        this.root = gameDir.resolve(ROOT_DIR_NAME);
    }

    /** player-to-player 根目录。 */
    public Path root() {
        return root;
    }

    /** 总配置文件路径。 */
    public Path globalConfig() {
        return root.resolve("config.json");
    }

    /** [服务端] 区块注册表持久化目录。 */
    public Path registryDir() {
        return root.resolve("registry");
    }

    /** [服务端] 主客户端日志收集目录。 */
    public Path logsDir() {
        return root.resolve("logs");
    }

    /**
     * 世界文件夹：{@code root/<IP>+<世界名>}。
     * 规范：当加入世界时，以 IP 地址+世界名称检测是否有相应的文件夹，
     * 之后该世界的一切文件读写都基于该文件夹。
     */
    public Path worldFolder(String serverIp, String worldName) {
        return root.resolve(sanitize(serverIp) + "+" + sanitize(worldName));
    }

    /** 世界配置文件。 */
    public Path worldConfig(Path worldFolder) {
        return worldFolder.resolve("config.json");
    }

    /** 角色子目录（primary / secondary）。UNASSIGNED 无专属目录，取 secondary 兜底。 */
    public Path roleDir(Path worldFolder, ClientRole role) {
        return worldFolder.resolve(role == ClientRole.PRIMARY ? "primary" : "secondary");
    }

    /** 角色专属环境文件目录。 */
    public Path environmentDir(Path worldFolder, ClientRole role) {
        return roleDir(worldFolder, role).resolve("environment");
    }

    /** 角色专属数据文件目录。 */
    public Path dataDir(Path worldFolder, ClientRole role) {
        return roleDir(worldFolder, role).resolve("data");
    }

    /** 创建基础目录（模组加载事件中调用；规范：检测没有 player-to-player 文件夹则创建）。 */
    public void ensureBaseDirs() throws IOException {
        Files.createDirectories(root);
    }

    /** 创建某个世界文件夹的完整骨架（加入世界时调用）。 */
    public void ensureWorldDirs(Path worldFolder) throws IOException {
        for (ClientRole role : new ClientRole[]{ClientRole.PRIMARY, ClientRole.SECONDARY}) {
            Files.createDirectories(environmentDir(worldFolder, role));
            Files.createDirectories(dataDir(worldFolder, role));
        }
    }

    /**
     * 文件名合法化：IP/世界名可能含 Windows 非法字符（如 ":" 出现在 "ip:端口"）。
     * 非法字符统一替换为下划线，保证跨平台可用。
     */
    public static String sanitize(String name) {
        if (name == null || name.isBlank()) {
            return "unknown";
        }
        return name.trim().replaceAll("[\\\\/:*?\"<>|\\s]", "_");
    }
}
