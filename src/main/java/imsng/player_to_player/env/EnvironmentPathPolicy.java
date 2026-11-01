package imsng.player_to_player.env;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * 环境文件路径规则。
 * <p>
 * 环境扫描、不可变快照构建和文件监听必须使用完全相同的包含/排除判定；如果各自
 * 维护一份规则，最危险的结果不是多扫一个文件，而是清单认为文件存在、快照却没有
 * 对应内容。因此把规范化与排除逻辑集中在本不可变对象中，创建后可被多个后台任务
 * 安全并发读取。
 */
public final class EnvironmentPathPolicy {

    /**
     * 已有专门同步通道的世界状态。玩家位置/背包通过玩家数据耐久通道上传；
     * level.dat 通过版本化元数据通道同步，均不应推动共享环境版本。
     * stats、advancements 尚无独立完整持久化路径，不能仅为降负载在这里排除。
     */
    private static final List<String> WORLD_STATE_PATHS = List.of(
            "level.dat", "level.dat_old", "session.lock", "playerdata");

    /**
     * 内置非环境路径。比较统一使用小写正斜杠形式，但清单键仍保留源文件原始大小写。
     */
    private static final List<String> BUILTIN_EXCLUSIONS = builtinExclusions();

    private static List<String> builtinExclusions() {
        List<String> paths = new ArrayList<>(List.of(
            "logs",
            "player-to-player",
            // Loader会重新提取/重映射这些缓存；.fabric/server是真实启动依赖，必须保留。
            ".fabric/processedMods",
            ".fabric/remappedJars",
            ".fabric/tmp",
            "world/region",
            "world/poi",
            "world/entities",
            "world/DIM-1",
            "world/DIM1",
            "session.lock",
            "server.properties",
            "ops.json",
            "whitelist.json",
            "banned-ips.json",
            "banned-players.json",
            "usercache.json",
            "eula.txt"));
        // 默认存档和预检返回的自定义存档必须来自同一份状态分类，避免各端再次漏项。
        paths.addAll(worldMetadataExclusions("world"));
        return List.copyOf(paths);
    }

    /** 已规范化为小写正斜杠形式的完整排除表。 */
    private final List<String> exclusions;

    private EnvironmentPathPolicy(List<String> exclusions) {
        this.exclusions = Collections.unmodifiableList(exclusions);
    }

    /**
     * 创建路径规则，将内置排除项与管理员配置的非环境路径合并。
     *
     * @param extraExclusions 管理员追加的相对路径；null、空串和纯空白项会被忽略
     */
    public static EnvironmentPathPolicy create(List<String> extraExclusions) {
        List<String> merged = new ArrayList<>(BUILTIN_EXCLUSIONS.size()
                + (extraExclusions != null ? extraExclusions.size() : 0));
        for (String builtin : BUILTIN_EXCLUSIONS) {
            merged.add(normalizePath(builtin).toLowerCase(Locale.ROOT));
        }
        if (extraExclusions != null) {
            for (String extra : extraExclusions) {
                if (extra != null && !extra.isBlank()) {
                    merged.add(normalizePath(extra).toLowerCase(Locale.ROOT));
                }
            }
        }
        return new EnvironmentPathPolicy(merged);
    }

    /**
     * 下载端和中转端没有 MinecraftServer，使用预检给出的实际存档相对路径建立同一规则。
     * 保留既有方法名，同时覆盖已由独立通道持久化的玩家存档及整个备份子目录。
     */
    public static List<String> worldMetadataExclusions(String worldPath) {
        String world = EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath);
        return qualifyWorldStatePaths(world + "/");
    }

    /**
     * 中心服务已知真实磁盘目录，用相同状态分类计算排除路径。
     * 存档位于扫描根之外时不需要额外排除；存档恰为扫描根时使用空前缀，
     * 不把它伪装成 "world" 子目录，也不放宽网络预检对相对路径的校验。
     */
    public static List<String> worldMetadataExclusions(Path sourceRoot, Path worldRoot) {
        Path source = sourceRoot.toAbsolutePath().normalize();
        Path world = worldRoot.toAbsolutePath().normalize();
        if (!world.startsWith(source)) return List.of();
        String relative = source.relativize(world).toString().replace('\\', '/');
        return qualifyWorldStatePaths(relative.isEmpty() ? "" : relative + "/");
    }

    private static List<String> qualifyWorldStatePaths(String prefix) {
        return WORLD_STATE_PATHS.stream().map(path -> prefix + path).toList();
    }

    /**
     * 判断相对路径是否属于环境文件。排除项既匹配自身，也匹配其全部子路径；
     * 排除通用 {@code *.tmp} 和本模组实际使用的 {@code *.p2p-tmp}，防止
     * 玩家状态原子替换的中间文件在扫描与复制之间消失，或触发无意义的环境更新。
     */
    public boolean includes(String relativePath) {
        String normalized = normalize(relativePath);
        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.isEmpty() || lower.endsWith(".tmp") || lower.endsWith(".p2p-tmp")) {
            return false;
        }
        // 音量、语言和按键属于每位玩家自己的设置，不能被打包下发或推动环境版本。
        if (ClientLocalSettings.isLocalPath(lower)) return false;
        for (String exclusion : exclusions) {
            if (lower.equals(exclusion) || lower.startsWith(exclusion + "/")) {
                return false;
            }
        }
        return true;
    }

    /**
     * 把任意平台的相对路径规范成清单使用的正斜杠形式，同时去除首尾多余分隔符。
     * 本方法保留大小写，调用方可直接把返回值作为清单键。
     */
    public String normalize(String path) {
        return normalizePath(path);
    }

    private static String normalizePath(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        String normalized = path.replace('\\', '/');
        while (normalized.startsWith("./")) {
            normalized = normalized.substring(2);
        }
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/") && !normalized.isEmpty()) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
