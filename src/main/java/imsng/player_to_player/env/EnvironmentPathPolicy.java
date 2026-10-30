package imsng.player_to_player.env;

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
     * 内置非环境路径。比较统一使用小写正斜杠形式，但清单键仍保留源文件原始大小写。
     */
    private static final List<String> BUILTIN_EXCLUSIONS = List.of(
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
            // v9：可变元数据只经版本化NBT通道同步，自动保存不得推动环境版本。
            "world/level.dat",
            "world/level.dat_old",
            "world/session.lock",
            "session.lock",
            "server.properties",
            "ops.json",
            "whitelist.json",
            "banned-ips.json",
            "banned-players.json",
            "usercache.json",
            "eula.txt");

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

    /** 下载端和中转端没有 MinecraftServer，使用预检给出的实际存档相对路径建立同一规则。 */
    public static List<String> worldMetadataExclusions(String worldPath) {
        String world = EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath);
        return List.of(world + "/level.dat", world + "/level.dat_old", world + "/session.lock");
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
