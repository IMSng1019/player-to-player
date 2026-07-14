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

    /**
     * 判断相对路径是否属于环境文件。排除项既匹配自身，也匹配其全部子路径；
     * 所有 {@code *.tmp} 文件一律排除，防止原子写中间文件进入清单或快照。
     */
    public boolean includes(String relativePath) {
        String normalized = normalize(relativePath);
        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.isEmpty() || lower.endsWith(".tmp")) {
            return false;
        }
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
