package imsng.player_to_player.env;

import java.util.Map;
import java.util.TreeMap;

/**
 * 静态运行环境版本。世界保存会不断改变 level.dat、玩家文件和模组自己的存档，
 * 这些变化不能使下载完成的玩家在登录阶段无限重连。世界目录中仅数据包及
 * serverconfig 属于启动环境，其余内容由世界数据面负责；目录外文件仍全部计入。
 */
public final class EnvironmentRuntimeIdentity {
    private EnvironmentRuntimeIdentity() { }

    public static String hash(EnvironmentManifest manifest, String worldPath) {
        Map<String, EnvironmentManifest.Entry> kept = new TreeMap<>();
        manifest.files().forEach((path, entry) -> {
            if (isRuntimePath(path, worldPath)) kept.put(path, entry);
        });
        return new EnvironmentManifest(kept).globalHash();
    }

    /** 游戏设置与文件共同决定运行版，防止配置相同但难度/视距等启动参数串版。 */
    public static String hash(EnvironmentManifest manifest, String worldPath, Map<String, String> properties) {
        return imsng.player_to_player.util.Sha256.hex(hash(manifest, worldPath)
                + "\n" + new TreeMap<>(properties));
    }

    public static boolean isRuntimePath(String path, String worldPath) {
        // 与EnvironmentPathPolicy保持同一大小写口径。Windows上的Foo与foo指向同一
        // 存档；若只做大小写敏感比较，上传者可把元数据伪装成世界目录外的普通配置。
        String prefix = normalizeWorldPath(worldPath).toLowerCase(java.util.Locale.ROOT) + "/";
        String normalized = path.replace('\\', '/').toLowerCase(java.util.Locale.ROOT);
        if (!normalized.startsWith(prefix)) return true;
        String relative = normalized.substring(prefix.length());
        return relative.startsWith("datapacks/") || relative.startsWith("serverconfig/");
    }

    /** 服务端只发布根目录以内的实际存档路径，拒绝绝对路径和上跳路径。 */
    public static String normalizeWorldPath(String worldPath) {
        String value = worldPath == null ? "world" : worldPath.replace('\\', '/');
        while (value.endsWith("/")) value = value.substring(0, value.length() - 1);
        if (value.isBlank() || value.startsWith("/") || value.contains(":")
                || java.util.Arrays.stream(value.split("/")).anyMatch(s -> s.equals("..") || s.equals("."))) {
            throw new IllegalArgumentException("世界路径必须是服务端根目录内的相对路径");
        }
        return value;
    }
}
