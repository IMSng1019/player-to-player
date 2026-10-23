package imsng.player_to_player.runtime;

import imsng.player_to_player.env.EnvironmentManifest;
import imsng.player_to_player.env.EnvironmentRuntimeIdentity;
import imsng.player_to_player.util.Sha256;
import java.io.IOException;
import java.nio.file.*;
import java.util.Set;

/**
 * 启动前校验实际运行副本，避免“下载目录通过校验”与“将要执行的目录”之间出现版本错位。
 * 动态世界文件由世界数据面管理；数据包/serverconfig须映射到真实存档再次验证。
 */
public final class WorkerEnvironmentProof {
    private static final Set<String> LOCAL_SETTINGS = Set.of("server.properties", "eula.txt", "ops.json",
            "whitelist.json", "banned-ips.json", "banned-players.json", "usercache.json", "session.lock");
    private WorkerEnvironmentProof() { }
    public static void verifyInstalled(Path runtimeRoot, Path saveWorld, String worldPath,
                                       EnvironmentManifest manifest) throws IOException {
        if (manifest == null) throw new IOException("缺少冻结环境清单，拒绝启动计算进程");
        String prefix = EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath) + "/";
        Path runtime = runtimeRoot.toAbsolutePath().normalize();
        Path world = saveWorld.toAbsolutePath().normalize();
        for (var entry : manifest.files().entrySet()) {
            String relative = entry.getKey();
            if (LOCAL_SETTINGS.contains(relative)) continue;
            Path root = runtime;
            if (relative.startsWith(prefix)) {
                if (!EnvironmentRuntimeIdentity.isRuntimePath(relative, worldPath)) continue;
                relative = relative.substring(prefix.length()); root = world;
            }
            Path file = root.resolve(relative).normalize();
            if (!file.startsWith(root) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)
                    || Files.size(file) != entry.getValue().size()
                    || !Sha256.hexOfFile(file).equals(entry.getValue().sha256()))
                throw new IOException("实际运行环境未匹配冻结清单: " + entry.getKey());
        }
    }
}
