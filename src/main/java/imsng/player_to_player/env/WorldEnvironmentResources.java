package imsng.player_to_player.env;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * 在世界尚未启动时更新数据包和世界级启动配置。世界身份相同也必须执行此步骤，
 * 否则 LocalSaveGuard 复用存档会使新环境只存在于下载缓存中。只替换两个明确的资源树，
 * level.dat、玩家/区块、模组 SavedData 均由原持久化通道继续管理。
 */
public final class WorldEnvironmentResources {
    private static final List<String> DIRECTORIES = List.of("datapacks", "serverconfig");
    private WorldEnvironmentResources() { }

    public static void sync(Path environmentWorld, Path saveWorld) throws IOException {
        Path source = environmentWorld.toAbsolutePath().normalize();
        Path destination = saveWorld.toAbsolutePath().normalize();
        if (source.startsWith(destination) || destination.startsWith(source)) {
            throw new IOException("世界环境与运行存档目录不能重叠");
        }
        EnvironmentDirectoryTransaction.validateRoot(source);
        EnvironmentDirectoryTransaction.validateRoot(destination);
        Path[] targets = DIRECTORIES.stream().map(destination::resolve).toArray(Path[]::new);
        EnvironmentDirectoryTransaction.recover(targets);
        try (EnvironmentDirectoryTransaction transaction = new EnvironmentDirectoryTransaction()) {
            for (String name : DIRECTORIES) {
                Path resources = source.resolve(name);
                EnvironmentDirectoryTransaction.validateRoot(resources);
                EnvironmentManifest manifest = EnvironmentScanner.scan(resources, List.of());
                Path staging = transaction.stage(destination.resolve(name), manifest);
                for (String path : manifest.files().keySet()) {
                    Path original = EnvironmentContentReuse.resolveDestination(resources, path);
                    Path target = EnvironmentContentReuse.resolveDestination(staging, path);
                    Files.createDirectories(target.getParent());
                    Files.copy(original, target);
                }
            }
            transaction.commit();
        }
    }
}
