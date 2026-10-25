package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.Tag;
import imsng.player_to_player.world.WorldIdentity;
import imsng.player_to_player.util.Quarantine;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Set;

/**
 * 世界打开之前的元数据安装器。调用者必须先确认旧计算进程已经退出；运行中的世界
 * 只能经过 MetadataClient 的服务器线程应用入口，不能替换它正在保存的 level.dat。
 */
public final class MetadataFiles {
    private MetadataFiles() { }

    public static String prepareSave(Path environmentWorld, Path saveWorld,
                                     MetadataSnapshot snapshot, long epochMillis) throws IOException {
        CompoundTag authoritative = snapshot.data();
        validateIdentity(snapshot);
        Path environment = environmentWorld.toAbsolutePath().normalize();
        Path save = saveWorld.toAbsolutePath().normalize();
        requireNoLinks(environment); requireNoLinks(save);
        if (environment.startsWith(save) || save.startsWith(environment))
            throw new IOException("环境资源与运行存档目录不得重叠");
        Path levelDat = save.resolve("level.dat");
        boolean sameWorld = WorldIdentity.ofLevelDatFile(levelDat).filter(snapshot.worldId()::equals).isPresent();
        CompoundTag root = new CompoundTag();
        CompoundTag local = new CompoundTag();
        if (sameWorld) {
            root = NbtIo.readCompressed(levelDat, NbtAccounter.create(64L * 1024 * 1024));
            local = root.getCompound("Data");
        } else {
            if (Files.exists(save, LinkOption.NOFOLLOW_LINKS)) Quarantine.quarantine(save, "metadata-world", epochMillis);
            Files.createDirectories(save);
            // 保留既有环境资源/SavedData启动流程，但世界元数据和玩家文件不允许再走骨架复制。
            if (Files.isDirectory(environment)) {
                try (var files = Files.walk(environment)) {
                    for (Path source : files.toList()) {
                        requireNoLinks(source);
                        Path relative = environment.relativize(source);
                        if (relative.getNameCount() > 0 && Set.of("level.dat", "level.dat_old", "session.lock", "playerdata")
                                .contains(relative.getName(0).toString().toLowerCase(java.util.Locale.ROOT))) continue;
                        Path target = save.resolve(relative);
                        if (Files.isDirectory(source)) Files.createDirectories(target);
                        else if (Files.isRegularFile(source)) {
                            Files.createDirectories(target.getParent()); Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                        }
                    }
                }
            }
        }
        // 对扩展字段应用明确声明的启动适配器；玩家与模拟时钟的所有权仍由统一策略守门。
        authoritative = MetadataRuntime.startupData(local, authoritative);
        for (String key : Set.copyOf(authoritative.getAllKeys())) if (key.startsWith("__p2p")) authoritative.remove(key);
        root.put("Data", authoritative);
        Files.createDirectories(save);
        Path temporary = Files.createTempFile(save, ".metadata-", ".tmp");
        try {
            NbtIo.writeCompressed(root, temporary);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
            try { Files.move(temporary, levelDat, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
            catch (AtomicMoveNotSupportedException unsupported) { Files.move(temporary, levelDat, StandardCopyOption.REPLACE_EXISTING); }
            // 旧缓存的后备文件也必须更新，否则原版读取失败会回退到过期的全局状态。
            Files.copy(levelDat, save.resolve("level.dat_old"), StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(temporary); }
        return snapshot.worldId();
    }

    /** 在落盘或启动前核对协议世界身份和生成身份，不能仅信任缓存目录名。 */
    public static void validateIdentity(MetadataSnapshot snapshot) throws IOException {
        CompoundTag root = new CompoundTag(); root.put("Data", snapshot.data());
        if (!WorldIdentity.ofLevelData(root).filter(snapshot.worldId()::equals).isPresent())
            throw new IOException("元数据快照世界身份与生成设置不一致");
    }

    private static void requireNoLinks(Path path) throws IOException {
        for (Path current = path; current != null; current = current.getParent()) {
            if (Files.isSymbolicLink(current)) throw new IOException("世界元数据路径不得经过符号链接: " + path);
        }
    }
}
