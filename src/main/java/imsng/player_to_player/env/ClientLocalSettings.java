package imsng.player_to_player.env;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;

/**
 * 渲染客户端的本机偏好。语言、音量、按键和无障碍引导都在 options.txt 中，不能因为
 * 服务端环境更新而恢复默认值。配置存放于世界自己的 client-settings，既不上传给
 * 服务端，也不反向覆盖启动器目录；同一玩家访问不同服务器时可保留各自的偏好。
 *
 * <p>这里只复制明确的个人选项文件。模组 config 仍由环境同步管理，不能把整个本机
 * config 覆盖进新实例，否则可能绕过服务端的玩法配置。完整文件按原始字节保留，
 * 不解析或重新序列化键值，避免丢失模组追加的选项与未来版本的字段。
 */
public final class ClientLocalSettings {
    private static final List<String> FILES = List.of("options.txt", "optionsof.txt", "optionsshaders.txt");
    private static final int MAX_FILE_BYTES = 4 * 1024 * 1024;
    private static final String PROFILE_DIRECTORY = "client-settings";

    private ClientLocalSettings() { }

    /** 根目录个人选项永不参与共享环境；同名 config/options.txt 仍属于模组环境。 */
    public static boolean isLocalPath(String relative) {
        return relative != null && FILES.contains(relative.toLowerCase(Locale.ROOT));
    }

    /**
     * 相同环境哈希不代表相同服务器。只有运行目录属于目标世界才能复用当前进程，
     * 否则应重新启动并加载目标服务器的个人选项，退出时也才会归档到正确的世界。
     */
    public static boolean belongsToWorld(Path worldFolder, Path gameDir) throws IOException {
        return worldFolder.toAbsolutePath().normalize().equals(runtimeWorld(gameDir.toAbsolutePath().normalize()));
    }

    /**
     * 在启动新实例前装入个人偏好。首次访问时从当前客户端继承；已经有该服务器的
     * 偏好时，以已有档案为准。唯有当前进程就是该服务器的隔离实例，才用其最新设置
     * 更新档案。这样从启动器重进或从另一个服务器切换，都不会把旧偏好重置掉。
     * 调用方应先在渲染线程执行原版 options.save()，再在后台调用本方法。
     */
    public static void prepare(Path worldFolder, Path currentGame, Path newGame) throws IOException {
        Path world = worldFolder.toAbsolutePath().normalize();
        Path current = currentGame.toAbsolutePath().normalize();
        Path target = newGame.toAbsolutePath().normalize();
        requireNoLinks(world);
        requireNoLinks(current);
        requireNoLinks(target);
        // 只能写入本安装器的生成目录，不能误将启动器本身作为新实例覆盖。
        if (!isRuntimeLayout(target) || !target.getParent().getParent().equals(world.resolve("client-runtime"))
                || current.equals(target)) {
            throw new IOException("个人设置的安装目标不是新的客户端运行目录");
        }
        Path profile = world.resolve(PROFILE_DIRECTORY);
        requireNoLinks(profile);
        Files.createDirectories(profile);
        Files.createDirectories(target);
        if (world.equals(runtimeWorld(current))) copyPresent(current, profile);

        for (String name : FILES) {
            Path saved = profile.resolve(name);
            // 按文件继承，允许后来新增的客户端渲染模组补上自己的个人选项。
            if (!regularFile(saved) && regularFile(current.resolve(name))) {
                copyAtomic(current.resolve(name), saved);
            }
            Path installed = target.resolve(name);
            if (regularFile(saved)) {
                copyAtomic(saved, installed);
            } else {
                // 兼容旧服务端清单：没有本地偏好时也不能接受服务端附带的个人设置。
                if (regularFile(installed)) Files.delete(installed);
            }
        }
    }

    /**
     * 正常退出时归档当前服务器的偏好。普通启动器目录返回 false，不创建任何档案。
     * 同时验证目录布局和本机启动记录，不能只凭一个 JVM 版本属性推断设置的归属。
     */
    public static boolean saveCurrent(Path gameDir) throws IOException {
        Path game = gameDir.toAbsolutePath().normalize();
        Path world = runtimeWorld(game);
        if (world == null) return false;
        Path profile = world.resolve(PROFILE_DIRECTORY);
        requireNoLinks(profile);
        Files.createDirectories(profile);
        copyPresent(game, profile);
        return true;
    }

    private static Path runtimeWorld(Path game) throws IOException {
        if (!isRuntimeLayout(game)) return null;
        requireNoLinks(game);
        Path record = game.getParent().resolve(ClientRuntimeStartup.MANIFEST_FILE);
        return regularFile(record) ? game.getParent().getParent().getParent() : null;
    }

    private static boolean isRuntimeLayout(Path game) {
        Path instance = game.getParent();
        Path runtimes = instance == null ? null : instance.getParent();
        return game.getFileName() != null && game.getFileName().toString().equals("game")
                && runtimes != null && runtimes.getFileName() != null && runtimes.getParent() != null
                && runtimes.getFileName().toString().equals("client-runtime");
    }

    private static void copyPresent(Path from, Path to) throws IOException {
        for (String name : FILES) {
            if (regularFile(from.resolve(name))) copyAtomic(from.resolve(name), to.resolve(name));
        }
    }

    /** 缺失文件可以跳过，链接和非常规文件必须报错，不能静默穿透到实例之外。 */
    private static boolean regularFile(Path path) throws IOException {
        requireNoLinks(path);
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return false;
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("客户端个人设置不是普通文件: " + path.getFileName());
        }
        return true;
    }

    /** 小文件先有界读取，再写唯一临时文件并原子替换；失败时保留上一份完整档案。 */
    private static void copyAtomic(Path from, Path to) throws IOException {
        requireNoLinks(from);
        requireNoLinks(to);
        byte[] content;
        try (var input = Files.newInputStream(from, LinkOption.NOFOLLOW_LINKS)) {
            content = input.readNBytes(MAX_FILE_BYTES + 1);
        }
        if (content.length > MAX_FILE_BYTES) throw new IOException("客户端个人设置文件超过 4 MiB");
        Path temporary = Files.createTempFile(to.getParent(), ".client-options-", ".tmp");
        try {
            Files.write(temporary, content);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
            try {
                Files.move(temporary, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, to, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void requireNoLinks(Path path) throws IOException {
        for (Path part = path; part != null; part = part.getParent()) {
            if (Files.isSymbolicLink(part)) throw new IOException("客户端个人设置路径不能经过符号链接");
        }
    }
}
