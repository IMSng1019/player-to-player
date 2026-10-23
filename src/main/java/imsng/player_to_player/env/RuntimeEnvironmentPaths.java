package imsng.player_to_player.env;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** 环境清单路径与实际运行目录的映射；世界资源位于独立存档，不能误判为被删除。 */
public final class RuntimeEnvironmentPaths {
    private RuntimeEnvironmentPaths() { }

    public static Path resolve(Path game, Path save, String worldPath, String relative) throws IOException {
        String world = EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath);
        if (relative == null || relative.isBlank() || relative.startsWith("/") || relative.contains(":")
                || relative.contains("\\")) throw new IOException("非法环境路径");
        for (String part : relative.split("/", -1)) {
            if (part.isEmpty() || part.equals(".") || part.equals("..")) throw new IOException("非法环境路径");
        }
        boolean inWorld = relative.equals(world) || relative.startsWith(world + "/");
        if (inWorld && save == null) throw new IOException("实际运行存档路径尚未就绪");
        Path root = (inWorld ? save : game).toAbsolutePath().normalize();
        String child = inWorld ? (relative.equals(world) ? "" : relative.substring(world.length() + 1)) : relative;
        Path target = root.resolve(child).normalize();
        if (!target.startsWith(root)) throw new IOException("环境路径越过运行目录");
        for (Path part = target; part != null; part = part.getParent()) {
            if (Files.isSymbolicLink(part)) throw new IOException("运行环境路径不能经过符号链接");
        }
        return target;
    }

    public static String manifestPath(Path game, Path save, String worldPath, Path file) throws IOException {
        Path target = file.toAbsolutePath().normalize();
        if (save != null && target.startsWith(save.toAbsolutePath().normalize())) {
            return EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath) + "/"
                    + save.toAbsolutePath().normalize().relativize(target).toString().replace('\\', '/');
        }
        Path root = game.toAbsolutePath().normalize();
        if (!target.startsWith(root)) throw new IOException("运行文件不属于当前实例");
        return root.relativize(target).toString().replace('\\', '/');
    }
}
