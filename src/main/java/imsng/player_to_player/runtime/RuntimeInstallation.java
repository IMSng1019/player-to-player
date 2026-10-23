package imsng.player_to_player.runtime;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** 已校验环境到独立计算实例的安装边界。 */
public final class RuntimeInstallation {
    /** 这些值影响组世界演算；网络监听、认证、RCON 等必须由本机宿主控制。 */
    public static final Set<String> WORLD_SETTINGS = Set.of("difficulty", "gamemode", "hardcore",
            "pvp", "view-distance", "simulation-distance", "allow-nether", "spawn-animals",
            "spawn-monsters", "spawn-npcs", "spawn-protection", "allow-flight", "max-players",
            "max-world-size", "entity-broadcast-range-percentage", "function-permission-level",
            "op-permission-level", "max-chained-neighbor-updates", "force-gamemode",
            "enable-command-block", "generate-structures", "level-seed", "level-type", "generator-settings");

    private RuntimeInstallation() { }

    /**
     * 只在旧实例已停止后安装。先构造完整的新目录，再交换目录；旧目录保留为 previous
     * 副本，既提供失败回滚，也保留旧进程日志。可变世界始终位于独立 data 目录，绝不复制
     * 到新运行目录。任何路径链接都拒绝，防止复制或替换越过本世界的边界。
     */
    public static Path install(Path environment, Path runtime, String worldPath,
                               Map<String, String> properties) throws IOException {
        Path source = environment.toAbsolutePath().normalize();
        Path target = runtime.toAbsolutePath().normalize();
        requireNoLinks(source);
        requireNoLinks(target);
        if (!Files.isDirectory(source) || target.startsWith(source) || source.startsWith(target)) {
            throw new IOException("环境源与运行目录必须是两个独立目录");
        }
        String world = normalizedWorldPath(worldPath);
        Files.createDirectories(target.getParent());
        Path staging = Files.createTempDirectory(target.getParent(), ".runtime-install-");
        Path backup = target.resolveSibling(target.getFileName() + ".previous-" + UUID.randomUUID());
        boolean movedOld = false;
        try {
            Files.walkFileTree(source, new SimpleFileVisitor<>() {
                @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                        throws IOException {
                    String relative = source.relativize(dir).toString().replace('\\', '/');
                    if (!relative.isEmpty() && excluded(relative, world)) return FileVisitResult.SKIP_SUBTREE;
                    requireNoLinks(dir);
                    Files.createDirectories(staging.resolve(source.relativize(dir)));
                    return FileVisitResult.CONTINUE;
                }
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    String relative = source.relativize(file).toString().replace('\\', '/');
                    if (excluded(relative, world)) return FileVisitResult.CONTINUE;
                    if (!attrs.isRegularFile() || attrs.isSymbolicLink()) {
                        throw new IOException("运行环境包含非常规文件: " + relative);
                    }
                    Files.copy(file, staging.resolve(source.relativize(file)));
                    return FileVisitResult.CONTINUE;
                }
            });
            writeSettings(staging, properties);
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                move(target, backup);
                movedOld = true;
            }
            try { move(staging, target); }
            catch (IOException failed) {
                if (movedOld) {
                    try { move(backup, target); }
                    catch (IOException rollback) { failed.addSuppressed(rollback); }
                }
                throw failed;
            }
            // 只保留上一次完整实例，避免每次加入都永久复制一套大型整合包。清理对象限定为
            // 本安装器生成的同级“目标名.previous-UUID”普通目录；不跟随任何符号链接。
            try (var siblings = Files.list(target.getParent())) {
                String prefix = target.getFileName() + ".previous-";
                for (Path old : siblings.toList()) {
                    String name = old.getFileName().toString();
                    if (old.equals(backup) || !name.startsWith(prefix)
                            || !name.substring(prefix.length()).matches("[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}")
                            || !Files.isDirectory(old, LinkOption.NOFOLLOW_LINKS)) continue;
                    try { removeStaging(old); }
                    catch (IOException cleanup) {
                        org.slf4j.LoggerFactory.getLogger("player_to_player/runtime")
                                .warn("旧运行目录暂未清理: {}", old);
                    }
                }
            }
            return target;
        } finally {
            if (Files.exists(staging, LinkOption.NOFOLLOW_LINKS)) removeStaging(staging);
        }
    }

    private static String normalizedWorldPath(String path) throws IOException {
        if (path == null || path.isBlank()) throw new IOException("缺少实际存档相对路径");
        Path relative = Path.of(path.replace('\\', '/')).normalize();
        if (relative.isAbsolute() || relative.startsWith("..") || relative.toString().isBlank()) {
            throw new IOException("非法实际存档路径");
        }
        return relative.toString().replace('\\', '/');
    }

    private static boolean excluded(String relative, String world) {
        String lower = relative.toLowerCase(java.util.Locale.ROOT);
        String lowerWorld = world.toLowerCase(java.util.Locale.ROOT);
        if (lower.equals(lowerWorld) || lower.startsWith(lowerWorld + "/")) return true;
        return lower.equals("player-to-player") || lower.startsWith("player-to-player/")
                || lower.equals("logs") || lower.startsWith("logs/")
                || Set.of("server.properties", "eula.txt", "ops.json", "whitelist.json",
                "banned-players.json", "banned-ips.json", "usercache.json", "session.lock").contains(lower);
    }

    private static void writeSettings(Path directory, Map<String, String> exported) throws IOException {
        Properties properties = new Properties();
        exported.forEach((name, value) -> {
            if (WORLD_SETTINGS.contains(name) && value != null) properties.setProperty(name, value);
        });
        properties.setProperty("server-ip", "127.0.0.1");
        String localPort = exported.getOrDefault("p2p.local-port", "0");
        try {
            int port = Integer.parseInt(localPort);
            if (port < 0 || port > 65535) throw new NumberFormatException();
        } catch (NumberFormatException invalid) {
            throw new IOException("本机计算实例端口非法");
        }
        properties.setProperty("server-port", localPort);
        // 玩家身份由已认证父子桥在登录时映射；独立服不能向公网接受未经桥接的连接。
        properties.setProperty("online-mode", "false");
        properties.setProperty("enforce-secure-profile", "false");
        properties.setProperty("enable-rcon", "false");
        properties.setProperty("enable-query", "false");
        properties.setProperty("white-list", "false");
        properties.setProperty("max-tick-time", "-1");
        try (var writer = Files.newBufferedWriter(directory.resolve("server.properties"))) {
            properties.store(writer, "P2P managed local computation server");
        }
        // 只沿用已成功启动的上游服务器所报告的同意状态；没有该事实时保持未同意。
        Files.writeString(directory.resolve("eula.txt"),
                "eula=" + Boolean.parseBoolean(exported.getOrDefault("eula", "false")) + "\n");
    }

    private static void requireNoLinks(Path path) throws IOException {
        for (Path current = path; current != null; current = current.getParent()) {
            if (Files.isSymbolicLink(current)) throw new IOException("运行环境路径不能经过符号链接: " + path);
        }
    }

    private static void move(Path from, Path to) throws IOException {
        try { Files.move(from, to, StandardCopyOption.ATOMIC_MOVE); }
        catch (AtomicMoveNotSupportedException unsupported) { Files.move(from, to); }
    }

    /** 仅清理本次创建且已验证的暂存树，不跟随链接，不递归清理用户存档或旧运行目录。 */
    private static void removeStaging(Path staging) throws IOException {
        Files.walkFileTree(staging, new SimpleFileVisitor<>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult postVisitDirectory(Path dir, IOException failure) throws IOException {
                if (failure != null) throw failure;
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
