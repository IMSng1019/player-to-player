package imsng.player_to_player.env;

import imsng.player_to_player.util.Sha256;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;

/**
 * 环境修改的逐文件乐观并发提交。基础 SHA-256 是文件版本，空串表示原先不存在；
 * 多组修改同一文件时只有第一个匹配版本的写者成功。绝不对任意配置做猜测式合并。
 * 同内容重试视为成功，覆盖提交 ACK 丢失；写入先强制临时文件落盘，再替换目标。
 */
public final class EnvironmentUploadStore {
    private EnvironmentUploadStore() { }

    public static synchronized String apply(Path root, List<String> allowedPaths, String path,
                                             String baseSha256, byte[] replacement) throws IOException {
        if (!allowed(path, allowedPaths) || baseSha256 == null
                || (!baseSha256.isEmpty() && !baseSha256.matches("[0-9a-f]{64}"))) return "forbidden";
        Path destination;
        try { destination = EnvironmentContentReuse.resolveDestination(root, path); }
        catch (IOException | RuntimeException invalid) { return "forbidden"; }
        boolean exists = Files.exists(destination, LinkOption.NOFOLLOW_LINKS);
        if (exists && !Files.isRegularFile(destination, LinkOption.NOFOLLOW_LINKS)) return "forbidden";
        String actual = exists ? Sha256.hexOfFile(destination) : "";
        String desired = replacement == null ? "" : Sha256.hex(replacement);
        if (actual.equals(desired)) return "unchanged";
        if (!actual.equals(baseSha256)) return "conflict";
        if (replacement == null) {
            Files.delete(destination);
            return "applied";
        }
        Files.createDirectories(destination.getParent());
        // 创建父目录后重新验证，保证普通路径不会无意间跟随预先存在的目录链接。
        EnvironmentContentReuse.resolveDestination(root, path);
        Path temporary = Files.createTempFile(destination.getParent(), ".p2p-upload-", ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                ByteBuffer bytes = ByteBuffer.wrap(replacement);
                while (bytes.hasRemaining()) channel.write(bytes);
                channel.force(true);
            }
            // 管理员可能在临时文件写入期间编辑配置；发布前再次检查基础字节版本。
            String beforeReplace = Files.exists(destination) ? Sha256.hexOfFile(destination) : "";
            if (!actual.equals(beforeReplace)) return "conflict";
            try { Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING); }
            catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING);
            }
            return "applied";
        } finally { Files.deleteIfExists(temporary); }
    }

    /** 允许表限制的是路径树，不是字符串前缀；无论配置如何，都不允许传输可执行程序。 */
    public static boolean allowed(String path, List<String> allowedPaths) {
        if (path == null || path.isBlank() || path.contains("\\") || path.contains(":")
                || path.startsWith("/") || path.endsWith("/")) return false;
        for (String segment : path.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) return false;
        }
        if (!EnvironmentPathPolicy.create(List.of()).includes(path)) return false;
        String lower = path.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".jar") || lower.endsWith(".class") || lower.endsWith(".exe")
                || lower.endsWith(".dll") || lower.endsWith(".bat") || lower.endsWith(".cmd")
                || lower.endsWith(".sh") || lower.endsWith(".ps1")) return false;
        if (allowedPaths == null) return false;
        for (String allowed : allowedPaths) {
            if (allowed == null || allowed.isBlank()) continue;
            String prefix = allowed.replace('\\', '/');
            while (prefix.endsWith("/")) prefix = prefix.substring(0, prefix.length() - 1);
            if (path.equals(prefix) || path.startsWith(prefix + "/")) return true;
        }
        return false;
    }
}
