package imsng.player_to_player.env;

import imsng.player_to_player.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 单次双角色同步的内容索引，不能存入静态变量或跨世界会话复用。
 * <p>
 * Entry 同时包含 SHA-256 与大小。索引只记录本批次角色清单已确认的文件路径，
 * 不把目录中的额外旧文件当成缓存。文件仍可能被模组修改，因此每次复制到独立临时
 * 文件后必须重新校验，校验失败只放弃该缓存源，由调用方使用目标清单正常下载。
 */
final class EnvironmentContentReuse {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env");

    private record Source(Path root, String relativePath) {
    }

    private final Map<EnvironmentManifest.Entry, List<Source>> sources = new HashMap<>();

    /**
     * 先按角色清单和大小登记候选；真正复制后必须验证 SHA-256。
     * 不在这里重复读全文件，正常 diff 扫描会哈希一次，避免热缓存同步读盘量翻倍。
     */
    void indexMatching(Path root, EnvironmentManifest manifest) {
        for (Map.Entry<String, EnvironmentManifest.Entry> file : manifest.files().entrySet()) {
            try {
                Path source = resolveDestination(root, file.getKey());
                if (isSafeSource(root, source) && Files.size(source) == file.getValue().size()) {
                    remember(root, file.getKey(), file.getValue());
                }
            } catch (IOException | SecurityException e) {
                // 索引只是优化；本地目录不可读时仍保留原有按服务端清单下载的机会。
                LOGGER.debug("环境复用源不可读，跳过: {} ({})", file.getKey(), e.toString());
            }
        }
    }

    /** 仅由已完成清单校验、下载校验或复制校验的调用点登记，不在这里持有文件句柄。 */
    void remember(Path root, String relativePath, EnvironmentManifest.Entry entry) {
        Source candidate = new Source(root.toAbsolutePath().normalize(), relativePath);
        List<Source> candidates = sources.computeIfAbsent(entry, ignored -> new ArrayList<>());
        if (!candidates.contains(candidate)) {
            candidates.add(candidate);
        }
    }

    /**
     * 尝试复用，成功返回 true；复制/校验失败返回 false，由调用方回退下载。
     * 目标非法路径直接抛出，不得以缓存命中绕过下载分支的路径防护。
     */
    boolean copyTo(Path root, String relativePath, EnvironmentManifest.Entry entry) throws IOException {
        Path destination = resolveDestination(root, relativePath);
        for (Source candidate : sources.getOrDefault(entry, List.of())) {
            Path temporary = null;
            try {
                Path source = resolveDestination(candidate.root(), candidate.relativePath());
                if (source.equals(destination) || !isSafeSource(candidate.root(), source)) {
                    continue;
                }
                Files.createDirectories(destination.getParent());
                // 使用新临时文件，既不共享 inode，也不跟随预先存在的临时符号链接。
                temporary = Files.createTempFile(destination.getParent(), ".p2pcopy-", ".tmp");
                Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING);
                if (!matches(temporary, entry)) {
                    LOGGER.debug("环境复用源已变化，回退下载: {}", relativePath);
                    continue;
                }
                try {
                    Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING);
                }
                return true;
            } catch (IOException | SecurityException e) {
                LOGGER.debug("环境文件复用失败，回退下载: {} ({})", relativePath, e.toString());
            } finally {
                if (temporary != null) {
                    try {
                        Files.deleteIfExists(temporary);
                    } catch (IOException e) {
                        // .tmp 被扫描器排除；清理失败不能遮蔽下载回退或已成功的复制结果。
                        LOGGER.debug("清理环境复用临时文件失败: {} ({})", temporary, e.toString());
                    }
                }
            }
        }
        return false;
    }

    /** 大小预检避免无谓读大文件；哈希完成后再次核对大小，覆盖复制期间内容变化。 */
    private static boolean matches(Path file, EnvironmentManifest.Entry entry) throws IOException {
        return Files.size(file) == entry.size() && Sha256.hexOfFile(file).equals(entry.sha256())
                && Files.size(file) == entry.size();
    }

    /** 额外检查真实路径，避免目录链接将看似位于本世界目录的源指向另一世界。 */
    private static boolean isSafeSource(Path root, Path source) throws IOException {
        return Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)
                && source.toRealPath().startsWith(root.toRealPath());
    }

    /** 下载和复制共享同一目标检查，拒绝目录穿越以及已有路径上的符号链接。 */
    static Path resolveDestination(Path root, String relativePath) throws IOException {
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Path destination = normalizedRoot.resolve(relativePath).normalize();
        if (!destination.startsWith(normalizedRoot) || destination.equals(normalizedRoot)) {
            throw new IOException("清单含非法路径（目录穿越）: " + relativePath);
        }
        Path current = destination;
        while (current != null && current.startsWith(normalizedRoot)) {
            if (Files.isSymbolicLink(current)) {
                throw new IOException("环境路径不能经过符号链接: " + relativePath);
            }
            current = current.getParent();
        }
        return destination;
    }
}
