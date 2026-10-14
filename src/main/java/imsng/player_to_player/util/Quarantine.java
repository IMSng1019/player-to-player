package imsng.player_to_player.util;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 陈旧状态隔离：把"不再属于当前世界"的目录<b>整体改名保留</b>在同级目录下。
 * <p>
 * 设计红线（见 {@code docs/plans/2026-10-14-world-identity-mismatch-corruption.md} §5）：
 * 世界身份不一致时<b>隔离而非删除</b>。旧存档/旧状态可能还有人工价值，改名后既不参与
 * 后续读写，也不会被误当作当前世界的数据；代价只是占用磁盘。
 * <p>
 * 命名规则：{@code <原名>.stale-<标签>-<yyyyMMdd-HHmmss>[-序号]}。
 * 标签由调用方给出（通常是旧世界身份短标识），时间戳与序号保证同名不覆盖。
 */
public final class Quarantine {

    /** 隔离目录名里的时间戳格式（本地时区，人工可读优先）。 */
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private Quarantine() {
    }

    /**
     * 把目录改名为同级下的隔离目录。
     *
     * @param target      待隔离目录（不存在时返回 {@code null}，不视为错误）
     * @param label       隔离标签（如旧世界身份短标识）；非法字符会被替换
     * @param epochMillis 时间戳（毫秒）；由调用方提供，便于测试与日志对照
     * @return 隔离后的目录，或 {@code null}（目标不存在）
     * @throws IOException 目标存在但不是目录，或改名失败
     */
    public static Path quarantine(Path target, String label, long epochMillis)
            throws IOException {
        if (target == null || !Files.exists(target)) {
            return null;
        }
        if (!Files.isDirectory(target)) {
            throw new IOException("隔离目标不是目录: " + target);
        }
        Path parent = target.getParent();
        if (parent == null) {
            throw new IOException("隔离目标没有父目录，无法同级改名: " + target);
        }
        String safeLabel = sanitizeLabel(label);
        String stamp = TIMESTAMP.format(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis),
                        ZoneId.systemDefault()));
        String base = target.getFileName() + ".stale-" + safeLabel + "-" + stamp;
        Path destination = parent.resolve(base);
        for (int suffix = 2; Files.exists(destination); suffix++) {
            destination = parent.resolve(base + "-" + suffix);
        }
        try {
            Files.move(target, destination, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            // 部分文件系统不支持原子改名，降级为普通移动（同卷内仍等价于改名）
            Files.move(target, destination);
        }
        return destination;
    }

    /** 标签只用于目录名：剔除路径分隔符等非文件名安全字符。 */
    private static String sanitizeLabel(String label) {
        if (label == null || label.isBlank()) {
            return "unknown";
        }
        return label.trim().replaceAll("[\\\\/:*?\"<>|\\s]", "_");
    }
}
