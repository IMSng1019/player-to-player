package imsng.player_to_player.env;

import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileVisitResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * 下载目录的发布事务。暂存目录与目标目录处于同一文件系统，下载、复用和复验均只
 * 改动暂存目录；提交时保留各个旧目录并逐个安装完整新目录，异常时逆序回滚。
 * <p>
 * 文件系统并没有“同时重命名两个任意目录”的原语，因此调用方必须等待同步 future
 * 成功后再读取/启动环境，不能在提交窗口直接打开这些目录。这里的锁用于串行化本
 * JVM 的目录提交；持久日志在下一次同步前恢复崩溃留下的半安装状态。它不代替运行
 * 目录隔离，其他进程仍必须经过同步屏障才能使用这两份可替换的下载缓存。
 */
final class EnvironmentDirectoryTransaction implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env");
    private static final Object COMMIT_LOCK = new Object();

    private static final class Directory {
        private final Path target;
        private final Path staging;
        private final EnvironmentManifest manifest;
        private Path backup;

        private Directory(Path target, Path staging, EnvironmentManifest manifest) {
            this.target = target;
            this.staging = staging;
            this.manifest = manifest;
        }
    }

    private final List<Directory> directories = new ArrayList<>();
    private final DirectoryMove directoryMove;

    /** 把目录系统调用作为可替换边界，测试可稳定复现磁盘/文件占用导致的安装失败。 */
    @FunctionalInterface
    interface DirectoryMove {
        void move(Path source, Path target) throws IOException;
    }

    EnvironmentDirectoryTransaction() { this(EnvironmentDirectoryTransaction::move); }

    EnvironmentDirectoryTransaction(DirectoryMove directoryMove) {
        this.directoryMove = java.util.Objects.requireNonNull(directoryMove);
    }

    /** 仅存目标绝对路径与暂存/备份的文件名；恢复前必须与本次调用的目标逐项匹配。 */
    private record JournalEntry(String target, String staging, String backup, boolean hadOriginal) { }
    private record Journal(String transactionId, String status, List<JournalEntry> directories) { }

    /** 每个角色取得全新的空目录，旧目录中不在清单的文件自然不会进入新版本。 */
    Path stage(Path target, EnvironmentManifest manifest) throws IOException {
        Path normalized = target.toAbsolutePath().normalize();
        validateRoot(normalized);
        for (String path : manifest.files().keySet()) {
            validateManifestPath(normalized, path);
        }
        Files.createDirectories(normalized.getParent());
        Path staging = Files.createTempDirectory(normalized.getParent(), ".p2p-env-stage-");
        directories.add(new Directory(normalized, staging, manifest));
        return staging;
    }

    /** 原环境根及祖先都不得为链接，避免整个事务被重定向到另一个世界或磁盘目录。 */
    static void validateRoot(Path root) throws IOException {
        Path normalized = root.toAbsolutePath().normalize();
        if (normalized.getParent() == null) throw new IOException("不能把文件系统根目录作为环境目录");
        for (Path current = normalized; current != null; current = current.getParent()) {
            if (Files.isSymbolicLink(current)) throw new IOException("环境目录不能经过符号链接: " + current);
        }
        if (Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)
                && !Files.isDirectory(normalized, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("环境目标不是普通目录: " + normalized);
        }
    }

    /** 网络清单只允许规范的相对路径，不接受重复分隔符、Windows ADS 或别名路径。 */
    static void validateManifestPath(Path root, String path) throws IOException {
        if (path == null || path.isBlank() || path.indexOf('\\') >= 0 || path.indexOf(':') >= 0
                || path.startsWith("/") || path.endsWith("/")) {
            throw new IOException("清单含非法相对路径: " + path);
        }
        for (String component : path.split("/", -1)) {
            if (component.isEmpty() || component.equals(".") || component.equals("..")) {
                throw new IOException("清单含非法相对路径: " + path);
            }
        }
        EnvironmentContentReuse.resolveDestination(root, path);
    }

    /**
     * 完整文件集校验不能使用 EnvironmentScanner：源扫描器会排除 *.tmp、日志等路径，
     * 而下载目录内的任何额外文件都可能影响实际运行，必须同样被发现。
     */
    static void verify(Path root, EnvironmentManifest expected) throws IOException {
        Map<String, EnvironmentManifest.Entry> observed = new TreeMap<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs)
                    throws IOException {
                if (attrs.isSymbolicLink()) throw new IOException("环境包含目录链接: " + directory);
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!attrs.isRegularFile() || attrs.isSymbolicLink()) {
                    throw new IOException("环境包含非普通文件: " + file);
                }
                String path = root.relativize(file).toString().replace('\\', '/');
                observed.put(path, new EnvironmentManifest.Entry(Sha256.hexOfFile(file), Files.size(file)));
                return FileVisitResult.CONTINUE;
            }
        });
        if (!expected.files().equals(observed)) throw new IOException("环境完整文件集校验失败: " + root);
    }

    void commit() throws IOException {
        commit(() -> true);
    }

    void commit(java.util.function.BooleanSupplier stillCurrent) throws IOException {
        synchronized (COMMIT_LOCK) {
            requireCurrent(stillCurrent);
            // 两个角色下载完成后再复验一次，覆盖下载另一角色期间暂存内容被意外改动。
            for (Directory directory : directories) {
                validateRoot(directory.target);
                verify(directory.staging, directory.manifest);
            }
            Path[] targets = directories.stream().map(directory -> directory.target).toArray(Path[]::new);
            recover(targets);
            String transactionId = UUID.randomUUID().toString();
            List<JournalEntry> entries = new ArrayList<>();
            for (int index = 0; index < directories.size(); index++) {
                Directory directory = directories.get(index);
                directory.backup = directory.target.resolveSibling(".p2p-env-backup-" + transactionId + "-" + index);
                entries.add(new JournalEntry(directory.target.toString(), directory.staging.getFileName().toString(),
                        directory.backup.getFileName().toString(), Files.exists(directory.target, LinkOption.NOFOLLOW_LINKS)));
            }
            Path journalFile = journalPath(targets);
            // 复验可能读取大文件；在真正写意图日志/移动旧目录之前再次确认会话归属。
            requireCurrent(stillCurrent);
            // 意图日志先强制落盘，随后每一步移动都可依据“备份/暂存是否还存在”恢复。
            writeJournal(journalFile, new Journal(transactionId, "PREPARED", entries));
            try {
                for (int index = 0; index < directories.size(); index++) {
                    Directory directory = directories.get(index);
                    if (entries.get(index).hadOriginal()) directoryMove.move(directory.target, directory.backup);
                    directoryMove.move(directory.staging, directory.target);
                }
                writeJournal(journalFile, new Journal(transactionId, "COMMITTED", entries));
            } catch (IOException failure) {
                try { recover(targets); }
                catch (IOException rollbackFailure) {
                    // 回滚未完成时保留日志和所有尚在的备份，下次启动可以继续恢复。
                    failure.addSuppressed(new IOException("环境回滚未完成，恢复记录: " + journalFile, rollbackFailure));
                }
                throw failure;
            }
            // 发布成功后清理失败不反转结果；COMMITTED 日志会让下次同步继续清理。
            try { recover(targets); }
            catch (IOException cleanupFailure) { LOGGER.warn("环境已发布，待下次清理事务记录: {}", journalFile, cleanupFailure); }
        }
    }

    private static void requireCurrent(java.util.function.BooleanSupplier stillCurrent) {
        if (!stillCurrent.getAsBoolean()) throw new java.util.concurrent.CancellationException("环境同步会话已取消，拒绝发布旧目录");
    }

    /**
     * 同步开始前恢复中断安装。磁盘日志也是不可信输入：先验证整份记录的目标、UUID、
     * 备份名和暂存名，全部通过后才能改动目录，禁止用被篡改的 JSON 删除任意路径。
     */
    static void recover(Path... suppliedTargets) throws IOException {
        synchronized (COMMIT_LOCK) {
            Path[] targets = java.util.Arrays.stream(suppliedTargets)
                    .map(path -> path.toAbsolutePath().normalize()).toArray(Path[]::new);
            Path journalFile = journalPath(targets);
            if (!Files.exists(journalFile, LinkOption.NOFOLLOW_LINKS)) return;
            if (Files.isSymbolicLink(journalFile) || !Files.isRegularFile(journalFile, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("环境事务日志不是普通文件: " + journalFile);
            }
            Journal journal;
            try {
                journal = JsonUtil.readFile(journalFile, Journal.class);
                if (journal == null || journal.directories() == null || journal.directories().size() != targets.length
                        || !("PREPARED".equals(journal.status()) || "COMMITTED".equals(journal.status()))) {
                    throw new IllegalArgumentException("事务状态或目标数量错误");
                }
                UUID.fromString(journal.transactionId());
                for (int index = 0; index < targets.length; index++) {
                    JournalEntry entry = journal.directories().get(index);
                    if (!targets[index].toString().equals(entry.target())
                            || !entry.backup().equals(".p2p-env-backup-" + journal.transactionId() + "-" + index)
                            || entry.staging() == null || !entry.staging().startsWith(".p2p-env-stage-")
                            || entry.staging().contains("/") || entry.staging().contains("\\") || entry.staging().contains(":")) {
                        throw new IllegalArgumentException("事务路径与当前环境不匹配");
                    }
                    validateRoot(targets[index]);
                    validateRoot(targets[index].resolveSibling(entry.backup()));
                    validateRoot(targets[index].resolveSibling(entry.staging()));
                }
            } catch (RuntimeException invalid) {
                throw new IOException("环境事务日志校验失败: " + journalFile, invalid);
            }
            boolean committed = "COMMITTED".equals(journal.status());
            for (int index = targets.length - 1; index >= 0; index--) {
                JournalEntry entry = journal.directories().get(index);
                Path target = targets[index];
                Path backup = target.resolveSibling(entry.backup());
                Path staging = target.resolveSibling(entry.staging());
                if (!committed) {
                    if (Files.exists(backup, LinkOption.NOFOLLOW_LINKS)) {
                        deleteTree(target);
                        move(backup, target);
                    } else if (!entry.hadOriginal() && !Files.exists(staging, LinkOption.NOFOLLOW_LINKS)) {
                        deleteTree(target);
                    } else if (entry.hadOriginal() && !Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException("环境恢复缺少原目录与备份: " + target);
                    }
                } else {
                    if (!Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException("已提交环境目录缺失，保留备份等待恢复: " + target);
                    }
                    deleteTree(backup);
                }
                deleteTree(staging);
            }
            Files.delete(journalFile);
        }
    }

    /** 日志位于目标目录的共同父目录，避免被安装替换或被环境清单当作运行文件。 */
    private static Path journalPath(Path[] targets) throws IOException {
        if (targets.length == 0) throw new IOException("环境安装事务缺少目标目录");
        Path parent = targets[0].getParent();
        for (Path target : targets) {
            while (parent != null && !target.getParent().startsWith(parent)) parent = parent.getParent();
        }
        if (parent == null) throw new IOException("环境安装目录必须位于同一文件系统");
        String identity = java.util.Arrays.stream(targets).map(Path::toString)
                .collect(java.util.stream.Collectors.joining("\n"));
        return parent.resolve(".p2p-env-install-" + Sha256.hex(identity.getBytes(StandardCharsets.UTF_8)) + ".json");
    }

    /** 使用唯一临时文件并 force，避免跟随预先准备的固定 .tmp 路径。 */
    private static void writeJournal(Path file, Journal journal) throws IOException {
        Files.createDirectories(file.getParent());
        Path temporary = Files.createTempFile(file.getParent(), ".p2p-env-journal-", ".tmp");
        try {
            Files.writeString(temporary, JsonUtil.PRETTY.toJson(journal), StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
            try { Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
            catch (AtomicMoveNotSupportedException unsupported) { Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING); }
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) { channel.force(true); }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void move(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(from, to);
        }
    }

    /** 不跟随链接删除，仅在已保存的事务暂存/备份路径上调用。 */
    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) return;
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult postVisitDirectory(Path directory, IOException error) throws IOException {
                if (error != null) throw error;
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void cleanup(Path path) {
        try { deleteTree(path); }
        catch (IOException error) { LOGGER.warn("清理环境事务目录失败: {}", path, error); }
    }

    @Override public void close() {
        for (Directory directory : directories) cleanup(directory.staging);
    }
}
