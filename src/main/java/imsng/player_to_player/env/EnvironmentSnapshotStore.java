package imsng.player_to_player.env;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * 内容寻址环境快照仓库。
 * <p>
 * 源目录可以持续变化，但一旦快照发布，后续读取只会访问
 * {@code repository/blobs/<sha256>}，绝不回读源文件。因此清单记录的大小、哈希和
 * 实际下发内容天然来自同一份不可变字节序列。
 */
public final class EnvironmentSnapshotStore {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env-snapshot");

    private static final int COPY_BUFFER_BYTES = 128 * 1024;
    private static final int MAX_STABLE_COPY_ATTEMPTS = 3;

    private final Path sourceRoot;
    private final Path repositoryRoot;
    private final Path blobsDir;
    private final Path manifestsDir;
    private final Path stagingDir;
    private final EnvironmentPathPolicy pathPolicy;
    /** 只缓存本进程成功构建时稳定读取的版本；重启后重新验证，不相信磁盘上的属性缓存。 */
    private Map<String, CachedFile> sourceCache = new HashMap<>();
    private EnvironmentSnapshot lastBuilt;
    private volatile BuildStats lastBuildStats = new BuildStats(0, 0, 0);

    public record BuildStats(int copiedFiles, long copiedBytes, int reusedFiles) { }

    public BuildStats lastBuildStats() { return lastBuildStats; }

    public EnvironmentSnapshotStore(Path sourceRoot, Path repositoryRoot,
                                    List<String> extraExclusions) {
        this.sourceRoot = sourceRoot.toAbsolutePath().normalize();
        this.repositoryRoot = repositoryRoot.toAbsolutePath().normalize();
        this.blobsDir = this.repositoryRoot.resolve("blobs");
        this.manifestsDir = this.repositoryRoot.resolve("manifests");
        this.stagingDir = this.repositoryRoot.resolve("staging");
        this.pathPolicy = EnvironmentPathPolicy.create(extraExclusions);
    }

    /**
     * 从当前源目录构建一份完整快照。只有所有文件都成功物化为 Blob 后才返回快照，
     * 调用方可以据此原子发布；任何文件持续变化都会让整轮构建失败。
     */
    public EnvironmentSnapshot buildSnapshot() throws IOException {
        // 无事件上下文的调用方采用完整内容校验；相同内容仍可复用 Blob 而不写临时副本。
        return buildSnapshot(Set.of(), true);
    }

    /**
     * 事件已知时只重新校验受影响的路径树，其他条目同时核对文件版本与 Blob 是否存在。
     * verifyAll 用于启动、监听溢出和周期兜底，即使同大小/同时间戳的修改也能被识别。
     * 只有整轮成功才更换缓存，失败不会把尚未发布的部分清单变成后续构建的依据。
     */
    synchronized EnvironmentSnapshot buildSnapshot(Set<String> changedPaths, boolean verifyAll) throws IOException {
        Files.createDirectories(blobsDir);
        Files.createDirectories(manifestsDir);
        Files.createDirectories(stagingDir);

        Map<String, EnvironmentManifest.Entry> entries = new TreeMap<>();
        Map<String, CachedFile> nextCache = new HashMap<>();
        int copiedFiles = 0;
        long copiedBytes = 0;
        int reusedFiles = 0;
        for (Path source : listIncludedFiles()) {
            String relative = pathPolicy.normalize(sourceRoot.relativize(source).toString());
            CachedFile cached = sourceCache.get(relative);
            BasicFileAttributes observed = Files.readAttributes(source, BasicFileAttributes.class);
            boolean blobPresent = cached != null && Files.isRegularFile(blobPath(cached.entry().sha256()))
                    && Files.size(blobPath(cached.entry().sha256())) == cached.entry().size();
            boolean affected = verifyAll || changedPaths.stream().anyMatch(path ->
                    relative.equalsIgnoreCase(path)
                            || relative.regionMatches(true, 0, path + "/", 0, path.length() + 1));
            CachedFile next;
            if (blobPresent && !affected && sameSourceVersion(cached.attributes(), observed)) {
                next = cached;
                reusedFiles++;
            } else if (blobPresent && matchesCachedContent(source, observed, cached.entry())) {
                // 重复通知/mtime 改变不代表内容改变：哈希相同就复用，不重复写完整 JAR。
                next = new CachedFile(observed, cached.entry());
                reusedFiles++;
            } else {
                next = materializeStableBlob(source);
                copiedFiles++;
                copiedBytes += next.entry().size();
            }
            nextCache.put(relative, next);
            entries.put(relative, next.entry());
        }
        EnvironmentSnapshot snapshot = new EnvironmentSnapshot(
                new EnvironmentManifest(entries), System.currentTimeMillis());
        if (lastBuilt != null && lastBuilt.snapshotId().equals(snapshot.snapshotId())
                && Files.isRegularFile(manifestsDir.resolve(snapshot.snapshotId() + ".json"))) {
            snapshot = lastBuilt;
            snapshot.touch();
        } else {
            persistSnapshot(snapshot);
        }
        sourceCache = nextCache;
        lastBuilt = snapshot;
        lastBuildStats = new BuildStats(copiedFiles, copiedBytes, reusedFiles);
        return snapshot;
    }

    private boolean matchesCachedContent(Path source, BasicFileAttributes before,
                                         EnvironmentManifest.Entry entry) throws IOException {
        if (before.size() != entry.size()) return false;
        String hash = Sha256.hexOfFile(source);
        BasicFileAttributes after = Files.readAttributes(source, BasicFileAttributes.class);
        return sameSourceVersion(before, after) && entry.sha256().equals(hash);
    }

    /**
     * 从磁盘恢复全部完整快照。损坏的 manifest 或缺失 Blob 只会使对应版本被跳过，
     * 不影响其他版本和服务启动；管理器随后会基于实时源目录生成新版本。
     */
    public Map<String, EnvironmentSnapshot> loadSnapshots() throws IOException {
        Files.createDirectories(blobsDir);
        Files.createDirectories(manifestsDir);
        Map<String, EnvironmentSnapshot> loaded = new TreeMap<>();
        int invalid = 0;
        int quarantined = 0;
        try (Stream<Path> files = Files.list(manifestsDir)) {
            for (Path manifestFile : files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .toList()) {
                try {
                    EnvironmentSnapshot snapshot = readSnapshot(manifestFile);
                    // 协议升级或管理员新增排除项后，旧清单不能绕过当前路径规则继续分发。
                    // 不能简单删掉条目：那会改变snapshotId，因此整份旧版本必须失效。
                    for (String path : snapshot.manifest().files().keySet()) {
                        if (!pathPolicy.includes(path)) throw new IOException("旧环境快照包含当前已排除的路径: " + path);
                    }
                    validateSnapshotBlobs(snapshot);
                    loaded.put(snapshot.snapshotId(), snapshot);
                } catch (Exception e) {
                    invalid++;
                    // 只移动无效清单并保留原文，绝不删除世界或 WAL；下次启动不再逐份解析。
                    try {
                        Path quarantine = repositoryRoot.resolve("quarantine");
                        Files.createDirectories(quarantine);
                        Files.move(manifestFile, quarantine.resolve(
                                manifestFile.getFileName() + "." + UUID.randomUUID()));
                        quarantined++;
                    } catch (IOException moveFailure) {
                        LOGGER.warn("无法隔离无效环境清单 {}: {}", manifestFile, moveFailure.toString());
                    }
                    LOGGER.debug("无效环境快照: {} ({})", manifestFile, e.toString());
                }
            }
        }
        if (invalid > 0) LOGGER.warn("已跳过 {} 份无效环境快照清单，其中 {} 份已隔离保留", invalid, quarantined);
        return loaded;
    }

    /** 读取快照中的完整文件内容；测试和少量元数据读取使用，网络分块后续走 readChunk。 */
    public byte[] readAllBytes(EnvironmentSnapshot snapshot, String relativePath)
            throws IOException {
        EnvironmentManifest.Entry entry = entry(snapshot, relativePath);
        Path blob = blobPath(entry.sha256());
        byte[] bytes = Files.readAllBytes(blob);
        if (bytes.length != entry.size()) {
            throw new IOException("快照 Blob 大小与清单不一致: " + relativePath);
        }
        snapshot.touch();
        return bytes;
    }

    /**
     * 从不可变 Blob 的指定偏移读取一块数据。返回长度可能小于 maxBytes（文件尾部），
     * offset 等于文件大小时返回空数组；任何读取都不会访问实时源目录。
     */
    public byte[] readChunk(EnvironmentSnapshot snapshot, String relativePath,
                            long offset, int maxBytes) throws IOException {
        EnvironmentManifest.Entry entry = entry(snapshot, relativePath);
        if (offset < 0 || offset > entry.size()) {
            throw new IOException("offset 超出快照文件范围: " + offset);
        }
        if (maxBytes <= 0) {
            throw new IOException("maxBytes 必须大于 0");
        }
        int length = (int) Math.min(maxBytes, entry.size() - offset);
        if (length == 0) {
            snapshot.touch();
            return new byte[0];
        }

        ByteBuffer buffer = ByteBuffer.allocate(length);
        try (SeekableByteChannel channel = Files.newByteChannel(blobPath(entry.sha256()))) {
            channel.position(offset);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) {
                    break;
                }
            }
        }
        snapshot.touch();
        return buffer.position() == length
                ? buffer.array()
                : Arrays.copyOf(buffer.array(), buffer.position());
    }

    /** 获取快照清单条目；路径不属于该快照时抛出明确异常。 */
    public EnvironmentManifest.Entry entry(EnvironmentSnapshot snapshot,
                                           String relativePath) throws IOException {
        return requireEntry(snapshot, relativePath);
    }

    public Path blobPath(String sha256) {
        return blobsDir.resolve(sha256);
    }

    /**
     * 计算源目录轻量指纹，用于 60 秒兜底校验 WatchService 是否漏报。这里只读取
     * 路径、大小和修改时间，不重新哈希文件内容，因此可高频执行而不会持续占满磁盘。
     */
    public String sourceFingerprint() throws IOException {
        StringBuilder fingerprint = new StringBuilder();
        for (Path source : listIncludedFiles()) {
            BasicFileAttributes attributes = Files.readAttributes(
                    source, BasicFileAttributes.class);
            String relative = pathPolicy.normalize(sourceRoot.relativize(source).toString());
            fingerprint.append(relative).append('\0')
                    .append(attributes.size()).append('\0')
                    .append(attributes.lastModifiedTime().toMillis()).append('\n');
        }
        return Sha256.hex(fingerprint.toString());
    }

    /** 删除一个已回收快照的持久化清单；Blob 由统一引用扫描单独处理。 */
    public void deleteSnapshotManifest(String snapshotId) throws IOException {
        if (snapshotId == null || !snapshotId.matches("[0-9a-f]{64}")) {
            throw new IOException("非法 snapshotId: " + snapshotId);
        }
        Files.deleteIfExists(manifestsDir.resolve(snapshotId + ".json"));
    }

    /**
     * 删除所有未被保留快照引用的 Blob。调用方必须保证没有快照构建与本方法并发，
     * 否则刚写入但尚未发布的 Blob 可能被误判为孤儿。
     */
    public void deleteUnreferencedBlobs(Collection<EnvironmentSnapshot> retained)
            throws IOException {
        Set<String> referenced = new HashSet<>();
        for (EnvironmentSnapshot snapshot : retained) {
            for (EnvironmentManifest.Entry entry : snapshot.manifest().files().values()) {
                referenced.add(entry.sha256());
            }
        }
        if (!Files.isDirectory(blobsDir)) {
            return;
        }
        try (Stream<Path> blobs = Files.list(blobsDir)) {
            for (Path blob : blobs.filter(Files::isRegularFile).toList()) {
                if (!referenced.contains(blob.getFileName().toString())) {
                    Files.deleteIfExists(blob);
                }
            }
        }
    }

    /** 清除构建异常遗留且早于 cutoffMillis 的 staging 临时文件。 */
    public void cleanupStaging(long cutoffMillis) throws IOException {
        if (!Files.isDirectory(stagingDir)) {
            return;
        }
        try (Stream<Path> staging = Files.list(stagingDir)) {
            for (Path path : staging.toList()) {
                if (Files.getLastModifiedTime(path).toMillis() < cutoffMillis) {
                    deleteTree(path);
                }
            }
        }
    }

    private void persistSnapshot(EnvironmentSnapshot snapshot) throws IOException {
        JsonObject root = new JsonObject();
        root.addProperty("snapshotId", snapshot.snapshotId());
        root.addProperty("createdAtMillis", snapshot.createdAtMillis());
        root.addProperty("lastAccessMillis", snapshot.lastAccessMillis());
        root.add("manifest", snapshot.manifest().toJson());
        JsonUtil.writeFileAtomic(
                manifestsDir.resolve(snapshot.snapshotId() + ".json"), root);
    }

    private EnvironmentSnapshot readSnapshot(Path file) throws IOException {
        JsonObject root = JsonUtil.parseObject(Files.readString(file, StandardCharsets.UTF_8));
        String snapshotId = JsonUtil.getString(root, "snapshotId", "");
        long createdAt = JsonUtil.getLong(root, "createdAtMillis", 0L);
        long lastAccess = JsonUtil.getLong(root, "lastAccessMillis", createdAt);
        JsonElement manifestElement = root.get("manifest");
        if (manifestElement == null || !manifestElement.isJsonObject()) {
            throw new IOException("缺少 manifest 对象");
        }
        EnvironmentManifest manifest = EnvironmentManifest.fromJson(
                manifestElement.getAsJsonObject());
        return new EnvironmentSnapshot(snapshotId, manifest, createdAt, lastAccess);
    }

    private void validateSnapshotBlobs(EnvironmentSnapshot snapshot) throws IOException {
        for (Map.Entry<String, EnvironmentManifest.Entry> file
                : snapshot.manifest().files().entrySet()) {
            Path blob = blobPath(file.getValue().sha256());
            if (!Files.isRegularFile(blob) || Files.size(blob) != file.getValue().size()) {
                throw new IOException("快照引用的 Blob 缺失或大小不符: " + file.getKey());
            }
        }
    }

    private EnvironmentManifest.Entry requireEntry(EnvironmentSnapshot snapshot,
                                                     String relativePath) throws IOException {
        if (snapshot == null) {
            throw new IOException("快照不存在");
        }
        String normalized = pathPolicy.normalize(relativePath);
        if (!pathPolicy.includes(normalized)) throw new IOException("路径已不属于环境同步范围: " + normalized);
        EnvironmentManifest.Entry entry = snapshot.manifest().files().get(normalized);
        if (entry == null) {
            throw new IOException("路径不在快照清单中: " + normalized);
        }
        return entry;
    }

    /** 按相对路径排序，保证同一批文件生成清单时具有确定顺序。 */
    private List<Path> listIncludedFiles() throws IOException {
        if (!Files.isDirectory(sourceRoot)) {
            return List.of();
        }
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(sourceRoot, new SimpleFileVisitor<>() {
            @Override public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes) {
                String relative = pathPolicy.normalize(sourceRoot.relativize(directory).toString());
                return relative.isEmpty() || pathPolicy.includes(relative)
                        ? FileVisitResult.CONTINUE : FileVisitResult.SKIP_SUBTREE;
            }

            @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attributes) {
                String relative = pathPolicy.normalize(sourceRoot.relativize(path).toString());
                if (Files.isRegularFile(path) && pathPolicy.includes(relative)) files.add(path);
                return FileVisitResult.CONTINUE;
            }
        });
        files.sort(Comparator.comparing(
                path -> pathPolicy.normalize(sourceRoot.relativize(path).toString())));
        return files;
    }

    /**
     * 把一个源文件复制为稳定 Blob。复制前后同时比较大小、修改时间和 fileKey：
     * Vanilla 常用临时文件原子替换 level.dat，fileKey 能识别“路径相同但文件已换代”；
     * 普通原地写则由大小/修改时间识别。变化时丢弃本次结果并重试。
     */
    private CachedFile materializeStableBlob(Path source) throws IOException {
        IOException lastFailure = null;
        for (int attempt = 1; attempt <= MAX_STABLE_COPY_ATTEMPTS; attempt++) {
            Path staged = stagingDir.resolve(UUID.randomUUID() + ".blob.tmp");
            try {
                BasicFileAttributes before = Files.readAttributes(
                        source, BasicFileAttributes.class);
                CopyResult copied = copyAndHash(source, staged);
                BasicFileAttributes after = Files.readAttributes(
                        source, BasicFileAttributes.class);

                if (!sameSourceVersion(before, after) || copied.size() != after.size()) {
                    lastFailure = new IOException("复制期间源文件发生变化: " + source);
                    continue;
                }

                Path blob = blobPath(copied.sha256());
                publishBlob(staged, blob);
                return new CachedFile(after, new EnvironmentManifest.Entry(copied.sha256(), copied.size()));
            } catch (IOException e) {
                lastFailure = e;
            } finally {
                Files.deleteIfExists(staged);
            }
        }
        throw new IOException("源文件连续变化，无法生成稳定快照: " + source,
                lastFailure);
    }

    private static CopyResult copyAndHash(Path source, Path target) throws IOException {
        MessageDigest digest = Sha256.newDigest();
        long total = 0;
        try (InputStream in = Files.newInputStream(source);
             OutputStream out = Files.newOutputStream(target,
                     StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            byte[] buffer = new byte[COPY_BUFFER_BYTES];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
                digest.update(buffer, 0, read);
                total += read;
            }
        }
        return new CopyResult(Sha256.toHex(digest.digest()), total);
    }

    private static boolean sameSourceVersion(BasicFileAttributes before,
                                             BasicFileAttributes after) {
        return before.size() == after.size()
                && before.lastModifiedTime().equals(after.lastModifiedTime())
                && Objects.equals(before.fileKey(), after.fileKey());
    }

    /** 原子发布 Blob；相同哈希已存在表示内容可复用，不重复占用磁盘。 */
    private static void publishBlob(Path staged, Path blob) throws IOException {
        if (Files.isRegularFile(blob)) {
            return;
        }
        Files.createDirectories(blob.getParent());
        try {
            Files.move(staged, blob, StandardCopyOption.ATOMIC_MOVE);
        } catch (FileAlreadyExistsException ignored) {
            // 并发构建已发布同一哈希，直接复用。
        } catch (AtomicMoveNotSupportedException e) {
            try {
                Files.move(staged, blob);
            } catch (FileAlreadyExistsException ignored) {
                // 与上面相同：另一个构建先完成。
            }
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private record CopyResult(String sha256, long size) {
    }

    private record CachedFile(BasicFileAttributes attributes, EnvironmentManifest.Entry entry) { }
}
