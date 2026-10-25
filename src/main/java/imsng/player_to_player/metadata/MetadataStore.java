package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * 中心服的世界元数据单写者存储。
 *
 * <p>只有完整补丁追加 WAL 并 force 后才能公布新版本和返回成功。检查点保存已提交
 * 快照及当前任期，启动先读取检查点，再补齐 WAL；不能拿尚未保存的 level.dat 覆盖
 * 已确认状态。每次打开都会先耐久递增任期，旧进程或旧连接不能继续提交。</p>
 *
 * <p>WAL 也承担操作回执账本，检查点不会删除历史回执。内存仅保留回执的文件位置和
 * 有界补丁窗口；很旧的重复请求按位置读取原补丁。只有不完整的物理尾记录可以截断，
 * 完整记录校验失败一律拒绝启动，避免把磁盘损坏误认为尚未提交的操作。</p>
 *
 * <p>不能把“临时文件 force + rename”误认为跨平台的目录耐久屏障。新任期直接在 WAL
 * 写入完整基准锚点；自然计时之后的首条离散提交把基准锚点与补丁一起 force。即使断电
 * 使检查点目录项回退，WAL 仍有完整的基础版本，已确认操作不会因缺少基础快照而丢失。</p>
 */
public final class MetadataStore implements AutoCloseable {
    private static final int MAGIC = 0x50324D39; // P2M9
    private static final int HEADER_BYTES = Integer.BYTES * 2;
    private static final int TRAILER_BYTES = Integer.BYTES;
    private static final int HISTORY_LIMIT = 256;

    private record Receipt(long offset, int bytes) { }

    private final Path directory;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final FileChannel wal;
    private final Map<String, Receipt> receipts = new HashMap<>();
    private final ArrayDeque<MetadataPatch> history = new ArrayDeque<>();
    private MetadataSnapshot current;
    /** 检查点确认时 WAL 的物理边界；该边界之前的回执绝不允许按断尾丢弃。 */
    private long checkpointWalBytes;
    /** WAL 能独立重建到的版本/任期；自然检查点不会推进这两个值。 */
    private long walVersion = -1;
    private long walTerm;
    private boolean closed;
    private boolean failed;

    private MetadataStore(Path directory, FileChannel lockChannel, FileLock lock,
                          FileChannel wal, MetadataSnapshot current) {
        this.directory = directory;
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.wal = wal;
        this.current = current;
    }

    /** 首次升级导入原版 Data；已有存储时 imported 完全不参与恢复。 */
    public static MetadataStore open(Path directory, String worldId, CompoundTag imported) throws IOException {
        Objects.requireNonNull(directory, "directory");
        // 提前校验参数，禁止创建了目录以后才发现身份无效。
        MetadataSnapshot initial = new MetadataSnapshot(worldId, 0, 1, imported);
        Path root = directory.toAbsolutePath().normalize();
        Files.createDirectories(root);
        FileChannel lockChannel = FileChannel.open(root.resolve("metadata.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock = null;
        FileChannel wal = null;
        try {
            try {
                lock = lockChannel.tryLock();
            } catch (OverlappingFileLockException exception) {
                throw new IOException("世界元数据已有活动写入者", exception);
            }
            if (lock == null) {
                throw new IOException("世界元数据已有活动写入者");
            }
            Path checkpoint = root.resolve("metadata.checkpoint");
            boolean existing = Files.exists(checkpoint);
            long checkpointWalBytes = 0;
            if (existing) {
                if (Files.size(checkpoint) > MetadataWire.MAX_COMPRESSED_BYTES) {
                    throw new IOException("元数据检查点超过上限");
                }
                CompoundTag stored = MetadataWire.decode(Files.readAllBytes(checkpoint));
                initial = MetadataWire.snapshotFromTag(stored);
                if (!stored.contains("walBytes", net.minecraft.nbt.Tag.TAG_LONG)
                        || stored.getLong("walBytes") < 0) {
                    throw new IOException("元数据检查点缺少合法 WAL 高水位");
                }
                checkpointWalBytes = stored.getLong("walBytes");
                if (!worldId.equals(initial.worldId())) {
                    throw new IOException("元数据检查点属于不同世界，必须隔离旧缓存");
                }
            }
            wal = FileChannel.open(root.resolve("metadata.wal"), StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            boolean hasJournal = wal.size() != 0;
            MetadataStore store = new MetadataStore(root, lockChannel, lock, wal, initial);
            store.checkpointWalBytes = checkpointWalBytes;
            store.recover(existing);
            long nextTerm;
            try {
                nextTerm = existing || hasJournal ? Math.addExact(store.current.term(), 1) : 1;
            } catch (ArithmeticException exception) {
                throw new IOException("元数据任期已耗尽", exception);
            }
            MetadataSnapshot next = new MetadataSnapshot(worldId, store.current.version(), nextTerm, store.current.data());
            // 任期先在固定 WAL 文件中 force，不能依赖检查点 rename 的目录项已经落盘。
            store.appendAnchor(next);
            store.current = next;
            store.checkpoint();
            return store;
        } catch (Throwable failure) {
            if (wal != null) {
                try { wal.close(); } catch (IOException suppressed) { failure.addSuppressed(suppressed); }
            }
            if (lock != null) {
                try { lock.close(); } catch (IOException suppressed) { failure.addSuppressed(suppressed); }
            }
            try { lockChannel.close(); } catch (IOException suppressed) { failure.addSuppressed(suppressed); }
            throw failure;
        }
    }

    public synchronized MetadataSnapshot snapshot() {
        requireOpen();
        return current;
    }

    /**
     * 在执行非幂等命令之前查询耐久结果，避免已确认的 /time add 等动作再次运行。
     * 调用者仍须验证当前连接的世界身份、任期，以及回执中的原请求内容。
     */
    public synchronized Optional<MetadataPatch> lookupOperation(String operationId) throws IOException {
        requireOpen();
        Receipt receipt = receipts.get(Objects.requireNonNull(operationId, "operationId"));
        return receipt == null ? Optional.empty()
                : Optional.of(readPatch(receipt.offset(), receipt.bytes()));
    }

    /**
     * 原子比较基础版本、验证任期并提交。重复 ID 可以跨重启重试，但重试者仍须先取得
     * 当前任期；相同 ID 若承载不同操作则拒绝，不能把客户端错误悄悄当作成功。
     */
    public synchronized MetadataPatch commit(String worldId, long baseVersion, long term,
                                             String operationId, CompoundTag set,
                                             Set<String> removed) throws IOException {
        requireOpen();
        if (!current.worldId().equals(worldId)) {
            throw new IllegalArgumentException("元数据世界身份不匹配");
        }
        if (term != current.term()) {
            throw new IllegalStateException("元数据写入任期已过期，必须重新获取快照");
        }
        MetadataPatch patch = new MetadataPatch(worldId, baseVersion,
                Math.addExact(baseVersion, 1), term, operationId, set, removed);
        Receipt receipt = receipts.get(operationId);
        if (receipt != null) {
            MetadataPatch previous = readPatch(receipt.offset(), receipt.bytes());
            if (previous.baseVersion() != baseVersion || !previous.set().equals(patch.set())
                    || !previous.removed().equals(patch.removed())) {
                throw new IllegalArgumentException("元数据操作 ID 已用于不同载荷");
            }
            return previous;
        }
        if (baseVersion != current.version()) {
            throw new IllegalStateException("元数据基础版本冲突，必须先补齐状态");
        }
        MetadataSnapshot next = patch.applyTo(current);
        // 整个结果必须可作为快照发送；不能提交许多合法小补丁，最终构造出无法恢复的世界。
        CompoundTag checkpointCandidate = MetadataWire.snapshotToTag(next);
        checkpointCandidate.putLong("walBytes", 0L);
        checkpointCandidate.putString("recordType", "checkpoint");
        MetadataWire.encode(checkpointCandidate);
        CompoundTag record = MetadataWire.patchToTag(patch);
        record.putString("recordType", "patch");
        byte[] bytes = MetadataWire.encode(record);
        byte[] anchor = walVersion != current.version() || walTerm != current.term()
                ? encodeAnchor(current) : null;
        // anchor 与 patch 属于同一次耐久提交。完整 anchor + 不完整 patch 可以恢复基准，
        // 但不会发布该 patch 的操作回执；两者 force 成功以后才允许 ACK。
        long offset = appendDurable(anchor == null ? List.of(bytes) : List.of(anchor, bytes));
        if (anchor != null) offset += HEADER_BYTES + anchor.length + TRAILER_BYTES;
        current = next;
        walVersion = next.version();
        walTerm = next.term();
        receipts.put(operationId, new Receipt(offset + HEADER_BYTES, bytes.length));
        remember(patch);
        return patch;
    }

    /**
     * 每秒自然计时检查点：推进版本并发布补丁，但不为无外部重试者的计时事件永久登记
     * 操作回执。完整新状态耐久替换成功后才公布；崩溃后使用新快照补齐此类历史缺口。
     */
    public synchronized MetadataPatch checkpointChanges(CompoundTag set, Set<String> removed) throws IOException {
        requireOpen();
        MetadataPatch patch = new MetadataPatch(current.worldId(), current.version(),
                Math.addExact(current.version(), 1), current.term(),
                "checkpoint:" + UUID.randomUUID(), set, removed);
        MetadataSnapshot next = patch.applyTo(current);
        writeCheckpoint(next);
        current = next;
        remember(patch);
        return patch;
    }

    /** 合并、分离或写入者集合改变时建立新的耐久任期屏障；客户端必须重新确认快照。 */
    public synchronized MetadataSnapshot advanceTerm() throws IOException {
        requireOpen();
        MetadataSnapshot next = new MetadataSnapshot(current.worldId(), current.version(),
                Math.addExact(current.term(), 1), current.data());
        appendAnchor(next);
        writeCheckpoint(next);
        current = next;
        history.clear();
        return current;
    }

    /** 返回连续补丁窗口；窗口已淘汰、版本超前或任期切换时调用者获取完整快照。 */
    public synchronized Optional<List<MetadataPatch>> patchesAfter(long version) {
        requireOpen();
        if (version < 0 || version > current.version()) {
            return Optional.empty();
        }
        if (version == current.version()) {
            return Optional.of(List.of());
        }
        List<MetadataPatch> result = new ArrayList<>();
        long expected = version;
        for (MetadataPatch patch : history) {
            if (patch.version() <= version) {
                continue;
            }
            if (patch.baseVersion() != expected || patch.term() != current.term()) {
                return Optional.empty();
            }
            result.add(patch);
            expected = patch.version();
        }
        return expected == current.version() ? Optional.of(List.copyOf(result)) : Optional.empty();
    }

    /** 原子替换完整检查点；WAL 回执保留，重复操作不能因一次普通保存而失去去重能力。 */
    public synchronized void checkpoint() throws IOException {
        requireOpen();
        writeCheckpoint(current);
    }

    private void writeCheckpoint(MetadataSnapshot snapshot) throws IOException {
        long highWater = wal.size();
        CompoundTag stored = MetadataWire.snapshotToTag(snapshot);
        stored.putLong("walBytes", highWater);
        // 此头比 WAL anchor 略大；检查点能编码即保证同一状态之后也能形成恢复锚点。
        stored.putString("recordType", "checkpoint");
        byte[] bytes = MetadataWire.encode(stored);
        Path temporary = directory.resolve("metadata.checkpoint.tmp");
        try {
            try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    output.write(buffer);
                }
                output.force(true);
            }
            // 不退化为先删后拷贝；不支持原子替换的文件系统必须明确失败。
            Files.move(temporary, directory.resolve("metadata.checkpoint"),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            checkpointWalBytes = highWater;
        } catch (IOException exception) {
            // 文件替换结果不明确时也不能继续用旧内存版本接受写入。
            failed = true;
            throw exception;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        IOException failure = null;
        try {
            if (!failed) {
                checkpoint();
            }
        } catch (IOException exception) {
            failure = exception;
        } finally {
            closed = true;
            try { wal.close(); } catch (IOException exception) { failure = combine(failure, exception); }
            try { lock.close(); } catch (IOException exception) { failure = combine(failure, exception); }
            try { lockChannel.close(); } catch (IOException exception) { failure = combine(failure, exception); }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void recover(boolean hasCheckpoint) throws IOException {
        long offset = 0;
        long expectedVersion = 0;
        long previousTerm = 0;
        long size = wal.size();
        MetadataSnapshot journalState = null;
        boolean hasBaseline = hasCheckpoint;
        if (size < checkpointWalBytes) {
            throw new IOException("元数据 WAL 小于检查点确认的高水位，耐久回执已丢失");
        }
        while (offset < size) {
            if (size - offset < HEADER_BYTES) {
                truncateTail(offset);
                break;
            }
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
            readFully(header, offset);
            header.flip();
            int magic = header.getInt();
            int length = header.getInt();
            if (magic != MAGIC || length <= 0 || length > MetadataWire.MAX_COMPRESSED_BYTES) {
                throw new IOException("元数据 WAL 记录头损坏，偏移 " + offset);
            }
            long end = offset + HEADER_BYTES + length + TRAILER_BYTES;
            if (offset < checkpointWalBytes && end > checkpointWalBytes) {
                throw new IOException("元数据检查点 WAL 高水位不在完整记录边界");
            }
            if (end > size) {
                truncateTail(offset);
                break;
            }
            byte[] bytes = new byte[length];
            readFully(ByteBuffer.wrap(bytes), offset + HEADER_BYTES);
            ByteBuffer trailer = ByteBuffer.allocate(TRAILER_BYTES);
            readFully(trailer, end - TRAILER_BYTES);
            trailer.flip();
            if (trailer.getInt() != checksum(bytes)) {
                throw new IOException("元数据 WAL 校验失败，偏移 " + offset);
            }
            CompoundTag record = MetadataWire.decode(bytes);
            String kind = record.getString("recordType");
            if (kind.equals("anchor")) {
                MetadataSnapshot anchor = MetadataWire.snapshotFromTag(record);
                if (!anchor.worldId().equals(current.worldId()) || anchor.version() < expectedVersion
                        || anchor.term() < previousTerm) {
                    throw new IOException("元数据 WAL 基准锚点的身份、版本或任期非法");
                }
                if (journalState != null && journalState.version() == anchor.version()
                        && !journalState.data().equals(anchor.data())) {
                    throw new IOException("同版本 WAL 基准锚点携带不同状态");
                }
                journalState = anchor;
                if (!hasBaseline) { current = anchor; hasBaseline = true; }
                else adoptRecoveredState(anchor);
                expectedVersion = anchor.version();
                previousTerm = anchor.term();
            } else if (kind.equals("patch") || kind.isEmpty()) {
                MetadataPatch patch = MetadataWire.patchFromTag(record);
                if (!patch.worldId().equals(current.worldId()) || patch.baseVersion() < expectedVersion
                        || patch.term() < previousTerm || receipts.containsKey(patch.operationId())) {
                    throw new IOException("元数据 WAL 的身份、顺序、任期或操作 ID 不一致");
                }
                if (journalState != null) {
                    try { journalState = patch.applyTo(journalState); }
                    catch (IllegalStateException invalid) { throw new IOException("元数据 WAL 缺少补丁对应的基准锚点", invalid); }
                    adoptRecoveredState(journalState);
                } else {
                    // 仅兼容开发期尚无 recordType 的旧记录。缺少检查点时，绝不靠 imported
                    // 重新猜测已确认操作的基准；必须有自包含的 anchor 才能恢复。
                    if (!hasBaseline || !kind.isEmpty() || patch.term() > current.term()) {
                        throw new IOException("元数据 WAL 缺少自包含的基准锚点");
                    }
                    if (patch.version() > current.version()) {
                        if (patch.baseVersion() != current.version()) throw new IOException("旧元数据 WAL 与检查点存在版本缺口");
                        CompoundTag data = current.data();
                        patch.removed().forEach(data::remove);
                        CompoundTag values = patch.set();
                        values.getAllKeys().forEach(key -> data.put(key, Objects.requireNonNull(values.get(key)).copy()));
                        current = new MetadataSnapshot(current.worldId(), patch.version(), current.term(), data);
                    }
                }
                receipts.put(patch.operationId(), new Receipt(offset + HEADER_BYTES, length));
                remember(patch);
                expectedVersion = patch.version();
                previousTerm = patch.term();
            } else {
                throw new IOException("未知元数据 WAL 记录类型: " + kind);
            }
            walVersion = expectedVersion;
            walTerm = previousTerm;
            offset = end;
        }
        if (!hasBaseline && size != 0) throw new IOException("元数据 WAL 不含可恢复的完整基准锚点");
        wal.position(wal.size());
    }

    /** WAL 与检查点可分别较新，但版本和任期必须同向；交叉倒序表示损坏。 */
    private void adoptRecoveredState(MetadataSnapshot recovered) throws IOException {
        if ((recovered.version() > current.version() && recovered.term() < current.term())
                || (recovered.version() < current.version() && recovered.term() > current.term())) {
            throw new IOException("元数据 WAL 与检查点的版本、任期交叉倒序");
        }
        if (recovered.version() == current.version() && !recovered.data().equals(current.data())) {
            throw new IOException("元数据 WAL 与检查点的同版本状态不一致");
        }
        if (recovered.version() > current.version()
                || (recovered.version() == current.version() && recovered.term() > current.term())) current = recovered;
    }

    private static byte[] encodeAnchor(MetadataSnapshot snapshot) throws IOException {
        CompoundTag anchor = MetadataWire.snapshotToTag(snapshot);
        anchor.putString("recordType", "anchor");
        return MetadataWire.encode(anchor);
    }

    private void appendAnchor(MetadataSnapshot snapshot) throws IOException {
        appendDurable(List.of(encodeAnchor(snapshot)));
        walVersion = snapshot.version();
        walTerm = snapshot.term();
    }

    /** 固定 WAL 上多物理记录共用一次 force；任何不明确的 I/O 结果都会封停当前写入者。 */
    private long appendDurable(List<byte[]> records) throws IOException {
        long offset = wal.size();
        try {
            wal.position(offset);
            for (byte[] bytes : records) {
                ByteBuffer frame = ByteBuffer.allocate(HEADER_BYTES + bytes.length + TRAILER_BYTES);
                frame.putInt(MAGIC).putInt(bytes.length).put(bytes).putInt(checksum(bytes)).flip();
                while (frame.hasRemaining()) wal.write(frame);
            }
            wal.force(true);
            return offset;
        } catch (IOException exception) {
            failed = true;
            throw exception;
        }
    }

    private MetadataPatch readPatch(long offset, int length) throws IOException {
        byte[] bytes = new byte[length];
        readFully(ByteBuffer.wrap(bytes), offset);
        // 重试回执也校验原记录 CRC，磁盘后续损坏不能伪装成首次成功结果。
        ByteBuffer trailer = ByteBuffer.allocate(TRAILER_BYTES);
        readFully(trailer, offset + length);
        trailer.flip();
        if (trailer.getInt() != checksum(bytes)) {
            throw new IOException("元数据操作回执校验失败");
        }
        return MetadataWire.patchFromTag(MetadataWire.decode(bytes));
    }

    private void readFully(ByteBuffer target, long position) throws IOException {
        while (target.hasRemaining()) {
            int read = wal.read(target, position);
            if (read < 0) {
                throw new IOException("元数据 WAL 意外结束");
            }
            position += read;
        }
    }

    private void truncateTail(long length) throws IOException {
        if (length < checkpointWalBytes) {
            throw new IOException("元数据 WAL 断尾影响已确认回执，禁止截断");
        }
        wal.truncate(length);
        wal.force(true);
    }

    private void remember(MetadataPatch patch) {
        history.addLast(patch);
        while (history.size() > HISTORY_LIMIT) {
            history.removeFirst();
        }
    }

    private void requireOpen() {
        if (closed || failed) {
            throw new IllegalStateException(closed ? "元数据存储已经关闭" : "元数据耐久写入失败，必须停止并恢复");
        }
    }

    private static int checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    private static IOException combine(IOException first, IOException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }
}
