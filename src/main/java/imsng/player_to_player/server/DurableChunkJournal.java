package imsng.player_to_player.server;

import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * 区块耐久检查点日志（WAL）。
 *
 * <p>网络 ACK 只在一整个批次完成一次 {@link FileChannel#force(boolean)} 后返回。
 * region/MCA 文件是异步物化视图，崩溃恢复和读取可以先从本日志索引取到最新快照，
 * 因而退出时不需要再为每个区块单独做磁盘同步。</p>
 *
 * <p>日志记录采用长度前缀 + CRC32。进程崩溃造成的半条尾记录会被安全截断，之前已经
 * force 的完整前缀仍然可恢复。所有入站批次在写入前都经过 {@code ChunkDeltaBatch.decode}
 * 和 SHA-256 校验，重复 batch 使用原来的 epoch 幂等应答。</p>
 */
public final class DurableChunkJournal implements AutoCloseable {

    private static final int MAGIC = 0x50545057; // "PTPW"
    private static final int FORMAT_VERSION = 1;
    private static final int TYPED_FORMAT_VERSION = 2;
    private static final int RECORD_BATCH = 1;
    /** v5 初版的单记录退出提交，恢复时继续兼容读取。 */
    private static final int RECORD_EXIT_COMMIT = 2;
    /** 分段退出事务的数据片；只有后续 marker 完整时才会发布。 */
    private static final int RECORD_EXIT_PART = 3;
    /** 分段退出事务的原子终结标记。 */
    private static final int RECORD_EXIT_MARKER = 4;
    private static final int EXIT_FRAGMENT_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int TYPED_RECORD_HEADER_BYTES =
            Integer.BYTES + 1 + 1 + 16 + Long.BYTES + Integer.BYTES + DIGEST_BYTES;
    private static final int EXIT_PART_HEADER_BYTES =
            1 + 16 + Integer.BYTES + Integer.BYTES;
    private static final int MAX_RECORD_BYTES = 128 * 1024 * 1024;
    private static final int DEFAULT_EXIT_PART_BYTES = 4 * 1024 * 1024;
    private static final String FILE_NAME = "chunks.wal";

    /** 一次 append 的结果；duplicate=true 表示已在日志中的同一批次重放。 */
    public record AppendResult(long durableEpoch, boolean duplicate, int deltaCount,
                               long bytes) {
    }

    /** 日志索引中的最新区块快照。payload 为压缩 NBT。 */
    public record Entry(UUID ownerId, ChunkKey key, long version, long durableEpoch,
                        byte[] payload, String sha256) {
        public Entry {
            Objects.requireNonNull(ownerId, "ownerId");
            Objects.requireNonNull(key, "key");
            if (payload == null || payload.length == 0) {
                throw new IllegalArgumentException("日志区块载荷为空");
            }
            payload = payload.clone();
            String expected = Sha256.hex(payload);
            if (sha256 == null || !expected.equalsIgnoreCase(sha256)) {
                throw new IllegalArgumentException("日志区块摘要不匹配");
            }
            sha256 = expected;
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    private record BatchKey(UUID ownerId, UUID batchId, int sequence) {
    }

    private record SeenBatch(long epoch, String sha256, int deltaCount, long bytes) {
    }

    /** 已确认退出记录及其全局耐久 epoch。 */
    public record ExitCommitEntry(long durableEpoch, ExitCommitPayload payload,
                                  String sha256) {
        public ExitCommitEntry {
            if (durableEpoch <= 0) {
                throw new IllegalArgumentException("退出耐久 epoch 非法");
            }
            Objects.requireNonNull(payload, "payload");
            Objects.requireNonNull(sha256, "sha256");
        }
    }

    private record ExitPayloadDescriptor(long bytes, String sha256) {
        private ExitPayloadDescriptor {
            if (bytes <= 0 || bytes > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "退出复合载荷超过当前 JVM 可恢复上限");
            }
            Objects.requireNonNull(sha256, "sha256");
        }
    }

    private record ExitPart(UUID shutdownId, int index, int payloadLength) {
        private ExitPart {
            Objects.requireNonNull(shutdownId, "shutdownId");
            if (index < 0 || payloadLength <= 0) {
                throw new IllegalArgumentException("退出 WAL 数据片非法");
            }
        }
    }

    private record ExitMarker(UUID shutdownId, int partCount,
                              long payloadBytes, String payloadSha256) {
        private ExitMarker {
            Objects.requireNonNull(shutdownId, "shutdownId");
            if (partCount <= 0 || payloadBytes <= 0
                    || payloadBytes > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("退出 WAL 终结标记非法");
            }
            if (payloadSha256 == null
                    || payloadSha256.length() != DIGEST_BYTES * 2) {
                throw new IllegalArgumentException("退出 WAL 总摘要非法");
            }
        }
    }

    private record ExitRecoveryKey(UUID ownerId, UUID shutdownId, long epoch) {
    }

    private record ExitPartLocation(long payloadOffset, int payloadLength) {
    }

    /** 恢复扫描只保存 WAL 文件位置；看到 marker 后才分配完整逻辑载荷。 */
    private static final class ExitRecoveryParts {
        private final long startOffset;
        private final TreeMap<Integer, ExitPartLocation> parts = new TreeMap<>();

        private ExitRecoveryParts(long startOffset) {
            this.startOffset = startOffset;
        }

        private boolean add(int index, long payloadOffset, int payloadLength) {
            return parts.putIfAbsent(index,
                    new ExitPartLocation(payloadOffset, payloadLength)) == null;
        }

        private byte[] assemble(ExitMarker marker, FileChannel channel)
                throws IOException {
            if (parts.size() != marker.partCount()) {
                throw new IOException("退出 WAL 数据片数量不完整");
            }
            byte[] assembled = new byte[Math.toIntExact(marker.payloadBytes())];
            MessageDigest digest = Sha256.newDigest();
            int offset = 0;
            for (int index = 0; index < marker.partCount(); index++) {
                ExitPartLocation part = parts.get(index);
                if (part == null
                        || part.payloadLength() > assembled.length - offset) {
                    throw new IOException("退出 WAL 数据片序号或长度非法");
                }
                ByteBuffer target = ByteBuffer.wrap(
                        assembled, offset, part.payloadLength());
                readFully(channel, target, part.payloadOffset());
                digest.update(assembled, offset, part.payloadLength());
                offset += part.payloadLength();
            }
            if (offset != assembled.length
                    || !MessageDigest.isEqual(digest.digest(),
                    hexToBytes(marker.payloadSha256()))) {
                throw new IOException("退出 WAL 数据片总摘要不匹配");
            }
            return assembled;
        }
    }

    /**
     * appendExitCommit 在 journal 全局锁内连续写 part + marker，因此恢复时只可能有
     * 一个尚未终结的连续事务。遇到其他记录即证明之前的 part 从未获得 ACK，可丢弃
     * 其内存索引；若它位于文件尾，则可连同字节一起安全截断。
     */
    private static final class ExitRecoveryTracker {
        private ExitRecoveryKey activeKey;
        private ExitRecoveryParts activeParts;

        private void add(ExitRecoveryKey key, int index, long recordOffset,
                         long payloadOffset, int payloadLength)
                throws IOException {
            if (!key.equals(activeKey)) {
                activeKey = key;
                activeParts = new ExitRecoveryParts(recordOffset);
            }
            if (!activeParts.add(index, payloadOffset, payloadLength)) {
                throw new IOException("退出 WAL 数据片序号重复");
            }
        }

        private byte[] finish(ExitRecoveryKey key, ExitMarker marker,
                              FileChannel channel) throws IOException {
            if (!key.equals(activeKey) || activeParts == null) {
                throw new IOException("退出 WAL 终结标记缺少连续数据片");
            }
            byte[] result = activeParts.assemble(marker, channel);
            clear();
            return result;
        }

        private void discardBeforeNonPart() {
            clear();
        }

        private long tailStartOffset() {
            return activeParts != null ? activeParts.startOffset : -1L;
        }

        private void clear() {
            activeKey = null;
            activeParts = null;
        }
    }

    /** 可选测试/监控钩子，在每个批次 force 完成后回调。 */
    @FunctionalInterface
    public interface ForceObserver {
        void onForced(long elapsedNanos, long epoch, int deltaCount);
    }

    private final Path wal;
    private final Object lock = new Object();
    private final Map<BatchKey, SeenBatch> seenBatches = new HashMap<>();
    private final Map<UUID, ExitCommitEntry> exitCommits = new HashMap<>();
    private final Map<ChunkKey, Entry> latest = new HashMap<>();
    private final AtomicLong nextEpoch = new AtomicLong(1);
    private final ForceObserver forceObserver;
    /** 单个退出数据片的逻辑字节预算；物理记录仍受 MAX_RECORD_BYTES 保护。 */
    private final int exitPartBytes;
    private volatile boolean closed;

    public DurableChunkJournal(Path directory) {
        this(directory, (elapsedNanos, epoch, deltaCount) -> {
        }, DEFAULT_EXIT_PART_BYTES);
    }

    public DurableChunkJournal(Path directory, ForceObserver forceObserver) {
        this(directory, forceObserver, DEFAULT_EXIT_PART_BYTES);
    }

    /** 包级构造器允许测试用很小的数据片验证跨记录原子提交。 */
    DurableChunkJournal(Path directory, ForceObserver forceObserver,
                        int exitPartBytes) {
        Objects.requireNonNull(directory, "directory");
        this.wal = directory.resolve(FILE_NAME);
        this.forceObserver = Objects.requireNonNull(forceObserver, "forceObserver");
        if (exitPartBytes <= 0 || exitPartBytes > MAX_RECORD_BYTES - 1024) {
            throw new IllegalArgumentException("退出 WAL 数据片大小非法");
        }
        this.exitPartBytes = exitPartBytes;
        try {
            Files.createDirectories(directory);
            recover();
        } catch (IOException e) {
            throw new IllegalStateException("打开区块耐久日志失败: " + wal, e);
        }
    }

    /**
     * 追加一个批次并一次 force。ownerId 由控制连接身份提供，不能信任批次内字段。
     */
    public AppendResult append(UUID ownerId, ChunkDeltaBatch batch) {
        Objects.requireNonNull(ownerId, "ownerId");
        Objects.requireNonNull(batch, "batch");
        byte[] encoded = batch.encode();
        String digest = Sha256.hex(encoded);
        BatchKey batchKey = new BatchKey(ownerId, batch.batchId(), batch.sequence());
        synchronized (lock) {
            ensureOpen();
            SeenBatch old = seenBatches.get(batchKey);
            if (old != null) {
                if (!old.sha256().equals(digest)) {
                    throw new IllegalArgumentException("批次 ID 重用但载荷不同");
                }
                return new AppendResult(old.epoch(), true, old.deltaCount(), old.bytes());
            }
            long epoch = nextEpoch.getAndIncrement();
            byte[] record = encodeRecord(ownerId, epoch, digest, encoded);
            if (record.length - Integer.BYTES > MAX_RECORD_BYTES) {
                throw new IllegalArgumentException("区块批次超过 WAL 单记录上限");
            }
            long started = System.nanoTime();
            try (FileChannel channel = FileChannel.open(wal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                writeFully(channel, record);
                // 这是整个批次唯一的同步刷盘点；调用方收到返回值即可视为 durable。
                channel.force(true);
            } catch (IOException e) {
                // write/force 抛错时磁盘结果可能不确定。epoch 只能留下空洞，绝不能
                // 回退后复用，否则完整落盘但 ACK 丢失的旧记录会与后续记录同 epoch。
                throw new IllegalStateException("追加区块耐久日志失败", e);
            }
            applyToIndex(ownerId, epoch, batch);
            seenBatches.put(batchKey,
                    new SeenBatch(epoch, digest, batch.deltas().size(), encoded.length));
            forceObserver.onForced(System.nanoTime() - started, epoch,
                    batch.deltas().size());
            return new AppendResult(epoch, false, batch.deltas().size(), encoded.length);
        }
    }

    /** 无 owner 的工具重载，主要供离线恢复/单测使用。 */
    public AppendResult append(ChunkDeltaBatch batch) {
        return append(new UUID(0L, 0L), batch);
    }

    /**
     * 把最终玩家数据、区块尾部和退出终态写成一个分段复合事务，并且只 force 一次。
     * 相同 shutdownId 只能逐字节重放同一载荷；内容变化代表事务身份误用，必须拒绝。
     */
    public AppendResult appendExitCommit(ExitCommitPayload payload) {
        Objects.requireNonNull(payload, "payload");
        // playerData() 只做一次深复制；两遍稳定编码（先摘要计数、再分段写入）复用
        // 同一不可变视图，避免玩家数较多时重复复制整张载荷表。
        Map<UUID, byte[]> players = payload.playerData();
        ExitPayloadDescriptor descriptor = describeExitPayload(payload, players);
        String digest = descriptor.sha256();
        UUID shutdownId = payload.manifest().shutdownId();
        synchronized (lock) {
            ensureOpen();
            ExitCommitEntry old = exitCommits.get(shutdownId);
            if (old != null) {
                if (!old.sha256().equals(digest)) {
                    throw new IllegalArgumentException(
                            "shutdownId 已绑定到另一份退出载荷");
                }
                return new AppendResult(old.durableEpoch(), true,
                        old.payload().chunkDeltas().size(), descriptor.bytes());
            }
            long epoch = nextEpoch.getAndIncrement();
            long started = System.nanoTime();
            try (FileChannel channel = FileChannel.open(wal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                long startOffset = channel.size();
                try {
                    ExitPartOutputStream parts = new ExitPartOutputStream(
                            channel, payload.manifest().groupId(), shutdownId,
                            epoch, exitPartBytes);
                    DataOutputStream out = new DataOutputStream(parts);
                    writeExitPayload(payload, players, out);
                    out.flush();
                    parts.finish();
                    byte[] marker = encodeExitMarker(
                            shutdownId, parts.partCount(), descriptor.bytes(), digest);
                    byte[] markerRecord = encodeTypedRecord(
                            RECORD_EXIT_MARKER, payload.manifest().groupId(), epoch,
                            Sha256.hex(marker), marker);
                    if (markerRecord.length - Integer.BYTES > MAX_RECORD_BYTES) {
                        throw new IOException("退出 WAL 终结标记超过记录上限");
                    }
                    writeFully(channel, markerRecord);
                    // 所有数据片和终结标记共享这一个同步刷盘点。marker 只有与前面
                    // 的完整前缀同时耐久，恢复时才会发布 committed 状态。
                    channel.force(true);
                } catch (IOException | RuntimeException failure) {
                    rollbackUnacknowledged(channel, startOffset, failure);
                    throw failure;
                }
            } catch (IOException e) {
                // 未回 ACK 的事务允许回滚；若磁盘连回滚也失败，日志会被标记关闭，
                // 由重启恢复截断半条尾记录后再接受客户端无限重放。
                throw new IllegalStateException("追加退出复合耐久记录失败", e);
            }
            applyExitCommit(epoch, payload, digest);
            forceObserver.onForced(System.nanoTime() - started, epoch,
                    payload.chunkDeltas().size());
            return new AppendResult(epoch, false,
                    payload.chunkDeltas().size(), descriptor.bytes());
        }
    }

    /** 查询已耐久确认的退出复合记录。 */
    public Optional<ExitCommitEntry> exitCommit(UUID shutdownId) {
        synchronized (lock) {
            ExitCommitEntry entry = exitCommits.get(shutdownId);
            return entry == null ? Optional.empty()
                    : Optional.of(new ExitCommitEntry(entry.durableEpoch(),
                    entry.payload(), entry.sha256()));
        }
    }

    /** 启动恢复时使用的退出记录快照。 */
    public Map<UUID, ExitCommitEntry> exitCommits() {
        synchronized (lock) {
            return Map.copyOf(exitCommits);
        }
    }

    /** 返回指定区块最新耐久快照；调用者可以安全修改返回对象的 payload。 */
    public Optional<Entry> latest(ChunkKey key) {
        synchronized (lock) {
            Entry entry = latest.get(key);
            return entry == null ? Optional.empty()
                    : Optional.of(new Entry(entry.ownerId(), entry.key(), entry.version(),
                    entry.durableEpoch(), entry.payload(), entry.sha256()));
        }
    }

    public boolean contains(ChunkKey key) {
        synchronized (lock) {
            return latest.containsKey(key);
        }
    }

    public long durableEpoch() {
        return Math.max(0, nextEpoch.get() - 1);
    }

    public int batchCount() {
        synchronized (lock) {
            return seenBatches.size();
        }
    }

    public int latestCount() {
        synchronized (lock) {
            return latest.size();
        }
    }

    /** 遍历当前索引，用于后台 MCA 物化；回调不得修改返回 Entry。 */
    public void forEachLatest(Consumer<Entry> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        Map<ChunkKey, Entry> snapshot;
        synchronized (lock) {
            snapshot = new HashMap<>(latest);
        }
        snapshot.values().forEach(entry -> consumer.accept(
                new Entry(entry.ownerId(), entry.key(), entry.version(),
                        entry.durableEpoch(), entry.payload(), entry.sha256())));
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
        }
    }

    private void recover() throws IOException {
        if (!Files.isRegularFile(wal)) {
            return;
        }
        long fileSize = Files.size(wal);
        long offset = 0;
        long lastGood = 0;
        boolean incompleteTail = false;
        ExitRecoveryTracker exitRecovery = new ExitRecoveryTracker();
        try (FileChannel channel = FileChannel.open(wal, StandardOpenOption.READ)) {
            while (offset < fileSize) {
                if (fileSize - offset < Integer.BYTES) {
                    incompleteTail = true;
                    break;
                }
                ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
                readFully(channel, lengthBuffer, offset);
                lengthBuffer.flip();
                int length = lengthBuffer.getInt();
                if (length <= 0 || length > MAX_RECORD_BYTES) {
                    throw new IOException("WAL 记录长度损坏: offset="
                            + offset + " length=" + length);
                }
                if (fileSize - offset - Integer.BYTES < length) {
                    incompleteTail = true;
                    break;
                }
                ByteBuffer body = ByteBuffer.allocate(length);
                readFully(channel, body, offset + Integer.BYTES);
                byte[] bytes = body.array();
                if (!decodeRecord(bytes, offset, channel, exitRecovery)) {
                    // 记录长度完整却校验失败，可能是中段损坏；不能截断并静默删除
                    // 后续已经 ACK 的记录，必须拒绝启动等待人工恢复。
                    throw new IOException("WAL 完整记录校验失败: offset=" + offset);
                }
                offset += Integer.BYTES + length;
                lastGood = offset;
            }
        }
        long orphanStart = exitRecovery.tailStartOffset();
        long truncateAt = incompleteTail ? lastGood : fileSize;
        if (orphanStart >= 0) {
            truncateAt = Math.min(truncateAt, orphanStart);
        }
        if (truncateAt < fileSize) {
            try (FileChannel channel = FileChannel.open(wal, StandardOpenOption.WRITE)) {
                channel.truncate(truncateAt);
                channel.force(true);
            }
        }
    }

    /** 返回 false 表示损坏记录，调用者会截断到上一条完整记录。 */
    private boolean decodeRecord(
            byte[] body,
            long recordOffset,
            FileChannel channel,
            ExitRecoveryTracker exitRecovery) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            if (in.readInt() != MAGIC) {
                return false;
            }
            int formatVersion = in.readUnsignedByte();
            int recordType;
            if (formatVersion == FORMAT_VERSION) {
                recordType = RECORD_BATCH;
            } else if (formatVersion == TYPED_FORMAT_VERSION) {
                recordType = in.readUnsignedByte();
            } else {
                return false;
            }
            UUID owner = new UUID(in.readLong(), in.readLong());
            long epoch = in.readLong();
            int payloadLength = in.readInt();
            if (epoch <= 0 || payloadLength <= 0
                    || payloadLength > ChunkDelta.MAX_PAYLOAD_BYTES * 65_536L
                    || payloadLength > in.available() - DIGEST_BYTES - Integer.BYTES) {
                return false;
            }
            byte[] digest = in.readNBytes(DIGEST_BYTES);
            byte[] payload = in.readNBytes(payloadLength);
            int expectedCrc = in.readInt();
            if (digest.length != DIGEST_BYTES || payload.length != payloadLength
                    || in.available() != 0) {
                return false;
            }
            if (!MessageDigest.isEqual(digest, hexToBytes(Sha256.hex(payload)))) {
                return false;
            }
            CRC32 crc = new CRC32();
            crc.update(body, 0, body.length - Integer.BYTES);
            if ((int) crc.getValue() != expectedCrc) {
                return false;
            }
            if (recordType == RECORD_EXIT_PART) {
                ExitPart part = decodeExitPart(payload);
                ExitRecoveryKey key = new ExitRecoveryKey(
                        owner, part.shutdownId(), epoch);
                long partPayloadOffset = Math.addExact(recordOffset,
                        Integer.BYTES + TYPED_RECORD_HEADER_BYTES
                                + EXIT_PART_HEADER_BYTES);
                exitRecovery.add(key, part.index(), recordOffset,
                        partPayloadOffset, part.payloadLength());
                // 即使终结标记未落盘，这个 epoch 也已经物理出现在 WAL 中，后续
                // 新记录不能复用它。
                nextEpoch.updateAndGet(value -> Math.max(value, epoch + 1));
                return true;
            }
            if (recordType == RECORD_EXIT_MARKER) {
                ExitMarker marker = decodeExitMarker(payload);
                ExitRecoveryKey key = new ExitRecoveryKey(
                        owner, marker.shutdownId(), epoch);
                byte[] assembled = exitRecovery.finish(key, marker, channel);
                ExitCommitPayload exitPayload = decodeExitPayload(assembled);
                if (!owner.equals(exitPayload.manifest().groupId())
                        || !marker.shutdownId().equals(
                        exitPayload.manifest().shutdownId())) {
                    return false;
                }
                nextEpoch.updateAndGet(value -> Math.max(value, epoch + 1));
                return recoverExitCommit(
                        epoch, exitPayload, marker.payloadSha256());
            }
            if (recordType == RECORD_EXIT_COMMIT) {
                exitRecovery.discardBeforeNonPart();
                ExitCommitPayload exitPayload = decodeExitPayload(payload);
                if (!owner.equals(exitPayload.manifest().groupId())) {
                    return false;
                }
                String digestHex = Sha256.hex(payload);
                nextEpoch.updateAndGet(value -> Math.max(value, epoch + 1));
                return recoverExitCommit(epoch, exitPayload, digestHex);
            }
            if (recordType != RECORD_BATCH) {
                return false;
            }
            exitRecovery.discardBeforeNonPart();
            ChunkDeltaBatch batch = ChunkDeltaBatch.decode(payload);
            // 恢复时 duplicate 记录是合法的，后面的完整记录仍可继续扫描。
            BatchKey key = new BatchKey(owner, batch.batchId(), batch.sequence());
            SeenBatch previous = seenBatches.get(key);
            String digestHex = Sha256.hex(payload);
            if (previous != null) {
                boolean samePayload = previous.sha256().equals(digestHex);
                if (samePayload) {
                    // force 结果不确定时同一批次可能物理出现两次。即使第二条不再
                    // 改变内容，也必须以日志中的后一个 epoch 重新发布并推进编号。
                    applyToIndex(owner, epoch, batch);
                    seenBatches.put(key, new SeenBatch(epoch, digestHex,
                            batch.deltas().size(), payload.length));
                    nextEpoch.updateAndGet(value -> Math.max(value, epoch + 1));
                }
                return samePayload;
            }
            applyToIndex(owner, epoch, batch);
            seenBatches.put(key, new SeenBatch(epoch, digestHex,
                    batch.deltas().size(), payload.length));
            nextEpoch.updateAndGet(value -> Math.max(value, epoch + 1));
            return true;
        } catch (EOFException | IllegalArgumentException e) {
            return false;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    private void applyToIndex(UUID owner, long epoch, ChunkDeltaBatch batch) {
        for (ChunkDelta delta : batch.deltas()) {
            Entry old = latest.get(delta.key());
            if (old == null || epoch >= old.durableEpoch()) {
                latest.put(delta.key(), new Entry(owner, delta.key(), delta.version(), epoch,
                        delta.payload(), delta.sha256()));
            }
        }
    }

    private void applyExitCommit(long epoch, ExitCommitPayload payload,
                                 String digest) {
        for (ChunkDelta delta : payload.chunkDeltas()) {
            Entry old = latest.get(delta.key());
            if (old == null || epoch >= old.durableEpoch()) {
                latest.put(delta.key(), new Entry(
                        payload.manifest().groupId(), delta.key(), delta.version(), epoch,
                        delta.payload(), delta.sha256()));
            }
        }
        exitCommits.put(payload.manifest().shutdownId(),
                new ExitCommitEntry(epoch, payload, digest));
    }

    /** 恢复旧单记录或新分段事务，并处理 force 结果不确定造成的物理重放。 */
    private boolean recoverExitCommit(long epoch, ExitCommitPayload payload,
                                      String digest) {
        UUID shutdownId = payload.manifest().shutdownId();
        ExitCommitEntry previous = exitCommits.get(shutdownId);
        if (previous != null) {
            boolean samePayload = previous.sha256().equals(digest);
            if (samePayload) {
                // 后一个完整物理事务才是客户端最终可能收到的确认顺序，必须用
                // 更高 epoch 重新发布最新区块索引。
                applyExitCommit(epoch, payload, digest);
            }
            return samePayload;
        }
        applyExitCommit(epoch, payload, digest);
        return true;
    }

    private static byte[] encodeRecord(UUID owner, long epoch, String digest,
                                       byte[] payload) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(payload.length + 96);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.writeInt(MAGIC);
            body.writeByte(FORMAT_VERSION);
            body.writeLong(owner.getMostSignificantBits());
            body.writeLong(owner.getLeastSignificantBits());
            body.writeLong(epoch);
            body.writeInt(payload.length);
            body.write(hexToBytes(digest));
            body.write(payload);
            body.flush();
            byte[] withoutCrc = bodyBytes.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(withoutCrc);
            ByteArrayOutputStream result = new ByteArrayOutputStream(withoutCrc.length + 8);
            DataOutputStream out = new DataOutputStream(result);
            out.writeInt(withoutCrc.length + Integer.BYTES);
            out.write(withoutCrc);
            out.writeInt((int) crc.getValue());
            out.flush();
            return result.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("编码耐久日志记录失败", impossible);
        }
    }

    private static byte[] encodeTypedRecord(int recordType, UUID owner, long epoch,
                                            String digest, byte[] payload) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(payload.length + 96);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.writeInt(MAGIC);
            body.writeByte(TYPED_FORMAT_VERSION);
            body.writeByte(recordType);
            body.writeLong(owner.getMostSignificantBits());
            body.writeLong(owner.getLeastSignificantBits());
            body.writeLong(epoch);
            body.writeInt(payload.length);
            body.write(hexToBytes(digest));
            body.write(payload);
            body.flush();
            byte[] withoutCrc = bodyBytes.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(withoutCrc);
            ByteArrayOutputStream result = new ByteArrayOutputStream(withoutCrc.length + 8);
            DataOutputStream out = new DataOutputStream(result);
            out.writeInt(withoutCrc.length + Integer.BYTES);
            out.write(withoutCrc);
            out.writeInt((int) crc.getValue());
            out.flush();
            return result.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("编码类型化耐久日志记录失败", impossible);
        }
    }

    /** 第一遍只流式计数并计算总摘要，不构造可能超过 128 MiB 的聚合字节数组。 */
    private static ExitPayloadDescriptor describeExitPayload(
            ExitCommitPayload payload, Map<UUID, byte[]> players) {
        DigestCounter counter = new DigestCounter();
        try {
            DataOutputStream out = new DataOutputStream(counter);
            writeExitPayload(payload, players, out);
            out.flush();
            return new ExitPayloadDescriptor(
                    counter.bytes(), Sha256.toHex(counter.finishDigest()));
        } catch (IOException impossible) {
            throw new IllegalStateException("计算退出复合载荷摘要失败", impossible);
        }
    }

    /** 复合退出载荷使用稳定排序编码，确保重连重放与旧单记录格式摘要一致。 */
    private static void writeExitPayload(ExitCommitPayload payload,
                                         Map<UUID, byte[]> players,
                                         DataOutputStream out)
            throws IOException {
        byte[] manifest = JsonUtil.GSON.toJson(payload.manifest().toJson())
                .getBytes(StandardCharsets.UTF_8);
        out.writeInt(manifest.length);
        out.write(manifest);
        out.writeInt(players.size());
        for (Map.Entry<UUID, byte[]> entry : players.entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).toList()) {
            out.writeLong(entry.getKey().getMostSignificantBits());
            out.writeLong(entry.getKey().getLeastSignificantBits());
            byte[] player = entry.getValue();
            out.writeInt(player.length);
            out.write(player);
        }
        out.writeInt(payload.chunkDeltas().size());
        for (ChunkDelta delta : payload.chunkDeltas()) {
            byte[] dimension = delta.key().dimension()
                    .getBytes(StandardCharsets.UTF_8);
            out.writeInt(dimension.length);
            out.write(dimension);
            out.writeInt(delta.key().x());
            out.writeInt(delta.key().z());
            out.writeLong(delta.version());
            byte[] chunk = delta.payload();
            out.writeInt(chunk.length);
            out.write(hexToBytes(delta.sha256()));
            out.write(chunk);
        }
    }

    private static byte[] encodeExitPart(UUID shutdownId, int index,
                                         byte[] payload) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(EXIT_FRAGMENT_VERSION);
            out.writeLong(shutdownId.getMostSignificantBits());
            out.writeLong(shutdownId.getLeastSignificantBits());
            out.writeInt(index);
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("编码退出 WAL 数据片失败", impossible);
        }
    }

    private static ExitPart decodeExitPart(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        if (in.readUnsignedByte() != EXIT_FRAGMENT_VERSION) {
            throw new IOException("退出 WAL 数据片版本不兼容");
        }
        UUID shutdownId = new UUID(in.readLong(), in.readLong());
        int index = in.readInt();
        int length = in.readInt();
        if (index < 0 || length <= 0 || length != in.available()) {
            throw new IOException("退出 WAL 数据片长度非法");
        }
        in.skipNBytes(length);
        return new ExitPart(shutdownId, index, length);
    }

    private static byte[] encodeExitMarker(UUID shutdownId, int partCount,
                                           long payloadBytes,
                                           String payloadSha256) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(80);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(EXIT_FRAGMENT_VERSION);
            out.writeLong(shutdownId.getMostSignificantBits());
            out.writeLong(shutdownId.getLeastSignificantBits());
            out.writeInt(partCount);
            out.writeLong(payloadBytes);
            out.write(hexToBytes(payloadSha256));
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("编码退出 WAL 终结标记失败", impossible);
        }
    }

    private static ExitMarker decodeExitMarker(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        if (in.readUnsignedByte() != EXIT_FRAGMENT_VERSION) {
            throw new IOException("退出 WAL 终结标记版本不兼容");
        }
        UUID shutdownId = new UUID(in.readLong(), in.readLong());
        int partCount = in.readInt();
        long payloadBytes = in.readLong();
        byte[] digest = in.readNBytes(DIGEST_BYTES);
        if (partCount <= 0 || partCount > 1_000_000
                || payloadBytes <= 0 || payloadBytes > Integer.MAX_VALUE
                || digest.length != DIGEST_BYTES || in.available() != 0) {
            throw new IOException("退出 WAL 终结标记字段非法");
        }
        return new ExitMarker(
                shutdownId, partCount, payloadBytes, bytesToHex(digest));
    }

    private static ExitCommitPayload decodeExitPayload(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int manifestLength = in.readInt();
        if (manifestLength <= 0 || manifestLength > in.available()) {
            throw new IOException("退出复合载荷 manifest 长度非法");
        }
        String manifestJson = new String(
                in.readNBytes(manifestLength), StandardCharsets.UTF_8);
        ExitManifest manifest = ExitManifest.fromJson(
                JsonUtil.parseObject(manifestJson));
        int playerCount = in.readInt();
        if (playerCount < 0 || playerCount > 65_536) {
            throw new IOException("退出复合载荷玩家数量非法");
        }
        Map<UUID, byte[]> players = new LinkedHashMap<>();
        for (int i = 0; i < playerCount; i++) {
            UUID playerId = new UUID(in.readLong(), in.readLong());
            int length = in.readInt();
            if (length <= 0 || length > in.available()) {
                throw new IOException("退出玩家载荷长度非法");
            }
            byte[] player = in.readNBytes(length);
            if (players.putIfAbsent(playerId, player) != null) {
                throw new IOException("退出复合载荷含重复玩家");
            }
        }
        int chunkCount = in.readInt();
        if (chunkCount < 0 || chunkCount > 1_000_000) {
            throw new IOException("退出复合载荷区块数量非法");
        }
        List<ChunkDelta> chunks = new java.util.ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            int dimensionLength = in.readInt();
            if (dimensionLength <= 0 || dimensionLength > 32_768
                    || dimensionLength > in.available()) {
                throw new IOException("退出区块维度长度非法");
            }
            String dimension = new String(
                    in.readNBytes(dimensionLength), StandardCharsets.UTF_8);
            ChunkKey key = new ChunkKey(dimension, in.readInt(), in.readInt());
            long version = in.readLong();
            int length = in.readInt();
            if (length <= 0 || length > ChunkDelta.MAX_PAYLOAD_BYTES
                    || length > in.available() - DIGEST_BYTES) {
                throw new IOException("退出区块载荷长度非法");
            }
            byte[] digest = in.readNBytes(DIGEST_BYTES);
            byte[] chunk = in.readNBytes(length);
            chunks.add(new ChunkDelta(key, version, chunk, bytesToHex(digest)));
        }
        if (in.available() != 0) {
            throw new IOException("退出复合载荷存在尾随字节");
        }
        return new ExitCommitPayload(manifest, players, chunks);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0x0f, 16));
            result.append(Character.forDigit(value & 0x0f, 16));
        }
        return result.toString();
    }

    private static byte[] hexToBytes(String hex) {
        if (hex == null || hex.length() != DIGEST_BYTES * 2) {
            throw new IllegalArgumentException("摘要长度非法");
        }
        byte[] result = new byte[DIGEST_BYTES];
        for (int i = 0; i < result.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("摘要包含非法字符");
            }
            result[i] = (byte) ((high << 4) | low);
        }
        return result;
    }

    /** 只计数并更新摘要的空输出流，用于退出载荷第一遍稳定编码。 */
    private static final class DigestCounter extends OutputStream {
        private final MessageDigest digest = Sha256.newDigest();
        private long bytes;

        @Override
        public void write(int value) {
            digest.update((byte) value);
            bytes++;
        }

        @Override
        public void write(byte[] values, int offset, int length) {
            Objects.checkFromIndexSize(offset, length, values.length);
            digest.update(values, offset, length);
            bytes += length;
        }

        private long bytes() {
            return bytes;
        }

        private byte[] finishDigest() {
            return digest.digest();
        }
    }

    /**
     * 把稳定逻辑编码切成多个有界物理记录。该流不拥有 FileChannel，finish 只写出
     * 最后一个数据片；终结 marker 和唯一 force 由 appendExitCommit 负责。
     */
    private static final class ExitPartOutputStream extends OutputStream {
        private final FileChannel channel;
        private final UUID ownerId;
        private final UUID shutdownId;
        private final long epoch;
        private final int partBytes;
        private final ByteArrayOutputStream buffer;
        private int partCount;

        private ExitPartOutputStream(FileChannel channel, UUID ownerId,
                                     UUID shutdownId, long epoch,
                                     int partBytes) {
            this.channel = channel;
            this.ownerId = ownerId;
            this.shutdownId = shutdownId;
            this.epoch = epoch;
            this.partBytes = partBytes;
            this.buffer = new ByteArrayOutputStream(Math.min(partBytes, 64 * 1024));
        }

        @Override
        public void write(int value) throws IOException {
            if (buffer.size() == partBytes) {
                flushPart();
            }
            buffer.write(value);
        }

        @Override
        public void write(byte[] values, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, values.length);
            int cursor = offset;
            int remaining = length;
            while (remaining > 0) {
                if (buffer.size() == partBytes) {
                    flushPart();
                }
                int copied = Math.min(remaining, partBytes - buffer.size());
                buffer.write(values, cursor, copied);
                cursor += copied;
                remaining -= copied;
            }
        }

        private void finish() throws IOException {
            flushPart();
            if (partCount == 0) {
                throw new IOException("退出复合载荷没有可写数据片");
            }
        }

        private int partCount() {
            return partCount;
        }

        private void flushPart() throws IOException {
            if (buffer.size() == 0) {
                return;
            }
            if (partCount == Integer.MAX_VALUE) {
                throw new IOException("退出 WAL 数据片数量溢出");
            }
            byte[] part = encodeExitPart(
                    shutdownId, partCount, buffer.toByteArray());
            byte[] record = encodeTypedRecord(
                    RECORD_EXIT_PART, ownerId, epoch, Sha256.hex(part), part);
            if (record.length - Integer.BYTES > MAX_RECORD_BYTES) {
                throw new IOException("退出 WAL 数据片超过记录上限");
            }
            writeFully(channel, record);
            partCount++;
            buffer.reset();
        }
    }

    /**
     * 正常路径 force 失败后回滚本次未确认后缀。若连回滚都失败，禁止本进程继续
     * 追加，避免在半条记录后写入新 ACK 数据；重启恢复会处理尾部截断。
     */
    private void rollbackUnacknowledged(FileChannel channel, long startOffset,
                                        Throwable failure) {
        try {
            channel.truncate(startOffset);
            channel.force(true);
        } catch (IOException rollbackFailure) {
            closed = true;
            failure.addSuppressed(rollbackFailure);
        }
    }

    private static void writeFully(FileChannel channel, byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position)
            throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new EOFException("耐久日志记录截断");
            }
            position += read;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("区块耐久日志已关闭");
        }
    }
}
