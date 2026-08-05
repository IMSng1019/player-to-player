package imsng.player_to_player.registry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 区块注册表先行变更日志。每条已确认变更先追加并 force，再修改内存；服务端启动时
 * 在最近全量快照上顺序重放，消除 30 秒周期快照造成的已确认所有权丢失窗口。
 */
public final class RegistryMutationJournal {

    private static final int FORMAT_VERSION = 1;
    private static final int READ_BUFFER_BYTES = 64 * 1024;
    private static final int DEFAULT_MAX_RECORD_BYTES = 64 * 1024 * 1024;

    private static final RegistryMutationJournal DISABLED =
            new RegistryMutationJournal(null);

    private final Path file;
    /** 周期 checkpoint 后保存回执与断连凭证的原子快照；WAL 负责跨文件窗口恢复。 */
    private final Path receiptSnapshotFile;
    private final int maxRecordBytes;

    /** 注册表 WAL 重放后的完整状态；回执与 ownership 必须来自同一条有序日志。 */
    public record ReplayState(
            Map<ChunkKey, UUID> claims,
            Map<UUID, ChunkReleaseReceipt> releaseReceipts,
            Map<UUID, DisconnectClaimRecovery> disconnectRecoveries) {

        public ReplayState {
            claims = Map.copyOf(claims);
            releaseReceipts = Map.copyOf(releaseReceipts);
            disconnectRecoveries = Map.copyOf(disconnectRecoveries);
        }
    }

    /** checkpoint 辅助快照，不包含由 RegistryStore 保存的 ownership 主表。 */
    public record AuxiliaryState(
            Map<UUID, ChunkReleaseReceipt> releaseReceipts,
            Map<UUID, DisconnectClaimRecovery> disconnectRecoveries) {

        public AuxiliaryState {
            releaseReceipts = Map.copyOf(releaseReceipts);
            disconnectRecoveries = Map.copyOf(disconnectRecoveries);
        }
    }

    /** 只表示写入中断造成的帧不完整；仅最后一条允许截断恢复。 */
    private static final class RecoverableTailException
            extends IllegalArgumentException {
        private RecoverableTailException(String message) {
            super(message);
        }

        private RecoverableTailException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public RegistryMutationJournal(Path file) {
        this(file, DEFAULT_MAX_RECORD_BYTES);
    }

    /** 包内测试可缩小阈值；生产构造器使用 64 MiB 硬上限。 */
    RegistryMutationJournal(Path file, int maxRecordBytes) {
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("日志单帧上限必须为正数");
        }
        this.file = file;
        this.receiptSnapshotFile = file != null
                // JsonRegistryStore 会把同目录 *.json 当作维度快照并清理；专用扩展名
                // 防止 ownership 保存阶段误删回执，破坏双文件崩溃恢复协议。
                ? file.resolveSibling(file.getFileName() + ".receipts")
                : null;
        this.maxRecordBytes = maxRecordBytes;
    }

    static RegistryMutationJournal disabled() {
        return DISABLED;
    }

    public void appendPut(ChunkKey key, UUID groupId) {
        JsonObject json = base("put", groupId);
        json.addProperty("chunk", key.asString());
        append(json);
    }

    public void appendRemove(ChunkKey key, UUID groupId) {
        JsonObject json = base("remove", groupId);
        json.addProperty("chunk", key.asString());
        append(json);
    }

    /**
     * 原子记录一次成功释放：同一 WAL 帧同时代表 ownership 删除与幂等回执建立。
     * append 返回前帧已经 force，因而不存在“owner 已删但成功凭据尚未落盘”的窗口。
     */
    public void appendRelease(ChunkReleaseReceipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        append(releaseJson(receipt));
    }

    /** 兼容旧调用：读取上次 checkpoint 已整体落盘的成功回执。 */
    public synchronized Map<UUID, ChunkReleaseReceipt> loadReleaseReceipts() {
        return loadAuxiliaryState().releaseReceipts();
    }

    /**
     * 读取成功回执与断连恢复凭证。损坏时拒绝启动，不能丢失幂等或 ownership 证据。
     * version=1 的旧快照只包含回执，按空恢复凭证兼容加载。
     */
    public synchronized AuxiliaryState loadAuxiliaryState() {
        if (receiptSnapshotFile == null
                || !Files.isRegularFile(receiptSnapshotFile)) {
            return new AuxiliaryState(Map.of(), Map.of());
        }
        try {
            JsonObject root = JsonUtil.readFile(
                    receiptSnapshotFile, JsonObject.class);
            int version = root != null
                    ? JsonUtil.getInt(root, "version", -1) : -1;
            if (version != 1 && version != 2) {
                throw new IllegalArgumentException("释放回执快照版本缺失或不支持");
            }
            JsonElement rawReceipts = root.get("receipts");
            if (rawReceipts == null || !rawReceipts.isJsonArray()) {
                throw new IllegalArgumentException("释放回执快照缺少 receipts 数组");
            }
            Map<UUID, ChunkReleaseReceipt> result = new HashMap<>();
            for (JsonElement raw : rawReceipts.getAsJsonArray()) {
                if (!raw.isJsonObject()) {
                    throw new IllegalArgumentException("释放回执条目不是对象");
                }
                JsonObject json = raw.getAsJsonObject();
                UUID group = UUID.fromString(requiredString(json, "group"));
                ChunkReleaseReceipt receipt = parseReleaseReceipt(json, group);
                ChunkReleaseReceipt duplicate = result.putIfAbsent(
                        receipt.releaseId(), receipt);
                if (duplicate != null) {
                    throw new IllegalArgumentException(
                            "释放回执快照含重复 releaseId: " + receipt.releaseId());
                }
            }
            Map<UUID, DisconnectClaimRecovery> recoveries = new HashMap<>();
            if (version >= 2) {
                JsonElement rawRecoveries = root.get("disconnectRecoveries");
                if (rawRecoveries == null || !rawRecoveries.isJsonArray()) {
                    throw new IllegalArgumentException(
                            "辅助快照缺少 disconnectRecoveries 数组");
                }
                for (JsonElement raw : rawRecoveries.getAsJsonArray()) {
                    if (!raw.isJsonObject()) {
                        throw new IllegalArgumentException("断连恢复凭证条目不是对象");
                    }
                    DisconnectClaimRecovery recovery = parseDisconnectRecovery(
                            raw.getAsJsonObject());
                    if (recoveries.putIfAbsent(recovery.groupId(), recovery) != null) {
                        throw new IllegalArgumentException(
                                "辅助快照含重复断连恢复组: " + recovery.groupId());
                    }
                }
            }
            return new AuxiliaryState(result, recoveries);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "读取注册表辅助快照失败: " + receiptSnapshotFile, e);
        }
    }

    /**
     * 原子保存全部成功回执。调用顺序必须是 WAL checkpoint、ownership 快照、本方法、
     * 最后 clear WAL；任一步崩溃都能由尚存 WAL 收敛到同一状态。
     */
    public synchronized void saveReleaseReceipts(
            Map<UUID, ChunkReleaseReceipt> receipts) {
        saveAuxiliaryState(receipts, Map.of());
    }

    /** 原子保存成功回执与仍可使用的断连 ownership 恢复凭证。 */
    public synchronized void saveAuxiliaryState(
            Map<UUID, ChunkReleaseReceipt> receipts,
            Map<UUID, DisconnectClaimRecovery> recoveries) {
        if (receiptSnapshotFile == null) {
            return;
        }
        Objects.requireNonNull(receipts, "receipts");
        Objects.requireNonNull(recoveries, "recoveries");
        JsonObject root = new JsonObject();
        root.addProperty("version", 2);
        JsonArray array = new JsonArray();
        receipts.values().stream()
                .sorted(java.util.Comparator.comparing(
                        receipt -> receipt.releaseId().toString()))
                .map(RegistryMutationJournal::releaseJson)
                .forEach(array::add);
        root.add("receipts", array);
        JsonArray recoveryArray = new JsonArray();
        recoveries.values().stream()
                .sorted(java.util.Comparator.comparing(
                        recovery -> recovery.groupId().toString()))
                .map(RegistryMutationJournal::disconnectRecoveryJson)
                .forEach(recoveryArray::add);
        root.add("disconnectRecoveries", recoveryArray);
        try {
            JsonUtil.writeFileAtomic(receiptSnapshotFile, root);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "保存注册表辅助快照失败: " + receiptSnapshotFile, e);
        }
    }

    public void appendRemoveGroup(UUID groupId) {
        append(base("remove_group", groupId));
    }

    /** 一帧同时耐久记录普通断连 ownership 删除和可验证恢复凭证建立。 */
    public void appendDisconnectRelease(DisconnectClaimRecovery recovery) {
        Objects.requireNonNull(recovery, "recovery");
        JsonObject json = disconnectRecoveryJson(recovery);
        json.addProperty("type", "disconnect_release");
        append(json);
    }

    /** 正常 ROLE 已选择放弃旧退出时，按 recoveryId 幂等消费旧断连凭证。 */
    public void appendConsumeDisconnectRecovery(UUID groupId, UUID recoveryId) {
        JsonObject json = base("consume_disconnect_recovery", groupId);
        json.addProperty("recoveryId", recoveryId.toString());
        append(json);
    }

    public void appendRemoveKeys(UUID groupId, Set<ChunkKey> keys) {
        JsonObject json = base("remove_keys", groupId);
        json.add("chunks", chunks(keys));
        append(json);
    }

    public void appendReplaceGroup(UUID groupId, Set<ChunkKey> keys) {
        JsonObject json = base("replace_group", groupId);
        json.add("chunks", chunks(keys));
        append(json);
    }

    public void appendMigrateGroup(UUID from, UUID to) {
        JsonObject json = base("migrate_group", from);
        json.addProperty("to", to.toString());
        append(json);
    }

    public void appendMigrateKeys(UUID from, UUID to, Set<ChunkKey> keys) {
        JsonObject json = base("migrate_keys", from);
        json.addProperty("to", to.toString());
        json.add("chunks", chunks(keys));
        append(json);
    }

    /**
     * 快照收敛屏障。必须在 store.save(snapshot) 之前 force；这样崩溃发生在
     * 快照替换前或日志 clear 前，重放到本记录都会得到同一个绝对状态。
     */
    void appendCheckpoint(Map<ChunkKey, UUID> snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        JsonObject json = new JsonObject();
        json.addProperty("type", "replace_all");
        json.add("claims", claimEntries(snapshot));
        append(json);
    }

    public synchronized Map<ChunkKey, UUID> replay(
            Map<ChunkKey, UUID> baseline) {
        return replayState(baseline, Map.of(), Map.of()).claims();
    }

    /** 在快照后端的 ownership 与回执基线上重放 WAL。 */
    public synchronized ReplayState replayState(
            Map<ChunkKey, UUID> baseline,
            Map<UUID, ChunkReleaseReceipt> receiptBaseline) {
        return replayState(baseline, receiptBaseline, Map.of());
    }

    /** 在 ownership、回执和断连凭证三个 checkpoint 基线上重放 WAL。 */
    public synchronized ReplayState replayState(
            Map<ChunkKey, UUID> baseline,
            Map<UUID, ChunkReleaseReceipt> receiptBaseline,
            Map<UUID, DisconnectClaimRecovery> recoveryBaseline) {
        Map<ChunkKey, UUID> result = new HashMap<>(baseline);
        Map<UUID, ChunkReleaseReceipt> receipts = new HashMap<>(receiptBaseline);
        Map<UUID, DisconnectClaimRecovery> recoveries =
                new HashMap<>(recoveryBaseline);
        if (file == null || !Files.isRegularFile(file)) {
            return new ReplayState(result, receipts, recoveries);
        }
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long fileSize = channel.size();
            long offset = 0;
            long lineStart = 0;
            int lineNumber = 0;
            ByteBuffer input = ByteBuffer.allocate(READ_BUFFER_BYTES);
            ByteArrayOutputStream line = new ByteArrayOutputStream();
            while (offset < fileSize) {
                int read = channel.read(input);
                if (read < 0) {
                    break;
                }
                input.flip();
                while (input.hasRemaining()) {
                    byte value = input.get();
                    offset++;
                    if (value != '\n') {
                        if (line.size() >= maxRecordBytes) {
                            throw new IllegalStateException(
                                    "注册表变更日志单帧超过上限 "
                                            + maxRecordBytes + " 字节，行 "
                                            + (lineNumber + 1));
                        }
                        line.write(value);
                        continue;
                    }
                    lineNumber++;
                    byte[] bytes = line.toByteArray();
                    int length = bytes.length;
                    if (length > 0 && bytes[length - 1] == '\r') {
                        length--;
                    }
                    String text = new String(
                            bytes, 0, length, StandardCharsets.UTF_8);
                    if (!text.isBlank()) {
                        try {
                            apply(result, receipts, recoveries, decodeRecord(text));
                        } catch (RecoverableTailException e) {
                            if (offset == fileSize) {
                                truncateTail(channel, lineStart);
                                return new ReplayState(result, receipts, recoveries);
                            }
                            throw new IllegalStateException(
                                    "注册表变更日志损坏，行 " + lineNumber, e);
                        } catch (Exception e) {
                            // 校验完整后才暴露的未知版本/操作/UUID/ChunkKey 属于
                            // 已确认记录的语义损坏，不能借“最后一行”静默丢弃。
                            throw new IllegalStateException(
                                    "注册表变更日志损坏，行 " + lineNumber, e);
                        }
                    }
                    line.reset();
                    lineStart = offset;
                }
                input.clear();
            }
            if (line.size() > 0) {
                // append 只有在完整写入换行并 force 后才会返回 ACK；无换行尾部
                // 必然属于未确认写入，截断后才能保证下次 append 不接在残片后。
                truncateTail(channel, lineStart);
            }
            return new ReplayState(result, receipts, recoveries);
        } catch (IOException e) {
            throw new IllegalStateException("读取注册表变更日志失败: " + file, e);
        }
    }

    public synchronized boolean hasEntries() {
        if (file == null || !Files.isRegularFile(file)) {
            return false;
        }
        try {
            return Files.size(file) > 0;
        } catch (IOException e) {
            throw new IllegalStateException("检查注册表变更日志失败: " + file, e);
        }
    }

    /** 全量快照已落盘后原子替换为空日志；调用方必须阻止并发 append。 */
    public synchronized void clear() {
        if (file == null) {
            return;
        }
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
            Files.write(temporary, new byte[0]);
            force(temporary);
            try {
                Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
            }
            force(file);
        } catch (IOException e) {
            throw new IllegalStateException("清空注册表变更日志失败: " + file, e);
        }
    }

    private synchronized void append(JsonObject json) {
        if (file == null) {
            return;
        }
        byte[] line = (JsonUtil.GSON.toJson(frame(json)) + "\n").getBytes(
                StandardCharsets.UTF_8);
        if (line.length - 1 > maxRecordBytes) {
            throw new IllegalStateException("注册表变更日志单帧超过上限 "
                    + maxRecordBytes + " 字节");
        }
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            try (FileChannel channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                ByteBuffer buffer = ByteBuffer.wrap(line);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
        } catch (IOException e) {
            throw new IllegalStateException("追加注册表变更日志失败: " + file, e);
        }
    }

    private static void apply(Map<ChunkKey, UUID> state,
                              Map<UUID, ChunkReleaseReceipt> receipts,
                              Map<UUID, DisconnectClaimRecovery> recoveries,
                              JsonObject json) {
        String type = requiredString(json, "type");
        if ("replace_all".equals(type)) {
            Map<ChunkKey, UUID> replacement = parseClaimEntries(json);
            state.clear();
            state.putAll(replacement);
            return;
        }
        UUID group = UUID.fromString(requiredString(json, "group"));
        switch (type) {
            case "put" -> state.put(
                    ChunkKey.parse(requiredString(json, "chunk")), group);
            case "remove" -> state.remove(
                    ChunkKey.parse(requiredString(json, "chunk")), group);
            case "release" -> {
                ChunkReleaseReceipt receipt = parseReleaseReceipt(json, group);
                ChunkReleaseReceipt existing = receipts.get(receipt.releaseId());
                if (existing != null) {
                    if (!existing.equals(receipt)) {
                        throw new IllegalArgumentException(
                                "releaseId 对应的耐久回执内容冲突: "
                                        + receipt.releaseId());
                    }
                    // 同一帧若因外部复制重复出现也必须是严格幂等的，不能删除其后
                    // 可能由同组重新取得的新 ownership。
                    return;
                }
                state.remove(receipt.key(), group);
                receipts.put(receipt.releaseId(), receipt);
            }
            case "remove_group" -> {
                state.entrySet().removeIf(
                        entry -> group.equals(entry.getValue()));
                receipts.values().removeIf(
                        receipt -> group.equals(receipt.groupId()));
                recoveries.remove(group);
            }
            case "disconnect_release" -> {
                DisconnectClaimRecovery recovery = parseDisconnectRecovery(json);
                if (!group.equals(recovery.groupId())) {
                    throw new IllegalArgumentException("断连恢复凭证组身份不一致");
                }
                state.entrySet().removeIf(
                        entry -> group.equals(entry.getValue()));
                receipts.values().removeIf(
                        receipt -> group.equals(receipt.groupId()));
                recoveries.put(group, recovery);
            }
            case "consume_disconnect_recovery" -> {
                UUID recoveryId = UUID.fromString(
                        requiredString(json, "recoveryId"));
                recoveries.computeIfPresent(group,
                        (ignored, current) -> current.recoveryId().equals(recoveryId)
                                ? null : current);
            }
            case "remove_keys" -> {
                parseChunks(json).forEach(key -> state.remove(key, group));
                // remove_keys 只用于整组退出清单提交；会话终结后不会再有合法的
                // 旧释放重试，连同该组遗留回执一起收敛，避免异常退出造成泄漏。
                receipts.values().removeIf(
                        receipt -> group.equals(receipt.groupId()));
                recoveries.remove(group);
            }
            case "replace_group" -> {
                Set<ChunkKey> replacement = parseChunks(json);
                state.entrySet().removeIf(entry -> group.equals(entry.getValue()));
                replacement.forEach(key -> state.put(key, group));
                // pending EXIT_BEGIN 已在退出 ledger 中耐久后才追加本记录；从这里起
                // manifest 自身成为恢复证据，旧断连 tombstone 必须原子失效。
                recoveries.remove(group);
            }
            case "migrate_group" -> {
                UUID to = UUID.fromString(requiredString(json, "to"));
                state.replaceAll((key, owner) -> group.equals(owner) ? to : owner);
            }
            case "migrate_keys" -> {
                UUID to = UUID.fromString(requiredString(json, "to"));
                for (ChunkKey key : parseChunks(json)) {
                    state.computeIfPresent(key,
                            (ignored, owner) -> group.equals(owner) ? to : owner);
                }
            }
            default -> throw new IllegalArgumentException("未知注册表日志操作: " + type);
        }
    }

    private static JsonObject base(String type, UUID groupId) {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("group", groupId.toString());
        return json;
    }

    private static JsonObject releaseJson(ChunkReleaseReceipt receipt) {
        JsonObject json = base("release", receipt.groupId());
        json.addProperty("releaseId", receipt.releaseId().toString());
        json.addProperty("chunk", receipt.key().asString());
        json.addProperty("payloadBytes", receipt.payloadBytes());
        json.addProperty("payloadSha256", receipt.payloadSha256());
        json.addProperty("finalDataWritten", receipt.finalDataWritten());
        return json;
    }

    private static JsonObject disconnectRecoveryJson(
            DisconnectClaimRecovery recovery) {
        JsonObject json = new JsonObject();
        json.addProperty("group", recovery.groupId().toString());
        json.addProperty("recoveryId", recovery.recoveryId().toString());
        json.addProperty("createdAtMillis", recovery.createdAtMillis());
        json.add("chunks", chunks(recovery.chunks()));
        JsonArray members = new JsonArray();
        recovery.members().stream().map(UUID::toString).sorted().forEach(members::add);
        json.add("members", members);
        return json;
    }

    /** 新日志帧：长度与哈希都针对紧凑序列化后的 operation UTF-8 字节。 */
    private static JsonObject frame(JsonObject operation) {
        JsonObject copied = operation.deepCopy();
        byte[] payload = operationBytes(copied);
        JsonObject record = new JsonObject();
        record.addProperty("version", FORMAT_VERSION);
        record.addProperty("payloadBytes", payload.length);
        record.addProperty("sha256", Sha256.hex(payload));
        record.add("operation", copied);
        return record;
    }

    /** 校验新帧；兼容工作树早期生成的合法无校验 operation 行。 */
    private static JsonObject decodeRecord(String line) {
        JsonObject record;
        try {
            record = JsonUtil.parseObject(line);
        } catch (RuntimeException e) {
            throw new RecoverableTailException("注册表日志 JSON 未完整写入", e);
        }
        if (record.has("type") && !record.has("operation")) {
            return record;
        }
        JsonElement rawVersion = record.get("version");
        if (rawVersion == null || !rawVersion.isJsonPrimitive()) {
            throw new RecoverableTailException("注册表日志缺少完整 version");
        }
        int version;
        try {
            version = rawVersion.getAsInt();
        } catch (RuntimeException e) {
            throw new RecoverableTailException("注册表日志 version 未完整写入", e);
        }
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("未知注册表日志版本: " + version);
        }
        JsonElement operation = record.get("operation");
        if (operation == null || !operation.isJsonObject()) {
            throw new RecoverableTailException("注册表日志缺少完整 operation");
        }
        byte[] payload = operationBytes(operation.getAsJsonObject());
        JsonElement rawBytes = record.get("payloadBytes");
        if (rawBytes == null || !rawBytes.isJsonPrimitive()) {
            throw new RecoverableTailException("注册表日志缺少完整 payloadBytes");
        }
        long expectedBytes;
        try {
            expectedBytes = rawBytes.getAsLong();
        } catch (RuntimeException e) {
            throw new RecoverableTailException(
                    "注册表日志 payloadBytes 未完整写入", e);
        }
        if (expectedBytes != payload.length) {
            throw new RecoverableTailException("注册表日志载荷长度校验失败");
        }
        String expectedHash = JsonUtil.getString(record, "sha256", "");
        if (expectedHash.isBlank()) {
            throw new RecoverableTailException("注册表日志缺少完整 sha256");
        }
        if (!expectedHash.equals(Sha256.hex(payload))) {
            throw new RecoverableTailException("注册表日志 SHA-256 校验失败");
        }
        return operation.getAsJsonObject();
    }

    private static byte[] operationBytes(JsonObject operation) {
        return JsonUtil.GSON.toJson(operation).getBytes(StandardCharsets.UTF_8);
    }

    private static JsonArray chunks(Set<ChunkKey> keys) {
        JsonArray array = new JsonArray();
        keys.stream().map(ChunkKey::asString).sorted().forEach(array::add);
        return array;
    }

    private static JsonArray claimEntries(Map<ChunkKey, UUID> snapshot) {
        JsonArray array = new JsonArray();
        snapshot.entrySet().stream()
                .sorted(java.util.Comparator.comparing(
                        entry -> entry.getKey().asString()))
                .forEach(entry -> {
                    JsonObject item = new JsonObject();
                    item.addProperty("chunk", entry.getKey().asString());
                    item.addProperty("group", entry.getValue().toString());
                    array.add(item);
                });
        return array;
    }

    private static Map<ChunkKey, UUID> parseClaimEntries(JsonObject json) {
        JsonElement element = json.get("claims");
        if (element == null || !element.isJsonArray()) {
            throw new IllegalArgumentException("注册表 checkpoint 缺少 claims");
        }
        Map<ChunkKey, UUID> result = new HashMap<>();
        for (JsonElement raw : element.getAsJsonArray()) {
            if (!raw.isJsonObject()) {
                throw new IllegalArgumentException("注册表 checkpoint claim 非对象");
            }
            JsonObject item = raw.getAsJsonObject();
            ChunkKey key = ChunkKey.parse(requiredString(item, "chunk"));
            UUID group = UUID.fromString(requiredString(item, "group"));
            if (result.putIfAbsent(key, group) != null) {
                throw new IllegalArgumentException(
                        "注册表 checkpoint 含重复区块: " + key.asString());
            }
        }
        return result;
    }

    private static ChunkReleaseReceipt parseReleaseReceipt(
            JsonObject json, UUID group) {
        UUID releaseId = UUID.fromString(requiredString(json, "releaseId"));
        ChunkKey key = ChunkKey.parse(requiredString(json, "chunk"));
        int payloadBytes = json.get("payloadBytes").getAsInt();
        String payloadSha256 = requiredString(json, "payloadSha256");
        boolean finalDataWritten = json.get("finalDataWritten").getAsBoolean();
        return new ChunkReleaseReceipt(releaseId, key, group, payloadBytes,
                payloadSha256, finalDataWritten);
    }

    private static DisconnectClaimRecovery parseDisconnectRecovery(
            JsonObject json) {
        UUID group = UUID.fromString(requiredString(json, "group"));
        UUID recoveryId = UUID.fromString(requiredString(json, "recoveryId"));
        JsonElement rawCreated = json.get("createdAtMillis");
        if (rawCreated == null || !rawCreated.isJsonPrimitive()) {
            throw new IllegalArgumentException("断连恢复凭证缺少 createdAtMillis");
        }
        return new DisconnectClaimRecovery(recoveryId, group,
                parseChunks(json), parseMembers(json), rawCreated.getAsLong());
    }

    private static Set<UUID> parseMembers(JsonObject json) {
        JsonElement element = json.get("members");
        if (element == null || !element.isJsonArray()) {
            throw new IllegalArgumentException("断连恢复凭证缺少 members");
        }
        java.util.HashSet<UUID> result = new java.util.HashSet<>();
        for (JsonElement item : element.getAsJsonArray()) {
            result.add(UUID.fromString(item.getAsString()));
        }
        return result;
    }

    private static Set<ChunkKey> parseChunks(JsonObject json) {
        JsonElement element = json.get("chunks");
        if (element == null || !element.isJsonArray()) {
            throw new IllegalArgumentException("注册表日志缺少 chunks");
        }
        java.util.HashSet<ChunkKey> result = new java.util.HashSet<>();
        for (JsonElement item : element.getAsJsonArray()) {
            result.add(ChunkKey.parse(item.getAsString()));
        }
        return result;
    }

    private static String requiredString(JsonObject json, String name) {
        String value = JsonUtil.getString(json, name, "");
        if (value.isBlank()) {
            throw new IllegalArgumentException("注册表日志缺少字段: " + name);
        }
        return value;
    }

    /** 截断未确认尾帧并同步文件内容与长度元数据。 */
    private static void truncateTail(FileChannel channel, long length)
            throws IOException {
        channel.truncate(length);
        channel.force(true);
    }

    private static void force(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }
}
