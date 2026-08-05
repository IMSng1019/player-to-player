package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 退出事务元数据的追加式 WAL。
 *
 * <p>每次状态跃迁追加一个带长度与 SHA-256 的完整 JSON 帧，并只执行一次
 * {@link FileChannel#force(boolean)}。加载时同一 shutdownId 的最后一帧获胜；旧版每事务
 * 一个 {@code *.json} 文件仍作为基线兼容读取。</p>
 */
public final class ExitTransactionStore implements ExitTransactionPersistence {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/exit-store");
    private static final String WAL_FILE = "transactions.wal";
    private static final int FORMAT_VERSION = 1;
    private static final int READ_BUFFER_BYTES = 64 * 1024;
    private static final int MAX_RECORD_BYTES = 32 * 1024 * 1024;

    @FunctionalInterface
    interface ForceObserver {
        void onForced(long elapsedNanos);
    }

    private final Path directory;
    private final Path wal;
    private final ForceObserver forceObserver;

    public ExitTransactionStore(Path directory) {
        this(directory, elapsedNanos -> {
        });
    }

    ExitTransactionStore(Path directory, ForceObserver forceObserver) {
        this.directory = directory;
        this.wal = directory.resolve(WAL_FILE);
        this.forceObserver = forceObserver;
    }

    @Override
    public synchronized Map<UUID, ExitTransactionRecord> load() {
        Map<UUID, ExitTransactionRecord> records = loadLegacyFiles();
        replayWal(records);
        return records;
    }

    @Override
    public synchronized void save(ExitTransactionRecord record) {
        JsonObject operation = encode(record);
        byte[] payload = JsonUtil.GSON.toJson(operation).getBytes(StandardCharsets.UTF_8);
        JsonObject frame = new JsonObject();
        frame.addProperty("version", FORMAT_VERSION);
        frame.addProperty("payloadBytes", payload.length);
        frame.addProperty("sha256", Sha256.hex(payload));
        frame.add("record", operation);
        byte[] line = (JsonUtil.GSON.toJson(frame) + "\n")
                .getBytes(StandardCharsets.UTF_8);
        if (line.length - 1 > MAX_RECORD_BYTES) {
            throw new IllegalStateException("退出事务 WAL 单帧超过上限");
        }
        long started = System.nanoTime();
        try {
            Files.createDirectories(directory);
            try (FileChannel channel = FileChannel.open(wal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                ByteBuffer buffer = ByteBuffer.wrap(line);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            forceObserver.onForced(System.nanoTime() - started);
        } catch (IOException e) {
            throw new IllegalStateException("退出事务 WAL 追加失败: " + wal, e);
        }
    }

    private Map<UUID, ExitTransactionRecord> loadLegacyFiles() {
        Map<UUID, ExitTransactionRecord> records = new HashMap<>();
        if (!Files.isDirectory(directory)) {
            return records;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path file : stream) {
                try {
                    JsonObject root = JsonUtil.readFile(file, JsonObject.class);
                    ExitTransactionRecord record = decode(root);
                    records.put(record.manifest().shutdownId(), record);
                } catch (Exception e) {
                    LOGGER.warn("旧退出事务文件读取失败，跳过: {}", file, e);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("无法读取退出事务目录: " + directory, e);
        }
        return records;
    }

    /** 仅无换行的未确认尾帧可截断；完整帧损坏必须拒绝启动。 */
    private void replayWal(Map<UUID, ExitTransactionRecord> records) {
        if (!Files.isRegularFile(wal)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(wal,
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
                        if (line.size() >= MAX_RECORD_BYTES) {
                            throw new IllegalStateException(
                                    "退出事务 WAL 单帧超过上限，行 " + (lineNumber + 1));
                        }
                        line.write(value);
                        continue;
                    }
                    lineNumber++;
                    String text = line.toString(StandardCharsets.UTF_8);
                    if (!text.isBlank()) {
                        try {
                            ExitTransactionRecord record = decodeFrame(text);
                            records.put(record.manifest().shutdownId(), record);
                        } catch (Exception e) {
                            throw new IllegalStateException(
                                    "退出事务 WAL 完整帧损坏，行 " + lineNumber, e);
                        }
                    }
                    line.reset();
                    lineStart = offset;
                }
                input.clear();
            }
            if (line.size() > 0) {
                channel.truncate(lineStart);
                channel.force(true);
            }
        } catch (IOException e) {
            throw new IllegalStateException("退出事务 WAL 读取失败: " + wal, e);
        }
    }

    private static ExitTransactionRecord decodeFrame(String line) {
        JsonObject frame = JsonUtil.parseObject(line);
        if (JsonUtil.getInt(frame, "version", -1) != FORMAT_VERSION) {
            throw new IllegalArgumentException("退出事务 WAL 版本不支持");
        }
        JsonElement rawRecord = frame.get("record");
        if (rawRecord == null || !rawRecord.isJsonObject()) {
            throw new IllegalArgumentException("退出事务 WAL 缺少 record");
        }
        byte[] payload = JsonUtil.GSON.toJson(rawRecord.getAsJsonObject())
                .getBytes(StandardCharsets.UTF_8);
        if (JsonUtil.getInt(frame, "payloadBytes", -1) != payload.length
                || !Sha256.hex(payload).equals(
                JsonUtil.getString(frame, "sha256", ""))) {
            throw new IllegalArgumentException("退出事务 WAL 完整性校验失败");
        }
        return decode(rawRecord.getAsJsonObject());
    }

    private static JsonObject encode(ExitTransactionRecord record) {
        ExitManifest manifest = record.manifest();
        JsonObject root = new JsonObject();
        root.addProperty("shutdownId", manifest.shutdownId().toString());
        root.addProperty("clientId", manifest.clientId().toString());
        root.addProperty("groupId", manifest.groupId().toString());
        root.addProperty("committed", record.committed());
        root.addProperty("dataApplied", record.dataApplied());
        root.addProperty("cleanupComplete", record.cleanupComplete());
        root.addProperty("abandoned", record.abandoned());
        JsonArray players = new JsonArray();
        manifest.playerIds().stream().map(UUID::toString).sorted().forEach(players::add);
        root.add("players", players);
        JsonArray chunks = new JsonArray();
        manifest.chunks().stream().map(ChunkKey::asString).sorted().forEach(chunks::add);
        root.add("chunks", chunks);
        if (record.writtenChunksDeclared()) {
            JsonArray writtenChunks = new JsonArray();
            record.writtenChunks().stream().map(ChunkKey::asString).sorted()
                    .forEach(writtenChunks::add);
            root.add("writtenChunks", writtenChunks);
        }
        return root;
    }

    private static ExitTransactionRecord decode(JsonObject root) {
        if (root == null) {
            throw new IllegalArgumentException("退出事务 JSON 为空");
        }
        UUID shutdownId = UUID.fromString(JsonUtil.getString(root, "shutdownId", ""));
        UUID clientId = UUID.fromString(JsonUtil.getString(root, "clientId", ""));
        UUID groupId = UUID.fromString(JsonUtil.getString(root, "groupId", ""));
        Set<UUID> players = new HashSet<>();
        for (JsonElement element : requiredArray(root, "players")) {
            players.add(UUID.fromString(element.getAsString()));
        }
        Set<ChunkKey> chunks = new HashSet<>();
        for (JsonElement element : requiredArray(root, "chunks")) {
            chunks.add(ChunkKey.parse(element.getAsString()));
        }
        ExitManifest manifest = new ExitManifest(
                shutdownId, clientId, groupId, players, chunks);
        boolean committed = JsonUtil.getBoolean(root, "committed", false);
        boolean cleanupComplete = JsonUtil.getBoolean(
                root, "cleanupComplete", false);
        boolean abandoned = JsonUtil.getBoolean(root, "abandoned", false);
        boolean dataApplied = root.has("dataApplied")
                ? JsonUtil.getBoolean(root, "dataApplied", false)
                : committed && !abandoned;
        boolean writtenChunksDeclared = root.has("writtenChunks");
        Set<ChunkKey> writtenChunks = new HashSet<>();
        if (writtenChunksDeclared) {
            for (JsonElement element : requiredArray(root, "writtenChunks")) {
                writtenChunks.add(ChunkKey.parse(element.getAsString()));
            }
        }
        return new ExitTransactionRecord(manifest, committed, writtenChunks,
                writtenChunksDeclared, dataApplied, cleanupComplete, abandoned);
    }

    private static JsonArray requiredArray(JsonObject root, String name) {
        JsonElement element = root.get(name);
        if (element == null || !element.isJsonArray()) {
            throw new IllegalArgumentException("退出事务缺少数组字段: " + name);
        }
        return element.getAsJsonArray();
    }
}
