package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 每个 shutdownId 一个原子 JSON 文件的退出事务持久化实现。 */
public final class ExitTransactionStore implements ExitTransactionPersistence {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/exit-store");

    private final Path directory;

    public ExitTransactionStore(Path directory) {
        this.directory = directory;
    }

    @Override
    public Map<UUID, ExitTransactionRecord> load() {
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
                    LOGGER.warn("退出事务文件读取失败，跳过: {}", file, e);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("无法读取退出事务目录: " + directory, e);
        }
        return records;
    }

    @Override
    public void save(ExitTransactionRecord record) {
        Path file = directory.resolve(record.manifest().shutdownId() + ".json");
        try {
            JsonUtil.writeFileAtomic(file, encode(record));
        } catch (Exception e) {
            throw new IllegalStateException("退出事务元数据落盘失败: " + file, e);
        }
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
                // 旧协议在 committed 前已经把全部最终帧直接写入正式世界；
                // 因此旧 committed 即使 cleanup 尚未完成，也不能再寻找新暂存目录。
                : committed && !abandoned;
        return new ExitTransactionRecord(manifest, committed, dataApplied,
                cleanupComplete, abandoned);
    }

    private static JsonArray requiredArray(JsonObject root, String name) {
        JsonElement element = root.get(name);
        if (element == null || !element.isJsonArray()) {
            throw new IllegalArgumentException("退出事务缺少数组字段: " + name);
        }
        return element.getAsJsonArray();
    }
}
