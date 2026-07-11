package imsng.player_to_player.registry;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * 区块注册表的本地 JSON 后端（默认；Phase 1 起的既有行为，Phase 4 抽成
 * {@link RegistryStore} 实现，为 MySQL 后端让出插槽）。
 * <p>
 * 布局：{@code player-to-player/registry/<维度>.json}（维度名经
 * {@link P2PPaths#sanitize} 合法化），每个文件一个 {@code claims} 对象
 * （区块键 → 占用组 UUID），原子写防半写损坏。
 */
public final class JsonRegistryStore implements RegistryStore {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/registry");

    /** 持久化目录（{@code player-to-player/registry/}）。 */
    private final Path persistDir;

    public JsonRegistryStore(Path persistDir) {
        this.persistDir = persistDir;
    }

    @Override
    public Map<ChunkKey, UUID> load() {
        Map<ChunkKey, UUID> result = new HashMap<>();
        if (!Files.isDirectory(persistDir)) {
            return result; // 首次运行还没有注册表目录
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(persistDir, "*.json")) {
            for (Path file : stream) {
                loadFile(file, result);
            }
        } catch (IOException e) {
            LOGGER.error("区块注册表目录读取失败: {}", persistDir, e);
        }
        return result;
    }

    /** 读单个维度文件；单条坏记录跳过不中断。 */
    private void loadFile(Path file, Map<ChunkKey, UUID> into) {
        try {
            JsonObject root = JsonUtil.readFile(file, JsonObject.class);
            if (root == null || !root.has("claims") || !root.get("claims").isJsonObject()) {
                return;
            }
            JsonObject claimsObj = root.getAsJsonObject("claims");
            for (String keyString : claimsObj.keySet()) {
                try {
                    into.put(ChunkKey.parse(keyString),
                            UUID.fromString(claimsObj.get(keyString).getAsString()));
                } catch (Exception e) {
                    LOGGER.warn("跳过注册表坏记录: {} ({})", keyString, e.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("注册表文件读取失败，跳过: {}", file, e);
        }
    }

    /**
     * 按维度分组写 {@code <维度>.json}（原子写）。
     * 本轮已无占用的维度删除旧文件（等价写空），防止重启复活已释放的占用。
     */
    @Override
    public void save(Map<ChunkKey, UUID> snapshot) {
        // 按维度分组（TreeMap：输出键序稳定，便于人工 diff）
        Map<String, Map<String, String>> byDimension = new TreeMap<>();
        for (Map.Entry<ChunkKey, UUID> e : snapshot.entrySet()) {
            byDimension.computeIfAbsent(e.getKey().dimension(), d -> new TreeMap<>())
                    .put(e.getKey().asString(), e.getValue().toString());
        }
        try {
            Files.createDirectories(persistDir);
            List<Path> existing = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(persistDir, "*.json")) {
                stream.forEach(existing::add);
            }
            for (Map.Entry<String, Map<String, String>> e : byDimension.entrySet()) {
                Path file = persistDir.resolve(P2PPaths.sanitize(e.getKey()) + ".json");
                JsonObject root = new JsonObject();
                JsonObject claimsObj = new JsonObject();
                e.getValue().forEach(claimsObj::addProperty);
                root.add("claims", claimsObj);
                JsonUtil.writeFileAtomic(file, root);
                existing.remove(file.toAbsolutePath().normalize());
                existing.remove(file);
            }
            for (Path stale : existing) {
                // 该维度已无任何占用：删除旧文件（见方法 Javadoc）
                Files.deleteIfExists(stale);
            }
        } catch (IOException e) {
            LOGGER.error("区块注册表落盘失败: {}", persistDir, e);
        }
    }

    @Override
    public String describe() {
        return "本地 JSON (" + persistDir + ")";
    }
}
