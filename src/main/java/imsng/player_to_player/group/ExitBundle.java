package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 集成服停止后用于耐久退出提交的完整不可变数据包。
 * 每个清单区块必须恰好选择“上传最终 NBT”或“保留服务端已有状态”之一。
 */
public final class ExitBundle {

    private final ExitManifest manifest;
    private final Map<UUID, byte[]> players;
    private final Map<ChunkKey, byte[]> chunks;
    private final Set<ChunkKey> preserveExisting;

    public ExitBundle(ExitManifest manifest, Map<UUID, byte[]> players,
                      Map<ChunkKey, byte[]> chunks,
                      Set<ChunkKey> preserveExisting) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.players = copyBytes(players);
        this.chunks = copyBytes(chunks);
        this.preserveExisting = Set.copyOf(
                Objects.requireNonNull(preserveExisting, "preserveExisting"));

        if (!this.players.keySet().equals(manifest.playerIds())) {
            throw new IllegalArgumentException("最终玩家数据与退出清单不一致");
        }
        Set<ChunkKey> allChunks = new HashSet<>(this.chunks.keySet());
        if (!java.util.Collections.disjoint(allChunks, this.preserveExisting)) {
            throw new IllegalArgumentException("同一区块不能同时上传并保留旧状态");
        }
        allChunks.addAll(this.preserveExisting);
        if (!allChunks.equals(manifest.chunks())) {
            throw new IllegalArgumentException("最终区块数据与退出清单不一致");
        }
    }

    public ExitManifest manifest() {
        return manifest;
    }

    public Set<UUID> playerIds() {
        return players.keySet();
    }

    public Set<ChunkKey> writtenChunks() {
        return chunks.keySet();
    }

    public Set<ChunkKey> preserveExistingChunks() {
        return preserveExisting;
    }

    public byte[] playerData(UUID playerId) {
        return copy(players.get(playerId));
    }

    public byte[] chunkData(ChunkKey key) {
        return copy(chunks.get(key));
    }

    private static <K> Map<K, byte[]> copyBytes(Map<K, byte[]> source) {
        Objects.requireNonNull(source, "source");
        Map<K, byte[]> copied = new HashMap<>();
        for (Map.Entry<K, byte[]> entry : source.entrySet()) {
            byte[] value = Objects.requireNonNull(entry.getValue(), "binary payload");
            copied.put(entry.getKey(), value.clone());
        }
        return Map.copyOf(copied);
    }

    private static byte[] copy(byte[] value) {
        return value != null ? value.clone() : null;
    }
}

