package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 集成服停止后用于耐久退出提交的完整不可变数据包。
 * ownership manifest 保持完整，但区块 payload 只包含最后一次物理服确认之后仍未确认的版本；
 * 未出现在 written 集合中的清单区块隐式保留服务端现状，不再逐项生成 preserve marker。
 */
public final class ExitBundle {

    private final ExitManifest manifest;
    private final Map<UUID, byte[]> players;
    private final Map<ChunkKey, byte[]> chunks;

    public ExitBundle(ExitManifest manifest, Map<UUID, byte[]> players,
                      Map<ChunkKey, byte[]> chunks) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.players = copyBytes(players);
        this.chunks = copyBytes(chunks);

        if (!this.players.keySet().equals(manifest.playerIds())) {
            throw new IllegalArgumentException("最终玩家数据与退出清单不一致");
        }
        if (!manifest.chunks().containsAll(this.chunks.keySet())) {
            throw new IllegalArgumentException("written 区块必须是退出 ownership 清单的子集");
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

    public byte[] playerData(UUID playerId) {
        return copy(players.get(playerId));
    }

    public byte[] chunkData(ChunkKey key) {
        return copy(chunks.get(key));
    }

    /** 返回当前不可变快照中所有已压缩 payload 的总字节数，不复制二进制内容。 */
    public long compressedBytes() {
        long total = 0;
        for (byte[] data : players.values()) {
            total += data.length;
        }
        for (byte[] data : chunks.values()) {
            total += data.length;
        }
        return total;
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
