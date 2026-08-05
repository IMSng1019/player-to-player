package imsng.player_to_player.server;

import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.group.ExitPayloadProtocol;
import imsng.player_to_player.registry.ChunkKey;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 一次主客户端退出的完整耐久载荷。
 *
 * <p>该对象同时包含权威退出清单、全部玩家最终 NBT 和实际发生变化的区块尾部。
 * 它会作为一个逻辑 WAL 事务写成若干有界数据片和一个终结标记，并一次性 force；
 * 因此客户端收到最终 ACK 后，即使服务端随即崩溃，也能只依赖该事务恢复玩家数据、
 * 区块最新索引和 ownership 释放意图。</p>
 */
public record ExitCommitPayload(ExitManifest manifest,
                                Map<UUID, byte[]> playerData,
                                List<ChunkDelta> chunkDeltas) {

    public ExitCommitPayload {
        Objects.requireNonNull(manifest, "manifest");
        Objects.requireNonNull(playerData, "playerData");
        Objects.requireNonNull(chunkDeltas, "chunkDeltas");
        if (!playerData.keySet().equals(manifest.playerIds())) {
            throw new IllegalArgumentException("退出玩家载荷必须与清单完全一致");
        }

        LinkedHashMap<UUID, byte[]> copiedPlayers = new LinkedHashMap<>();
        playerData.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    byte[] payload = entry.getValue();
                    if (payload == null || payload.length == 0
                            || payload.length > ExitPayloadProtocol.MAX_ASSEMBLED_BYTES) {
                        throw new IllegalArgumentException("退出玩家载荷为空或超出上限");
                    }
                    copiedPlayers.put(entry.getKey(), payload.clone());
                });

        ArrayList<ChunkDelta> copiedChunks = new ArrayList<>(chunkDeltas.size());
        java.util.HashSet<ChunkKey> keys = new java.util.HashSet<>();
        chunkDeltas.stream()
                .sorted(Comparator.comparing(delta -> delta.key().asString()))
                .forEach(delta -> {
                    Objects.requireNonNull(delta, "chunkDelta");
                    if (!manifest.chunks().contains(delta.key())) {
                        throw new IllegalArgumentException(
                                "退出区块载荷超出 ownership 清单: "
                                        + delta.key().asString());
                    }
                    if (!keys.add(delta.key())) {
                        throw new IllegalArgumentException(
                                "退出区块载荷含重复区块: " + delta.key().asString());
                    }
                    copiedChunks.add(new ChunkDelta(
                            delta.key(), delta.version(), delta.payload(), delta.sha256()));
                });

        playerData = Map.copyOf(copiedPlayers);
        chunkDeltas = List.copyOf(copiedChunks);
    }

    /** 返回防御性复制，避免后台物化代码修改已经耐久确认的玩家载荷。 */
    @Override
    public Map<UUID, byte[]> playerData() {
        LinkedHashMap<UUID, byte[]> copy = new LinkedHashMap<>();
        playerData.forEach((id, payload) -> copy.put(id, payload.clone()));
        return Map.copyOf(copy);
    }

    /** 实际写入 WAL 的区块集合；manifest 中其余区块沿用运行期最新耐久快照。 */
    public Set<ChunkKey> writtenChunks() {
        java.util.HashSet<ChunkKey> result = new java.util.HashSet<>();
        chunkDeltas.forEach(delta -> result.add(delta.key()));
        return Set.copyOf(result);
    }
}
