package imsng.player_to_player.metadata;

/**
 * 网络补齐期间的版本屏障。快照请求发出后仍可能观察到更晚的补丁；即使旧请求成功，
 * 也必须追到已观察到的最高版本才可开放模拟。这里只保存版本，不接触游戏对象。
 */
final class MetadataVersionBarrier {
    private final String worldId;
    private long term;
    private long version;

    MetadataVersionBarrier(MetadataSnapshot initial) {
        worldId = initial.worldId(); term = initial.term(); version = initial.version();
    }

    synchronized void observe(MetadataPatch patch) {
        if (!worldId.equals(patch.worldId()) || patch.term() < term) return;
        if (patch.term() > term) { term = patch.term(); version = patch.version(); }
        else version = Math.max(version, patch.version());
    }

    synchronized boolean allows(MetadataSnapshot applied, MetadataSnapshot confirmed) {
        return worldId.equals(applied.worldId()) && worldId.equals(confirmed.worldId())
                && applied.term() == confirmed.term() && confirmed.version() <= applied.version()
                && (applied.term() > term || (applied.term() == term && applied.version() >= version));
    }
}
