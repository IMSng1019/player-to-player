package imsng.player_to_player.metadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

/** 跨组睡眠的纯判定：按玩家身份去重，旁观者及其他维度不计入分母。 */
public final class MetadataSleepConsensus {
    public record Player(UUID id, String dimension, boolean eligible, boolean deepSleeping, boolean canSkip) { }

    private MetadataSleepConsensus() { }

    public static boolean canSkip(Collection<Player> players, String dimension, int percentage) {
        if (percentage > 100) return false;
        var unique = new HashMap<UUID, Player>();
        for (Player player : players) {
            if (dimension.equals(player.dimension()) && player.eligible()) unique.put(player.id(), player);
        }
        long sleeping = unique.values().stream().filter(p -> p.deepSleeping() && p.canSkip()).count();
        // 原版至少要求一名玩家真正睡熟，即使 playersSleepingPercentage 为零。
        int needed = Math.max(1, (int) Math.ceil(unique.size() * Math.max(0, percentage) / 100.0));
        return sleeping >= needed;
    }
}
