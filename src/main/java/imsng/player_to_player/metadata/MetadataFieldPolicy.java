package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.Tag;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** 原版 Data 字段分类以及可独立验证的启动、睡眠和渐变规则。 */
public final class MetadataFieldPolicy {
    private static final Set<String> STARTUP = Set.of("WorldGenSettings", "DataPacks", "enabled_features",
            "Version", "DataVersion", "version", "LevelName", "hardcore", "allowCommands", "initialized");
    private static final Set<String> RECORD = Set.of("LastPlayed", "SizeOnDisk", "WasModded", "ServerBrands",
            "removed_features", "confirmedExperimentalSettings");
    private static final Set<String> GLOBAL = Set.of("GameRules", "Difficulty", "DifficultyLocked", "GameType",
            "SpawnX", "SpawnY", "SpawnZ", "SpawnAngle", "DayTime", "clearWeatherTime", "rainTime", "raining",
            "thunderTime", "thundering", "CustomBossEvents", "ScheduledEvents", "WanderingTraderSpawnDelay",
            "WanderingTraderSpawnChance", "WanderingTraderId", "BorderCenterX", "BorderCenterZ", "BorderSize",
            "BorderSizeLerpTime", "BorderSizeLerpTarget", "BorderSafeZone", "BorderDamagePerBlock",
            "BorderWarningBlocks", "BorderWarningTime");
    private static final Map<String, MetadataAdapter.Ownership> EXTENSIONS = new ConcurrentHashMap<>();

    private MetadataFieldPolicy() { }

    public static MetadataAdapter.Ownership ownership(String field) {
        if (STARTUP.contains(field)) return MetadataAdapter.Ownership.STARTUP;
        if (RECORD.contains(field)) return MetadataAdapter.Ownership.RECORD;
        if (GLOBAL.contains(field)) return MetadataAdapter.Ownership.COORDINATOR;
        if (field.equals("Player")) return MetadataAdapter.Ownership.PLAYER;
        if (field.equals("Time")) return MetadataAdapter.Ownership.LOCAL_CLOCK;
        if (field.equals("DragonFight")) return MetadataAdapter.Ownership.SIMULATION_GROUP;
        return EXTENSIONS.getOrDefault(field, MetadataAdapter.Ownership.PRESERVED);
    }

    static void registerField(String field, MetadataAdapter.Ownership ownership) {
        if (field == null || field.isBlank() || ownership(field) != MetadataAdapter.Ownership.PRESERVED
                || EXTENSIONS.putIfAbsent(field, ownership) != null) {
            throw new IllegalArgumentException("元数据字段已被占用或无效: " + field);
        }
    }

    /**
     * 启动前以权威快照整体替换全局字段，避免缓存残留和已删除 Boss 血条复活。
     * 已有存档的模拟时钟及 Player 属独立生命周期，不受全局快照覆盖；新存档采用初始 Time。
     */
    public static CompoundTag startupData(CompoundTag local, CompoundTag authority) {
        CompoundTag result = authority.copy();
        result.remove("Player");
        if (local.contains("Player")) result.put("Player", local.get("Player").copy());
        if (local.contains("Time", Tag.TAG_ANY_NUMERIC)) result.putLong("Time", local.getLong("Time"));
        return result;
    }

    /** 按完整快照检测启动字段的修改和删除；这些字段无法安全地仅替换运行时保存镜像。 */
    public static Set<String> restartFields(CompoundTag running, CompoundTag proposed) {
        Set<String> keys = new HashSet<>(running.getAllKeys());
        keys.addAll(proposed.getAllKeys());
        keys.removeIf(key -> ownership(key) != MetadataAdapter.Ownership.STARTUP
                || Objects.equals(running.get(key), proposed.get(key)));
        return Set.copyOf(keys);
    }

    /** 与原版 SleepStatus 同样向上取整，至少需要一个深睡玩家，百分比大于 100 时禁止跳夜。 */
    public static boolean enoughSleeping(int eligiblePlayers, int deepSleepers, int percentage) {
        if (eligiblePlayers <= 0 || percentage > 100) return false;
        long required = Math.max(1L, ((long) eligiblePlayers * Math.max(0, percentage) + 99L) / 100L);
        return deepSleepers >= required;
    }

    /** 发送的是采样时的剩余时长；补发时扣除经过时间，时钟暂时回拨也不延长原始时长。 */
    public static long remainingLerp(long remaining, long sampledAtMillis, long nowMillis) {
        if (remaining <= 0) return 0;
        long elapsed = nowMillis > sampledAtMillis ? nowMillis - sampledAtMillis : 0;
        return elapsed >= remaining ? 0 : remaining - elapsed;
    }
}
