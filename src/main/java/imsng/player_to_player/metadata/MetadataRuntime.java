package imsng.player_to_player.metadata;

import com.mojang.serialization.Dynamic;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.mixin.MetadataLevelAccess;
import imsng.player_to_player.mixin.MetadataDragonAccess;
import imsng.player_to_player.mixin.MetadataTimerAccess;
import imsng.player_to_player.mixin.MetadataTraderAccess;
import net.minecraft.core.BlockPos;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtOps;
import net.minecraft.nbt.Tag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.bossevents.CustomBossEvent;
import net.minecraft.server.bossevents.CustomBossEvents;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.Difficulty;
import net.minecraft.world.entity.npc.WanderingTraderSpawner;
import net.minecraft.world.level.CustomSpawner;
import net.minecraft.world.level.GameRules;
import net.minecraft.world.level.GameType;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.border.WorldBorder;
import net.minecraft.world.level.dimension.end.EndDragonFight;
import net.minecraft.world.level.storage.ServerLevelData;
import net.minecraft.world.level.storage.WorldData;
import net.minecraft.world.level.storage.PrimaryLevelData;
import net.minecraft.world.level.timers.TimerQueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.Set;
import java.util.HashSet;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * level.dat 的运行对象适配层。调用方负责世界身份、版本、任期及耐久提交，本层只在游戏线程
 * 应用已确认状态，绝不覆盖运行中存档文件。Time 与 Player 永远不在运行时应用列表中。
 * apply 的 ThreadLocal 标记让命令/自然事件观察器辨别同步回放，防止二次上报。
 */
public final class MetadataRuntime {
    private static final List<MetadataAdapter> ADAPTERS = new CopyOnWriteArrayList<>();
    private static final Map<MinecraftServer, RuntimeState> STATES = Collections.synchronizedMap(new WeakHashMap<>());
    private static final ThreadLocal<Boolean> APPLYING = ThreadLocal.withInitial(() -> false);
    private static final ThreadLocal<Boolean> CAPTURING = ThreadLocal.withInitial(() -> false);
    private static final ThreadLocal<UUID> TRADER_TARGET = new ThreadLocal<>();

    private static final class RuntimeState {
        CompoundTag mirror = new CompoundTag();
        CompoundTag committed;
        CompoundTag lastBorder;
        boolean dragonAuthority;
        int traderTickDelay = 1200;
    }

    private MetadataRuntime() { }

    private static RuntimeState state(MinecraftServer server) {
        synchronized (STATES) { return STATES.computeIfAbsent(server, ignored -> new RuntimeState()); }
    }

    public static boolean isApplying() { return APPLYING.get(); }

    /** 原版 DragonFight 的归属由中心单独验证；扩展字段必须通过注册适配器显式授权。 */
    public static boolean canPropose(String field, UUID groupId, CompoundTag authority) {
        if (groupId == null) return false;
        for (MetadataAdapter adapter : ADAPTERS)
            if (adapter.fields().contains(field) && adapter.ownership() == MetadataAdapter.Ownership.SIMULATION_GROUP)
                return adapter.canWrite(groupId, authority.copy());
        return false;
    }

    /** 定期状态核对只检查显式声明的模拟字段，不对未知 NBT 推断写入权。 */
    public static Set<String> simulationFields() {
        Set<String> fields = new HashSet<>();
        for (MetadataAdapter adapter : ADAPTERS)
            if (adapter.ownership() == MetadataAdapter.Ownership.SIMULATION_GROUP) fields.addAll(adapter.fields());
        return Set.copyOf(fields);
    }

    /** 注册只能在启动期完成；字段命名冲突会立即报错，避免两个适配器争写。 */
    public static synchronized void registerAdapter(MetadataAdapter adapter) {
        Objects.requireNonNull(adapter);
        for (String field : adapter.fields()) {
            if (MetadataFieldPolicy.ownership(field) != MetadataAdapter.Ownership.PRESERVED)
                throw new IllegalArgumentException("字段已有原版或模组适配器: " + field);
        }
        for (String field : adapter.fields()) MetadataFieldPolicy.registerField(field, adapter.ownership());
        ADAPTERS.add(adapter);
    }

    /** 创建启动 Data，先隔离独立玩家/模拟时钟，再允许扩展准备自己的派生字段。 */
    public static CompoundTag startupData(CompoundTag localData, CompoundTag authority) {
        CompoundTag result = MetadataFieldPolicy.startupData(localData, authority);
        for (MetadataAdapter adapter : ADAPTERS) adapter.applyStartup(result, authority.copy());
        // 扩展也不得越过玩家数据和模拟时钟的边界。
        result.remove("Player");
        if (localData.contains("Player")) result.put("Player", localData.get("Player").copy());
        if (localData.contains("Time", Tag.TAG_ANY_NUMERIC)) result.putLong("Time", localData.getLong("Time"));
        return result;
    }

    /**
     * 从内存采集当前 Data。Boss/边界/龙的保存镜像只有原版保存时才刷新，故先从真实对象取值。
     * 未知字段以最近快照的镜像保留；原版 createTag 不认识它们，不能把一次自动保存当作删除。
     */
    public static CompoundTag capture(MinecraftServer server) {
        checkThread(server);
        WorldData world = server.getWorldData();
        world.setCustomBossEvents(server.getCustomBossEvents().save());
        if (server.overworld() != null) world.overworldData().setWorldBorder(server.overworld().getWorldBorder().createSettings());
        ServerLevel end = server.getLevel(Level.END);
        if (hasDragonAuthority(server) && end != null && end.getDragonFight() != null)
            world.setEndDragonFightData(end.getDragonFight().saveData());
        CompoundTag data;
        boolean wasCapturing = CAPTURING.get();
        CAPTURING.set(true);
        try { data = world.createTag(server.registryAccess(), null); }
        finally { CAPTURING.set(wasCapturing); }
        data.remove("Player");
        RuntimeState state = state(server);
        for (String key : state.mirror.getAllKeys()) {
            if (MetadataFieldPolicy.ownership(key) == MetadataAdapter.Ownership.PRESERVED && !data.contains(key))
                data.put(key, state.mirror.get(key).copy());
        }
        for (MetadataAdapter adapter : ADAPTERS) {
            CompoundTag extension = adapter.capture(server);
            for (String key : extension.getAllKeys()) {
                if (!adapter.fields().contains(key)) throw new IllegalStateException("适配器采集了未声明字段: " + key);
                data.put(key, extension.get(key).copy());
            }
        }
        return data;
    }

    public static void apply(MinecraftServer server, CompoundTag authoritativeData) {
        apply(server, authoritativeData, authoritativeData.contains("__p2pSampledAtMillis", Tag.TAG_ANY_NUMERIC)
                ? authoritativeData.getLong("__p2pSampledAtMillis") : System.currentTimeMillis());
    }

    /** sampledAtMillis 为发送端采样时刻；重复相同边界镜像不会重新启动渐变。 */
    public static void apply(MinecraftServer server, CompoundTag authoritativeData, long sampledAtMillis) {
        checkThread(server);
        boolean previous = APPLYING.get();
        APPLYING.set(true);
        try {
            CompoundTag data = authoritativeData.copy(); data.remove("Player");
            RuntimeState state = state(server);
            // 首次应用校验真实内存中已经打开的生成/数据包配置；后续按最近成功快照比较。
            // 必须在任何 setter 之前失败，避免一半旧生成器、一半新规则的混合运行状态。
            CompoundTag running = state.mirror.isEmpty() ? capture(server) : state.mirror;
            Set<String> restart = MetadataFieldPolicy.restartFields(running, data);
            if (!restart.isEmpty()) throw new MetadataRestartRequiredException(restart);
            WorldData world = server.getWorldData();
            ServerLevelData levelData = world.overworldData();
            if (data.contains("GameRules", Tag.TAG_COMPOUND)) {
                GameRules rules = new GameRules(new Dynamic<>(NbtOps.INSTANCE, data.getCompound("GameRules")));
                // setFrom 触发 onChanged；只应用实际变化的键，避免每秒快照重复发送调试/重生通知。
                GameRules.visitGameRuleTypes(new GameRules.GameRuleTypeVisitor() {
                    @Override public <T extends GameRules.Value<T>> void visit(GameRules.Key<T> key, GameRules.Type<T> type) {
                        T current = world.getGameRules().getRule(key), incoming = rules.getRule(key);
                        if (!current.serialize().equals(incoming.serialize())) current.setFrom(incoming, server);
                    }
                });
            }
            if (data.contains("Difficulty", Tag.TAG_ANY_NUMERIC)
                    && world.getDifficulty() != Difficulty.byId(data.getByte("Difficulty")))
                server.setDifficulty(Difficulty.byId(data.getByte("Difficulty")), true);
            if (data.contains("DifficultyLocked") && world.isDifficultyLocked() != data.getBoolean("DifficultyLocked"))
                server.setDifficultyLocked(data.getBoolean("DifficultyLocked"));
            if (data.contains("GameType", Tag.TAG_ANY_NUMERIC) && world.getGameType() != GameType.byId(data.getInt("GameType"))) {
                server.setDefaultGameType(GameType.byId(data.getInt("GameType")));
                // DefaultGameModeCommands 的原版副作用：force-gamemode 开启时还要更新在线玩家。
                GameType forced = server.getForcedGameType();
                if (forced != null) for (ServerPlayer player : server.getPlayerList().getPlayers()) player.setGameMode(forced);
            }
            ServerLevel overworld = server.overworld();
            if (overworld != null && data.contains("SpawnX", Tag.TAG_ANY_NUMERIC)) {
                BlockPos spawn = new BlockPos(data.getInt("SpawnX"), data.getInt("SpawnY"), data.getInt("SpawnZ"));
                if (!spawn.equals(overworld.getSharedSpawnPos()) || data.getFloat("SpawnAngle") != overworld.getSharedSpawnAngle())
                    overworld.setDefaultSpawnPos(spawn, data.getFloat("SpawnAngle"));
            }
            if (data.contains("DayTime", Tag.TAG_ANY_NUMERIC)) levelData.setDayTime(data.getLong("DayTime"));
            if (data.contains("clearWeatherTime")) levelData.setClearWeatherTime(data.getInt("clearWeatherTime"));
            if (data.contains("rainTime")) levelData.setRainTime(data.getInt("rainTime"));
            if (data.contains("thunderTime")) levelData.setThunderTime(data.getInt("thunderTime"));
            if (data.contains("raining")) levelData.setRaining(data.getBoolean("raining"));
            if (data.contains("thundering")) levelData.setThundering(data.getBoolean("thundering"));
            applyBorder(server, state, data, sampledAtMillis);
            applyBossEvents(server, data.getCompound("CustomBossEvents"));
            applyScheduledEvents(levelData.getScheduledEvents(), data.getList("ScheduledEvents", Tag.TAG_COMPOUND));
            applyTrader(server, data);
            if (data.contains("DragonFight", Tag.TAG_COMPOUND)) {
                EndDragonFight.Data dragon = EndDragonFight.Data.CODEC.parse(NbtOps.INSTANCE, data.getCompound("DragonFight"))
                        .result().orElseThrow(() -> new IllegalArgumentException("无效的 DragonFight NBT"));
                world.setEndDragonFightData(dragon);
                // 有权组继续运行其真实战斗对象；普通同步不能重置扫描计时器或正在进行的复活。
                ServerLevel end = server.getLevel(Level.END);
                if (!state.dragonAuthority && end != null && (end.getDragonFight() == null
                        || !end.getDragonFight().saveData().equals(dragon)))
                    end.setDragonFight(new EndDragonFight(end, world.worldGenOptions().seed(), dragon));
            }
            for (MetadataAdapter adapter : ADAPTERS) {
                if (adapter.ownership() == MetadataAdapter.Ownership.COORDINATOR
                        || adapter.ownership() == MetadataAdapter.Ownership.SIMULATION_GROUP)
                    adapter.applyRuntime(server, data.copy());
            }
            state.mirror = data;
        } finally { APPLYING.set(previous); }
    }

    private static void applyBorder(MinecraftServer server, RuntimeState state, CompoundTag data, long sampledAt) {
        if (server.overworld() == null || !data.contains("BorderSize", Tag.TAG_ANY_NUMERIC)) return;
        CompoundTag borderData = new CompoundTag();
        for (String key : data.getAllKeys()) if (key.startsWith("Border")) borderData.put(key, data.get(key).copy());
        if (borderData.equals(state.lastBorder)) return;
        WorldBorder border = server.overworld().getWorldBorder();
        applyBorderSettings(border, borderData, sampledAt, System.currentTimeMillis());
        server.getWorldData().overworldData().setWorldBorder(border.createSettings());
        state.lastBorder = borderData;
    }

    /** 直接操作原版边界对象，供运行入口和无游戏进程的对象级回归测试共用。 */
    static void applyBorderSettings(WorldBorder border, CompoundTag borderData, long sampledAt, long nowMillis) {
        WorldBorder.Settings settings = WorldBorder.Settings.read(new Dynamic<>(NbtOps.INSTANCE, borderData), border.createSettings());
        long remaining = MetadataFieldPolicy.remainingLerp(settings.getSizeLerpTime(), sampledAt, nowMillis);
        border.setCenter(settings.getCenterX(), settings.getCenterZ());
        border.setDamagePerBlock(settings.getDamagePerBlock()); border.setDamageSafeZone(settings.getSafeZone());
        border.setWarningBlocks(settings.getWarningBlocks()); border.setWarningTime(settings.getWarningTime());
        if (remaining > 0) {
            double progress = 1.0 - (double) remaining / settings.getSizeLerpTime();
            double size = settings.getSize() + (settings.getSizeLerpTarget() - settings.getSize()) * progress;
            border.lerpSizeBetween(size, settings.getSizeLerpTarget(), remaining);
        } else border.setSize(settings.getSizeLerpTime() > 0 ? settings.getSizeLerpTarget() : settings.getSize());
    }

    private static void applyBossEvents(MinecraftServer server, CompoundTag incoming) {
        replaceBossEvents(server.getCustomBossEvents(), incoming, server.getPlayerList().getPlayers());
        server.getWorldData().setCustomBossEvents(incoming.copy());
    }

    static void replaceBossEvents(CustomBossEvents events, CompoundTag incoming, List<ServerPlayer> localPlayers) {
        if (events.save().equals(incoming)) return;
        // load 只 put 不清空，必须显式移除旧事件及客户端可见条，再加载 UUID 名单。
        for (CustomBossEvent event : new ArrayList<>(events.getEvents())) { event.removeAllPlayers(); events.remove(event); }
        events.load(incoming.copy());
        for (ServerPlayer player : localPlayers) events.onPlayerConnect(player);
    }

    private static void applyScheduledEvents(TimerQueue<MinecraftServer> queue, ListTag events) {
        if (queue.store().equals(events)) return;
        for (String id : new ArrayList<>(queue.getEventsIds())) queue.remove(id);
        for (Tag event : events) ((MetadataTimerAccess) queue).player_to_player$loadEvent(((CompoundTag) event).copy());
    }

    /** 协调端先记录持久化派发状态，再用此入口删除已派发事件，同时保留同名的未来事件。 */
    public static void replaceScheduled(MinecraftServer server, ListTag events) {
        checkThread(server);
        applyScheduledEvents(server.getWorldData().overworldData().getScheduledEvents(), events.copy());
    }

    private static void applyTrader(MinecraftServer server, CompoundTag data) {
        ServerLevelData world = server.getWorldData().overworldData();
        if (data.contains("WanderingTraderSpawnDelay")) world.setWanderingTraderSpawnDelay(data.getInt("WanderingTraderSpawnDelay"));
        if (data.contains("WanderingTraderSpawnChance")) world.setWanderingTraderSpawnChance(data.getInt("WanderingTraderSpawnChance"));
        world.setWanderingTraderId(data.hasUUID("WanderingTraderId") ? data.getUUID("WanderingTraderId") : null);
        if (server.overworld() != null) {
            for (CustomSpawner spawner : ((MetadataLevelAccess) server.overworld()).player_to_player$customSpawners()) {
                if (spawner instanceof WanderingTraderSpawner) {
                    MetadataTraderAccess cache = (MetadataTraderAccess) spawner;
                    cache.player_to_player$spawnDelay(world.getWanderingTraderSpawnDelay());
                    cache.player_to_player$spawnChance(world.getWanderingTraderSpawnChance());
                    cache.player_to_player$tickDelay(state(server).traderTickDelay);
                }
            }
        }
    }

    /**
     * 协调端每个逻辑 tick 调用，全部操作均为轻量全局状态，无区块生成、实体或随机 tick。
     * TimerQueue 不在此执行：持久化派发器必须先记录稳定调用 ID 后才能调用函数。
     */
    public static void advanceAuthority(MinecraftServer server) {
        checkThread(server);
        ServerLevel overworld = server.overworld();
        if (overworld == null) return;
        ServerLevelData data = server.getWorldData().overworldData();
        data.setGameTime(data.getGameTime() + 1L);
        if (server.getGameRules().getBoolean(GameRules.RULE_DAYLIGHT)) data.setDayTime(data.getDayTime() + 1L);
        ((MetadataLevelAccess) overworld).player_to_player$advanceWeatherCycle();
        overworld.getWorldBorder().tick();
    }

    /**
     * 原版每 1200 tick 推进一次商人机会；机会通过后交给协调端选定一个全局玩家并派发。
     * 这里仅改变保存字段，绝不在协调端尝试找地面/生成实体。
     */
    public static boolean advanceTraderChance(MinecraftServer server) {
        checkThread(server);
        if (!server.getGameRules().getBoolean(GameRules.RULE_DO_TRADER_SPAWNING)) return false;
        RuntimeState runtime = state(server);
        if (--runtime.traderTickDelay > 0) return false;
        runtime.traderTickDelay = 1200;
        ServerLevelData data = server.getWorldData().overworldData();
        int delay = data.getWanderingTraderSpawnDelay() - 1200;
        data.setWanderingTraderSpawnDelay(delay);
        if (delay > 0) return false;
        data.setWanderingTraderSpawnDelay(24000);
        if (!server.getGameRules().getBoolean(GameRules.RULE_DOMOBSPAWNING)) return false;
        int oldChance = data.getWanderingTraderSpawnChance();
        data.setWanderingTraderSpawnChance(Math.max(25, Math.min(75, oldChance + 25)));
        return server.overworld().random.nextInt(100) <= oldChance;
    }

    public static UUID traderTarget() { return TRADER_TARGET.get(); }

    /** 派发方须先去重/检查任期；该方法只执行一次原版生成位置尝试及其 1/10 二次概率。 */
    public static boolean attemptTraderSpawn(MinecraftServer server, UUID playerId) {
        checkThread(server);
        ServerPlayer player = server.getPlayerList().getPlayer(playerId);
        if (player == null || player.serverLevel() != server.overworld()) return false;
        if (!server.getGameRules().getBoolean(GameRules.RULE_DO_TRADER_SPAWNING)
                || !server.getGameRules().getBoolean(GameRules.RULE_DOMOBSPAWNING)) return false;
        TRADER_TARGET.set(playerId);
        try {
            for (CustomSpawner spawner : ((MetadataLevelAccess) server.overworld()).player_to_player$customSpawners())
                if (spawner instanceof WanderingTraderSpawner)
                    return ((MetadataTraderAccess) spawner).player_to_player$spawn(server.overworld());
            return false;
        } finally { TRADER_TARGET.remove(); }
    }

    /** 每名真实玩家独立上报，协调端按 UUID 和当前组归属去重，不能相加组内缓存计数。 */
    public static CompoundTag sleepReport(MinecraftServer server) {
        checkThread(server);
        CompoundTag report = new CompoundTag(); ListTag players = new ListTag();
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            CompoundTag entry = new CompoundTag();
            entry.putUUID("uuid", player.getUUID());
            entry.putString("dimension", player.serverLevel().dimension().location().toString());
            entry.putBoolean("eligible", !player.isSpectator());
            entry.putBoolean("sleeping", player.isSleeping());
            entry.putBoolean("deep", player.isSleepingLongEnough());
            entry.putBoolean("canSkip", player.serverLevel().canSleepThroughNights());
            players.add(entry);
        }
        report.put("Players", players); return report;
    }

    public static void wakeSleepingPlayers(MinecraftServer server) {
        checkThread(server);
        for (ServerLevel level : server.getAllLevels())
            if (level.canSleepThroughNights()) ((MetadataLevelAccess) level).player_to_player$wakeUpAllPlayers();
    }

    public static boolean hasDragonAuthority(MinecraftServer server) { return state(server).dragonAuthority; }

    /** 只能在所有权版本屏障确认之后授权；默认全部组均为只读镜像，失联必须立即撤销授权。 */
    public static void setDragonAuthority(MinecraftServer server, boolean granted) {
        checkThread(server);
        RuntimeState state = state(server);
        if (granted && !state.dragonAuthority) {
            assertTransferSafe(server);
            EndDragonFight.Data data = server.getWorldData().endDragonFightData();
            if (data.isRespawning()) throw new IllegalStateException("末影龙复活中间态不能仅凭字段快照接管");
            ServerLevel end = server.getLevel(Level.END);
            if (end != null && (end.getDragonFight() == null || !end.getDragonFight().saveData().equals(data)))
                end.setDragonFight(new EndDragonFight(end, server.getWorldData().worldGenOptions().seed(), data));
        }
        if (!granted && state.dragonAuthority) {
            ServerLevel end = server.getLevel(Level.END);
            if (end != null && end.getDragonFight() != null)
                ((MetadataDragonAccess) end.getDragonFight()).player_to_player$dragonEvent().removeAllPlayers();
        }
        state.dragonAuthority = granted;
    }

    /** 字段 ACK 不等于迁移完整；原版快照缺少复活动画计数及水晶对象引用，此时明确拒绝。 */
    public static void assertTransferSafe(MinecraftServer server) {
        checkThread(server);
        ServerLevel end = server.getLevel(Level.END);
        if (end != null && end.getDragonFight() != null && end.getDragonFight().saveData().isRespawning())
            throw new IllegalStateException("末影龙复活中间态尚不能完整迁移，请保持旧模拟组运行");
    }

    public static void forget(MinecraftServer server) { STATES.remove(server); }

    /** 耐久提交成功后设置保存镜像；写日志失败时不得调用，更不能把内存候选当作已提交状态。 */
    public static void setCommittedData(MinecraftServer server, CompoundTag data) {
        checkThread(server);
        state(server).committed = data.copy();
    }

    /**
     * 原版保存入口的最后一道收口：中心保存已提交镜像，计算端保存最后收到的权威镜像。
     * 原版序列化不认识的模组字段也随镜像保留；计算端 Time 和任何 Player 始终保留独立原值。
     * 内部持久化路由记录只属于元数据 WAL，不能污染用户的原版 level.dat。
     */
    public static CompoundTag dataForVanillaSave(PrimaryLevelData owner, CompoundTag original) {
        if (CAPTURING.get()) return original;
        synchronized (STATES) {
            for (Map.Entry<MinecraftServer, RuntimeState> entry : STATES.entrySet()) {
                MinecraftServer server = entry.getKey();
                if (server.getWorldData() != owner) continue;
                RuntimeState state = entry.getValue();
                CompoundTag authority = NodeContext.get().isServer() ? state.committed : state.mirror;
                if (authority == null || authority.isEmpty()) return original;
                CompoundTag result = authority.copy();
                for (String key : new ArrayList<>(result.getAllKeys()))
                    if (key.startsWith("__p2p")) result.remove(key);
                result.remove("Player");
                if (original.contains("Player")) result.put("Player", original.get("Player").copy());
                if (!NodeContext.get().isServer() && original.contains("Time"))
                    result.put("Time", original.get("Time").copy());
                if (original.contains("LastPlayed")) result.put("LastPlayed", original.get("LastPlayed").copy());
                return result;
            }
        }
        return original;
    }

    private static void checkThread(MinecraftServer server) {
        if (!server.isSameThread()) throw new IllegalStateException("世界元数据只能在所属服务器线程采集或应用");
    }
}
