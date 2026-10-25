package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import com.mojang.authlib.GameProfile;
import imsng.player_to_player.group.CommandFeedback;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.server.GroupTable;
import imsng.player_to_player.server.HelloHandler;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.world.WorldIdentity;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.core.registries.Registries;
import net.minecraft.nbt.*;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.GameRules;
import net.minecraft.world.level.storage.LevelResource;
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * 中心的世界元数据协调器。所有世界对象只在服务器线程读写；网络线程仅投递任务。
 * 离散请求先得到原版字段结果，再写入带 force 的日志，只有提交成功才返回成功响应。
 * 自然时间每秒检查点，恢复先应用日志领先于原版 level.dat 的状态。
 */
public final class MetadataService implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/metadata");
    private static volatile MetadataService current;
    private final MinecraftServer server;
    private final GroupTable groups;
    private final ChunkRegistry registry;
    private final imsng.player_to_player.registry.PlayerTable players;
    private final MetadataStore store;
    private final Map<UUID, Long> acknowledgements = new HashMap<>();
    private final Map<UUID, SleepReport> sleepReports = new HashMap<>();
    private final Map<UUID, SafetyReport> transferSafety = new java.util.concurrent.ConcurrentHashMap<>();
    private record SafetyReport(long tick, long term, boolean safe) { }
    private final MetadataSchedules schedules;
    private record SleepReport(long tick, List<MetadataSleepConsensus.Player> players) { }
    private static final class StaleTerm extends IllegalStateException {
        StaleTerm() { super("写入任期已过期，请重新获取快照"); }
    }
    private volatile long ticks;
    private boolean closed;
    private boolean executingRequest;
    private String topology = "";
    private UUID dragonOwner;
    private boolean traderPending;
    private UUID commandPlayer;
    private List<UUID> commandBossPlayers;

    public MetadataService(MinecraftServer server, GroupTable groups, ChunkRegistry registry,
                           imsng.player_to_player.registry.PlayerTable players,
                           HandlerRegistry handlers, Path directory) throws IOException {
        this.server = server;
        this.groups = groups;
        this.registry = registry;
        this.players = players;
        CompoundTag imported = new CompoundTag();
        Path level = server.getWorldPath(LevelResource.ROOT).resolve("level.dat");
        if (Files.isRegularFile(level)) imported = NbtIo.readCompressed(level, NbtAccounter.create(64L * 1024 * 1024)).getCompound("Data");
        // 仅首次导入读磁盘保留未知字段，运行中始终从内存采集。
        imported.merge(MetadataRuntime.capture(server));
        imported.remove("Player");
        CompoundTag root = new CompoundTag();
        root.put("Data", imported);
        String worldId = WorldIdentity.ofLevelData(root).orElseThrow(() -> new IOException("无法确定元数据世界身份"));
        store = MetadataStore.open(directory.resolve(worldId), worldId, imported);
        // 管理员停服后启用的数据包等启动配置已由当前Minecraft成功加载；将它们作为
        // 本次启动的新权威配置提交。运行中的计算实例遇到差异必须重启，不能仅修改镜像。
        CompoundTag startup = new CompoundTag();
        for (String field : imported.getAllKeys()) {
            if (MetadataFieldPolicy.ownership(field) == MetadataAdapter.Ownership.STARTUP
                    && !Objects.equals(imported.get(field), store.snapshot().data().get(field))) {
                startup.put(field, Objects.requireNonNull(imported.get(field)).copy());
            }
        }
        if (!startup.isEmpty()) {
            var before = store.snapshot();
            store.commit(before.worldId(), before.version(), before.term(), UUID.randomUUID().toString(), startup, Set.of());
        }
        MetadataRuntime.apply(server, store.snapshot().data());
        // Time 是中心计划事件逻辑时钟，恢复时中心需要恢复它；计算组不应用此字段。
        server.getWorldData().overworldData().setGameTime(store.snapshot().data().getLong("Time"));
        MetadataRuntime.setCommittedData(server, store.snapshot().data());
        schedules = new MetadataSchedules(this, server, groups, registry);
        current = this;
        handlers.on(MessageType.METADATA_SNAPSHOT_REQUEST, (c, m) -> enqueue(c, m, () -> snapshot(c, m)));
        handlers.on(MessageType.METADATA_CATCHUP, (c, m) -> enqueue(c, m, () -> catchup(c, m)));
        handlers.on(MessageType.METADATA_ACK, (c, m) -> enqueue(c, m, () -> acknowledge(c, m)));
        handlers.on(MessageType.METADATA_CHANGE, (c, m) -> enqueue(c, m, () -> change(c, m)));
        handlers.on(MessageType.METADATA_EVENT, (c, m) -> enqueue(c, m, () -> event(c, m)));
    }

    @FunctionalInterface private interface Task { void run() throws Exception; }
    private void enqueue(ControlConnection connection, ControlMessage message, Task task) {
        server.execute(() -> {
            try {
                if (closed) throw new IllegalStateException("元数据服务已关闭");
                Exception[] failure = {null};
                // 会话检查和实际提交必须处于同一个会话锁内，不能在检查之后让新HELLO
                // 替换当前连接，再由旧连接排队的任务修改新会话世界。
                boolean active = HelloHandler.withCurrentSession(connection, () -> {
                    try { task.run(); } catch (Exception error) { failure[0] = error; }
                    return true;
                });
                if (!active) {
                    throw new IllegalStateException("控制会话已过期");
                }
                if (failure[0] != null) throw failure[0];
            } catch (Exception error) {
                JsonObject reply = new JsonObject();
                reply.addProperty("accepted", false);
                reply.addProperty("message", error.getMessage());
                reply.addProperty("result", 0);
                reply.addProperty("code", error instanceof StaleTerm ? "metadata_stale_term" : "metadata_rejected");
                connection.send(message.reply(MessageType.ERROR, reply, null));
                LOGGER.warn("元数据请求被拒绝 {}: {}", message.type(), error.toString());
            }
        });
    }

    private void checkWorld(JsonObject request, boolean initial) {
        String id = JsonUtil.getString(request, "worldId", "");
        MetadataSnapshot snapshot = store.snapshot();
        if (!(initial && id.isEmpty()) && !snapshot.worldId().equals(id)) throw new IllegalArgumentException("世界身份不匹配");
        if (!initial && JsonUtil.getLong(request, "term", -1) != snapshot.term()) throw new StaleTerm();
    }

    private void snapshot(ControlConnection connection, ControlMessage request) throws IOException {
        checkWorld(request.json(), true);
        MetadataSnapshot snapshot = store.snapshot();
        JsonObject header = header(snapshot);
        // 全量快照是有界补齐的可靠回退；客户端仍使用连续补丁推进正常增量。
        connection.send(request.reply(MessageType.METADATA_SNAPSHOT, header,
                MetadataWire.encode(MetadataWire.snapshotToTag(snapshot))));
    }

    private void acknowledge(ControlConnection connection, ControlMessage request) {
        checkWorld(request.json(), false);
        long version = JsonUtil.getLong(request.json(), "version", -1);
        if (version < 0 || version > store.snapshot().version()) throw new IllegalArgumentException("确认版本非法");
        acknowledgements.merge(connection.peerId(), version, Math::max);
        JsonObject reply = header(store.snapshot());
        reply.addProperty("accepted", true);
        connection.send(request.reply(MessageType.METADATA_ACK, reply, null));
    }

    /** 连续日志窗口可用时只传缺失补丁；任期变化、窗口淘汰或合包过大均明确退回快照。 */
    private void catchup(ControlConnection connection, ControlMessage request) throws IOException {
        checkWorld(request.json(), true);
        MetadataSnapshot latest = store.snapshot();
        if (JsonUtil.getLong(request.json(), "term", -1) != latest.term()) {
            snapshot(connection, request);
            return;
        }
        var available = store.patchesAfter(JsonUtil.getLong(request.json(), "version", -1));
        if (available.isEmpty()) {
            snapshot(connection, request);
            return;
        }
        ListTag patches = new ListTag();
        long bytes = 0;
        for (MetadataPatch patch : available.get()) {
            CompoundTag tag = MetadataWire.patchToTag(patch);
            // 合包前逐条累计压缩预算，避免将256个极大扩展字段同时拼成超限控制帧。
            bytes += MetadataWire.encode(tag).length;
            if (bytes > Protocol.MAX_FRAME_BYTES / 2) {
                snapshot(connection, request);
                return;
            }
            patches.add(tag);
        }
        CompoundTag body = new CompoundTag(); body.put("Patches", patches);
        byte[] encoded;
        try { encoded = MetadataWire.encode(body); }
        catch (IOException tooLarge) { snapshot(connection, request); return; }
        connection.send(request.reply(MessageType.METADATA_CATCHUP, header(latest), encoded));
    }

    private UUID primaryGroup(ControlConnection connection) {
        UUID client = connection.peerId();
        UUID group = groups.groupOf(client);
        if (group == null || !client.equals(groups.primaryOf(group))) throw new IllegalStateException("当前会话不是模拟组写入者");
        return group;
    }

    private void change(ControlConnection connection, ControlMessage request) throws Exception {
        checkWorld(request.json(), false);
        UUID group = primaryGroup(connection);
        String operation = JsonUtil.getString(request.json(), "operationId", "");
        UUID.fromString(operation);
        if ("command".equals(JsonUtil.getString(request.json(), "action", ""))) {
            command(connection, request, group, operation);
            return;
        }
        // 自然事件只有被明确授权的模拟字段可以写入；未知字段不默认授予主客户端。
        MetadataPatch proposed = MetadataWire.patchFromTag(MetadataWire.decode(request.binary()));
        if (!operation.equals(proposed.operationId())) throw new IllegalArgumentException("操作身份不一致");
        for (String field : proposed.set().getAllKeys()) {
            if (!("DragonFight".equals(field) && group.equals(dragonOwner))
                    && !MetadataRuntime.canPropose(field, group, store.snapshot().data()))
                throw new IllegalStateException("未取得字段写入权: " + field);
        }
        for (String field : proposed.removed()) {
            if (!MetadataRuntime.canPropose(field, group, store.snapshot().data()))
                throw new IllegalStateException("模拟端不能删除未授权字段: " + field);
        }
        long beforeVersion = store.snapshot().version();
        MetadataPatch committed = store.commit(proposed.worldId(), proposed.baseVersion(), proposed.term(),
                proposed.operationId(), proposed.set(), proposed.removed());
        if (committed.version() > beforeVersion) {
            // 中心自然时钟可能已领先每秒检查点；只将已提交字段覆盖到当前运行态。
            // 重复操作仅返回旧回执，不能把其旧值再次覆盖后来已提交的其他操作。
            MetadataRuntime.apply(server, overlayCommittedFields(MetadataRuntime.capture(server), committed));
        }
        MetadataRuntime.setCommittedData(server, store.snapshot().data());
        publish(committed);
        connection.send(request.reply(MessageType.METADATA_PATCH, header(store.snapshot()),
                MetadataWire.encode(MetadataWire.patchToTag(committed))));
    }

    private void command(ControlConnection connection, ControlMessage request, UUID group, String operation) throws Exception {
        String command = JsonUtil.getString(request.json(), "command", "");
        String player = JsonUtil.getString(request.json(), "playerUuid", "");
        String signature = player + "|" + group + "|" + command + "|"
                + JsonUtil.getString(request.json(), "dimension", "") + "|"
                + JsonUtil.getDouble(request.json(), "x", 0) + "," + JsonUtil.getDouble(request.json(), "y", 0) + "," + JsonUtil.getDouble(request.json(), "z", 0)
                + "|" + JsonUtil.getDouble(request.json(), "xRot", 0) + "," + JsonUtil.getDouble(request.json(), "yRot", 0)
                + "|" + request.json().get("bossPlayers");
        if (!MetadataCommands.isGlobal(command) || command.length() > 32767) throw new IllegalArgumentException("不是受支持的全局叶指令");
        var previous = store.lookupOperation(operation);
        if (previous.isPresent()) {
            CompoundTag result = previous.get().set().getCompound("__p2pCommandResult");
            if (!signature.equals(result.getString("signature"))) throw new IllegalArgumentException("操作 ID 被不同请求复用");
            replyCommand(connection, request, result);
            return;
        }
        int permission = Math.min(2, JsonUtil.getInt(request.json(), "permission", 0));
        if (!player.isEmpty()) {
            UUID id = UUID.fromString(player);
            if (!groups.membersOf(group).contains(id)) throw new IllegalArgumentException("命令玩家不属于当前模拟组");
            permission = server.getProfilePermissions(new GameProfile(id, JsonUtil.getString(request.json(), "source", "")));
        }
        var dimension = ResourceLocation.tryParse(JsonUtil.getString(request.json(), "dimension", "minecraft:overworld"));
        var level = dimension == null ? null : server.getLevel(ResourceKey.create(Registries.DIMENSION, dimension));
        if (level == null) throw new IllegalArgumentException("命令维度不存在");
        var feedback = new CommandFeedback();
        var source = server.createCommandSourceStack().withSource(feedback).withPermission(permission).withLevel(level)
                .withPosition(new Vec3(JsonUtil.getDouble(request.json(), "x", 0), JsonUtil.getDouble(request.json(), "y", 0), JsonUtil.getDouble(request.json(), "z", 0)))
                .withRotation(new Vec2((float) JsonUtil.getDouble(request.json(), "xRot", 0), (float) JsonUtil.getDouble(request.json(), "yRot", 0)));
        int[] result = {0};
        boolean[] succeeded = {false};
        source = source.withCallback((success, value) -> { result[0] = value; succeeded[0] = success; });
        executingRequest = true;
        try {
            commandPlayer = player.isEmpty() ? null : UUID.fromString(player);
            commandBossPlayers = null;
            if (request.json().has("bossPlayers")) {
                var targets = new ArrayList<UUID>();
                for (var id : request.json().getAsJsonArray("bossPlayers")) {
                    UUID target = UUID.fromString(id.getAsString());
                    if (groups.groupOf(target) == null) throw new IllegalArgumentException("Boss血条目标不在线");
                    targets.add(target);
                }
                commandBossPlayers = List.copyOf(targets);
            }
            schedules.setCommandOrigin(group, source);
            server.getCommands().performPrefixedCommand(source, command);
            CompoundTag receipt = new CompoundTag();
            receipt.putInt("result", result[0]);
            receipt.putBoolean("success", succeeded[0]);
            receipt.putString("message", feedback.joined());
            receipt.putString("signature", signature);
            CompoundTag data = collect();
            data.put("__p2pCommandResult", receipt);
            commitData(data, operation, false);
            replyCommand(connection, request, receipt);
        } catch (Exception error) {
            MetadataRuntime.apply(server, store.snapshot().data());
            throw error;
        } finally {
            executingRequest = false;
            schedules.clearCommandOrigin();
            commandPlayer = null;
            commandBossPlayers = null;
        }
    }

    private void replyCommand(ControlConnection c, ControlMessage request, CompoundTag result) {
        JsonObject reply = header(store.snapshot());
        reply.addProperty("accepted", true);
        reply.addProperty("result", result.getInt("result"));
        reply.addProperty("success", result.getBoolean("success"));
        reply.addProperty("message", result.getString("message"));
        c.send(request.reply(MessageType.METADATA_ACK, reply, null));
    }

    private void event(ControlConnection connection, ControlMessage request) throws Exception {
        checkWorld(request.json(), false);
        UUID group = primaryGroup(connection);
        CompoundTag report = MetadataWire.decode(request.binary());
        if ("state".equals(JsonUtil.getString(request.json(), "action", ""))) {
            List<MetadataSleepConsensus.Player> players = new ArrayList<>();
            for (Tag entry : report.getList("Players", Tag.TAG_COMPOUND)) {
                CompoundTag p = (CompoundTag) entry;
                UUID id = p.hasUUID("uuid") ? p.getUUID("uuid") : UUID.fromString(p.getString("uuid"));
                if (!groups.membersOf(group).contains(id)) continue;
                players.add(new MetadataSleepConsensus.Player(id, p.getString("dimension"),
                        p.getBoolean("eligible"), p.getBoolean("deep"), p.getBoolean("canSkip")));
            }
            sleepReports.put(group, new SleepReport(ticks, List.copyOf(players)));
            transferSafety.put(group, new SafetyReport(ticks, store.snapshot().term(), report.getBoolean("transferSafe")));
            if (group.equals(dragonOwner) && report.contains("DragonFight", Tag.TAG_COMPOUND)) {
                CompoundTag data = collect();
                data.put("DragonFight", report.getCompound("DragonFight").copy());
                commitData(data, UUID.randomUUID().toString(), false);
                MetadataRuntime.apply(server, store.snapshot().data());
            }
        }
        JsonObject reply = header(store.snapshot());
        reply.addProperty("accepted", true);
        connection.send(request.reply(MessageType.METADATA_ACK, reply, null));
    }

    public static void tick(MinecraftServer server) {
        MetadataService service = current;
        if (service != null && service.server == server && !service.closed) service.tick();
    }

    private void tick() {
        ticks++;
        try {
            String view = groups.primariesSnapshot().toString();
            if (!view.equals(topology)) {
                topology = view;
                store.advanceTerm();
                acknowledgements.clear();
                sleepReports.clear();
                transferSafety.clear();
                broadcastSnapshot();
            }
            boolean wasRaining = server.getWorldData().overworldData().isRaining();
            boolean wasThundering = server.getWorldData().overworldData().isThundering();
            MetadataRuntime.advanceAuthority(server);
            if (wasRaining != server.getWorldData().overworldData().isRaining()
                    || wasThundering != server.getWorldData().overworldData().isThundering()) {
                commitData(collect(), UUID.randomUUID().toString(), false);
            }
            schedules.tick();
            if (ticks % 20 == 0) {
                updateDragonOwner();
                decideSleep();
                commitData(collect(), UUID.randomUUID().toString(), true);
            }
            if (MetadataRuntime.advanceTraderChance(server) && !traderPending) dispatchTrader();
        } catch (Exception error) {
            // 无法确认权威状态时不继续推进，断开控制面令所有计算端进入暂停门控。
            LOGGER.error("世界元数据推进/耐久提交失败，关闭元数据会话", error);
            closed = true;
            for (UUID primary : groups.primariesSnapshot().values()) {
                ControlConnection connection = HelloHandler.connectionOf(primary);
                if (connection != null) connection.close();
            }
        }
    }

    private void decideSleep() throws IOException {
        List<MetadataSleepConsensus.Player> all = new ArrayList<>();
        // 任一在线组缺失新鲜报告时不跳夜，避免网络延迟将醒着的人从分母抹掉。
        for (UUID group : groups.primariesSnapshot().keySet()) {
            if (imsng.player_to_player.server.P2PServerService.isGroupTransitioning(group)) return;
            SleepReport report = sleepReports.get(group);
            if (report == null || ticks - report.tick() > 60) return;
            for (var player : report.players()) if (groups.membersOf(group).contains(player.id())) all.add(player);
        }
        for (var level : server.getAllLevels()) {
            String dimension = level.dimension().location().toString();
            if (!level.canSleepThroughNights() || !MetadataSleepConsensus.canSkip(all, dimension,
                    level.getGameRules().getInt(GameRules.RULE_PLAYERS_SLEEPING_PERCENTAGE))) continue;
            if (level.getGameRules().getBoolean(GameRules.RULE_DAYLIGHT)) {
                long next = server.overworld().getDayTime() + 24000L;
                server.overworld().setDayTime(next - next % 24000L);
            }
            if (level.getGameRules().getBoolean(GameRules.RULE_WEATHER_CYCLE)) server.overworld().setWeatherParameters(0, 0, false, false);
            commitData(collect(), UUID.randomUUID().toString(), false);
            JsonObject wake = new JsonObject(); wake.addProperty("dimension", dimension);
            broadcastEvent("wake", wake);
            sleepReports.clear();
            break; // 全局昼夜只跳一次；下一轮重新采集玩家，避免同一批报告重复跳夜。
        }
    }

    private void updateDragonOwner() {
        UUID owner = registry.ownerOf(new ChunkKey("minecraft:the_end", 0, 0));
        dragonOwner = owner;
        for (var entry : groups.primariesSnapshot().entrySet()) {
            JsonObject data = new JsonObject();
            data.addProperty("enabled", entry.getKey().equals(owner));
            sendEvent(entry.getValue(), "dragon_authority", data);
        }
    }

    private void dispatchTrader() throws IOException {
        commitData(collect(), UUID.randomUUID().toString(), false);
        List<UUID> candidates = new ArrayList<>();
        for (var entry : sleepReports.entrySet()) {
            if (ticks - entry.getValue().tick() > 60) continue;
            for (var p : entry.getValue().players())
                if (p.eligible() && p.dimension().equals("minecraft:overworld") && groups.membersOf(entry.getKey()).contains(p.id())) candidates.add(p.id());
        }
        if (candidates.isEmpty()) return;
        UUID player = candidates.get(server.overworld().getRandom().nextInt(candidates.size()));
        UUID group = groups.groupOf(player);
        UUID primary = group == null ? null : groups.primaryOf(group);
        ControlConnection c = primary == null ? null : HelloHandler.connectionOf(primary);
        if (c == null || !c.isOpen()) return;
        JsonObject data = header(store.snapshot());
        data.addProperty("action", "trader_attempt");
        data.addProperty("playerUuid", player.toString());
        data.addProperty("invocationId", UUID.randomUUID().toString());
        traderPending = true;
        c.request(ControlMessage.of(MessageType.METADATA_EVENT, data)).whenComplete((reply, error) -> server.execute(() -> {
            traderPending = false;
            // 生成是否成功不明时不补发；下次仍由唯一中心时钟产生新的机会。
            if (error == null && reply != null && reply.binary().length > 0) {
                try {
                    CompoundTag result = MetadataWire.decode(reply.binary());
                    CompoundTag fields = collect();
                    if (result.contains("WanderingTraderId")) fields.put("WanderingTraderId", Objects.requireNonNull(result.get("WanderingTraderId")).copy());
                    if (JsonUtil.getBoolean(reply.json(), "spawned", false)) fields.putInt("WanderingTraderSpawnChance", 25);
                    commitData(fields, UUID.randomUUID().toString(), false);
                    MetadataRuntime.apply(server, store.snapshot().data());
                } catch (Exception failure) { LOGGER.error("商人生成结果无法提交", failure); }
            }
        }));
    }

    CompoundTag collect() {
        CompoundTag data = store.snapshot().data();
        CompoundTag captured = MetadataRuntime.capture(server);
        for (String field : new ArrayList<>(data.getAllKeys())) {
            if (!field.startsWith("__p2p") && MetadataFieldPolicy.ownership(field) != MetadataAdapter.Ownership.PRESERVED
                    && !captured.contains(field)) data.remove(field);
        }
        for (String field : captured.getAllKeys()) {
            if (!field.startsWith("__p2p")) data.put(field, Objects.requireNonNull(captured.get(field)).copy());
        }
        data.putLong("__p2pSampledAtMillis", System.currentTimeMillis());
        if (schedules != null) schedules.decorateRoutes(data);
        return data;
    }

    static CompoundTag overlayCommittedFields(CompoundTag live, MetadataPatch patch) {
        CompoundTag result = live.copy();
        patch.removed().forEach(result::remove);
        CompoundTag set = patch.set();
        for (String key : set.getAllKeys()) result.put(key, Objects.requireNonNull(set.get(key)).copy());
        return result;
    }

    void commitData(CompoundTag data, String operation, boolean checkpoint) throws IOException {
        MetadataSnapshot previous = store.snapshot();
        CompoundTag before = previous.data();
        CompoundTag changed = new CompoundTag();
        for (String key : data.getAllKeys()) {
            if (key.equals("Player") || key.equals("LastPlayed") || key.equals("SizeOnDisk")) continue;
            if (!Objects.equals(data.get(key), before.get(key))) changed.put(key, Objects.requireNonNull(data.get(key)).copy());
        }
        Set<String> removed = new HashSet<>(before.getAllKeys());
        removed.removeAll(data.getAllKeys());
        if (changed.isEmpty() && removed.isEmpty()) return;
        MetadataPatch patch = checkpoint ? store.checkpointChanges(changed, removed)
                : store.commit(previous.worldId(), previous.version(), previous.term(), operation, changed, removed);
        MetadataRuntime.setCommittedData(server, store.snapshot().data());
        publish(patch);
    }

    private void publish(MetadataPatch patch) throws IOException {
        ControlMessage message = ControlMessage.of(MessageType.METADATA_PATCH, header(store.snapshot()),
                MetadataWire.encode(MetadataWire.patchToTag(patch)));
        for (UUID primary : groups.primariesSnapshot().values()) {
            ControlConnection connection = HelloHandler.connectionOf(primary);
            if (connection != null && connection.isOpen()) connection.send(message);
        }
    }

    private void broadcastSnapshot() throws IOException {
        var snapshot = store.snapshot();
        var message = ControlMessage.of(MessageType.METADATA_SNAPSHOT, header(snapshot), MetadataWire.encode(MetadataWire.snapshotToTag(snapshot)));
        for (UUID primary : groups.primariesSnapshot().values()) {
            var connection = HelloHandler.connectionOf(primary);
            if (connection != null && connection.isOpen()) connection.send(message);
        }
    }

    private void broadcastEvent(String action, JsonObject json) {
        for (UUID primary : groups.primariesSnapshot().values()) sendEvent(primary, action, json.deepCopy());
    }

    private void sendEvent(UUID primary, String action, JsonObject json) {
        var c = HelloHandler.connectionOf(primary);
        if (c == null || !c.isOpen()) return;
        json.addProperty("action", action);
        json.addProperty("worldId", store.snapshot().worldId());
        json.addProperty("term", store.snapshot().term());
        c.send(ControlMessage.of(MessageType.METADATA_EVENT, json));
    }

    static JsonObject header(MetadataSnapshot snapshot) {
        JsonObject header = new JsonObject();
        header.addProperty("worldId", snapshot.worldId());
        header.addProperty("version", snapshot.version());
        header.addProperty("term", snapshot.term());
        CompoundTag data = snapshot.data();
        header.addProperty("sampledAtMillis", data.contains("__p2pSampledAtMillis")
                ? data.getLong("__p2pSampledAtMillis") : System.currentTimeMillis());
        return header;
    }

    public static CompoundTag committedData() { var service = current; return service == null ? null : service.store.snapshot().data(); }
    public static boolean isAuthorityActive(MinecraftServer server) { var service = current; return service != null && service.server == server; }
    public static long version() { var service = current; return service == null ? -1 : service.store.snapshot().version(); }
    public static long term() { var service = current; return service == null ? -1 : service.store.snapshot().term(); }
    /** 原COMMAND_RELAY入口仍负责玩家权限校验，此上下文只用于计划事件的稳定路由。 */
    public static void beginLocalCommand(UUID group, UUID player, CommandSourceStack source) {
        var service = current;
        if (service != null && service.server == source.getServer()) {
            service.commandPlayer = player;
            service.schedules.setCommandOrigin(group, source);
        }
    }
    public static void endLocalCommand(MinecraftServer server) {
        var service = current;
        if (service != null && service.server == server) {
            service.schedules.clearCommandOrigin();
            service.commandPlayer = null;
            service.commandBossPlayers = null;
        }
    }
    public static Integer tryBossPlayers(com.mojang.brigadier.context.CommandContext<?> context, CommandSourceStack source) {
        var service = current;
        if (service == null || service.server != source.getServer()) return null;
        return MetadataBossPlayers.execute(context, source, service.players, service.commandPlayer, service.commandBossPlayers);
    }
    public static boolean transferAllowed(UUID group) {
        var service = current;
        if (service == null) return true;
        SafetyReport report = service.transferSafety.get(group);
        return report != null && report.safe() && report.term() == service.store.snapshot().term()
                && service.ticks - report.tick() <= 60;
    }

    public static void afterLocalCommand(MinecraftServer server) {
        var service = current;
        if (service == null || service.server != server || service.executingRequest || service.closed) return;
        try {
            service.commitData(service.collect(), UUID.randomUUID().toString(), false);
        } catch (IOException error) { throw new IllegalStateException("全局命令未能耐久提交", error); }
    }

    @Override public void close() throws IOException {
        if (current == this) current = null;
        if (!closed) commitData(collect(), UUID.randomUUID().toString(), true);
        closed = true;
        store.close();
    }
}
