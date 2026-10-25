package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.GroupControlTasks;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;

import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** 计算端的全局事件执行点。网络处理线程绝不直接访问世界；计划调用先落不重放标记。 */
public final class MetadataEvents {
    private static int reportTicks;
    private static volatile boolean proposalPending;
    private MetadataEvents() { }

    public static void install(HandlerRegistry registry) {
        registry.on(MessageType.METADATA_EVENT, (connection, message) -> {
            MinecraftServer server = GroupRuntime.server();
            if (server == null) return;
            // 函数可能同步等待中心提交全局命令。不能持迁移控制队列锁执行，否则先到的
            // 结果补丁阻塞Netty入站线程，后到的命令响应永远无法被读取。
            server.execute(() -> {
                JsonObject reply = new JsonObject();
                byte[] binary = null;
                boolean notStarted = true;
                try {
                    var snapshot = MetadataClient.snapshot();
                    if (snapshot == null || !snapshot.worldId().equals(JsonUtil.getString(message.json(), "worldId", ""))
                            || snapshot.term() != JsonUtil.getLong(message.json(), "term", -1)) {
                        throw new IllegalStateException("事件属于旧世界或旧任期");
                    }
                    String action = JsonUtil.getString(message.json(), "action", "");
                    if (action.equals("wake")) {
                        String dimension = JsonUtil.getString(message.json(), "dimension", "minecraft:overworld");
                        for (var level : server.getAllLevels())
                            if (level.dimension().location().toString().equals(dimension) && level.canSleepThroughNights())
                                ((imsng.player_to_player.mixin.MetadataLevelAccess) level).player_to_player$wakeUpAllPlayers();
                    }
                    else if (action.equals("dragon_authority")) MetadataRuntime.setDragonAuthority(server,
                            JsonUtil.getBoolean(message.json(), "enabled", false));
                    else if (action.equals("invocation_confirmed")) {
                        UUID invocation = UUID.fromString(JsonUtil.getString(message.json(), "invocationId", ""));
                        var receipt = server.getWorldPath(LevelResource.ROOT).resolve("p2p-invocations").resolve(invocation + ".state");
                        // 中心已经耐久记录完成才能清理本地成功回执；started不明状态绝不清理。
                        if (Files.isRegularFile(receipt) && Files.readString(receipt).equals("completed")) Files.delete(receipt);
                    }
                    else if (action.equals("function") || action.equals("trader_attempt")) {
                        if (!MetadataClient.isSimulationReady() || GroupRuntime.isTickFrozen() || GroupRuntime.isExitDraining())
                            throw new IllegalStateException("模拟尚未开放或正在交接，事件未执行");
                        UUID invocation = UUID.fromString(JsonUtil.getString(message.json(), "invocationId", ""));
                        var directory = server.getWorldPath(LevelResource.ROOT).resolve("p2p-invocations");
                        Files.createDirectories(directory);
                        var receipt = directory.resolve(invocation + ".state");
                        if (Files.exists(receipt)) {
                            notStarted = false;
                            throw new IllegalStateException("调用已开始，结果需核查，不能重复执行");
                        }
                        // 先force started，再执行非幂等函数。断电后宁可保留待核查，也不凭超时重放。
                        try (FileChannel file = FileChannel.open(receipt, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                            file.write(ByteBuffer.wrap("started".getBytes(StandardCharsets.UTF_8)));
                            file.force(true);
                        }
                        notStarted = false;
                        if (action.equals("function")) {
                            String function = JsonUtil.getString(message.json(), "function", "");
                            if (!function.matches("#?[a-z0-9_.-]+:[a-z0-9/._-]+")) throw new IllegalArgumentException("函数标识非法");
                            // 原版schedule用服务端源；不继承发起玩家实体、位置、权限或登录状态。
                            var source = server.getFunctions().getGameLoopSender();
                            server.getCommands().performPrefixedCommand(source, "function " + function);
                        } else {
                            UUID player = UUID.fromString(JsonUtil.getString(message.json(), "playerUuid", ""));
                            reply.addProperty("spawned", MetadataRuntime.attemptTraderSpawn(server, player));
                            CompoundTag data = MetadataRuntime.capture(server);
                            CompoundTag trader = new CompoundTag();
                            for (String key : java.util.List.of("WanderingTraderId", "WanderingTraderSpawnChance", "WanderingTraderSpawnDelay"))
                                if (data.contains(key)) trader.put(key, data.get(key).copy());
                            binary = MetadataWire.encode(trader);
                        }
                        try (FileChannel file = FileChannel.open(receipt, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                            file.write(ByteBuffer.wrap("completed".getBytes(StandardCharsets.UTF_8)));
                            file.force(true);
                        }
                    } else throw new IllegalArgumentException("未知世界事件");
                    reply.addProperty("accepted", true);
                } catch (Exception error) {
                    reply.addProperty("accepted", false);
                    reply.addProperty("message", error.getMessage());
                    reply.addProperty("notStarted", notStarted);
                }
                if (message.hasRid()) connection.send(message.reply(MessageType.METADATA_EVENT, reply, binary));
            });
        });
    }

    public static void tickWorker(MinecraftServer server) {
        if (!GroupRuntime.isManagedServer(server) || !MetadataClient.isSimulationReady() || ++reportTicks % 20 != 0) return;
        var snapshot = MetadataClient.snapshot();
        var connection = GroupRuntime.conn();
        if (snapshot == null || connection == null || !connection.isOpen()) return;
        try {
            CompoundTag report = MetadataRuntime.sleepReport(server);
            boolean safe = true;
            try { MetadataRuntime.assertTransferSafe(server); } catch (IllegalStateException blocked) { safe = false; }
            report.putBoolean("transferSafe", safe);
            var simulationFields = MetadataRuntime.simulationFields();
            if (!simulationFields.isEmpty() && !proposalPending) {
                CompoundTag live = MetadataRuntime.capture(server);
                CompoundTag previous = snapshot.data();
                CompoundTag changed = new CompoundTag();
                var removed = new java.util.HashSet<String>();
                for (String field : simulationFields) {
                    if (java.util.Objects.equals(live.get(field), previous.get(field))) continue;
                    if (live.contains(field)) changed.put(field, live.get(field).copy());
                    else removed.add(field);
                }
                if (!changed.isEmpty() || !removed.isEmpty()) {
                    proposalPending = true;
                    propose(server, changed, removed).whenComplete((reply, error) -> proposalPending = false);
                }
            }
            if (MetadataRuntime.hasDragonAuthority(server)) {
                CompoundTag data = MetadataRuntime.capture(server);
                if (data.contains("DragonFight")) report.put("DragonFight", data.get("DragonFight").copy());
            }
            JsonObject header = MetadataService.header(snapshot);
            header.addProperty("action", "state");
            connection.request(ControlMessage.of(MessageType.METADATA_EVENT, header, MetadataWire.encode(report)));
        } catch (Exception error) {
            org.slf4j.LoggerFactory.getLogger("player_to_player/metadata").warn("元数据状态核对失败", error);
        }
    }

    /**
     * 显式适配器也可在自然事件发生后主动调用。冲突由调用方重新采集处理，不悄悄
     * 用最新版本重放旧候选；中心再次验证当前写入者与每个字段声明的资源所有权。
     */
    public static java.util.concurrent.CompletableFuture<ControlMessage> propose(
            MinecraftServer server, CompoundTag changed, java.util.Set<String> removed) {
        if (!server.isSameThread() || !GroupRuntime.isManagedServer(server) || MetadataRuntime.isApplying()
                || !MetadataClient.isSimulationReady() || GroupRuntime.isTickFrozen() || GroupRuntime.isExitDraining())
            return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("当前世界不能提交自然字段变更"));
        var snapshot = MetadataClient.snapshot();
        try {
            String operation = UUID.randomUUID().toString();
            var proposal = new MetadataPatch(snapshot.worldId(), snapshot.version(), snapshot.version() + 1,
                    snapshot.term(), operation, changed, removed);
            JsonObject header = MetadataService.header(snapshot);
            header.addProperty("action", "fields"); header.addProperty("operationId", operation);
            return GroupRuntime.conn().request(ControlMessage.of(MessageType.METADATA_CHANGE, header,
                    MetadataWire.encode(MetadataWire.patchToTag(proposal)))).thenApply(reply -> {
                if (reply.type() != MessageType.METADATA_PATCH)
                    throw new java.util.concurrent.CompletionException(new IllegalStateException("字段变更未提交: " + reply.json()));
                return reply;
            });
        } catch (Exception error) { return java.util.concurrent.CompletableFuture.failedFuture(error); }
    }

    /**
     * 内部迁移/停服专用的冻结尾部提交。只在控制队列采集一次真实对象；网络提交、重取基础
     * 版本均由异步续体完成，绝不持控制队列锁等待网络。允许已冻结或 EXIT_BEGIN 后提交，
     * 但中心仍按当前主客户端、世界身份、任期和字段所有权执行同一套 CAS 校验。
     *
     * 返回的是已经耐久确认的版本。迁移调用者还必须让两端运行对象同步到该版本，
     * 停服调用者则可在此后走原版保存。版本冲突只刷新基础快照，不能重新采集并丢掉尾部。
     */
    public static java.util.concurrent.CompletableFuture<MetadataSnapshot> flushTransferState(MinecraftServer server) {
        try {
            return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                if (!GroupRuntime.isManagedServer(server) || !GroupRuntime.isTickFrozen())
                    throw new IllegalStateException("模拟字段尾部只能在当前受管世界冻结后采集");
                MetadataSnapshot snapshot = MetadataClient.snapshot();
                ControlConnection connection = GroupRuntime.conn();
                if (snapshot == null || connection == null || !connection.isOpen())
                    throw new IllegalStateException("控制连接或元数据身份未确认，不能丢弃模拟尾部");
                CompoundTag authority = snapshot.data();
                java.util.Set<String> owned = new java.util.HashSet<>();
                for (String field : MetadataRuntime.simulationFields())
                    if (MetadataRuntime.canPropose(field, GroupRuntime.activeGroupId(), authority)) owned.add(field);
                if (MetadataRuntime.hasDragonAuthority(server)) owned.add("DragonFight");
                CompoundTag live = MetadataRuntime.capture(server);
                return new TransferCapture(new MetadataTransferState(snapshot, owned, live), snapshot, connection);
            }, GroupControlTasks.executor(server)).thenCompose(capture ->
                    // 先取中心最新基准，包含此前已经发出但尚未回到本地镜像的状态报告。
                    // 不能因“尾部等于本地旧镜像”就跳过，否则A→B→A的变化可能遗失最后A。
                    MetadataClient.requestSnapshot(capture.connection(), capture.base().worldId())
                            .thenCompose(latest -> submitTransfer(capture, latest, 0)));
        } catch (Exception error) {
            return java.util.concurrent.CompletableFuture.failedFuture(error);
        }
    }

    private record TransferCapture(MetadataTransferState state, MetadataSnapshot base, ControlConnection connection) { }

    private static java.util.concurrent.CompletableFuture<MetadataSnapshot> submitTransfer(
            TransferCapture capture, MetadataSnapshot base, int conflicts) {
        try {
            if (!capture.connection().isOpen()) throw new IllegalStateException("尾部提交期间控制连接断开");
            var pending = capture.state().patchAgainst(base, UUID.randomUUID().toString());
            if (pending.isEmpty()) return java.util.concurrent.CompletableFuture.completedFuture(base);
            MetadataPatch proposed = pending.get();
            JsonObject header = MetadataService.header(base);
            header.addProperty("action", "fields"); header.addProperty("operationId", proposed.operationId());
            return capture.connection().request(ControlMessage.of(MessageType.METADATA_CHANGE, header,
                    MetadataWire.encode(MetadataWire.patchToTag(proposed)))).thenCompose(reply -> {
                try {
                    if (reply.type() == MessageType.METADATA_PATCH) {
                        MetadataPatch accepted = MetadataWire.patchFromTag(MetadataWire.decode(reply.binary()));
                        if (!accepted.operationId().equals(proposed.operationId()) || !accepted.set().equals(proposed.set())
                                || !accepted.removed().equals(proposed.removed()))
                            throw new IllegalStateException("模拟尾部回执与本次候选不符");
                        return java.util.concurrent.CompletableFuture.completedFuture(accepted.applyTo(base));
                    }
                    if (conflicts >= 5 || "metadata_stale_term".equals(JsonUtil.getString(reply.json(), "code", "")))
                        throw new IllegalStateException("模拟尾部未能耐久提交: " + reply.json());
                    // 明确拒绝时才重取基础版本；网络超时/断连直接失败，不把未知结果重放为新操作。
                    return MetadataClient.requestSnapshot(capture.connection(), base.worldId()).thenCompose(latest -> {
                        if (latest.version() == base.version() && latest.term() == base.term())
                            return java.util.concurrent.CompletableFuture.failedFuture(
                                    new IllegalStateException("模拟尾部被拒绝且基础版本未变化: " + reply.json()));
                        return submitTransfer(capture, latest, conflicts + 1);
                    });
                } catch (Exception error) {
                    return java.util.concurrent.CompletableFuture.failedFuture(error);
                }
            });
        } catch (Exception error) {
            return java.util.concurrent.CompletableFuture.failedFuture(error);
        }
    }
}
