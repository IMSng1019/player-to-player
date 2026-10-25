package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.server.GroupTable;
import imsng.player_to_player.server.HelloHandler;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.nbt.*;
import net.minecraft.server.MinecraftServer;

import java.io.IOException;
import java.util.*;

/**
 * 中心唯一计划队列。原版ScheduledEvents保留函数及逻辑触发时间；路由和稳定调用ID
 * 存在独立字段。派发前耐久写入pending；超时和重启只留下unknown，不自动重放函数。
 */
final class MetadataSchedules {
    private final MetadataService service;
    private final MinecraftServer server;
    private final GroupTable groups;
    private final ChunkRegistry registry;
    private UUID originGroup;
    private CommandSourceStack originSource;

    MetadataSchedules(MetadataService service, MinecraftServer server, GroupTable groups, ChunkRegistry registry) {
        this.service = service; this.server = server; this.groups = groups; this.registry = registry;
    }

    void setCommandOrigin(UUID group, CommandSourceStack source) { originGroup = group; originSource = source; }
    void clearCommandOrigin() { originGroup = null; originSource = null; }

    private static String key(CompoundTag event) { return event.getString("Name") + "@" + event.getLong("TriggerTime"); }

    void decorateRoutes(CompoundTag data) {
        CompoundTag routes = data.getCompound("__p2pScheduleRoutes").copy();
        for (Tag entry : data.getList("ScheduledEvents", Tag.TAG_COMPOUND)) {
            CompoundTag event = (CompoundTag) entry;
            String key = key(event);
            if (routes.contains(key)) continue;
            CompoundTag route = new CompoundTag();
            route.putString("invocationId", UUID.randomUUID().toString());
            UUID selected = originGroup != null ? originGroup : groups.primariesSnapshot().keySet().stream().sorted().findFirst().orElse(null);
            if (selected != null) {
                route.putUUID("group", selected);
                ListTag members = new ListTag();
                groups.membersOf(selected).forEach(id -> members.add(StringTag.valueOf(id.toString())));
                route.put("members", members);
            }
            var source = originSource == null ? server.createCommandSourceStack() : originSource;
            route.putString("dimension", source.getLevel().dimension().location().toString());
            route.putInt("chunkX", ((int) Math.floor(source.getPosition().x)) >> 4);
            route.putInt("chunkZ", ((int) Math.floor(source.getPosition().z)) >> 4);
            routes.put(key, route);
        }
        Set<String> active = new HashSet<>();
        for (Tag entry : data.getList("ScheduledEvents", Tag.TAG_COMPOUND)) active.add(key((CompoundTag) entry));
        CompoundTag pending = data.getCompound("__p2pSchedulePending");
        for (String invocation : pending.getAllKeys()) {
            CompoundTag attempt = pending.getCompound(invocation);
            if (!attempt.getString("status").equals("completed")) active.add(key(attempt.getCompound("event")));
        }
        for (String name : new ArrayList<>(routes.getAllKeys())) if (!active.contains(name)) routes.remove(name);
        data.put("__p2pScheduleRoutes", routes);
    }

    private UUID resolve(CompoundTag route) {
        UUID original = route.hasUUID("group") ? route.getUUID("group") : null;
        if (original != null && groups.exists(original)) return original;
        for (Tag member : route.getList("members", Tag.TAG_STRING)) {
            UUID current = groups.groupOf(UUID.fromString(member.getAsString()));
            if (current != null) return current;
        }
        UUID owner = registry.ownerOf(new ChunkKey(route.getString("dimension"), route.getInt("chunkX"), route.getInt("chunkZ")));
        return owner != null ? owner : groups.primariesSnapshot().keySet().stream().sorted().findFirst().orElse(null);
    }

    void tick() throws IOException {
        if (server.getWorldData().overworldData().getScheduledEvents().getEventsIds().isEmpty()) return;
        CompoundTag data = service.collect();
        ListTag events = data.getList("ScheduledEvents", Tag.TAG_COMPOUND);
        if (events.isEmpty()) return;
        CompoundTag routes = data.getCompound("__p2pScheduleRoutes");
        for (Tag entry : events) {
            CompoundTag event = (CompoundTag) entry;
            if (event.getLong("TriggerTime") > server.overworld().getGameTime()) continue;
            CompoundTag route = routes.getCompound(key(event));
            if (route.isEmpty()) { service.commitData(service.collect(), UUID.randomUUID().toString(), false); return; }
            if (route.getLong("retryAfter") > server.overworld().getGameTime()) continue;
            UUID target = resolve(route);
            if (target != null && imsng.player_to_player.server.P2PServerService.isGroupTransitioning(target)) continue;
            UUID primary = target == null ? null : groups.primaryOf(target);
            ControlConnection connection = primary == null ? null : HelloHandler.connectionOf(primary);
            if (connection == null || !connection.isOpen()) continue;
            CompoundTag callback = event.getCompound("Callback");
            String type = callback.getString("Type");
            if (!type.equals("minecraft:function") && !type.equals("minecraft:function_tag")) continue;
            String invocation = route.getString("invocationId");
            ListTag remaining = new ListTag();
            for (Tag keep : events) if (keep != entry) remaining.add(keep.copy());
            CompoundTag pending = data.getCompound("__p2pSchedulePending").copy();
            CompoundTag attempt = route.copy();
            attempt.put("event", event.copy());
            attempt.putString("status", "unknown");
            attempt.putUUID("target", target);
            pending.put(invocation, attempt);
            data.put("ScheduledEvents", remaining);
            data.put("__p2pSchedulePending", pending);
            // 先durable移除队列并登记可能执行，再派发；重启不会重复非幂等函数。
            service.commitData(data, UUID.randomUUID().toString(), false);
            MetadataRuntime.replaceScheduled(server, remaining);
            JsonObject message = new JsonObject();
            message.addProperty("worldId", WorldId.current());
            message.addProperty("term", MetadataService.term());
            message.addProperty("action", "function");
            message.addProperty("invocationId", invocation);
            message.addProperty("function", (type.endsWith("function_tag") ? "#" : "") + callback.getString("Name"));
            connection.request(ControlMessage.of(MessageType.METADATA_EVENT, message)).whenComplete((reply, error) -> server.execute(() -> {
                if (error != null || reply == null) return;
                boolean accepted = JsonUtil.getBoolean(reply.json(), "accepted", false);
                boolean notStarted = JsonUtil.getBoolean(reply.json(), "notStarted", false);
                if (!accepted && !notStarted) return;
                try {
                    CompoundTag result = service.collect();
                    CompoundTag statuses = result.getCompound("__p2pSchedulePending").copy();
                    if (statuses.contains(invocation)) {
                        statuses.getCompound(invocation).putString("status", accepted ? "completed" : "not_started");
                        statuses.getCompound(invocation).putLong("completedAt", server.overworld().getGameTime());
                    }
                    if (!accepted) {
                        // 只有执行端明确证明尚未写started标记才可重排。网络超时不会走这里。
                        ListTag queue = result.getList("ScheduledEvents", Tag.TAG_COMPOUND).copy();
                        boolean replaced = false;
                        for (Tag queued : queue) if (key((CompoundTag) queued).equals(key(event))) replaced = true;
                        if (!replaced) queue.add(event.copy());
                        result.put("ScheduledEvents", queue);
                        result.getCompound("__p2pScheduleRoutes").getCompound(key(event))
                                .putLong("retryAfter", server.overworld().getGameTime() + 20);
                    }
                    result.put("__p2pSchedulePending", statuses);
                    // 成功记录只留有限诊断窗口；unknown必须保留供人工核查，不能误当可重放任务。
                    var completed = new ArrayList<String>();
                    for (String id : statuses.getAllKeys())
                        if (statuses.getCompound(id).getString("status").equals("completed")) completed.add(id);
                    completed.sort(Comparator.comparingLong(id -> statuses.getCompound(id).getLong("completedAt")));
                    for (int i = 0; i < completed.size() - 256; i++) statuses.remove(completed.get(i));
                    decorateRoutes(result);
                    service.commitData(result, UUID.randomUUID().toString(), false);
                    if (!accepted) MetadataRuntime.replaceScheduled(server, result.getList("ScheduledEvents", Tag.TAG_COMPOUND));
                    else {
                        JsonObject confirmed = new JsonObject();
                        confirmed.addProperty("action", "invocation_confirmed");
                        confirmed.addProperty("worldId", WorldId.current());
                        confirmed.addProperty("term", MetadataService.term());
                        confirmed.addProperty("invocationId", invocation);
                        connection.send(ControlMessage.of(MessageType.METADATA_EVENT, confirmed));
                    }
                } catch (IOException failure) {
                    org.slf4j.LoggerFactory.getLogger("player_to_player/metadata").error("计划函数执行回执无法提交", failure);
                }
            }));
            return; // 单tick最多派发一次，避免大量过期函数阻塞控制线程。
        }
    }

    private static final class WorldId {
        static String current() {
            CompoundTag root = new CompoundTag(); root.put("Data", MetadataService.committedData());
            return imsng.player_to_player.world.WorldIdentity.ofLevelData(root).orElseThrow();
        }
    }
}
