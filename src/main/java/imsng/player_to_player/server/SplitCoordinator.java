package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.registry.SplitRegion;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 两阶段分离：先预约边界并让全部离组成员退出旧游戏连接，源主经原版卸载与
 * 耐久 release 排空，再提交组表并启动新主。准备阶段绝不迁移仍在加载的区块。
 * 事务状态均由本对象锁保护；已完成结果保留一段时间，供丢 ACK 的源查询。
 */
final class SplitCoordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/split-coordinator");
    private final GlobalConfig config;
    private final ChunkRegistry registry;
    private final GroupTable groups;
    private final ComputeTable computes;
    private final PlayerTable players;
    private final Function<UUID, ControlConnection> connections;
    private final Consumer<UUID> permissionPush;
    private final BiConsumer<UUID, Set<UUID>> redirectFollowers;
    private final Map<UUID, Transaction> transactions = new HashMap<>();

    private static final class Transaction {
        final UUID id, source, newPrimary;
        final ControlConnection sourceConnection;
        final Set<UUID> members, departing;
        String phase = "prepared";
        ScheduledFuture<?> timeout;
        Transaction(UUID id, UUID source, UUID newPrimary, ControlConnection sourceConnection,
                    Set<UUID> members, Set<UUID> departing) {
            this.id = id; this.source = source; this.newPrimary = newPrimary;
            this.sourceConnection = sourceConnection;
            this.members = Set.copyOf(members); this.departing = Set.copyOf(departing);
        }
        boolean active() { return phase.equals("prepared") || phase.equals("cancel_requested"); }
    }

    SplitCoordinator(GlobalConfig config, ChunkRegistry registry, GroupTable groups,
                     ComputeTable computes, PlayerTable players,
                     Function<UUID, ControlConnection> connections, Consumer<UUID> permissionPush,
                     BiConsumer<UUID, Set<UUID>> redirectFollowers) {
        this.config = config; this.registry = registry; this.groups = groups;
        this.computes = computes; this.players = players; this.connections = connections;
        this.permissionPush = permissionPush; this.redirectFollowers = redirectFollowers;
    }

    synchronized boolean involvesGroup(UUID group) {
        return transactions.values().stream().anyMatch(tx -> tx.active() && tx.source.equals(group));
    }

    synchronized void handle(ControlConnection connection, ControlMessage request) {
        UUID id = null;
        try {
            UUID source = UUID.fromString(JsonUtil.getString(request.json(), "groupId", ""));
            id = UUID.fromString(JsonUtil.getString(request.json(), "splitId", ""));
            String phase = JsonUtil.getString(request.json(), "phase", "");
            if (!source.equals(connection.peerId()) || connections.apply(source) != connection) {
                reject(connection, request, "stale_session", "分离源连接已失效"); return;
            }
            Transaction tx = transactions.get(id);
            if (tx == null) {
                if (!phase.equals("prepare")) {
                    reject(connection, request, "unknown_split", "分离事务不存在，不能推定提交成功"); return;
                }
                prepare(connection, request, source, id);
                return;
            }
            if (!tx.source.equals(source) || tx.sourceConnection != connection) {
                reject(connection, request, "wrong_source", "分离事务不属于本连接"); return;
            }
            if (phase.equals("status") || phase.equals("prepare") || !tx.active()) {
                reply(connection, request, tx); return;
            }
            if (phase.equals("cancel")) {
                cancel(tx, true, "源端已完成回滚准备");
                reply(connection, request, tx); return;
            }
            if (!phase.equals("commit") || !tx.phase.equals("prepared")) {
                reject(connection, request, "invalid_phase", "分离当前只允许取消或查询状态"); return;
            }
            // 源已经证明：所有离组玩家离线、最终玩家 ACK、旧 holder/claim 全部排空。
            // 物理服再检查成员/在线连接及 owner；任一条件变化都必须回滚重新准备。
            if (tx.departing.stream().anyMatch(member -> !online(member))) {
                reject(connection, request, "member_offline", "离组成员已离线"); return;
            }
            boolean committed = registry.finishSplit(id, source, () -> {
                if (groups.splitPreparedGroup(source, tx.departing, tx.newPrimary, tx.members) == null) {
                    throw new IllegalStateException("分离准备期间成员或原主已变化");
                }
            });
            if (!committed) {
                reject(connection, request, "not_drained", "旧区域尚未完成耐久释放"); return;
            }
            tx.phase = "committed";
            for (UUID member : tx.departing) players.updateGroupRole(member, tx.newPrimary,
                    member.equals(tx.newPrimary) ? "primary" : "secondary");
            finishLater(tx);
            // 所有权/组表已经提交，通知失败不能被解释为事务拒绝，也不能让后续角色通知漏发。
            // 各个通知分别隔离；权威阶段保留，源端丢失应答后仍可查询真实提交结果。
            notifySafely(id, "原组权限", () -> permissionPush.accept(source));
            notifySafely(id, "新组权限", () -> permissionPush.accept(tx.newPrimary));
            Set<UUID> followers = new HashSet<>(tx.departing); followers.remove(tx.newPrimary);
            // 先登记 followers，再启动新主，避免极快的 WORLD_READY 先到而漏掉通知。
            if (!followers.isEmpty()) notifySafely(id, "跟随成员重定向登记",
                    () -> redirectFollowers.accept(tx.newPrimary, Set.copyOf(followers)));
            sendRole(tx.newPrimary, tx.newPrimary, "primary", id);
            reply(connection, request, tx);
            LOGGER.info("分离已提交: splitId={} 离组成员={}，旧区域已卸载并耐久释放", id, tx.departing.size());
        } catch (Exception failure) {
            Transaction recorded = id == null ? null : transactions.get(id);
            if (recorded != null && recorded.sourceConnection == connection
                    && recorded.source.equals(connection.peerId())) {
                // prepare登记或commit成功之后发生的旁路异常绝不能回复“未发生”。
                // 即使实际阶段仍为prepared，也明确回报现状，由源端继续查询/安全取消。
                LOGGER.warn("分离请求处理异常，保留实际事务阶段: split={} phase={}",
                        recorded.id, recorded.phase, failure);
                reply(connection, request, recorded);
            } else {
                reject(connection, request, "split_failed", failure.getMessage());
            }
        }
    }

    private void prepare(ControlConnection connection, ControlMessage request, UUID source, UUID id) {
        if (!source.equals(groups.primaryOf(source)) || involvesGroup(source)) {
            reject(connection, request, "busy", "原组不存在或已有分离事务"); return;
        }
        Set<UUID> departing = new HashSet<>();
        for (var value : request.json().getAsJsonArray("departingClientIds"))
            departing.add(UUID.fromString(value.getAsString()));
        Set<UUID> members = groups.membersOf(source);
        if (departing.isEmpty() || departing.contains(source) || !members.containsAll(departing)
                || departing.stream().anyMatch(member -> !online(member))) {
            reject(connection, request, "invalid_members", "离组成员不在线或不属于原组"); return;
        }
        List<SplitRegion> regions = new ArrayList<>();
        for (var value : request.json().getAsJsonArray("regions"))
            regions.add(SplitRegion.fromJson(value.getAsJsonObject()));
        if (!registry.reserveSplit(id, source, regions)) {
            reject(connection, request, "boundary_conflict", "分离边界与现有占用/预约冲突"); return;
        }
        var election = computes.selectPrimaryOrRelaxed(departing, config.minFreeMemoryBytes);
        UUID newPrimary = election != null ? election.clientId()
                : departing.stream().min(UUID::compareTo).orElseThrow();
        Transaction tx = new Transaction(id, source, newPrimary, connection, members, departing);
        transactions.put(id, tx);
        tx.timeout = ThreadPools.scheduler().schedule(() -> requestTimeout(id), 120, TimeUnit.SECONDS);
        reply(connection, request, tx);
        JsonObject pause = new JsonObject();
        pause.addProperty("splitId", id.toString()); pause.addProperty("groupId", source.toString());
        for (UUID member : departing) send(member, ControlMessage.of(MessageType.SPLIT_PREPARE, pause.deepCopy()));
        LOGGER.info("分离准备: splitId={} 成员={} 范围={}，保持原有区块归属", id, departing.size(), regions.size());
    }

    /** 超时只要求源回滚，不拆除尚有旧 holder 的屏障，也不让客户端永久停在空屏。 */
    synchronized void requestTimeout(UUID id) {
        Transaction tx = transactions.get(id);
        if (tx == null || !tx.phase.equals("prepared")) return;
        tx.phase = "cancel_requested";
        JsonObject failure = new JsonObject(); failure.addProperty("splitId", id.toString());
        failure.addProperty("message", "分离交接超时，已停止进入新世界；旧区域正在安全收尾。");
        send(tx.source, ControlMessage.of(MessageType.SPLIT_ABORT, failure.deepCopy()));
        for (UUID member : tx.departing)
            send(member, ControlMessage.of(MessageType.SPLIT_FAILED, failure.deepCopy()));
    }

    synchronized void abortInvolving(UUID client) {
        for (Transaction tx : List.copyOf(transactions.values())) {
            if (!tx.active() || (!tx.source.equals(client) && !tx.departing.contains(client))) continue;
            if (tx.source.equals(client)) cancel(tx, false, "原主控制连接已断开");
            else requestTimeout(tx.id);
        }
    }

    private void cancel(Transaction tx, boolean reconnect, String reason) {
        // 准备阶段组表从未改变。源确认排空后重连原主；源掉线时只显示失败，
        // 剩余 claim 仍由既有耐久断连清理回收，不能在此直接释放实际区块。
        registry.cancelSplit(tx.id, tx.source);
        tx.phase = "cancelled";
        finishLater(tx);
        for (UUID member : tx.departing) {
            if (reconnect && tx.source.equals(groups.groupOf(member)) && online(tx.source)) {
                sendRole(member, tx.source, "secondary", tx.id);
            } else {
                JsonObject failure = new JsonObject(); failure.addProperty("splitId", tx.id.toString());
                failure.addProperty("message", reason);
                send(member, ControlMessage.of(MessageType.SPLIT_FAILED, failure));
            }
        }
    }

    private void finishLater(Transaction tx) {
        if (tx.timeout != null) tx.timeout.cancel(false);
        tx.timeout = ThreadPools.scheduler().schedule(() -> {
            synchronized (SplitCoordinator.this) { transactions.remove(tx.id, tx); }
        }, 10, TimeUnit.MINUTES);
    }

    synchronized void shutdown() {
        for (Transaction tx : transactions.values()) {
            if (tx.timeout != null) tx.timeout.cancel(false);
            if (tx.active()) registry.cancelSplit(tx.id, tx.source);
        }
        transactions.clear();
    }

    private boolean online(UUID member) {
        ControlConnection connection = connections.apply(member);
        return connection != null && connection.isOpen();
    }
    private void send(UUID member, ControlMessage message) {
        ControlConnection connection = connections.apply(member);
        if (connection != null && connection.isOpen()) sendSafely(connection, message);
    }

    /** 推送只影响送达情况；不得通过抛异常改写已经登记的事务结果或中断其他成员通知。 */
    private static void sendSafely(ControlConnection connection, ControlMessage message) {
        try {
            connection.send(message);
        } catch (RuntimeException failure) {
            LOGGER.warn("分离通知发送失败，保留权威事务状态: peer={} type={}",
                    connection.peerId(), message.type(), failure);
        }
    }

    private static void notifySafely(UUID splitId, String action, Runnable notification) {
        try {
            notification.run();
        } catch (RuntimeException failure) {
            LOGGER.warn("分离已提交，{}失败，不改变提交结果: split={}", action, splitId, failure);
        }
    }
    private void sendRole(UUID member, UUID primary, String role, UUID splitId) {
        JsonObject assign = new JsonObject(); assign.addProperty("role", role);
        assign.addProperty("groupId", primary.toString());
        assign.addProperty("primaryClientId", primary.toString());
        assign.addProperty("splitId", splitId.toString());
        send(member, ControlMessage.of(MessageType.ROLE_ASSIGN, assign));
    }
    private static void reply(ControlConnection connection, ControlMessage request, Transaction tx) {
        JsonObject out = new JsonObject(); out.addProperty("granted", true);
        out.addProperty("phase", tx.phase); out.addProperty("splitId", tx.id.toString());
        out.addProperty("newGroupId", tx.newPrimary.toString());
        out.addProperty("newPrimaryClientId", tx.newPrimary.toString());
        sendSafely(connection, request.reply(MessageType.SPLIT_ACK, out, null));
    }
    private static void reject(ControlConnection connection, ControlMessage request, String code, String reason) {
        JsonObject out = new JsonObject(); out.addProperty("code", code);
        out.addProperty("message", reason != null ? reason : "分离请求非法");
        sendSafely(connection, request.reply(MessageType.ERROR, out, null));
    }
}
