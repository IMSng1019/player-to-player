package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 主客户端正常退出的接管屏障。准备时仅关闭成员的旧游戏连接，控制连接、组表与
 * 区块所有权均保留；只有最终数据已应用且旧所有权已耐久释放，才能发布新主角色。
 * 复用已有分离等待与 GROUP_WORLD_READY 流程，不把计算进程被关闭当成玩家主动退出。
 */
final class PrimaryExitHandoff {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/exit-handoff");
    private final GlobalConfig config;
    private final GroupTable groups;
    private final ComputeTable computes;
    private final PlayerTable players;
    private final Function<UUID, ControlConnection> connections;
    private final Consumer<UUID> permissionPush;
    private final BiConsumer<UUID, Set<UUID>> redirectFollowers;
    private final Map<UUID, UUID> exitingGroups = new ConcurrentHashMap<>();
    /** 清理回调会因持久化/网络失败重试，必须记住已经完成的阶段，不能重建新组。 */
    private final Map<UUID, Completion> completions = new HashMap<>();

    private static final class Completion {
        final ExitManifest manifest;
        boolean released, grouped, permissionsSent, followersQueued, primarySent;
        UUID newPrimary;
        Set<UUID> members = Set.of();
        Completion(ExitManifest manifest) { this.manifest = manifest; }
    }

    private record Waiting(UUID member, ControlConnection connection,
                           CompletableFuture<ControlMessage> reply) { }

    PrimaryExitHandoff(GlobalConfig config, GroupTable groups, ComputeTable computes,
                       PlayerTable players, Function<UUID, ControlConnection> connections,
                       Consumer<UUID> permissionPush, BiConsumer<UUID, Set<UUID>> redirectFollowers) {
        this.config = config;
        this.groups = groups;
        this.computes = computes;
        this.players = players;
        this.connections = connections;
        this.permissionPush = permissionPush;
        this.redirectFollowers = redirectFollowers;
    }

    void prepare(ExitManifest manifest) {
        if (!manifest.clientId().equals(groups.primaryOf(manifest.groupId()))) return;
        reserve(manifest);
        List<Waiting> waiting = new ArrayList<>();
        for (UUID member : manifest.playerIds()) {
            if (member.equals(manifest.clientId()) || !manifest.groupId().equals(groups.groupOf(member))) continue;
            ControlConnection connection = connections.apply(member);
            if (connection == null || !connection.isOpen()) continue;
            JsonObject pause = new JsonObject();
            pause.addProperty("splitId", manifest.shutdownId().toString());
            pause.addProperty("groupId", manifest.groupId().toString());
            pause.addProperty("reason", "primary_exit");
            // 先向所有成员发出请求再逐一等待，网络时延不会按成员数量串行累加。
            waiting.add(new Waiting(member, connection,
                    connection.request(ControlMessage.of(MessageType.SPLIT_PREPARE, pause))));
        }
        for (Waiting item : waiting) {
            try {
                ControlMessage response = item.reply().join();
                if (response.type() != MessageType.SPLIT_PREPARE
                        || !JsonUtil.getBoolean(response.json(), "accepted", false)
                        || !manifest.shutdownId().toString().equals(JsonUtil.getString(response.json(), "splitId", ""))
                        || !manifest.groupId().toString().equals(JsonUtil.getString(response.json(), "groupId", ""))) {
                    throw new IllegalStateException("成员尚未确认退出等待屏障: " + item.member());
                }
            } catch (RuntimeException failure) {
                // 明确退出/已换组者不需要接管；仍在线的同组成员缺少 ACK 则不得停服。
                // 外层 EXIT_BEGIN 保留 durable pending，下一次 BEGIN 重放将安全地重新询问。
                ControlConnection current = connections.apply(item.member());
                if (current != null && current.isOpen()
                        && manifest.groupId().equals(groups.groupOf(item.member()))) {
                    throw new IllegalStateException("等待同组成员进入退出交接失败: " + item.member(), failure);
                }
            }
            ControlConnection current = connections.apply(item.member());
            if (current != null && current.isOpen() && current != item.connection()
                    && manifest.groupId().equals(groups.groupOf(item.member()))) {
                throw new IllegalStateException("成员控制会话已替换，必须重新确认等待屏障");
            }
        }
        LOGGER.info("主客户端退出成员等待已确认: shutdownId={} members={}", manifest.shutdownId(), waiting.size());
    }

    boolean isExiting(UUID group) { return group != null && exitingGroups.containsKey(group); }

    /** 在中止旧合并之前封住新合并入口；不能让另一退出事务覆盖现有门槛。 */
    void reserve(ExitManifest manifest) {
        groups.withMutationLock(() -> {
            // ROLE_ASSIGN检查门槛和addSecondary使用同一锁；先通过检查的准入必须先
            // 完成，随后authorizer重取最终成员，不能留下未收到等待通知的晚加入玩家。
            UUID existing = exitingGroups.putIfAbsent(manifest.groupId(), manifest.shutdownId());
            if (existing != null && !existing.equals(manifest.shutdownId())) {
                throw new IllegalStateException("该组已有另一主客户端退出事务");
            }
            return null;
        });
    }

    /** BEGIN 尚未持久化时可撤销本事务门槛，其他已开始的退出事务不受影响。 */
    void cancelUnpersisted(ExitManifest manifest) {
        exitingGroups.remove(manifest.groupId(), manifest.shutdownId());
    }

    /** 只能由 ledger 的 cleanupComplete 耐久确认调用，绝不能按超时回收防重放状态。 */
    synchronized void forget(ExitManifest manifest) {
        completions.remove(manifest.shutdownId());
        exitingGroups.remove(manifest.groupId(), manifest.shutdownId());
    }

    synchronized void complete(ExitManifest manifest, Runnable releaseDurably) {
        Completion state = completions.computeIfAbsent(manifest.shutdownId(), ignored -> new Completion(manifest));
        if (!state.manifest.equals(manifest)) throw new IllegalArgumentException("退出事务清单不能被替换");
        reserve(manifest);
        if (!state.released) {
            releaseDurably.run();
            state.released = true;
        }
        if (!state.grouped) {
            groups.withMutationLock(() -> {
                if (manifest.clientId().equals(groups.primaryOf(manifest.groupId()))) {
                    Set<UUID> remaining = new HashSet<>();
                    for (UUID member : manifest.playerIds()) {
                        ControlConnection connection = connections.apply(member);
                        if (!member.equals(manifest.clientId()) && connection != null && connection.isOpen()
                                && manifest.groupId().equals(groups.groupOf(member))) remaining.add(member);
                    }
                    if (!remaining.isEmpty()) {
                        var election = computes.selectPrimaryOrRelaxed(remaining, config.minFreeMemoryBytes);
                        UUID successor = election != null ? election.clientId()
                                : remaining.stream().min(UUID::compareTo).orElseThrow();
                        // splitGroup/removeClient 共用同一组表写锁，其他组变更不能插入交接中途。
                        if (groups.splitGroup(manifest.groupId(), remaining, successor) == null) {
                            throw new IllegalStateException("退出接管成员集合已变化");
                        }
                        state.newPrimary = successor;
                        state.members = Set.copyOf(remaining);
                        for (UUID member : remaining) players.updateGroupRole(member, successor,
                                member.equals(successor) ? "primary" : "secondary");
                    }
                    groups.removeClient(manifest.clientId());
                }
                state.grouped = true;
                return null;
            });
        }
        exitingGroups.remove(manifest.groupId(), manifest.shutdownId());
        UUID successor = state.newPrimary;
        // 服务端重启后没有旧在线成员；或接管方后来已经离开/换组，均不得重放旧角色。
        if (successor != null && successor.equals(groups.primaryOf(successor))
                && successor.equals(groups.groupOf(successor))) {
            if (!state.permissionsSent) {
                permissionPush.accept(successor);
                state.permissionsSent = true;
            }
            if (!state.followersQueued) {
                Set<UUID> followers = new HashSet<>(state.members);
                followers.remove(successor);
                followers.removeIf(member -> !successor.equals(groups.groupOf(member)));
                if (!followers.isEmpty()) redirectFollowers.accept(successor, Set.copyOf(followers));
                state.followersQueued = true;
            }
            if (!state.primarySent) {
                ControlConnection connection = connections.apply(successor);
                if (connection == null || !connection.isOpen()) throw new IllegalStateException("接管方控制连接暂不可用");
                JsonObject role = new JsonObject();
                role.addProperty("role", "primary");
                role.addProperty("groupId", successor.toString());
                role.addProperty("primaryClientId", successor.toString());
                role.addProperty("splitId", manifest.shutdownId().toString());
                connection.send(ControlMessage.of(MessageType.ROLE_ASSIGN, role));
                state.primarySent = true;
                LOGGER.info("主客户端退出后已指派接管: shutdownId={} newPrimary={} members={}",
                        manifest.shutdownId(), successor, state.members.size());
            }
        }
    }
}
