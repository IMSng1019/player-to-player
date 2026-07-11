package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 合并/分离协调器（服务端，Phase 3；规范"预连接→预同步→合并"与"分离"的服务端
 * 状态机）。协议时序：
 * <pre>
 *   发起方主客户端 ──MERGE_REQUEST──→ 服务端
 *     服务端：双方在线校验 + 算力分配（ComputeTable.selectPrimary）→ 生成 mergeId
 *   服务端 ──MERGE_PLAN──→ 双方主客户端（谁是新主 newPrimary 已定）
 *     A（让出方）/B（接管方）在既有 P2P 会话上跑预同步（快照 + 增量追赶）
 *   双方 ──MERGE_PROGRESS(phase)──→ 服务端（推进状态机；failed → 双方 MERGE_ABORT）
 *   A ──MERGE_PROGRESS(switched)──→ 服务端：
 *     组表 mergeGroups + 注册表 migrateAll（原子改挂，无释放空窗）
 *   服务端 ──MERGE_COMMIT──→ 新组全体成员（副客户端据此把隧道重定向到新主）
 * </pre>
 * <b>为何服务端只当记账员</b>：预同步数据面走 A↔B 的 P2P（规范"继承"减轻服务端
 * 带宽），服务端只做撮合、选主（算力表是它的权威数据）、超时兜底与最终的
 * 组表/注册表归属切换 —— 切换是纯内存原子操作，冻结窗口不受服务端 IO 影响。
 * <p>
 * <b>超时兜底</b>：{@value #MERGE_TIMEOUT_SECONDS} 秒内未走到 switched 的合并
 * 一律中止（A 继续运行，规范"若 B 追赶失败或断开，A 继续运行，服务端取消合并"）；
 * 当事方掉线（断连清理触发 {@link #abortInvolving}）同样中止。
 * <p>
 * 线程模型：handler 在 Netty 事件循环（纯内存查表 + 发消息）；超时由 scheduler
 * 驱动。合并状态的复合变更以会话对象内的锁串行化。
 */
public final class MergeCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/merge");

    /** 合并会话总超时（秒）：预同步快照 + 追赶在家用带宽下的宽裕上界。 */
    private static final long MERGE_TIMEOUT_SECONDS = 300;

    /** 同一组对之间两次合并尝试的最小间隔（毫秒）：失败后防打洞/预同步风暴。 */
    private static final long RETRY_COOLDOWN_MILLIS = 30_000;

    /** 合并会话状态。 */
    private enum State { PLANNED, PRESYNCING, CAUGHT_UP, COMMITTED, ABORTED }

    /** 一次合并会话（groupA = 发起方组，groupB = 目标组；newPrimary 由算力分配决出）。 */
    private static final class MergeSession {
        final UUID mergeId = UUID.randomUUID();
        final UUID groupA;
        final UUID groupB;
        final UUID newPrimary;
        final UUID oldPrimary;
        volatile State state = State.PLANNED;
        volatile ScheduledFuture<?> timeoutTask;

        MergeSession(UUID groupA, UUID groupB, UUID newPrimary, UUID oldPrimary) {
            this.groupA = groupA;
            this.groupB = groupB;
            this.newPrimary = newPrimary;
            this.oldPrimary = oldPrimary;
        }

        boolean involves(UUID clientOrGroup) {
            return groupA.equals(clientOrGroup) || groupB.equals(clientOrGroup)
                    || newPrimary.equals(clientOrGroup) || oldPrimary.equals(clientOrGroup);
        }
    }

    /** 活跃合并会话：mergeId → 会话。 */
    private final Map<UUID, MergeSession> sessions = new ConcurrentHashMap<>();

    /** 冷却表："小组id|大组id"（有序拼接）→ 上次尝试时刻。惰性清理。 */
    private final Map<String, Long> cooldowns = new ConcurrentHashMap<>();

    /**
     * 组分离（多端）的暂存重定向：新组 groupId → 待重定向的副客户端集合（Phase 4）。
     * 新主的组世界没就绪前推送 ROLE_ASSIGN(secondary) 必然导致副客户端隧道连接失败
     * （LAN 端口尚未发布），故暂存至收到该组的 {@code GROUP_WORLD_READY} 再冲刷；
     * {@value #REDIRECT_FLUSH_TIMEOUT_SECONDS} 秒未就绪则兜底冲刷（副客户端侧
     * P2P/隧道自带超时与失败留守，宁可尝试也不让玩家吊死在旧组的冻结视野里）。
     */
    private final Map<UUID, Set<UUID>> pendingRedirects = new ConcurrentHashMap<>();

    /** 暂存重定向的兜底冲刷时限（秒）：覆盖新主首次建世界（拷贝骨架+开服）的上界。 */
    private static final long REDIRECT_FLUSH_TIMEOUT_SECONDS = 60;

    private final GlobalConfig config;
    private final ChunkRegistry registry;
    private final GroupTable groups;
    private final ComputeTable computes;
    private final PlayerTable players;

    private MergeCoordinator(GlobalConfig config, ChunkRegistry registry, GroupTable groups,
                             ComputeTable computes, PlayerTable players) {
        this.config = config;
        this.registry = registry;
        this.groups = groups;
        this.computes = computes;
        this.players = players;
    }

    /**
     * 注册合并/分离处理器。
     *
     * @return 协调器实例（P2PServerService 断连清理时调用 {@link #abortInvolving}）
     */
    public static MergeCoordinator register(HandlerRegistry reg, GlobalConfig config,
                                            ChunkRegistry registry, GroupTable groups,
                                            ComputeTable computes, PlayerTable players) {
        MergeCoordinator coordinator =
                new MergeCoordinator(config, registry, groups, computes, players);
        reg.on(MessageType.MERGE_REQUEST, coordinator::handleMergeRequest);
        reg.on(MessageType.MERGE_PROGRESS, coordinator::handleProgress);
        reg.on(MessageType.SPLIT_REQUEST, coordinator::handleSplitRequest);
        reg.on(MessageType.GROUP_WORLD_READY, coordinator::handleGroupWorldReady);
        return coordinator;
    }

    // ------------------------------------------------------------ 合并申请

    /** MERGE_REQUEST：校验 + 算力分配 + 下发 MERGE_PLAN（Netty 线程，纯内存）。 */
    private void handleMergeRequest(ControlConnection conn, ControlMessage msg) {
        UUID requester = conn.peerId();
        if (requester == null) {
            conn.send(error(msg, "not_authenticated", "须先完成 HELLO 握手"));
            return;
        }
        UUID targetGroup = parseUuid(JsonUtil.getString(msg.json(), "targetGroupId", ""));
        // Phase 1/2/3 口径：groupId == 主客户端 clientId，发起方组即其自身 id
        UUID requesterGroup = groups.groupOf(requester);
        if (targetGroup == null || requesterGroup == null) {
            conn.send(error(msg, "invalid_request", "targetGroupId 非法或发起方无组"));
            return;
        }
        if (!requester.equals(groups.primaryOf(requesterGroup))) {
            conn.send(error(msg, "not_primary", "只有主客户端可发起合并"));
            return;
        }
        if (targetGroup.equals(requesterGroup)) {
            conn.send(error(msg, "self_merge", "不能与本组合并"));
            return;
        }
        UUID targetPrimary = groups.primaryOf(targetGroup);
        ControlConnection targetConn =
                targetPrimary != null ? HelloHandler.connectionOf(targetPrimary) : null;
        if (targetPrimary == null || targetConn == null || !targetConn.isOpen()) {
            conn.send(error(msg, "target_offline", "目标组不在线: " + targetGroup));
            return;
        }
        // 冷却：同组对短期内只允许一次尝试（失败风暴防护）
        String pairKey = pairKey(requesterGroup, targetGroup);
        long now = System.currentTimeMillis();
        Long last = cooldowns.get(pairKey);
        if (last != null && now - last < RETRY_COOLDOWN_MILLIS) {
            conn.send(error(msg, "cooldown", "该组对处于合并冷却期"));
            return;
        }
        // 双方任一已卷入其他合并会话：拒绝（一次只处理一对，状态机才可推理）
        for (MergeSession s : sessions.values()) {
            if (s.state != State.ABORTED && s.state != State.COMMITTED
                    && (s.involves(requesterGroup) || s.involves(targetGroup))) {
                conn.send(error(msg, "busy", "有合并会话进行中: " + s.mergeId));
                return;
            }
        }

        // ---- 算力分配（规范：两个主客户端选出算力更强的作为新的主客户端）----
        List<UUID> candidates = List.of(requester, targetPrimary);
        UUID newPrimary = computes.selectPrimary(candidates, config.minFreeMemoryBytes);
        if (newPrimary == null) {
            // 均不达内存门槛/未上报：兜底取目标组主（"既有主不动摇"最小扰动），记告警
            newPrimary = targetPrimary;
            LOGGER.warn("合并选主无合格候选（内存门槛 {}MB），兜底沿用目标组主 {}",
                    config.minFreeMemoryBytes / (1024 * 1024), targetPrimary);
        }
        UUID oldPrimary = newPrimary.equals(requester) ? targetPrimary : requester;

        MergeSession session = new MergeSession(requesterGroup, targetGroup, newPrimary, oldPrimary);
        sessions.put(session.mergeId, session);
        cooldowns.put(pairKey, now);
        purgeCooldowns(now);

        // 超时兜底：到点仍未 switched → 双方 MERGE_ABORT，A 继续运行
        session.timeoutTask = ThreadPools.scheduler().schedule(
                () -> abort(session, "合并超时（" + MERGE_TIMEOUT_SECONDS + "s）"),
                MERGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // MERGE_PLAN 发给双方主客户端（reply 给发起方带 _rid；目标方单发）
        JsonObject plan = buildPlan(session);
        conn.send(msg.reply(MessageType.MERGE_PLAN, plan.deepCopy(), null));
        targetConn.send(ControlMessage.of(MessageType.MERGE_PLAN, plan.deepCopy()));
        LOGGER.info("合并计划已下发: mergeId={} 组 {} + 组 {} → 新主 {}（让出方 {}）",
                session.mergeId, requesterGroup, targetGroup, newPrimary, oldPrimary);
    }

    /** 组装 MERGE_PLAN 消息体（双方收到同一份，靠 newPrimaryClientId 识别自己的角色）。 */
    private JsonObject buildPlan(MergeSession session) {
        JsonObject plan = new JsonObject();
        plan.addProperty("mergeId", session.mergeId.toString());
        plan.addProperty("newPrimaryClientId", session.newPrimary.toString());
        plan.addProperty("oldPrimaryClientId", session.oldPrimary.toString());
        plan.addProperty("groupAId", session.groupA.toString());
        plan.addProperty("groupBId", session.groupB.toString());
        JsonArray members = new JsonArray();
        for (UUID m : groups.membersOf(session.groupA)) {
            members.add(m.toString());
        }
        for (UUID m : groups.membersOf(session.groupB)) {
            members.add(m.toString());
        }
        plan.add("members", members);
        return plan;
    }

    // ------------------------------------------------------------ 阶段回报

    /** MERGE_PROGRESS：推进状态机；switched 即提交，failed 即中止。 */
    private void handleProgress(ControlConnection conn, ControlMessage msg) {
        UUID reporter = conn.peerId();
        UUID mergeId = parseUuid(JsonUtil.getString(msg.json(), "mergeId", ""));
        MergeSession session = mergeId != null ? sessions.get(mergeId) : null;
        if (reporter == null || session == null
                || !(reporter.equals(session.newPrimary) || reporter.equals(session.oldPrimary))) {
            return; // 未知会话 / 非当事方（入站不可信）：忽略
        }
        String phase = JsonUtil.getString(msg.json(), "phase", "");
        boolean success = JsonUtil.getBoolean(msg.json(), "success", false);
        LOGGER.info("合并进度: mergeId={} 回报方={} phase={} success={}",
                mergeId, reporter, phase, success);
        if (!success || "failed".equals(phase)) {
            abort(session, "当事方回报失败: " + JsonUtil.getString(msg.json(), "detail", phase));
            return;
        }
        switch (phase) {
            case "presync_started" -> session.state = State.PRESYNCING;
            case "caught_up" -> session.state = State.CAUGHT_UP;
            // switched 由让出方 A 在"暂停 tick + 尾部增量已被 B 确认"后回报：
            // 此刻 B 已接管演算，服务端执行归属切换（纯内存原子操作）
            case "switched" -> commit(session);
            default -> {
                // snapshot_done 等中间阶段：仅记日志（上面已 info），状态不回退
            }
        }
    }

    /** 提交合并：组表合并 + 注册表整组迁移 + 玩家表刷新 + MERGE_COMMIT 广播。 */
    private void commit(MergeSession session) {
        if (session.state == State.COMMITTED || session.state == State.ABORTED) {
            return; // 幂等（重复 switched 回报 / 与超时中止竞态）
        }
        GroupTable.GroupInfo merged =
                groups.mergeGroups(session.groupA, session.groupB, session.newPrimary);
        if (merged == null) {
            abort(session, "组表合并失败（组在提交前已解散？）");
            return;
        }
        session.state = State.COMMITTED;
        cancelTimeout(session);
        sessions.remove(session.mergeId);
        // 注册表：两旧组的占用全部原子改挂到新组（groupId == newPrimary），
        // 无"释放-再申请"空窗，第三组不可能趁隙抢占
        int migrated = registry.migrateAll(session.groupA, session.newPrimary)
                + registry.migrateAll(session.groupB, session.newPrimary);

        // MERGE_COMMIT 广播给新组全体在线成员（副客户端据此重定向隧道到新主）
        JsonObject commit = new JsonObject();
        commit.addProperty("mergeId", session.mergeId.toString());
        commit.addProperty("groupId", merged.groupId().toString());
        commit.addProperty("primaryClientId", session.newPrimary.toString());
        JsonArray memberArray = new JsonArray();
        for (UUID m : merged.members()) {
            memberArray.add(m.toString());
        }
        commit.add("members", memberArray);
        for (UUID member : merged.members()) {
            // 玩家表同步刷新组与端级别（规范：玩家表记录组客户端与主副等级）
            players.updateGroupRole(member, merged.groupId(),
                    member.equals(session.newPrimary) ? "primary" : "secondary");
            ControlConnection c = HelloHandler.connectionOf(member);
            if (c != null && c.isOpen()) {
                c.send(ControlMessage.of(MessageType.MERGE_COMMIT, commit.deepCopy()));
            }
        }
        LOGGER.info("合并已提交: mergeId={} 新组 {}（{} 名成员，迁移 {} 个区块占用）",
                session.mergeId, merged.groupId(), merged.members().size(), migrated);
    }

    /** 中止合并：双方 MERGE_ABORT，A 继续运行（规范"服务端取消合并"）。幂等。 */
    private void abort(MergeSession session, String reason) {
        if (session.state == State.COMMITTED || session.state == State.ABORTED) {
            return;
        }
        session.state = State.ABORTED;
        cancelTimeout(session);
        sessions.remove(session.mergeId);
        JsonObject json = new JsonObject();
        json.addProperty("mergeId", session.mergeId.toString());
        json.addProperty("reason", reason);
        for (UUID party : List.of(session.newPrimary, session.oldPrimary)) {
            ControlConnection c = HelloHandler.connectionOf(party);
            if (c != null && c.isOpen()) {
                c.send(ControlMessage.of(MessageType.MERGE_ABORT, json.deepCopy()));
            }
        }
        LOGGER.warn("合并已中止: mergeId={} 原因: {}", session.mergeId, reason);
    }

    /**
     * 中止涉及某客户端/组的全部合并会话（P2PServerService 断连清理调用）。
     * 规范"若 B 追赶失败或断开，A 继续运行"。
     */
    public void abortInvolving(UUID clientId) {
        if (clientId == null) {
            return;
        }
        for (MergeSession session : sessions.values()) {
            if (session.involves(clientId)) {
                abort(session, "当事方掉线: " + clientId);
            }
        }
    }

    // ------------------------------------------------------------ 分离申请

    /**
     * SPLIT_REQUEST：主客户端发来的分离申请（规范"单端分离/组分离"）。
     * <p>
     * 检测（渲染区域无交集 10s）在主客户端侧完成（它拥有实时视距数据），服务端
     * 只做权威切换：组表 splitGroup + 注册表定向迁移 departing 名下区块。
     * departing 成员在申请前已完成预同步（10s 窗口内后台进行），SPLIT_ACK 到达
     * 即可各自为政。
     * <p>
     * <b>组分离（多端，Phase 4）</b>：{@code departingClientIds} 携带多名离组成员时，
     * 服务端在离组集合内做算力分配（规范"分为两个组客户端……每个组内进行算力分配"，
     * {@link ComputeTable#selectPrimary} 与合并选主同一套规则），胜者直发
     * ROLE_ASSIGN(primary) 自立门户；其余成员的 ROLE_ASSIGN(secondary) 重定向指派
     * <b>暂存</b>到新主回报 {@code GROUP_WORLD_READY}（LAN 已发布）后冲刷 ——
     * 见 {@link #pendingRedirects}。单成员分离（departing=1）是其特例，无暂存环节。
     */
    private void handleSplitRequest(ControlConnection conn, ControlMessage msg) {
        UUID requester = conn.peerId();
        if (requester == null) {
            conn.send(error(msg, "not_authenticated", "须先完成 HELLO 握手"));
            return;
        }
        UUID groupId = parseUuid(JsonUtil.getString(msg.json(), "groupId", ""));
        // Phase 4：优先读多成员数组；缺失时回退 Phase 3 的单成员字段
        Set<UUID> departingSet = new HashSet<>();
        if (msg.json().has("departingClientIds") && msg.json().get("departingClientIds").isJsonArray()) {
            for (var el : msg.json().getAsJsonArray("departingClientIds")) {
                try {
                    departingSet.add(UUID.fromString(el.getAsString()));
                } catch (Exception ignored) {
                    // 单条坏数据跳过（入站不可信）
                }
            }
        }
        UUID single = parseUuid(JsonUtil.getString(msg.json(), "departingClientId", ""));
        if (single != null) {
            departingSet.add(single);
        }
        if (groupId == null || departingSet.isEmpty()) {
            conn.send(error(msg, "invalid_request", "groupId/departingClientId(s) 缺失或非法"));
            return;
        }
        if (!requester.equals(groups.primaryOf(groupId))) {
            conn.send(error(msg, "not_primary", "只有该组主客户端可申请分离"));
            return;
        }
        // 分离期间不与合并并行（状态机相互干扰）
        for (MergeSession s : sessions.values()) {
            if (s.state != State.ABORTED && s.state != State.COMMITTED && s.involves(groupId)) {
                conn.send(error(msg, "busy", "该组有合并会话进行中"));
                return;
            }
        }
        // 解析随请求声明的迁移区块（departing 成员的渲染区块；服务端只迁"确属原组"的）
        List<ChunkKey> chunks = new ArrayList<>();
        if (msg.json().has("chunks") && msg.json().get("chunks").isJsonArray()) {
            for (var el : msg.json().getAsJsonArray("chunks")) {
                try {
                    chunks.add(ChunkKey.parse(el.getAsString()));
                } catch (Exception ignored) {
                    // 单条坏数据跳过（入站不可信）
                }
            }
        }
        // 离组集合内算力分配（规范"每个组内进行算力分配"）；无算力数据时取
        // UUID 字典序最小者兜底 —— 与 ComputeTable 决胜规则同源，保证确定性
        UUID newPrimary = computes.selectPrimary(departingSet, config.minFreeMemoryBytes);
        if (newPrimary == null) {
            newPrimary = departingSet.stream().min(UUID::compareTo).orElseThrow();
            LOGGER.warn("离组成员无可用算力数据，按 UUID 字典序兜底选主: {}", newPrimary);
        }
        GroupTable.GroupInfo fresh = groups.splitGroup(groupId, departingSet, newPrimary);
        JsonObject out = new JsonObject();
        if (fresh == null) {
            out.addProperty("granted", false);
            conn.send(msg.reply(MessageType.SPLIT_ACK, out, null));
            LOGGER.warn("分离申请被拒（组不存在/成员不符）: 组={} 离组={}", groupId, departingSet);
            return;
        }
        int migrated = registry.migrate(chunks, groupId, fresh.groupId());
        for (UUID member : departingSet) {
            players.updateGroupRole(member, fresh.groupId(),
                    member.equals(newPrimary) ? "primary" : "secondary");
        }
        out.addProperty("granted", true);
        out.addProperty("newGroupId", fresh.groupId().toString());
        out.addProperty("newPrimaryClientId", newPrimary.toString());
        out.addProperty("migratedChunks", migrated);
        conn.send(msg.reply(MessageType.SPLIT_ACK, out, null));
        // 通知新主它已是新组的主（它可能在等 SPLIT_ACK 的转发，直发更可靠）
        ControlConnection primaryConn = HelloHandler.connectionOf(newPrimary);
        if (primaryConn != null && primaryConn.isOpen()) {
            JsonObject assign = new JsonObject();
            assign.addProperty("role", "primary");
            assign.addProperty("groupId", fresh.groupId().toString());
            assign.addProperty("primaryClientId", newPrimary.toString());
            primaryConn.send(ControlMessage.of(MessageType.ROLE_ASSIGN, assign));
        }
        // 其余离组成员：暂存重定向，等新主世界就绪（GROUP_WORLD_READY）再冲刷
        Set<UUID> followers = new HashSet<>(departingSet);
        followers.remove(newPrimary);
        if (!followers.isEmpty()) {
            UUID newGroupId = fresh.groupId();
            pendingRedirects.put(newGroupId, followers);
            ThreadPools.scheduler().schedule(
                    () -> flushRedirects(newGroupId, "超时兜底"),
                    REDIRECT_FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        LOGGER.info("分离已执行: 组 {} → {} 名离组成员自立新组 {}（新主 {}，迁移 {} 个区块）",
                groupId, departingSet.size(), fresh.groupId(), newPrimary, migrated);
    }

    /**
     * GROUP_WORLD_READY：主客户端的组世界完成 LAN 发布（Phase 4）。
     * 冲刷该组暂存的副客户端重定向 —— 此刻新主已可接待隧道，重定向不再扑空。
     * 普通主客户端（无暂存项）的就绪通告是无害的空操作。
     */
    private void handleGroupWorldReady(ControlConnection conn, ControlMessage msg) {
        UUID reporter = conn.peerId();
        if (reporter == null) {
            return; // 未握手连接：忽略（推送语义无应答）
        }
        // 口径：groupId == 主客户端 clientId，就绪方即组 id
        flushRedirects(reporter, "世界就绪");
    }

    /** 冲刷某组暂存的副客户端重定向（就绪通告与超时兜底竞争，remove 保证只发一次）。 */
    private void flushRedirects(UUID newGroupId, String cause) {
        Set<UUID> followers = pendingRedirects.remove(newGroupId);
        if (followers == null || followers.isEmpty()) {
            return;
        }
        UUID newPrimary = groups.primaryOf(newGroupId);
        if (newPrimary == null) {
            LOGGER.warn("重定向冲刷时新组 {} 已不存在，放弃（触发: {}）", newGroupId, cause);
            return;
        }
        JsonObject assign = new JsonObject();
        assign.addProperty("role", "secondary");
        assign.addProperty("groupId", newGroupId.toString());
        assign.addProperty("primaryClientId", newPrimary.toString());
        int sent = 0;
        for (UUID follower : followers) {
            ControlConnection c = HelloHandler.connectionOf(follower);
            if (c != null && c.isOpen()) {
                c.send(ControlMessage.of(MessageType.ROLE_ASSIGN, assign.deepCopy()));
                sent++;
            }
        }
        LOGGER.info("组分离重定向已冲刷: 新组 {} 新主 {}，通知 {}/{} 名副客户端（触发: {}）",
                newGroupId, newPrimary, sent, followers.size(), cause);
    }

    // ------------------------------------------------------------ 工具

    /** 服务停止清理：中止全部会话并清空表。 */
    public void shutdown() {
        for (MergeSession session : sessions.values()) {
            abort(session, "服务端停止");
        }
        sessions.clear();
        cooldowns.clear();
        pendingRedirects.clear();
    }

    private void cancelTimeout(MergeSession session) {
        ScheduledFuture<?> task = session.timeoutTask;
        if (task != null) {
            task.cancel(false);
        }
    }

    /** 组对的冷却键（UUID 有序拼接，方向无关）。 */
    private static String pairKey(UUID a, UUID b) {
        return a.compareTo(b) < 0 ? a + "|" + b : b + "|" + a;
    }

    /** 惰性清理超龄冷却条目（无定时器，借请求路径顺带做）。 */
    private void purgeCooldowns(long now) {
        cooldowns.entrySet().removeIf(e -> now - e.getValue() > RETRY_COOLDOWN_MILLIS * 4);
    }

    /** 防御性 UUID 解析：null / 格式非法返回 null 而非抛异常。 */
    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** 构造保留 _rid 的 ERROR 应答。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
