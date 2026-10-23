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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
 * <p>
 * <b>选主的两个概念（切勿混淆，见 {@link #handleMergeRequest}）</b>：
 * <ul>
 *   <li><b>算力分配（应然）</b>：规范"算力分配：重新分配此区域中的玩家的客户端等级
 *       （主客户端和副客户端） 通过算力表 算力最高的为主客户端" —— 候选集是
 *       合并双方的<b>全部成员</b>（含副客户端），它回答"谁<u>应该</u>是主"；</li>
 *   <li><b>接管方 newPrimary（实然 / 本版本能力边界）</b>：预同步的 B 侧必须
 *       "当前就在跑集成服务端"（客户端 handlePlan 只认当事双方，B 经 GroupHost
 *       把快照写进自己的组世界），故它只能是<b>当前主客户端</b> —— 它回答
 *       "谁<u>能够</u>接管"。副客户端当选时无法接管（避免死组），按能力边界执行
 *       并打 WARN 归因。</li>
 * </ul>
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
        final Set<UUID> preparingMembers;
        final Set<UUID> awaitingRedirects;
        final MergeReadiness readiness;
        boolean drainSent;
        volatile State state = State.PLANNED;
        volatile ScheduledFuture<?> timeoutTask;

        MergeSession(UUID groupA, UUID groupB, UUID newPrimary, UUID oldPrimary, Set<UUID> movingMembers) {
            this.groupA = groupA;
            this.groupB = groupB;
            this.newPrimary = newPrimary;
            this.oldPrimary = oldPrimary;
            this.preparingMembers = new HashSet<>(movingMembers);
            this.preparingMembers.add(newPrimary); // B 的接收器同样必须先完成挂接
            this.awaitingRedirects = new HashSet<>(movingMembers);
            this.awaitingRedirects.remove(oldPrimary);
            this.readiness = new MergeReadiness(oldPrimary, newPrimary, preparingMembers);
        }

        boolean involves(UUID clientOrGroup) {
            return groupA.equals(clientOrGroup) || groupB.equals(clientOrGroup)
                    || preparingMembers.contains(clientOrGroup);
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
     * {@value #REDIRECT_FLUSH_TIMEOUT_SECONDS} 秒未就绪则明确通知失败，
     * 不把超时当成就绪，也不让副客户端盲目连接尚未发布的 LAN。
     */
    private final PendingGroupRedirects pendingRedirects = new PendingGroupRedirects();

    /** 暂存重定向的兜底冲刷时限（秒）：覆盖新主首次建世界（拷贝骨架+开服）的上界。 */
    private static final long REDIRECT_FLUSH_TIMEOUT_SECONDS = 60;

    private final GlobalConfig config;
    private final ChunkRegistry registry;
    private final GroupTable groups;
    private final ComputeTable computes;
    private final PlayerTable players;
    /** 权限下发：合并/分离改变组成员后重推两边的名单（本地权限位跟着组走）。 */
    private final PermissionRelay permissions;
    private final SplitCoordinator splits;

    private MergeCoordinator(GlobalConfig config, ChunkRegistry registry, GroupTable groups,
                             ComputeTable computes, PlayerTable players,
                             PermissionRelay permissions) {
        this.config = config;
        this.registry = registry;
        this.groups = groups;
        this.computes = computes;
        this.players = players;
        this.permissions = permissions;
        this.splits = new SplitCoordinator(config, registry, groups, computes, players,
                HelloHandler::connectionOf, permissions::push, (newGroup, followers) -> {
                    PendingGroupRedirects.Entry pending = pendingRedirects.register(newGroup, followers);
                    ThreadPools.scheduler().schedule(() -> failPendingRedirects(newGroup, pending),
                            REDIRECT_FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                });
    }

    /**
     * 注册合并/分离处理器。
     *
     * @return 协调器实例（P2PServerService 断连清理时调用 {@link #abortInvolving}）
     */
    public static MergeCoordinator register(HandlerRegistry reg, GlobalConfig config,
                                            ChunkRegistry registry, GroupTable groups,
                                            ComputeTable computes, PlayerTable players,
                                            PermissionRelay permissions) {
        MergeCoordinator coordinator =
                new MergeCoordinator(config, registry, groups, computes, players, permissions);
        reg.on(MessageType.MERGE_REQUEST, coordinator::handleMergeRequest);
        reg.on(MessageType.MERGE_PROGRESS, coordinator::handleProgress);
        reg.on(MessageType.SPLIT_REQUEST, coordinator::handleSplitRequest);
        reg.on(MessageType.GROUP_WORLD_READY, coordinator::handleGroupWorldReady);
        return coordinator;
    }

    // ------------------------------------------------------------ 合并申请

    /** MERGE_REQUEST：校验 + 算力分配 + 下发 MERGE_PLAN（Netty 线程，纯内存）。 */
    private synchronized void handleMergeRequest(ControlConnection conn, ControlMessage msg) {
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
        if (splits.involvesGroup(requesterGroup) || splits.involvesGroup(targetGroup)) {
            conn.send(error(msg, "busy", "分离交接正在排空旧区域，暂不允许重叠合并"));
            return;
        }
        UUID targetPrimary = groups.primaryOf(targetGroup);
        ControlConnection targetConn =
                targetPrimary != null ? HelloHandler.connectionOf(targetPrimary) : null;
        if (targetPrimary == null || targetConn == null || !targetConn.isOpen()) {
            conn.send(error(msg, "target_offline", "目标组不在线: " + targetGroup));
            return;
        }
        // 不能把两个不同启动环境的NBT/注册表合并；只认角色准入时固定的版本证据，
        // 不认同UUID后来重新HELLO的声明，也不以动态存档snapshotId判定静态环境。
        if (!EnvironmentAdmissionGate.SERVER.sameRuntime(requester, targetPrimary)) {
            conn.send(error(msg, "environment_version_mismatch", "两组运行环境版本不同，禁止合并，请安全退出旧版本组后重新连接。"));
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
            if (s.state != State.ABORTED
                    && (s.involves(requesterGroup) || s.involves(targetGroup))) {
                conn.send(error(msg, "busy", "有合并会话进行中: " + s.mergeId));
                return;
            }
        }

        // ---- 算力分配（规范："算力分配：重新分配此区域中的玩家的客户端等级（主客户端和副客户端）
        //      通过算力表 算力最高的为主客户端"；"合并：……将两个组客户端合为一个组客户端"）----
        // 【概念一 · 应然】谁应该当主：候选集必须是两组的**全部成员**（含副客户端）。
        // 旧实现只比较两个主客户端，组里算力更高的副客户端永远无法当选，直接违反
        // "算力最高的为主客户端"；而分离选主（handleSplitRequest）早就是"在离组集合内选"，
        // 本处与它对齐。内存门槛的确定性降级兜底由 ComputeTable 内部统一负责。
        List<UUID> candidates = mergeCandidates(
                groups.membersOf(requesterGroup), groups.membersOf(targetGroup));
        ComputeTable.Selection election =
                computes.selectPrimaryOrRelaxed(candidates, config.minFreeMemoryBytes);
        UUID elected = election != null ? election.clientId() : null;
        if (election != null && election.memoryRelaxed()) {
            // 全员都不达内存门槛：规范"算力最高的为主客户端"优先于内存门槛（确定性降级）
            LOGGER.warn("合并选主：两组 {} 名候选均未达内存门槛 {}MB，按算力降级选出 {}（排序分 {}）",
                    candidates.size(), config.minFreeMemoryBytes / (1024 * 1024),
                    elected, computes.rankOf(elected));
        }
        // 【概念二 · 实然 / 本版本能力边界】谁能够**接管**：预同步 B 侧必须"当前就在跑
        // 集成服务端" —— 客户端 MergeClient.handlePlan 只认 self==newPrimary/oldPrimary，
        // 接管方要经 GroupHost 接收快照/增量并接进自己的组世界；副客户端没有组世界，
        // 一旦把它定为 newPrimary：MERGE_PLAN 无人可收、A 组主永不冻结让出，
        // 而 commit 仍把两组并到它名下 → 死组。故接管方只能从两个当前主客户端中产生。
        UUID newPrimary;
        if (elected != null && (elected.equals(requester) || elected.equals(targetPrimary))) {
            newPrimary = elected; // 应然 = 实然：规范"算力最高的为主客户端"达成
        } else {
            // 应然 ≠ 实然（算力最高者是副客户端，或两组都没上报算力）：
            // 退化为两个当前主客户端之间按同一套规则选强者接管 —— 不劣于旧实现，
            // 绝不出现"选出来的主比落选者更弱"的回归。
            ComputeTable.Selection takeover = computes.selectPrimaryOrRelaxed(
                    mergeCandidates(List.of(requester), List.of(targetPrimary)),
                    config.minFreeMemoryBytes);
            newPrimary = takeover != null ? takeover.clientId() : targetPrimary;
            if (elected != null) {
                LOGGER.warn("合并选主：应当当选者 {}（排序分 {}）是副客户端，无法在本版本接管；"
                        + "实际接管者 {}（排序分 {}）。下一步：需客户端侧实现“副→主就地晋升”"
                        + "（服务端推送 ROLE_ASSIGN(primary) → 客户端 LocalWorldLauncher 拉起本地组世界"
                        + " → 其余成员经 pendingRedirects 重定向）后，算力最高者才能真正当选",
                        elected, computes.rankOf(elected), newPrimary, computes.rankOf(newPrimary));
            } else {
                LOGGER.warn("合并选主：两组共 {} 名候选均未上报算力，无法判定应当当选者；"
                        + "实际接管者 {}（排序分 {}）",
                        candidates.size(), newPrimary, computes.rankOf(newPrimary));
            }
        }
        // 让出方 A = 不含接管方的那一组的当前主客户端 —— 两个角色因此恒为两个当前主客户端
        MergeRoles roles = rolesForTaker(newPrimary, requester, targetPrimary);
        if (roles == null) {
            conn.send(error(msg, "no_taker", "无可用接管方（当前主客户端状态异常）"));
            return;
        }
        UUID oldPrimary = roles.oldPrimary();

        MergeSession session = new MergeSession(requesterGroup, targetGroup, newPrimary, oldPrimary,
                groups.membersOf(groups.groupOf(oldPrimary)));
        sessions.put(session.mergeId, session);
        cooldowns.put(pairKey, now);
        purgeCooldowns(now);

        // 超时兜底：到点仍未 switched → 双方 MERGE_ABORT，A 继续运行
        session.timeoutTask = ThreadPools.scheduler().schedule(
                () -> abort(session, "合并超时（" + MERGE_TIMEOUT_SECONDS + "s）"),
                MERGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // 所有需要重定向的成员在旧世界仍运行时准备隧道；B 原组副客户端继续使用原连接。
        JsonObject plan = buildPlan(session);
        conn.send(msg.reply(MessageType.MERGE_PLAN, plan.deepCopy(), null));
        for (UUID member : session.preparingMembers) {
            if (member.equals(requester)) continue;
            ControlConnection recipient = HelloHandler.connectionOf(member);
            if (recipient == null || !recipient.isOpen()) {
                abort(session, "准备成员已离线");
                return;
            }
            recipient.send(ControlMessage.of(MessageType.MERGE_PLAN, plan.deepCopy()));
        }
        LOGGER.info("合并计划已下发: mergeId={} 组 {} + 组 {} → 新主 {}（让出方 {}；"
                        + "全成员选举胜者 {}，候选 {} 人）",
                session.mergeId, requesterGroup, targetGroup, newPrimary, oldPrimary,
                elected, candidates.size());
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
    private synchronized void handleProgress(ControlConnection conn, ControlMessage msg) {
        UUID reporter = conn.peerId();
        UUID mergeId = parseUuid(JsonUtil.getString(msg.json(), "mergeId", ""));
        MergeSession session = mergeId != null ? sessions.get(mergeId) : null;
        if (reporter == null || session == null
                || !session.preparingMembers.contains(reporter)
                || HelloHandler.connectionOf(reporter) != conn) {
            return; // 未知会话 / 非当事方（入站不可信）：忽略
        }
        String phase = JsonUtil.getString(msg.json(), "phase", "");
        if (session.state == State.COMMITTED) {
            if ("redirected".equals(phase)) {
                if (reporter.equals(session.oldPrimary) && session.drainSent) {
                    cancelTimeout(session);
                    sessions.remove(session.mergeId);
                } else {
                    session.awaitingRedirects.remove(reporter);
                    finishRedirects(session, false);
                }
            }
            return; // 提交后的迟到失败绝不能解冻旧写者
        }
        boolean success = JsonUtil.getBoolean(msg.json(), "success", false);
        LOGGER.info("合并进度: mergeId={} 回报方={} phase={} success={}",
                mergeId, reporter, phase, success);
        if (!success || "failed".equals(phase)) {
            abort(session, "当事方回报失败: " + JsonUtil.getString(msg.json(), "detail", phase));
            return;
        }
        switch (phase) {
            case "transport_ready" -> {
                if (session.readiness.prepared(reporter)) {
                    session.state = State.PRESYNCING;
                    JsonObject start = new JsonObject();
                    start.addProperty("mergeId", session.mergeId.toString());
                    ControlConnection source = HelloHandler.connectionOf(session.oldPrimary);
                    if (source != null && source.isOpen())
                        source.send(ControlMessage.of(MessageType.MERGE_START, start));
                    else abort(session, "原主在准备后离线");
                }
            }
            case "presync_started" -> { /* 诊断事件，不允许倒退状态 */ }
            case "caught_up" -> {
                if (session.readiness.tailReceived(reporter)) commit(session);
            }
            // switched 由让出方 A 在"暂停 tick + 尾部增量已被 B 确认"后回报：
            // 此刻 B 已接管演算，服务端执行归属切换（纯内存原子操作）
            case "switched" -> {
                if (session.readiness.sourceFrozen(reporter)) commit(session);
            }
            default -> {
                // snapshot_done 等中间阶段：仅记日志（上面已 info），状态不回退
            }
        }
    }

    /** 提交合并：组表合并 + 注册表整组迁移 + 玩家表刷新 + MERGE_COMMIT 广播。 */
    private synchronized void commit(MergeSession session) {
        if (session.state == State.COMMITTED || session.state == State.ABORTED) {
            return; // 幂等（重复 switched 回报 / 与超时中止竞态）
        }
        if (!EnvironmentAdmissionGate.SERVER.sameRuntime(
                groups.primaryOf(session.groupA), groups.primaryOf(session.groupB))) {
            abort(session, "合并提交前运行环境版本已变化或版本证据缺失");
            return;
        }
        if (!session.readiness.tryCommit()) return;
        Set<UUID> preparedSource = new HashSet<>(session.preparingMembers);
        preparedSource.remove(session.newPrimary);
        GroupTable.GroupInfo merged = groups.mergePreparedGroups(
                session.groupA, session.groupB, session.newPrimary, session.oldPrimary, preparedSource);
        if (merged == null) {
            abort(session, "组表合并失败（组在提交前已解散？）");
            return;
        }
        session.state = State.COMMITTED;
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
        // 组成员变了：新主拿到合并后的完整名单（旧两组的签名一并作废）
        permissions.push(session.groupA);
        permissions.push(session.groupB);
        permissions.push(merged.groupId());
        finishRedirects(session, false);
    }

    /** 先让副客户端主动断开旧连接，再允许 A 关服，避免旧隧道先断导致控制会话被拆。 */
    private void finishRedirects(MergeSession session, boolean timedOut) {
        if (session.state != State.COMMITTED || (!timedOut && !session.awaitingRedirects.isEmpty())) return;
        JsonObject drained = new JsonObject();
        drained.addProperty("mergeId", session.mergeId.toString());
        ControlConnection source = HelloHandler.connectionOf(session.oldPrimary);
        if (!session.drainSent && source != null && source.isOpen()) {
            session.drainSent = true;
            source.send(ControlMessage.of(MessageType.MERGE_DRAINED, drained));
        }
        // A 自身也完成原版断开前，组仍被本事务占用，防第三组计划覆盖客户端的 active。
        if (timedOut || source == null || !source.isOpen()) {
            cancelTimeout(session);
            sessions.remove(session.mergeId);
        }
        if (timedOut) LOGGER.warn("合并 {} 已提交，但部分旧连接未确认重定向，结束排空等待", session.mergeId);
    }

    /** 中止合并：双方 MERGE_ABORT，A 继续运行（规范"服务端取消合并"）。幂等。 */
    private synchronized void abort(MergeSession session, String reason) {
        if (session.state == State.COMMITTED) {
            finishRedirects(session, true);
            return;
        }
        if (session.state == State.ABORTED) {
            return;
        }
        session.state = State.ABORTED;
        session.readiness.abort();
        cancelTimeout(session);
        sessions.remove(session.mergeId);
        JsonObject json = new JsonObject();
        json.addProperty("mergeId", session.mergeId.toString());
        json.addProperty("reason", reason);
        for (UUID party : session.preparingMembers) {
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
    public synchronized void abortInvolving(UUID clientId) {
        splits.abortInvolving(clientId);
        if (clientId == null) {
            return;
        }
        for (MergeSession session : sessions.values()) {
            if (session.involves(clientId)) {
                if (session.state == State.COMMITTED) {
                    session.awaitingRedirects.remove(clientId);
                    finishRedirects(session, false);
                } else abort(session, "当事方掉线: " + clientId);
            }
        }
    }

    // ------------------------------------------------------------ 分离申请

    /**
     * SPLIT_REQUEST：主客户端发来的分离申请（规范"单端分离/组分离"）。
     * <p>
     * 检测（渲染区域无交集 10s）在主客户端侧完成（它拥有实时视距数据），服务端
     * prepare只预约边界并通知所有离组端断开旧游戏连接；源主完成旧holder卸载、
     * 最终写回与release后才commit。组表只在commit改变，不直接迁移仍加载的区块。
     * <p>
     * <b>组分离（多端，Phase 4）</b>：{@code departingClientIds} 携带多名离组成员时，
     * 服务端在离组集合内做算力分配（规范"分为两个组客户端……每个组内进行算力分配"，
     * {@link ComputeTable#selectPrimaryOrRelaxed} 与合并选主同一套规则），胜者直发
     * ROLE_ASSIGN(primary) 自立门户；其余成员的 ROLE_ASSIGN(secondary) 重定向指派
     * <b>暂存</b>到新主回报 {@code GROUP_WORLD_READY}（LAN 已发布）后冲刷 ——
     * 见 {@link #pendingRedirects}。单成员分离（departing=1）是其特例，无暂存环节。
     */
    private synchronized void handleSplitRequest(ControlConnection conn, ControlMessage msg) {
        UUID source = conn.peerId();
        if ("prepare".equals(JsonUtil.getString(msg.json(), "phase", ""))) {
            for (MergeSession session : sessions.values()) {
                if (session.state != State.ABORTED && session.involves(source)) {
                    conn.send(error(msg, "busy", "该组已有合并事务"));
                    return;
                }
            }
        }
        splits.handle(conn, msg);
    }

    /**
     * GROUP_WORLD_READY：主客户端的组世界完成 LAN 发布（Phase 4）。
     * 冲刷该组暂存的副客户端重定向 —— 此刻新主已可接待隧道，重定向不再扑空。
     * 普通主客户端（无暂存项）的就绪通告是无害的空操作。
     */
    private void handleGroupWorldReady(ControlConnection conn, ControlMessage msg) {
        UUID reporter = conn.peerId();
        if (reporter == null || HelloHandler.connectionOf(reporter) != conn
                || !reporter.equals(groups.primaryOf(reporter))) {
            return; // 未握手连接：忽略（推送语义无应答）
        }
        // 口径：groupId == 主客户端 clientId，就绪方即组 id
        flushRedirects(reporter, "世界就绪");
    }

    /** 冲刷某组暂存的副客户端重定向（就绪通告与超时兜底竞争，remove 保证只发一次）。 */
    private void flushRedirects(UUID newGroupId, String cause) {
        PendingGroupRedirects.Entry pending = pendingRedirects.ready(newGroupId);
        if (pending == null || pending.members().isEmpty()) {
            return;
        }
        Set<UUID> followers = pending.members();
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

    /** 新主未就绪不能以超时为由盲目重连；给跟随成员一个明确的失败终点。 */
    private void failPendingRedirects(UUID group, PendingGroupRedirects.Entry expected) {
        PendingGroupRedirects.Entry pending = pendingRedirects.expire(group, expected);
        if (pending == null) return;
        Set<UUID> followers = pending.members();
        JsonObject failure = new JsonObject();
        failure.addProperty("message", "分离后的主客户端尚未就绪，请稍后重新连接。");
        for (UUID member : followers) {
            ControlConnection connection = HelloHandler.connectionOf(member);
            if (connection != null && connection.isOpen())
                connection.send(ControlMessage.of(MessageType.SPLIT_FAILED, failure.deepCopy()));
        }
    }

    // ------------------------------------------------- 选主（纯函数，包内可见便于单测）

    /** 合并预同步的双方角色：newPrimary = 接管方 B，oldPrimary = 让出方 A。 */
    record MergeRoles(UUID newPrimary, UUID oldPrimary) {
    }

    /**
     * 合并选主候选集 = 两组<b>全部成员</b>的并集（包内可见纯函数，便于单测）。
     * <p>
     * 规范依据："算力分配：重新分配此区域中的玩家的客户端等级（主客户端和副客户端）
     * 通过算力表 算力最高的为主客户端" —— 副客户端同样有当选资格，候选集不能只有
     * 两个主客户端。返回值按 UUID 字典序升序（TreeSet 去重 + 排序；选举本身与顺序无关，
     * 排序只为日志与测试可复现）。
     *
     * @param membersA 一组全部成员（{@code GroupTable.membersOf}；null 视为空）
     * @param membersB 另一组全部成员（同上）
     * @return 去重排序后的候选 id 列表（两参数均为空时返回空列表）
     */
    static List<UUID> mergeCandidates(Collection<UUID> membersA, Collection<UUID> membersB) {
        TreeSet<UUID> union = new TreeSet<>();
        addMembers(union, membersA);
        addMembers(union, membersB);
        return new ArrayList<>(union);
    }

    /** 并入非空成员（防御性过滤 null，表数据不信任）。 */
    private static void addMembers(Set<UUID> union, Collection<UUID> members) {
        if (members == null) {
            return;
        }
        for (UUID member : members) {
            if (member != null) {
                union.add(member);
            }
        }
    }

    /**
     * 由"接管方"推导预同步双方角色（包内可见纯函数，便于单测）。
     * <p>
     * 语义（与客户端 MergeClient 的 A/B 分工严格对应）：
     * <ul>
     *   <li>{@code newPrimary} = 接管方 B：合并后新组的主客户端。其集成服务端全程不中断，
     *       经 GroupHost 接收快照/增量，A 让出后两旧组的区块占用整组迁入它名下；</li>
     *   <li>{@code oldPrimary} = 让出方 A：<b>不含接管方的那一组</b>的当前主客户端。
     *       它冻结 tick → 发尾部增量 → 回报 switched → 降级为副客户端经隧道重连 B。</li>
     * </ul>
     * 两个角色因此恒为两个不同的<b>当前主客户端</b>（预同步协议不变式：
     * {@link #handleProgress} 只接受这两者回报，{@link #abort} 只通知这两者）。
     *
     * @param taker            接管方（必须恰为 requesterPrimary 或 targetPrimary 之一）
     * @param requesterPrimary 发起方组的当前主客户端（MERGE_PLAN reply 的接收方）
     * @param targetPrimary    目标组的当前主客户端（MERGE_PLAN 单发的接收方）
     * @return 角色对；输入非法（null / 两主相同 / taker 不是当前主客户端）返回 null，
     *         调用方应回 ERROR 而不是硬造一个会话
     */
    static MergeRoles rolesForTaker(UUID taker, UUID requesterPrimary, UUID targetPrimary) {
        if (taker == null || requesterPrimary == null || targetPrimary == null
                || requesterPrimary.equals(targetPrimary)) {
            return null;
        }
        if (taker.equals(requesterPrimary)) {
            return new MergeRoles(requesterPrimary, targetPrimary);
        }
        if (taker.equals(targetPrimary)) {
            return new MergeRoles(targetPrimary, requesterPrimary);
        }
        // 接管方不是当前主客户端（副客户端没有组世界，接不了预同步）
        return null;
    }

    // ------------------------------------------------------------ 工具

    /** 服务停止清理：中止全部会话并清空表。 */
    public void shutdown() {
        splits.shutdown();
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
