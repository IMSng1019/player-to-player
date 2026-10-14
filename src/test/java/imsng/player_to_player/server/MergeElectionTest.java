package imsng.player_to_player.server;

import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.compute.ComputeTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * task-3 验收单测：合并/分离的<b>选主范围</b>与<b>内存门槛兜底</b>。
 * <p>
 * 覆盖的规范条款：
 * <ul>
 *   <li>"算力分配：重新分配此区域中的玩家的客户端等级（主客户端和副客户端）
 *       通过算力表 算力最高的为主客户端" —— 候选集必须是两组<b>全部成员</b>，
 *       只比两个主客户端会让强副客户端永远无法当选（{@link MergeCoordinator#mergeCandidates}）；</li>
 *   <li>"同时成为主客户段还有一个条件为目前剩余的内存大小" —— 内存门槛，
 *       但全员不达标时按"算力最高"确定性降级（{@link ComputeTable#selectPrimaryOrRelaxed}）；</li>
 *   <li>预同步不变式：{@code newPrimary}/{@code oldPrimary} 恒为两个<b>当前主客户端</b>
 *       （{@link MergeCoordinator#rolesForTaker}）。</li>
 * </ul>
 * 全部为纯内存测试：不触碰网络、world、Minecraft 服务端。
 */
class MergeElectionTest {

    /** 默认主客户端内存门槛（{@code GlobalConfig.minFreeMemoryBytes}，规范值 0.5GB）。 */
    private static final long MIN_FREE = 512L * 1024 * 1024;

    private static final long GB = 1024L * 1024 * 1024;

    // 固定 UUID：并列决胜按字典序，故取值刻意让大小关系明显
    private static final UUID PRIMARY_A = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID SECONDARY_A = UUID.fromString("22222222-2222-4222-8222-222222222222");
    private static final UUID PRIMARY_B = UUID.fromString("33333333-3333-4333-8333-333333333333");
    /**
     * 并列决胜用的两个 UUID（大小关系按 {@link UUID#compareTo}）。
     * <b>坑</b>：{@code UUID.compareTo} 先比 mostSigBits 再比 leastSigBits，两者都是
     * <b>有符号 long</b> —— 最高位为 1 的 UUID（例如 ffffffff-…）在有符号比较下反而更小，
     * 所以这里刻意避开它。选主决胜与 {@code TreeSet<UUID>}（mergeCandidates）用的都是
     * 同一个自然序，两者一致即可。
     */
    private static final UUID SMALL_UUID = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final UUID LARGE_UUID = UUID.fromString("7fffffff-ffff-4fff-bfff-fffffffffffe");

    /**
     * 造一个评分：本地微基准分为排序键（{@code rankScore()} 的主来源），
     * 总内存固定给足，可用内存由参数控制（用于构造达标 / 不达标候选）。
     */
    private static ComputeScore score(long rankScore, long freeMemoryBytes) {
        return new ComputeScore("test-cpu", rankScore, 0L, ComputeScore.VERSION_UNKNOWN,
                freeMemoryBytes, 16L * GB);
    }

    @Test
    void rankScoreIsTheLocalBenchmarkScore() {
        // 前置断言：测试里用的"分数"就是选主排序键本身（语义变化时这里先报错，便于定位）
        assertEquals(1234L, score(1234L, 2 * GB).rankScore());
    }

    // ------------------------------------------------- ① 全成员选举：强副击败弱主

    @Test
    void strongSecondaryBeatsWeakPrimaryInFullMemberElection() {
        ComputeTable table = new ComputeTable();
        table.report(PRIMARY_A, score(900L, 2 * GB));   // 弱主
        table.report(SECONDARY_A, score(3000L, 2 * GB)); // 强副

        List<UUID> candidates = MergeCoordinator.mergeCandidates(
                Set.of(PRIMARY_A, SECONDARY_A), Set.of());
        ComputeTable.Selection selection = table.selectPrimaryOrRelaxed(candidates, MIN_FREE);

        assertEquals(SECONDARY_A, selection.clientId(), "全成员候选集应让算力更强的副客户端当选");
        assertFalse(selection.memoryRelaxed(), "两人都达内存门槛，不应标记为降级选出");
    }

    @Test
    void mergeCandidatesUnionBothGroupsAndStayDeterministic() {
        List<UUID> union = MergeCoordinator.mergeCandidates(
                Set.of(PRIMARY_A, SECONDARY_A), Set.of(PRIMARY_B));
        // UUID 字典序升序 + 去重（PRIMARY_A < SECONDARY_A < PRIMARY_B）
        assertEquals(List.of(PRIMARY_A, SECONDARY_A, PRIMARY_B), union);

        // 同一成员同时出现在两组（表数据漂移）只保留一份
        assertEquals(List.of(PRIMARY_A, PRIMARY_B),
                MergeCoordinator.mergeCandidates(Set.of(PRIMARY_A), Set.of(PRIMARY_A, PRIMARY_B)));
        // null 集合按空处理（防御性）
        assertTrue(MergeCoordinator.mergeCandidates(null, null).isEmpty());
    }

    @Test
    void electionOverGroupTableMembersPromotesStrongSecondary() {
        // 端到端一点：候选集来自 GroupTable 的真实成员视图，而不是调用方手搓的两人列表
        GroupTable groups = new GroupTable();
        groups.createGroup(PRIMARY_A);
        groups.addSecondary(PRIMARY_A, SECONDARY_A);
        groups.createGroup(PRIMARY_B);

        ComputeTable table = new ComputeTable();
        table.report(PRIMARY_A, score(500L, 2 * GB));
        table.report(SECONDARY_A, score(4000L, 2 * GB));
        table.report(PRIMARY_B, score(1500L, 2 * GB));

        List<UUID> candidates = MergeCoordinator.mergeCandidates(
                groups.membersOf(PRIMARY_A), groups.membersOf(PRIMARY_B));
        assertEquals(3, candidates.size(), "候选集应含两组全部成员（含副客户端）");
        assertEquals(SECONDARY_A, table.selectPrimaryOrRelaxed(candidates, MIN_FREE).clientId());

        // 对照：旧口径（只比两个主客户端）会选出 PRIMARY_B —— 这正是本次修掉的偏差
        assertEquals(PRIMARY_B, table.selectPrimary(List.of(PRIMARY_A, PRIMARY_B), MIN_FREE));
    }

    // ------------------------------------------------- ② 全员不达内存门槛：确定性降级

    @Test
    void allBelowMemoryThresholdFallsBackToRankScoreAndIsFlagged() {
        ComputeTable table = new ComputeTable();
        long below = MIN_FREE - 1; // 差 1 字节也不算达标（>= 语义）
        table.report(PRIMARY_A, score(1200L, below));
        table.report(SECONDARY_A, score(2500L, 0L));

        List<UUID> candidates = List.of(PRIMARY_A, SECONDARY_A);
        ComputeTable.Selection selection = table.selectPrimaryOrRelaxed(candidates, MIN_FREE);

        assertEquals(SECONDARY_A, selection.clientId(), "无人达标时取算力最高者（不让组无主）");
        assertTrue(selection.memoryRelaxed(), "必须标记为降级选出，供调用方记 warn / 区别策略");
        // 兼容入口同样不再返回 null（旧行为会让调用方各自兜底、口径不一）
        assertEquals(SECONDARY_A, table.selectPrimary(candidates, MIN_FREE));
    }

    @Test
    void eligibleCandidateBeatsHigherScoringIneligibleOne() {
        ComputeTable table = new ComputeTable();
        table.report(PRIMARY_A, score(9000L, MIN_FREE - 1)); // 分高但内存不达标
        table.report(SECONDARY_A, score(800L, MIN_FREE));    // 分低但刚好达标

        ComputeTable.Selection selection =
                table.selectPrimaryOrRelaxed(List.of(PRIMARY_A, SECONDARY_A), MIN_FREE);
        assertEquals(SECONDARY_A, selection.clientId(), "有合格者时内存门槛是硬条件");
        assertFalse(selection.memoryRelaxed());
    }

    @Test
    void noReportedScoreReturnsNullSoCallerMustFallBack() {
        ComputeTable table = new ComputeTable();
        table.report(PRIMARY_A, score(1000L, 2 * GB));
        // SECONDARY_A 从未上报：连降级都无从选起（没有可比的分）
        assertNull(table.selectPrimaryOrRelaxed(List.of(SECONDARY_A), MIN_FREE));
        assertNull(table.selectPrimary(Set.of(SECONDARY_A), MIN_FREE));
        // 空候选
        assertNull(table.selectPrimaryOrRelaxed(List.of(), MIN_FREE));
        assertNull(table.selectPrimaryOrRelaxed(null, MIN_FREE));
    }

    // ------------------------------------------------- ③ 并列：UUID 字典序确定性

    @Test
    void equalRankScoresBreakTiesByUuidLexicographicOrder() {
        ComputeTable table = new ComputeTable();
        table.report(SMALL_UUID, score(2000L, 2 * GB));
        table.report(LARGE_UUID, score(2000L, 2 * GB));

        // 与候选传入顺序无关（两侧独立计算选主也必须得到同一结果）
        assertEquals(SMALL_UUID,
                table.selectPrimaryOrRelaxed(List.of(SMALL_UUID, LARGE_UUID), MIN_FREE).clientId());
        assertEquals(SMALL_UUID,
                table.selectPrimaryOrRelaxed(List.of(LARGE_UUID, SMALL_UUID), MIN_FREE).clientId());
        // 降级路径同样按 UUID 决胜
        ComputeTable relaxed = new ComputeTable();
        relaxed.report(SMALL_UUID, score(2000L, 0L));
        relaxed.report(LARGE_UUID, score(2000L, 0L));
        ComputeTable.Selection selection =
                relaxed.selectPrimaryOrRelaxed(List.of(LARGE_UUID, SMALL_UUID), MIN_FREE);
        assertEquals(SMALL_UUID, selection.clientId());
        assertTrue(selection.memoryRelaxed());
    }

    @Test
    void eligibleTieIsDecidedByUuidEvenWhenIneligibleScoresHigher() {
        ComputeTable table = new ComputeTable();
        table.report(SMALL_UUID, score(1000L, 2 * GB));        // 合格，UUID 小
        table.report(LARGE_UUID, score(1000L, 2 * GB));        // 合格，UUID 大
        table.report(SECONDARY_A, score(9999L, 0L));         // 不合格

        assertEquals(SMALL_UUID,
                table.selectPrimaryOrRelaxed(
                        List.of(LARGE_UUID, SECONDARY_A, SMALL_UUID), MIN_FREE).clientId());
    }

    // ------------------------------------------------- ④ 角色裁定：两个当事方恒为当前主

    @Test
    void takerFromRequesterGroupMakesTargetTheYieldingSide() {
        MergeCoordinator.MergeRoles roles =
                MergeCoordinator.rolesForTaker(PRIMARY_A, PRIMARY_A, PRIMARY_B);
        assertEquals(PRIMARY_A, roles.newPrimary());
        assertEquals(PRIMARY_B, roles.oldPrimary(), "让出方 = 不含接管方的那一组的当前主客户端");
    }

    @Test
    void takerFromTargetGroupMakesRequesterTheYieldingSide() {
        MergeCoordinator.MergeRoles roles =
                MergeCoordinator.rolesForTaker(PRIMARY_B, PRIMARY_A, PRIMARY_B);
        assertEquals(PRIMARY_B, roles.newPrimary());
        assertEquals(PRIMARY_A, roles.oldPrimary());
    }

    @Test
    void nonPrimaryTakerIsRejectedInsteadOfBuildingABrokenSession() {
        // 接管方必须是当前主客户端：副客户端没有组世界，接不了预同步（本版本能力边界）
        assertNull(MergeCoordinator.rolesForTaker(SECONDARY_A, PRIMARY_A, PRIMARY_B));
        // 两个"当前主"相同 / 缺失：会话无从建立
        assertNull(MergeCoordinator.rolesForTaker(PRIMARY_A, PRIMARY_A, PRIMARY_A));
        assertNull(MergeCoordinator.rolesForTaker(null, PRIMARY_A, PRIMARY_B));
        assertNull(MergeCoordinator.rolesForTaker(PRIMARY_A, null, PRIMARY_B));
    }

    @Test
    void noEligibleTakeoverKeepsStrongerCurrentPrimary() {
        // 应然 ≠ 实然时的退化路径（handleMergeRequest 内的 takeover 选举）：
        // 强副当选者是副客户端 → 仍在两个当前主客户端里选强者，不会选出更弱的主。
        ComputeTable table = new ComputeTable();
        table.report(PRIMARY_A, score(700L, 2 * GB));
        table.report(PRIMARY_B, score(2100L, 2 * GB));
        table.report(SECONDARY_A, score(5000L, 2 * GB)); // 应当当选者（副客户端）

        List<UUID> all = MergeCoordinator.mergeCandidates(
                Set.of(PRIMARY_A, SECONDARY_A), Set.of(PRIMARY_B));
        assertEquals(SECONDARY_A, table.selectPrimaryOrRelaxed(all, MIN_FREE).clientId());
        List<UUID> primariesOnly = MergeCoordinator.mergeCandidates(
                List.of(PRIMARY_A), List.of(PRIMARY_B));
        assertEquals(PRIMARY_B, table.selectPrimaryOrRelaxed(primariesOnly, MIN_FREE).clientId());
        // 角色仍然自洽：接管方是当前主客户端，让出方是另一个当前主客户端
        MergeCoordinator.MergeRoles roles =
                MergeCoordinator.rolesForTaker(PRIMARY_B, PRIMARY_A, PRIMARY_B);
        assertEquals(PRIMARY_B, roles.newPrimary());
        assertEquals(PRIMARY_A, roles.oldPrimary());
    }
}
