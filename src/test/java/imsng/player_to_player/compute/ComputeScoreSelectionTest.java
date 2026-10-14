package imsng.player_to_player.compute;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 选主端到端断言：算力表按 {@link ComputeScore#rankScore()} 选人，混合来源时以本地分为准。
 * <p>
 * 这里刻意调用真实的 {@link ComputeTable#selectPrimary}（而不是在测试里重写一遍比较逻辑），
 * 保证"分数可比"这个修复真的作用在选主决策上：
 * <ul>
 *   <li>Geekbench 参考分高但本地分低的候选，不能赢过本地分高的候选；</li>
 *   <li>旧版本节点上报的 local 分与新节点本地分同量纲，可以同台比较。</li>
 * </ul>
 * 两个候选的内存都给足（8 GB &gt; 默认门槛 0.5 GB），避免内存门槛语义干扰本测试关注的排序。
 */
class ComputeScoreSelectionTest {

    private static final long GIB = 1024L * 1024 * 1024;
    /** GlobalConfig.minFreeMemoryBytes 的默认值（0.5 GB）。 */
    private static final long MEMORY_GATE = 512L * 1024 * 1024;

    /** UUID 故意让"Geekbench 高"的候选字典序更小，排除"靠 UUID 决胜规则赢"的可能。 */
    private static final UUID FAMOUS = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID UNLISTED = UUID.fromString("00000000-0000-0000-0000-000000000002");

    @Test
    void selectPrimaryPrefersHigherLocalScoreDespiteLowerGeekbench() {
        ComputeTable table = new ComputeTable();
        // 候选 A：Geekbench 查得到（参考分 4500），但本地微基准只有 1200
        table.report(FAMOUS, new ComputeScore("Famous CPU", 1200L, 4500L,
                ComputeScore.VERSION_V6, 8 * GIB, 16 * GIB));
        // 候选 B：Geekbench 未收录（无参考分），但本地微基准 2000 —— 实际更强
        table.report(UNLISTED, new ComputeScore("Unlisted CPU", 2000L, 0L,
                ComputeScore.VERSION_UNKNOWN, 8 * GIB, 16 * GIB));

        assertEquals(UNLISTED, table.selectPrimary(List.of(FAMOUS, UNLISTED), MEMORY_GATE),
                "选主必须以本地微基准分为准，不能被 Geekbench 参考分带偏");
        // 候选遍历顺序不影响结论（选主必须确定性，合并双方各自算也不会分叉）
        assertEquals(UNLISTED, table.selectPrimary(List.of(UNLISTED, FAMOUS), MEMORY_GATE));
    }

    @Test
    void legacyReportIsComparedOnTheSameLocalScale() {
        ComputeTable table = new ComputeTable();
        // 旧版本节点上报：只有 singleCoreScore/source=local（本地量纲，字段平铺）
        JsonObject legacy = new JsonObject();
        legacy.addProperty("cpuModel", "Old CPU");
        legacy.addProperty("singleCoreScore", 1500L);
        legacy.addProperty("source", "local");
        legacy.addProperty("freeMemoryBytes", 8 * GIB);
        legacy.addProperty("totalMemoryBytes", 16 * GIB);
        table.report(FAMOUS, ComputeScore.fromJson(legacy));
        // 新版本节点上报：本地分 1200（同为本地量纲，可比较）
        table.report(UNLISTED, new ComputeScore("New CPU", 1200L, 0L,
                ComputeScore.VERSION_UNKNOWN, 8 * GIB, 16 * GIB));

        assertEquals(FAMOUS, table.selectPrimary(List.of(FAMOUS, UNLISTED), MEMORY_GATE),
                "旧节点的 local 分与新节点的本地分同量纲：1500 > 1200，旧节点应胜出");
    }
}
