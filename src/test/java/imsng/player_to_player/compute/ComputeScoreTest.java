package imsng.player_to_player.compute;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ComputeScore} 的排序语义与 JSON 兼容性测试（任务 task-2 的判据）。
 * <p>
 * 覆盖：
 * <ul>
 *   <li>同来源（都用本地微基准分）可横向比较；</li>
 *   <li>混合来源时以<b>本地分</b>为准：Geekbench 参考分高、本地分低的候选必须输给
 *       Geekbench 参考分低/没有、本地分高的候选；</li>
 *   <li>旧 JSON（只有 singleCoreScore/source）反序列化不炸且行为合理；</li>
 *   <li>新旧字段 JSON 往返不丢（协议向后兼容契约）。</li>
 * </ul>
 */
class ComputeScoreTest {

    private static final long GIB = 1024L * 1024 * 1024;
    /** GlobalConfig.minFreeMemoryBytes 的默认值（0.5 GB）。 */
    private static final long MEMORY_GATE = 512L * 1024 * 1024;

    /** 新版本客户端的常规上报：本地微基准分 + Geekbench 参考分都给。 */
    private static ComputeScore mixed(long localScore, long geekbenchScore, String geekbenchVersion) {
        return new ComputeScore("Test CPU", localScore, geekbenchScore, geekbenchVersion, 8 * GIB, 16 * GIB);
    }

    /** 旧版本节点的载荷：只有旧字段（COMPUTE_REPORT 平铺 / HELLO 的 compute 子对象同构）。 */
    private static JsonObject legacyJson(long singleCoreScore, String source) {
        JsonObject obj = new JsonObject();
        obj.addProperty("cpuModel", "Old CPU");
        obj.addProperty("singleCoreScore", singleCoreScore);
        obj.addProperty("source", source);
        obj.addProperty("freeMemoryBytes", 4 * GIB);
        obj.addProperty("totalMemoryBytes", 8 * GIB);
        return obj;
    }

    // ------------------------------------------------------------ 排序语义

    @Test
    void sameSourceScoresAreComparable() {
        ComputeScore fast = mixed(2600L, 0L, ComputeScore.VERSION_UNKNOWN);
        ComputeScore slow = mixed(1500L, 0L, ComputeScore.VERSION_UNKNOWN);
        assertTrue(fast.rankScore() > slow.rankScore(), "同一套确定性负载跑出的分数必须可横向排序");
        assertEquals(2600L, fast.rankScore());
    }

    @Test
    void mixedSourcesRankByLocalScoreNotGeekbench() {
        // A：Geekbench 查得到（参考分 4500，看着很唬人），但本地微基准只有 1200
        ComputeScore famousButWeak = mixed(1200L, 4500L, ComputeScore.VERSION_V6);
        // B：Geekbench 未收录（无参考分），但本地微基准 2000 —— 实际更强
        ComputeScore unlistedButStrong = mixed(2000L, 0L, ComputeScore.VERSION_UNKNOWN);

        assertTrue(unlistedButStrong.rankScore() > famousButWeak.rankScore(),
                "排序键必须是本地微基准分：Geekbench 参考分高不能弥补本地分低");
        // 旧字段与新排序键同源：未升级节点按 singleCoreScore 排序也会得到同一结论
        assertEquals(famousButWeak.rankScore(), famousButWeak.singleCoreScore());
        assertEquals(ComputeScore.SOURCE_LOCAL, famousButWeak.source());
        assertTrue(famousButWeak.hasLocalScore());
    }

    @Test
    void geekbenchScoreIsOnlyAFallbackWhenLocalScoreMissing() {
        ComputeScore fallback = mixed(0L, 4500L, ComputeScore.VERSION_V6);
        assertEquals(4500L, fallback.rankScore(), "本地分缺失（旧节点/跑分异常）时用 Geekbench 参考分兜底");
        assertEquals(ComputeScore.SOURCE_GEEKBENCH, fallback.source());
        assertFalse(fallback.hasLocalScore());
    }

    @Test
    void negativeScoresAreClampedToZero() {
        ComputeScore score = mixed(-5L, -1L, ComputeScore.VERSION_V6);
        assertEquals(0L, score.rankScore());
        assertEquals(0L, score.localSingleCoreScore());
        assertEquals(0L, score.geekbenchSingleCoreScore());
        assertEquals(ComputeScore.SOURCE_UNKNOWN, score.source());
    }

    @Test
    void nullFieldsAreNormalized() {
        ComputeScore score = new ComputeScore(null, 100L, 200L, null, 0L, 0L);
        assertEquals("unknown", score.cpuModel());
        assertEquals(ComputeScore.VERSION_UNKNOWN, score.geekbenchVersion());
        assertEquals(100L, score.rankScore());
    }

    // ------------------------------------------------------------ 旧 JSON 兼容

    @Test
    void legacyLocalReportKeepsComparableLocalScore() {
        ComputeScore score = ComputeScore.fromJson(legacyJson(1500L, ComputeScore.SOURCE_LOCAL));
        assertEquals(1500L, score.localSingleCoreScore(), "source=local 的旧分就是本地量纲，直接落到本地分字段");
        assertEquals(0L, score.geekbenchSingleCoreScore());
        assertEquals(ComputeScore.VERSION_UNKNOWN, score.geekbenchVersion());
        assertEquals(1500L, score.rankScore());
        assertEquals(ComputeScore.SOURCE_LOCAL, score.source());
        assertEquals("Old CPU", score.cpuModel());
        assertTrue(score.meetsMemoryRequirement(MEMORY_GATE));
    }

    @Test
    void legacyGeekbenchReportBecomesReferenceFallback() {
        ComputeScore score = ComputeScore.fromJson(legacyJson(3500L, ComputeScore.SOURCE_GEEKBENCH));
        assertEquals(0L, score.localSingleCoreScore());
        assertEquals(3500L, score.geekbenchSingleCoreScore());
        assertEquals(ComputeScore.VERSION_UNKNOWN, score.geekbenchVersion(),
                "旧报文没有代际信息，不能臆断是 v6（v5/v6 量纲不同）");
        assertEquals(3500L, score.rankScore(), "本地分缺失 → Geekbench 兜底");
        assertEquals(ComputeScore.SOURCE_GEEKBENCH, score.source());
        assertFalse(score.hasLocalScore());
    }

    @Test
    void legacyFiveArgConstructorMatchesLegacyJson() {
        // 旧节点的源码级构造路径（cpuModel, singleCoreScore, source, free, total）必须
        // 与新解析结果完全一致，否则并行开发中的旧调用点会得到不同语义
        ComputeScore viaJson = ComputeScore.fromJson(legacyJson(1500L, ComputeScore.SOURCE_LOCAL));
        ComputeScore viaConstructor = new ComputeScore(
                "Old CPU", 1500L, ComputeScore.SOURCE_LOCAL, 4 * GIB, 8 * GIB);
        assertEquals(viaConstructor, viaJson);
    }

    @Test
    void missingFieldsFallBackToSafeDefaults() {
        ComputeScore score = ComputeScore.fromJson(new JsonObject());
        assertEquals(0L, score.rankScore());
        assertEquals(ComputeScore.SOURCE_UNKNOWN, score.source());
        assertEquals("unknown", score.cpuModel());
        assertEquals(ComputeScore.VERSION_UNKNOWN, score.geekbenchVersion());
        assertFalse(score.meetsMemoryRequirement(1L));
        assertFalse(score.hasLocalScore());
        // null 载荷同样不能炸（入站数据不可信）
        assertEquals(0L, ComputeScore.fromJson(null).rankScore());
    }

    @Test
    void zeroedNewFieldsFallBackToLegacyScore() {
        JsonObject hybrid = legacyJson(1234L, ComputeScore.SOURCE_LOCAL);
        hybrid.addProperty("localSingleCoreScore", 0L);
        hybrid.addProperty("geekbenchSingleCoreScore", 0L);
        ComputeScore score = ComputeScore.fromJson(hybrid);
        assertEquals(1234L, score.rankScore(), "两个新分都无效时应回落到旧字段语义");
        assertEquals(1234L, score.localSingleCoreScore());
    }

    @Test
    void malformedFieldTypesDoNotBreakParsing() {
        // 入站 JSON 不可信：类型不符（字符串分数 / 布尔分数 / 数字版本）必须按缺失处理
        JsonObject junk = new JsonObject();
        junk.addProperty("cpuModel", "Junk CPU");
        junk.addProperty("singleCoreScore", "not-a-number");
        junk.addProperty("localSingleCoreScore", true);
        junk.addProperty("geekbenchSingleCoreScore", "NaN");
        junk.addProperty("geekbenchVersion", 42);
        junk.addProperty("freeMemoryBytes", "8G");
        ComputeScore score = ComputeScore.fromJson(junk);
        assertEquals(0L, score.rankScore());
        assertEquals(ComputeScore.SOURCE_UNKNOWN, score.source());
        assertFalse(score.meetsMemoryRequirement(1L));
    }

    // ------------------------------------------------------------ JSON 契约

    @Test
    void jsonRoundTripPreservesBothScoresAndLegacyFields() {
        ComputeScore original = mixed(2400L, 3300L, ComputeScore.VERSION_V6);
        JsonObject json = original.toJson();

        // 兼容契约：旧字段名与旧字段数量不变，singleCoreScore 就是当前排序键
        assertTrue(json.has("cpuModel"));
        assertTrue(json.has("singleCoreScore"));
        assertTrue(json.has("source"));
        assertTrue(json.has("freeMemoryBytes"));
        assertTrue(json.has("totalMemoryBytes"));
        assertEquals(2400L, json.get("singleCoreScore").getAsLong());
        assertEquals(ComputeScore.SOURCE_LOCAL, json.get("source").getAsString());
        // 新增字段齐全，供新节点读取
        assertEquals(2400L, json.get("localSingleCoreScore").getAsLong());
        assertEquals(3300L, json.get("geekbenchSingleCoreScore").getAsLong());
        assertEquals(ComputeScore.VERSION_V6, json.get("geekbenchVersion").getAsString());

        assertEquals(original, ComputeScore.fromJson(json), "往返后六个字段必须逐一相等");
    }
}
