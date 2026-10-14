package imsng.player_to_player.compute;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;

/**
 * 一台客户端机器的算力评分（算力表中的一行）。
 * <p>
 * 规范出处：player_to_player-prompt.txt "算力分配：……通过算力表 算力最高的为主客户端"、
 * "算力表：……取决于CPU的单核能力 数据通过查询使用Geekbench的非官方公开API可以得到"；
 * DESIGN.md 第 6 节"算力（compute 包）"。
 *
 * <h2>本类最重要的一条设计决定：排序键是本地微基准分，不是 Geekbench 分</h2>
 * 旧实现只有一个 {@code singleCoreScore}：Geekbench 查询成功就用 Geekbench 分，
 * 查询失败才跑 {@link LocalBenchmark}，两种来源的数值被 {@link ComputeTable} 直接
 * 按大小混排 —— 这是"两套量纲混用"，会让<b>查得到 Geekbench 的机器系统性地压过
 * 查不到的机器</b>，与真实算力无关。本类改为同时承载两个分，并把排序语义写死：
 * <ul>
 *   <li><b>排序键 = {@link #localSingleCoreScore()}（即 {@link #rankScore()}）</b>：
 *       本地微基准在每台机器上跑<b>逐字节相同的确定性负载</b>，不依赖网络、
 *       不会因页面改版 / 型号未收录 / 限流而缺失，是唯一真正可横向比较的量；</li>
 *   <li><b>{@link #geekbenchSingleCoreScore()} 仅作参考与展示</b>：它来自非官方公开
 *       搜索页，无任何 SLA，还可能取到"搜索结果第一条"（未必是目标型号，见
 *       {@link GeekbenchLookup}），且不同代际（v6/v5）量纲不同；</li>
 *   <li><b>唯一例外：本地分缺失时用 Geekbench 分兜底</b>（{@link #rankScore()} 的第二优先级）。
 *       本地跑分是纯计算，正常不可能失败；只会在"对端是未上报本地分的旧版本节点"
 *       这类极端情形走到兜底 —— 有分可比"直接按 0 分处理"更接近事实，但此时与
 *       其他候选的比较只是近似，调用方可用 {@link #hasLocalScore()} 识别。</li>
 * </ul>
 *
 * <h2>字段与 JSON 兼容性（协议契约）</h2>
 * {@link #toJson()} 同时写出新旧两套字段（COMPUTE_REPORT 与 HELLO.compute 都用这个对象）：
 * <ul>
 *   <li>旧字段 {@code singleCoreScore} / {@code source}：<b>名字不变，语义改为"当前排序键
 *       及其来源"</b>（{@code singleCoreScore == rankScore()}，{@code source} 见
 *       {@link #source()}）。这样未升级的节点拿到新版上报后，继续按老逻辑比较
 *       {@code singleCoreScore}，得到的排序结论与新版完全一致，不会因为升级产生分叉；</li>
 *   <li>新字段 {@code localSingleCoreScore} / {@code geekbenchSingleCoreScore} /
 *       {@code geekbenchVersion}：新节点优先读它们。</li>
 * </ul>
 * {@link #fromJson(JsonObject)} 对新字段缺失、类型非法、数值 ≤ 0 一律取安全默认值
 * （0 / {@link #VERSION_UNKNOWN}），并在"两个新分都拿不到有效值"时回落到旧字段语义，
 * 保证新旧节点混合组网不会解析失败。
 *
 * <h2>内存语义（重要）</h2>
 * {@code freeMemoryBytes} 是<b>整机可用物理内存</b>（不是 JVM 堆、也不是 MC 进程占用），
 * 由 {@link ComputeScoreProvider} 在模组加载时检测一次并长期缓存 ——
 * <b>它是"模组加载那一刻"的快照，之后不再刷新</b>。是否够格当主客户端由<b>服务端</b>
 * 用 {@link imsng.player_to_player.config.GlobalConfig#minFreeMemoryBytes}（默认 0.5 GB）
 * 与这个快照比较（见 {@link #meetsMemoryRequirement(long)}），客户端只负责如实上报。
 *
 * @param cpuModel                 CPU 型号字符串（如 "AMD Ryzen 7 5800X"），缺失时归一化为 "unknown"
 * @param localSingleCoreScore     本地微基准单核分（<b>排序键</b>；0 表示无有效本地分）
 * @param geekbenchSingleCoreScore Geekbench 参考单核分（可选；0 表示无参考分）
 * @param geekbenchVersion         Geekbench 代际版本（"v6"/"v5"/"unknown"）
 * @param freeMemoryBytes          检测时刻的整机可用物理内存（字节，模组加载时的一次快照）
 * @param totalMemoryBytes         检测时刻的整机总物理内存（字节，模组加载时的一次快照）
 */
public record ComputeScore(
        String cpuModel,
        long localSingleCoreScore,
        long geekbenchSingleCoreScore,
        String geekbenchVersion,
        long freeMemoryBytes,
        long totalMemoryBytes) {

    /** {@link #source()} 取值：排序键取自本地微基准分（正常路径）。 */
    public static final String SOURCE_LOCAL = "local";
    /**
     * {@link #source()} 取值：排序键取自 Geekbench 参考分。
     * 只在本地分缺失（旧版本节点上报 / 本地跑分异常）时出现，属兜底路径。
     */
    public static final String SOURCE_GEEKBENCH = "geekbench";
    /** {@link #source()} 取值：两个分都不可用，{@link #rankScore()} == 0，视为最弱候选。 */
    public static final String SOURCE_UNKNOWN = "unknown";

    /** Geekbench 6 代际标识（当前 {@link GeekbenchLookup} 只查 v6 榜）。 */
    public static final String VERSION_V6 = "v6";
    /** Geekbench 5 代际标识：本类预留 —— v5 与 v6 分数量纲不同（v5 数值约为 v6 的 2 倍）。 */
    public static final String VERSION_V5 = "v5";
    /** 版本未知：旧节点上报（报文里只有 singleCoreScore，没有版本信息）或未查到参考分。 */
    public static final String VERSION_UNKNOWN = "unknown";

    /**
     * 归一化（紧凑构造器）：入站 JSON 与组装代码都可能给出 null、空白或负值，
     * 这里统一收敛，避免下游每个使用点都判空；负分没有物理意义（入站数据不可信），一律归零。
     */
    public ComputeScore {
        cpuModel = (cpuModel == null || cpuModel.isBlank()) ? "unknown" : cpuModel;
        geekbenchVersion = (geekbenchVersion == null || geekbenchVersion.isBlank())
                ? VERSION_UNKNOWN : geekbenchVersion;
        localSingleCoreScore = Math.max(0L, localSingleCoreScore);
        geekbenchSingleCoreScore = Math.max(0L, geekbenchSingleCoreScore);
    }

    /**
     * 旧版 5 参数构造（{@code cpuModel, singleCoreScore, source, freeMemoryBytes,
     * totalMemoryBytes}）—— 仅为源码级兼容而保留。
     * <p>
     * 旧报文的字段语义就是"唯一的那个分 + 它的来源"，因此这里按来源把它路由到对应的
     * 新字段：{@code source="geekbench"} 落到参考分（并标注 {@link #VERSION_UNKNOWN}，
     * 因为旧报文没有代际信息，无法判断是 v6 还是 v5）；其余值一律按本地分处理
     * （旧 {@link #fromJson} 的默认值也是 local）。路由后 {@link #rankScore()}
     * 与旧实现"该报文里唯一的分"完全一致。
     */
    public ComputeScore(String cpuModel, long singleCoreScore, String source,
                        long freeMemoryBytes, long totalMemoryBytes) {
        this(cpuModel,
                SOURCE_GEEKBENCH.equals(source) ? 0L : singleCoreScore,
                SOURCE_GEEKBENCH.equals(source) ? singleCoreScore : 0L,
                VERSION_UNKNOWN,
                freeMemoryBytes,
                totalMemoryBytes);
    }

    /**
     * 算力排序键：算力表选主时的<b>唯一</b>比较量（越大越强）。
     * <p>
     * 取值优先级：
     * <ol>
     *   <li><b>本地微基准分</b>（&gt; 0）—— 正常路径，理由见类 Javadoc；</li>
     *   <li><b>Geekbench 参考分</b>（&gt; 0）—— 仅当本地分缺失时兜底
     *       （旧版本节点上报 / 本地跑分异常）；</li>
     *   <li>都没有 → {@code 0}：视为最弱候选。调用方仍需保证"并列时取确定性决胜规则"
     *       （例如 UUID 字典序，见 {@link ComputeTable#selectPrimary}）。</li>
     * </ol>
     * <b>注意</b>：兜底路径下 Geekbench 分与别人的本地分不同量纲，比较只是近似；
     * 需要更严格策略的调用方可以先看 {@link #hasLocalScore()}。
     */
    public long rankScore() {
        if (localSingleCoreScore > 0L) {
            return localSingleCoreScore;
        }
        // 本地分缺失（极端）：退回 Geekbench 参考分，至少有分可比，好过按 0 分淘汰
        return Math.max(0L, geekbenchSingleCoreScore);
    }

    /**
     * 旧字段 {@code singleCoreScore} 的读取入口：<b>恒等于 {@link #rankScore()}</b>。
     * <p>
     * 语义变更说明：该名字在旧版本里表示"唯一的那个分"，现在统一表示"当前排序键"。
     * 这样既保证未升级的节点按老逻辑比较时结论与新版一致，也保证本类内部
     * 不存在"两份真相"（排序键只有 {@link #rankScore()} 一个来源）。
     */
    public long singleCoreScore() {
        return rankScore();
    }

    /**
     * 旧字段 {@code source} 的读取入口：{@link #rankScore()} 的来源，取值为
     * {@link #SOURCE_LOCAL} / {@link #SOURCE_GEEKBENCH} / {@link #SOURCE_UNKNOWN}。
     * 仅用于展示与排查，<b>不参与排序</b>（按来源差别对待正是本次要修掉的问题）。
     */
    public String source() {
        if (localSingleCoreScore > 0L) {
            return SOURCE_LOCAL;
        }
        if (geekbenchSingleCoreScore > 0L) {
            return SOURCE_GEEKBENCH;
        }
        return SOURCE_UNKNOWN;
    }

    /**
     * 是否具备可横向比较的本地微基准分。
     * <p>
     * 供调用方实现更严格的策略（可选）：例如服务端在"新旧版本混合组网"时，可以优先在
     * {@code hasLocalScore()} 为 true 的候选里选主，把只有 Geekbench 参考分的旧节点
     * 当作次选（这是唯一还残留量纲混排的场景，见类 Javadoc）。
     * 默认策略 {@link #rankScore()} 不区分这一点 —— 规范只要求"算力最高者为主客户端"。
     */
    public boolean hasLocalScore() {
        return localSingleCoreScore > 0L;
    }

    /**
     * 是否满足主客户端的内存门槛。
     * <p>
     * 比较对象是 {@link #freeMemoryBytes()}（整机可用物理内存，模组加载时快照），
     * 门槛由服务端 {@link imsng.player_to_player.config.GlobalConfig#minFreeMemoryBytes}
     * （默认 0.5 GB）提供；快照不刷新意味着"玩家后来关掉别的程序"不会让本判定变宽松，
     * 方向偏保守（宁可错杀不选它当主客户端）。
     */
    public boolean meetsMemoryRequirement(long minFreeMemoryBytes) {
        return freeMemoryBytes >= minFreeMemoryBytes;
    }

    /**
     * 序列化为 JSON（COMPUTE_REPORT 消息体 / HELLO 的 compute 子对象）。
     * <p>
     * 同时写出旧字段（{@code singleCoreScore} = {@link #rankScore()}、{@code source}）
     * 与新字段，旧节点忽略不认识的键即可，新节点优先读新字段。
     */
    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("cpuModel", cpuModel);
        // 兼容字段：旧节点按 singleCoreScore 排序，这里写"当前排序键"，保证新旧口径一致
        obj.addProperty("singleCoreScore", rankScore());
        obj.addProperty("source", source());
        obj.addProperty("freeMemoryBytes", freeMemoryBytes);
        obj.addProperty("totalMemoryBytes", totalMemoryBytes);
        // 新增字段：本地分（排序键）+ Geekbench 参考分与代际
        obj.addProperty("localSingleCoreScore", localSingleCoreScore);
        obj.addProperty("geekbenchSingleCoreScore", geekbenchSingleCoreScore);
        obj.addProperty("geekbenchVersion", geekbenchVersion);
        return obj;
    }

    /**
     * 从 JSON 反序列化（字段缺失 / 类型非法一律取安全默认值，容忍旧版本节点）。
     * <p>
     * 解析顺序：优先读两个新分；若新字段缺失或都没给出有效值（≤ 0），
     * 说明对端是旧节点（或新字段被写成 0），回落到旧字段语义 ——
     * 由 {@code source} 决定那个唯一的分是本地分还是 Geekbench 参考分。
     *
     * @param obj 平铺的算力 JSON 对象（COMPUTE_REPORT 整体 / HELLO 的 compute 子对象）
     * @return 归一化后的评分；{@code obj} 为 null 时返回全零评分（不抛异常，入站数据不可信）
     */
    public static ComputeScore fromJson(JsonObject obj) {
        if (obj == null) {
            return new ComputeScore("unknown", 0L, 0L, VERSION_UNKNOWN, 0L, 0L);
        }
        long local = JsonUtil.getLong(obj, "localSingleCoreScore", 0L);
        long geekbench = JsonUtil.getLong(obj, "geekbenchSingleCoreScore", 0L);
        if (local <= 0L && geekbench <= 0L) {
            // 旧报文路径（也可能是新字段被写成 0 的畸形报文）：按 source 路由唯一的分
            return new ComputeScore(
                    JsonUtil.getString(obj, "cpuModel", "unknown"),
                    JsonUtil.getLong(obj, "singleCoreScore", 0L),
                    JsonUtil.getString(obj, "source", SOURCE_LOCAL),
                    JsonUtil.getLong(obj, "freeMemoryBytes", 0L),
                    JsonUtil.getLong(obj, "totalMemoryBytes", 0L));
        }
        return new ComputeScore(
                JsonUtil.getString(obj, "cpuModel", "unknown"),
                local,
                geekbench,
                JsonUtil.getString(obj, "geekbenchVersion", VERSION_UNKNOWN),
                JsonUtil.getLong(obj, "freeMemoryBytes", 0L),
                JsonUtil.getLong(obj, "totalMemoryBytes", 0L));
    }
}
