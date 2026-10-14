package imsng.player_to_player.compute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Geekbench 非官方公开接口查询 CPU 单核分（<b>仅作参考分</b>）。
 * <p>
 * 规范出处：player_to_player-prompt.txt "算力表：……取决于CPU的单核能力
 * 数据通过查询使用Geekbench的非官方公开API可以得到"。Geekbench 没有官方开放 API，
 * 这里请求其公开搜索页 {@code https://browser.geekbench.com/search?k=v6_cpu&q=<型号>}
 * 并用防御性正则从 HTML 中提取第一条结果的单核分。
 *
 * <h2>定位：参考分，绝不是排序键（重要）</h2>
 * 本类产出的分数由 {@link ComputeScore#geekbenchSingleCoreScore()} 承载，
 * <b>不参与 {@link ComputeScore#rankScore()} 的优先排序</b>，只在本地微基准分缺失
 * （旧版本节点上报 / 本地跑分异常）时兜底。原因有三，任何一条都足以否决它当排序键：
 * <ol>
 *   <li><b>无任何 SLA</b>：页面结构随时可能改版、可能限流/封禁 UA、可能网络不通、
 *       型号可能未收录 —— "能不能查到"取决于网络环境，与机器算力无关；</li>
 *   <li><b>取的是搜索结果第一条</b>：搜索页按 v6_cpu 单核榜排序，"第一条"未必是目标
 *       型号（型号名归一化差异、多型号同名前缀等都会命中别的 CPU），即分数可能属于
 *       另一台机器 —— 实测语义脆弱，本类不做型号精确匹配校验；</li>
 *   <li><b>代际量纲不同</b>：v5 与 v6 分数不可直接比较（v5 数值约为 v6 的 2 倍），
 *       因此结果用 {@link Reference#version()} 标注代际，混排时能一眼看出问题。</li>
 * </ol>
 * 正因如此，{@link ComputeScoreProvider} 现在<b>无条件先跑本地微基准</b>，
 * 再把本查询结果作为参考分附加上去。
 *
 * <h2>失败语义</h2>
 * 一切失败（网络异常、超时、非 200、解析不出数字、型号缺失）都收敛为
 * {@link Reference#NONE} 并只记 debug 日志 —— <b>失败是常态，绝不允许影响主流程</b>；
 * 参考分缺失不影响本地分，也不影响上报（{@link ComputeScore#geekbenchSingleCoreScore()}
 * 为 0、版本为 {@link ComputeScore#VERSION_UNKNOWN}）。
 * 亦可通过总配置 {@code geekbenchLookupEnabled=false} 直接关闭本查询（连 HttpClient 都不创建）。
 *
 * <h2>线程模型</h2>
 * 同步 HTTP 请求（最长约 5 秒），只能在 {@code ThreadPools.io()} 上调用，
 * 严禁 MC 主线程 / Netty 事件循环。
 */
public final class GeekbenchLookup {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/compute");

    /** 连接与整体请求超时：接口无 SLA，5 秒等不到就放弃降级，不拖慢客户端启动。 */
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    /** 伪装浏览器 UA：Geekbench 对无 UA / 明显机器人 UA 的请求可能直接拒绝。 */
    private static final String USER_AGENT =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    /**
     * 从搜索结果页提取分数的防御性正则。
     * <p>
     * 当前（2026 年初）页面里每条结果的分数形如
     * {@code <span class='list-col-text-score'>2214</span>}，
     * 第一个 score span 即第一条结果的单核分（搜索按 v6_cpu 单核榜排序）。
     * 正则对引号风格（单/双引号）与 class 内多余空白保持宽容；
     * 页面一旦大改导致匹配不到，按解析失败降级 —— 见类 Javadoc 脆弱性声明。
     */
    private static final Pattern SCORE_PATTERN = Pattern.compile(
            "list-col-text-score[^>]*>\s*([0-9]{2,6})\s*<");

    /** 响应体读取上限（1 MB）：防御异常肥大的响应拖垮内存（正常页面几百 KB）。 */
    private static final int MAX_BODY_BYTES = 1024 * 1024;

    /**
     * 惰性单例 HttpClient：JDK HttpClient 自带连接池与线程，
     * 只在真正需要查询时创建（geekbenchLookupEnabled=false 时完全不建）。
     */
    private static volatile HttpClient client;

    private GeekbenchLookup() {
    }

    /**
     * Geekbench 参考分：分数 + 代际版本。
     * <p>
     * 作为 {@link ComputeScore#geekbenchSingleCoreScore()} /
     * {@link ComputeScore#geekbenchVersion()} 的组装载体使用；
     * <b>它是参考量，不是排序键</b>（见类 Javadoc）。分数有效性判断用
     * {@code score() > 0}，失败与未启用统一用 {@link #NONE} 表示。
     *
     * @param score   单核分；查询失败 / 未收录 / 解析失败为 0
     * @param version 代际标识（当前只可能是 {@link ComputeScore#VERSION_V6} 或
     *                {@link ComputeScore#VERSION_UNKNOWN}）；见类 Javadoc 第 3 条
     */
    public record Reference(long score, String version) {

        /** 空参考分（查询失败 / 未启用）：score=0，版本 unknown。 */
        public static final Reference NONE = new Reference(0L, ComputeScore.VERSION_UNKNOWN);

        /** 归一化：负分按 0 处理，版本缺失按 unknown 处理（入站/异常值防御）。 */
        public Reference {
            score = Math.max(0L, score);
            version = (version == null || version.isBlank())
                    ? ComputeScore.VERSION_UNKNOWN : version;
        }
    }

    /**
     * 按 CPU 型号查询 Geekbench 6 单核<b>参考分</b>。
     *
     * @param cpuModel CPU 友好型号名（{@link CpuInfo#cpuModel()}）
     * @return 第一条搜索结果的单核分与代际；任何失败返回 {@link Reference#NONE}（永不为 null）
     */
    public static Reference lookupReference(String cpuModel) {
        if (cpuModel == null || cpuModel.isBlank() || "unknown".equals(cpuModel)) {
            // 型号都拿不到就没有搜索关键字，直接给空参考分
            return Reference.NONE;
        }
        try {
            String url = "https://browser.geekbench.com/search?utf8=%E2%9C%93&k=v6_cpu&q="
                    + URLEncoder.encode(cpuModel, StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(TIMEOUT)
                    .header("User-Agent", USER_AGENT)
                    .header("Accept", "text/html")
                    .GET()
                    .build();
            HttpResponse<byte[]> response = httpClient()
                    .send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                LOGGER.debug("Geekbench 查询非 200 响应: status={} cpu={}", response.statusCode(), cpuModel);
                return Reference.NONE;
            }
            byte[] body = response.body();
            if (body == null || body.length == 0) {
                LOGGER.debug("Geekbench 查询响应体为空: cpu={}", cpuModel);
                return Reference.NONE;
            }
            // 截断超长响应再解析：入站数据按不可信处理（编码铁律 5）
            int len = Math.min(body.length, MAX_BODY_BYTES);
            String html = new String(body, 0, len, StandardCharsets.UTF_8);
            return parseFirstReference(html, cpuModel);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // 网络失败是常态（断网 / 墙 / 限流），debug 级别即可，不打扰用户
            LOGGER.debug("Geekbench 查询失败（参考分记 0，不影响本地分）: cpu={}", cpuModel, e);
            return Reference.NONE;
        }
    }

    /** 从结果页 HTML 提取第一条单核分；匹配不到或数值不合理返回 {@link Reference#NONE}。 */
    private static Reference parseFirstReference(String html, String cpuModel) {
        Matcher matcher = SCORE_PATTERN.matcher(html);
        if (!matcher.find()) {
            LOGGER.debug("Geekbench 结果页未匹配到分数（页面可能改版）: cpu={}", cpuModel);
            return Reference.NONE;
        }
        try {
            long score = Long.parseLong(matcher.group(1));
            // 合理性检查：Geekbench 6 单核分现实范围大致 100~10000，出界视为解析到了错误元素
            if (score < 100 || score > 100_000) {
                LOGGER.debug("Geekbench 分数超出合理范围: {} cpu={}", score, cpuModel);
                return Reference.NONE;
            }
            LOGGER.debug("Geekbench 参考分查询成功: cpu={} singleCore={} version={}",
                    cpuModel, score, ComputeScore.VERSION_V6);
            // 请求固定 k=v6_cpu（v6 榜单），因此命中结果的代际必然是 v6；
            // 若将来增加 v5 回退查询，必须在解析处区分代际（v5 数值约为 v6 的 2 倍，不可混排）
            return new Reference(score, ComputeScore.VERSION_V6);
        } catch (NumberFormatException e) {
            // 正则已限定 2~6 位数字，理论到不了这里；防御性兜底
            return Reference.NONE;
        }
    }

    /** 双检锁惰性创建 HttpClient（followRedirects：搜索页偶尔 302 到规范化 URL）。 */
    private static HttpClient httpClient() {
        HttpClient local = client;
        if (local == null) {
            synchronized (GeekbenchLookup.class) {
                if (client == null) {
                    client = HttpClient.newBuilder()
                            .connectTimeout(TIMEOUT)
                            .followRedirects(HttpClient.Redirect.NORMAL)
                            .build();
                }
                local = client;
            }
        }
        return local;
    }
}
