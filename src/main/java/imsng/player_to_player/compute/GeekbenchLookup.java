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
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Geekbench 非官方公开接口查询 CPU 单核分。
 * <p>
 * 规范出处：player_to_player-prompt.txt "算力表：……取决于CPU的单核能力
 * 数据通过查询使用Geekbench的非官方公开API可以得到"。Geekbench 没有官方开放 API，
 * 这里请求其公开搜索页 {@code https://browser.geekbench.com/search?k=v6_cpu&q=<型号>}
 * 并用防御性正则从 HTML 中提取第一条结果的单核分。
 *
 * <h2>脆弱性声明与降级路径（重要）</h2>
 * 该接口<b>无任何 SLA</b>：页面结构随时可能改版、可能限流/封禁 UA、可能网络不通、
 * CPU 型号可能未收录。因此本类的一切失败（网络异常、超时、非 200、解析不出数字）
 * 都收敛为 {@link OptionalLong#empty()} 并仅记 debug 日志 ——
 * <b>失败是常态，绝不允许影响主流程</b>。上游 {@link ComputeScoreProvider}
 * 在拿到 empty 后自动降级为 {@link LocalBenchmark} 本地跑分，
 * 亦可通过总配置 {@code geekbenchLookupEnabled=false} 直接关闭本查询。
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
            "list-col-text-score[^>]*>\\s*([0-9]{2,6})\\s*<");

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
     * 按 CPU 型号查询 Geekbench 6 单核分。
     *
     * @param cpuModel CPU 友好型号名（{@link CpuInfo#cpuModel()}）
     * @return 第一条搜索结果的单核分；任何失败返回 empty（调用方降级本地跑分）
     */
    public static OptionalLong lookupSingleCore(String cpuModel) {
        if (cpuModel == null || cpuModel.isBlank() || "unknown".equals(cpuModel)) {
            // 型号都拿不到就没有搜索关键字，直接降级
            return OptionalLong.empty();
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
                return OptionalLong.empty();
            }
            byte[] body = response.body();
            if (body == null || body.length == 0) {
                LOGGER.debug("Geekbench 查询响应体为空: cpu={}", cpuModel);
                return OptionalLong.empty();
            }
            // 截断超长响应再解析：入站数据按不可信处理（编码铁律 5）
            int len = Math.min(body.length, MAX_BODY_BYTES);
            String html = new String(body, 0, len, StandardCharsets.UTF_8);
            return parseFirstScore(html, cpuModel);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // 网络失败是常态（断网 / 墙 / 限流），debug 级别即可，不打扰用户
            LOGGER.debug("Geekbench 查询失败（将降级本地跑分）: cpu={}", cpuModel, e);
            return OptionalLong.empty();
        }
    }

    /** 从结果页 HTML 提取第一条单核分；匹配不到或数值不合理返回 empty。 */
    private static OptionalLong parseFirstScore(String html, String cpuModel) {
        Matcher matcher = SCORE_PATTERN.matcher(html);
        if (!matcher.find()) {
            LOGGER.debug("Geekbench 结果页未匹配到分数（页面可能改版）: cpu={}", cpuModel);
            return OptionalLong.empty();
        }
        try {
            long score = Long.parseLong(matcher.group(1));
            // 合理性检查：Geekbench 6 单核分现实范围大致 100~10000，出界视为解析到了错误元素
            if (score < 100 || score > 100_000) {
                LOGGER.debug("Geekbench 分数超出合理范围: {} cpu={}", score, cpuModel);
                return OptionalLong.empty();
            }
            LOGGER.debug("Geekbench 查询成功: cpu={} singleCore={}", cpuModel, score);
            return OptionalLong.of(score);
        } catch (NumberFormatException e) {
            // 正则已限定 2~6 位数字，理论到不了这里；防御性兜底
            return OptionalLong.empty();
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
