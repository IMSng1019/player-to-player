package imsng.player_to_player.compute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * 本地单核微基准（Geekbench 查询失败时的降级评分来源）。
 * <p>
 * 规范出处：DESIGN.md 第 6 节 "评分来源：② 本地单核微基准（确定性工作负载跑分，
 * 映射到 Geekbench 量级）"。当 {@link GeekbenchLookup} 查不到（断网 / 型号未收录 /
 * 页面改版）时，{@link ComputeScoreProvider} 用本类兜底，保证任何机器都能得到一个
 * 可横向比较的单核分。
 *
 * <h2>工作负载设计（确定性，三段加权）</h2>
 * <ol>
 *   <li><b>SHA-256 链式哈希</b>（权重 0.4）：固定迭代数，前一轮摘要作为下一轮输入，
 *       强制串行依赖，考察整数/位运算与专用指令（SHA-NI 等）吞吐；</li>
 *   <li><b>整型 / 浮点混合运算</b>（权重 0.4）：xorshift 整数扰动 + 浮点乘加，
 *       循环携带依赖，考察 ALU/FPU 标量延迟；</li>
 *   <li><b>小数组排序</b>（权重 0.2）：确定性 LCG 填充 2048 长度 int 数组后
 *       Arrays.sort，考察分支预测与 L1/L2 缓存。</li>
 * </ol>
 * 三段各自计时，分数 = K_i / 耗时秒 后按权重加总。
 *
 * <h2>K 值标定依据</h2>
 * 各段 K_i = 参考单核分(2400) × 该段在参考机上的预期耗时秒。参考机取 2021~2023 年
 * 主流桌面 CPU（AMD Ryzen 7 5800X / Intel i5-12400 一档，Geekbench 6 单核约
 * 2000~2800）：SHA 段预期 ≈0.5s（K=1200）、混合运算段 ≈0.5s（K=1200）、
 * 排序段 ≈0.4s（K=960），总计正式计时约 1.5 秒。
 * <b>只保证同量级可比（同一套负载跑出来的分数可横向排序），不保证与 Geekbench
 * 绝对可比</b> —— 算力表选主只需要序关系，量级对齐仅为了与 Geekbench 来源的
 * 分数混排时不至于失衡。
 *
 * <h2>线程模型</h2>
 * 本方法只做纯计算、不做任何 IO / 线程切换，<b>必须由调用方投递到
 * {@code ThreadPools.compute()} 上运行</b>（约 1.7 秒满载单核），
 * 严禁在 MC 主线程或 Netty 事件循环上调用。
 */
public final class LocalBenchmark {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/compute");

    // ------------------------------------------------------------ 负载参数（固定，保证确定性）

    /** SHA-256 链式哈希迭代数（32 字节摘要反复哈希）。 */
    private static final int SHA_ITERATIONS = 1_200_000;
    /** 整型/浮点混合运算迭代数。 */
    private static final int ARITH_ITERATIONS = 150_000_000;
    /** 排序轮数（每轮重新确定性填充再排序）。 */
    private static final int SORT_ROUNDS = 8_000;
    /** 排序数组长度：2048 个 int = 8KB，落在 L1 数据缓存内，考察纯排序而非内存带宽。 */
    private static final int SORT_ARRAY_LENGTH = 2_048;

    // ------------------------------------------------------------ 权重与 K 值（标定见类 Javadoc）

    private static final double WEIGHT_SHA = 0.4;
    private static final double WEIGHT_ARITH = 0.4;
    private static final double WEIGHT_SORT = 0.2;
    /** K = 2400（参考单核分）× 参考机预期耗时秒。 */
    private static final double K_SHA = 2400 * 0.5;
    private static final double K_ARITH = 2400 * 0.5;
    private static final double K_SORT = 2400 * 0.4;

    /** 预热时长（纳秒）：让 JIT 完成分层编译、缓存/分支预测器进入稳态。 */
    private static final long WARMUP_NANOS = 200_000_000L;

    /**
     * 结果黑洞：把各段计算结果写进 volatile 静态字段，
     * 阻止 JIT 把"结果无人使用"的基准循环整体消除（dead code elimination）。
     */
    @SuppressWarnings("unused")
    private static volatile long sink;

    private LocalBenchmark() {
    }

    /**
     * 运行单核基准并返回 Geekbench 6 量级的单核分。
     * 纯计算方法，耗时约 200ms 预热 + 1.5s 正式计时；调用方负责放到 compute 线程池。
     *
     * @return 单核分（≥ 1）
     */
    public static long runSingleCoreScore() {
        // ---- 预热：跑缩小 1/16 的完整三段负载直到 200ms 耗尽 ----
        long warmupDeadline = System.nanoTime() + WARMUP_NANOS;
        long warmSink = 0;
        while (System.nanoTime() < warmupDeadline) {
            warmSink += shaSegment(SHA_ITERATIONS / 16);
            warmSink += arithSegment(ARITH_ITERATIONS / 16);
            warmSink += sortSegment(SORT_ROUNDS / 16);
        }
        sink = warmSink;

        // ---- 正式计时：三段各自计时后加权 ----
        long t0 = System.nanoTime();
        long r1 = shaSegment(SHA_ITERATIONS);
        long t1 = System.nanoTime();
        long r2 = arithSegment(ARITH_ITERATIONS);
        long t2 = System.nanoTime();
        long r3 = sortSegment(SORT_ROUNDS);
        long t3 = System.nanoTime();
        sink = r1 ^ r2 ^ r3;

        double shaScore = K_SHA / seconds(t1 - t0);
        double arithScore = K_ARITH / seconds(t2 - t1);
        double sortScore = K_SORT / seconds(t3 - t2);
        long score = Math.max(1, Math.round(
                WEIGHT_SHA * shaScore + WEIGHT_ARITH * arithScore + WEIGHT_SORT * sortScore));

        LOGGER.info("本地单核基准完成: 总分={} (sha={} arith={} sort={}, 总耗时={}ms)",
                score, Math.round(shaScore), Math.round(arithScore), Math.round(sortScore),
                (t3 - t0) / 1_000_000);
        return score;
    }

    /** 纳秒转秒，并下限保护：防止未来超高速硬件上耗时趋近 0 导致分数爆炸。 */
    private static double seconds(long nanos) {
        return Math.max(nanos / 1e9, 1e-3);
    }

    // ------------------------------------------------------------ 三段负载

    /** 第一段：SHA-256 链式哈希（每轮输入 = 上轮 32 字节摘要，强制串行）。 */
    private static long shaSegment(int iterations) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // JDK 强制要求实现 SHA-256，此处不可能发生
            throw new IllegalStateException("JDK 缺失 SHA-256 实现", e);
        }
        // 固定初始内容，保证每次运行的工作负载逐字节相同
        byte[] chain = new byte[32];
        for (int i = 0; i < chain.length; i++) {
            chain[i] = (byte) i;
        }
        for (int i = 0; i < iterations; i++) {
            chain = digest.digest(chain);
        }
        return chain[0] & 0xFFL;
    }

    /** 第二段：xorshift 整数扰动 + 浮点乘加，循环携带依赖（无法被向量化/并行化）。 */
    private static long arithSegment(int iterations) {
        long acc = 0x9E3779B97F4A7C15L; // 黄金分割常数作固定种子
        double f = 1.0;
        for (int i = 0; i < iterations; i++) {
            // xorshift64：3 次移位异或，纯整数依赖链
            acc ^= acc << 13;
            acc ^= acc >>> 7;
            acc ^= acc << 17;
            // 浮点乘加 + 区间回卷，防止溢出到 Infinity 后 JIT 走快捷路径
            f = f * 1.0000001 + 1e-9;
            if (f > 2.0) {
                f -= 1.0;
            }
        }
        return acc ^ Double.doubleToLongBits(f);
    }

    /** 第三段：确定性 LCG 填充小数组后排序（数据每轮不同但序列完全确定）。 */
    private static long sortSegment(int rounds) {
        int[] work = new int[SORT_ARRAY_LENGTH];
        long checksum = 0;
        for (int r = 0; r < rounds; r++) {
            // 每轮用"固定种子 + 轮号"重新生成乱序数据，避免对已排序数组排序走 O(n) 捷径
            long state = 0x5DEECE66DL + r;
            for (int i = 0; i < work.length; i++) {
                // Knuth MMIX LCG：乘加常数固定，序列确定
                state = state * 6364136223846793005L + 1442695040888963407L;
                work[i] = (int) (state >>> 33);
            }
            Arrays.sort(work);
            checksum += work[0] + work[work.length / 2] + work[work.length - 1];
        }
        return checksum;
    }
}
