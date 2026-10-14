package imsng.player_to_player.compute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;

/**
 * 系统物理内存信息读取。
 * <p>
 * 规范出处：player_to_player-prompt.txt "算力分配：……同时成为主客户段还有一个条件为
 * 目前剩余的内存大小 服务端可以设定剩余的内存大小 默认为0.5GB" ——
 * 这里读到的是<b>整机物理内存</b>（不是 JVM 堆、也不是 MC 进程占用），写入
 * {@link ComputeScore#freeMemoryBytes()} / {@link ComputeScore#totalMemoryBytes()}。
 *
 * <h2>快照语义（重要）</h2>
 * 本类方法每次调用都重新读取系统当前值，但 {@link ComputeScoreProvider} 整机只做一次
 * 算力检测并长期缓存结果 —— 因此真正上报给服务端的是"<b>模组加载那一刻</b>"的可用内存
 * 快照，此后不会因为玩家开/关其他程序而刷新。方向偏保守（玩家后来关掉程序腾出的内存
 * 不会被重新计入），是当前实现的已知取舍。
 *
 * <h2>门槛判定在服务端</h2>
 * 是否够格当主客户端，由服务端用
 * {@link imsng.player_to_player.config.GlobalConfig#minFreeMemoryBytes}（默认 0.5 GB）
 * 与这个快照比较（{@link ComputeScore#meetsMemoryRequirement(long)}，
 * {@link ComputeTable#selectPrimary} 用它过滤候选）；客户端只负责如实上报，不做判定。
 * <p>
 * 实现：强转 {@code com.sun.management.OperatingSystemMXBean}（HotSpot / OpenJDK 系
 * 均导出该接口，Java 17 上 {@code getFreeMemorySize}/{@code getTotalMemorySize} 可用，
 * 替代了 14 之前废弃的 getFreePhysicalMemorySize）；若运行在非 HotSpot JVM 上强转失败，
 * 降级返回 {@link Runtime} 的 JVM 堆数据并记 warn —— 堆数据远小于物理内存，
 * 会导致内存门槛判定偏保守（宁可错杀不选它当主客户端），方向安全。
 */
public final class MemoryInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/compute");

    /** 降级警告只记一次，避免每次上报都刷屏。 */
    private static volatile boolean fallbackWarned;

    private MemoryInfo() {
    }

    /** 系统当前可用物理内存（字节）；非 HotSpot 降级为 JVM 堆剩余可扩展空间。 */
    public static long freeMemoryBytes() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean sunBean) {
            return sunBean.getFreeMemorySize();
        }
        warnFallbackOnce();
        // 堆上限 - 已用堆 = JVM 还能拿到的内存，作为"可用内存"的保守近似
        Runtime rt = Runtime.getRuntime();
        return rt.maxMemory() - (rt.totalMemory() - rt.freeMemory());
    }

    /** 系统总物理内存（字节）；非 HotSpot 降级为 JVM 最大堆。 */
    public static long totalMemoryBytes() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean sunBean) {
            return sunBean.getTotalMemorySize();
        }
        warnFallbackOnce();
        return Runtime.getRuntime().maxMemory();
    }

    private static void warnFallbackOnce() {
        if (!fallbackWarned) {
            fallbackWarned = true;
            LOGGER.warn("当前 JVM 未导出 com.sun.management.OperatingSystemMXBean，"
                    + "无法读取物理内存，降级使用 JVM 堆数据（内存门槛判定将偏保守）");
        }
    }
}
