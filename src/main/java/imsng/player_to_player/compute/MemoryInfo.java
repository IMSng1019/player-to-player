package imsng.player_to_player.compute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;

/**
 * 系统物理内存信息读取。
 * <p>
 * 规范出处：player_to_player-prompt.txt "算力分配：……同时成为主客户段还有一个条件为
 * 目前剩余的内存大小 服务端可以设定剩余的内存大小 默认为0.5GB" ——
 * 这里读到的是<b>整机物理内存</b>（写入 {@link ComputeScore#freeMemoryBytes()} /
 * {@link ComputeScore#totalMemoryBytes()}），服务端 {@link ComputeTable#selectPrimary}
 * 据此过滤不满足 {@link imsng.player_to_player.config.GlobalConfig#minFreeMemoryBytes} 的候选。
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
