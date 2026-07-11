package imsng.player_to_player.compute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * 跨平台读取 CPU 友好型号名（如 "AMD Ryzen 7 5800X 8-Core Processor"）。
 * <p>
 * 规范出处：player_to_player-prompt.txt "玩家加载模组：模组检查该玩家的cpu得出其单核算力"、
 * "算力表：……取决于CPU的单核能力 数据通过查询使用Geekbench的非官方公开API可以得到" ——
 * 型号名既写入 {@link ComputeScore#cpuModel()} 供服务端算力表展示，
 * 也是 {@link GeekbenchLookup} 查询单核分的搜索关键字。
 * <p>
 * 平台策略（全部失败返回 {@code "unknown"}，绝不抛异常）：
 * <ul>
 *   <li>Windows：执行 {@code reg query "HKLM\HARDWARE\DESCRIPTION\System\CentralProcessor\0"
 *       /v ProcessorNameString} 解析注册表值；失败降级读 {@code PROCESSOR_IDENTIFIER} 环境变量
 *       （内容较难看但聊胜于无）；</li>
 *   <li>Linux：读 {@code /proc/cpuinfo} 的第一行 {@code model name}；</li>
 *   <li>macOS：执行 {@code sysctl -n machdep.cpu.brand_string}。</li>
 * </ul>
 * <b>线程模型</b>：首次调用会执行外部进程 / 读文件（阻塞数十毫秒量级），
 * 只能在 {@code ThreadPools.compute()}/{@code io()} 等后台线程调用，
 * 严禁在 MC 主线程或 Netty 事件循环上调用（DESIGN.md 第 9 节铁律）。
 * 结果整机不变，故用 volatile 缓存，之后的调用零开销。
 */
public final class CpuInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/compute");

    /** 外部命令执行超时（毫秒）：reg/sysctl 正常几十毫秒即返回，3 秒仍无结果视为异常环境。 */
    private static final long EXEC_TIMEOUT_MILLIS = 3_000;

    /** 检测结果缓存：CPU 型号整机运行期不会变化，只测一次。 */
    private static volatile String cached;

    private CpuInfo() {
    }

    /**
     * 取 CPU 友好型号名；所有手段失败返回 {@code "unknown"}（永不为 null / 空串）。
     * 首次调用阻塞（外部进程），后续调用直接命中缓存。
     */
    public static String cpuModel() {
        String local = cached;
        if (local != null) {
            return local;
        }
        synchronized (CpuInfo.class) {
            if (cached == null) {
                cached = detect();
                LOGGER.info("CPU 型号检测结果: {}", cached);
            }
            return cached;
        }
    }

    /** 按操作系统分发检测；任何异常都吞掉并降级，保证主流程不受影响。 */
    private static String detect() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        try {
            String model;
            if (os.contains("win")) {
                model = detectWindows();
            } else if (os.contains("mac") || os.contains("darwin")) {
                model = detectMac();
            } else {
                // 其余一律按 Linux 系处理（含各发行版 / 容器环境）
                model = detectLinux();
            }
            if (model != null && !model.isBlank()) {
                return normalize(model);
            }
        } catch (Exception e) {
            LOGGER.debug("CPU 型号检测异常（将返回 unknown）", e);
        }
        return "unknown";
    }

    // ------------------------------------------------------------ Windows

    private static String detectWindows() {
        // 注册表里的 ProcessorNameString 是市场型号名，与 Geekbench 数据库命名最接近
        String output = execAndCapture(
                "reg", "query",
                "HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0",
                "/v", "ProcessorNameString");
        String model = parseRegOutput(output);
        if (model != null) {
            return model;
        }
        // 降级：PROCESSOR_IDENTIFIER 形如 "Intel64 Family 6 Model 158 ..."，
        // 可读性差且 Geekbench 搜索命中率低，但仍能唯一标识 CPU 家族
        return System.getenv("PROCESSOR_IDENTIFIER");
    }

    /**
     * 解析 reg query 输出。目标行形如：
     * <pre>    ProcessorNameString    REG_SZ    AMD Ryzen 7 5800X 8-Core Processor</pre>
     * 用 "REG_SZ" 作为锚点截取值部分，对列宽 / 本地化输出都稳健。
     */
    private static String parseRegOutput(String output) {
        if (output == null) {
            return null;
        }
        for (String line : output.split("\\R")) {
            if (!line.contains("ProcessorNameString")) {
                continue;
            }
            int anchor = line.indexOf("REG_SZ");
            if (anchor >= 0) {
                String value = line.substring(anchor + "REG_SZ".length()).trim();
                if (!value.isEmpty()) {
                    return value;
                }
            }
        }
        return null;
    }

    // -------------------------------------------------------------- Linux

    private static String detectLinux() throws Exception {
        Path cpuinfo = Path.of("/proc/cpuinfo");
        if (!Files.isReadable(cpuinfo)) {
            return null;
        }
        // /proc/cpuinfo 每个逻辑核一段，型号全部相同，取第一条 model name 即可
        for (String line : Files.readAllLines(cpuinfo, StandardCharsets.UTF_8)) {
            if (line.startsWith("model name")) {
                int colon = line.indexOf(':');
                if (colon >= 0) {
                    return line.substring(colon + 1).trim();
                }
            }
        }
        return null;
    }

    // -------------------------------------------------------------- macOS

    private static String detectMac() {
        return execAndCapture("sysctl", "-n", "machdep.cpu.brand_string");
    }

    // ---------------------------------------------------------------- 工具

    /**
     * 执行外部命令并返回合并后的 stdout+stderr 文本；超时 / 失败返回 null。
     * <p>
     * 先 waitFor 再读输出：这里的命令输出都远小于管道缓冲区（几十字节到几 KB），
     * 不存在"缓冲区塞满导致进程卡死"的风险，反而能保证超时判定先于阻塞读。
     */
    private static String execAndCapture(String... command) {
        Process process = null;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            if (!process.waitFor(EXEC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                LOGGER.debug("命令执行超时: {}", String.join(" ", command));
                return null;
            }
            // CPU 型号名是纯 ASCII，用平台默认字符集解码即可
            //（Windows 控制台是 OEM 代码页，UTF-8 解 ASCII 也无损）
            String output = new String(process.getInputStream().readAllBytes(), Charset.defaultCharset());
            String trimmed = output.trim();
            return trimmed.isEmpty() ? null : trimmed;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOGGER.debug("命令执行失败: {}", String.join(" ", command), e);
            return null;
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

    /** 规范化：折叠连续空白（注册表 / sysctl 输出常带多余空格），便于日志展示与搜索命中。 */
    private static String normalize(String model) {
        return model.replaceAll("\\s+", " ").trim();
    }
}
