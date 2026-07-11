package imsng.player_to_player.env;

import imsng.player_to_player.util.Sha256;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

/**
 * 环境文件扫描器：遍历根目录生成 {@link EnvironmentManifest}。
 * <p>
 * 规范出处（player_to_player-prompt.txt）："环境文件：指服务器 fabric-server-launch.jar
 * 所在文件夹中除了 ./logs ./world/DIM-1 ./world/DIM ./world/poi ./world/region
 * ./world/entities 文件夹的其他文件，并且服务端可以指定哪些文件夹或者文件为非环境文件"。
 * 设计出处：DESIGN.md 第 4 节（内置排除 + 总配置 nonEnvironmentPaths 叠加）。
 * <p>
 * <b>线程模型</b>：目录遍历在调用线程执行（快），逐文件哈希提交
 * {@link ThreadPools#io()} 并行计算（信号量限并发，见 {@code MAX_CONCURRENT_HASHES}）
 * 后统一 join —— 本方法整体是阻塞的，
 * 只能在 IO/后台线程调用，严禁 MC 主线程 / Netty 事件循环。
 */
public final class EnvironmentScanner {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env");

    /**
     * 内置排除项（相对路径前缀，正斜杠、小写比较）。
     * 除规范列出的目录外追加：
     * <ul>
     *   <li>player-to-player（本模组自身数据，含日志收集与注册表，
     *       不属于要分发给客户端的服务器环境）与 session.lock（MC 运行期锁文件，
     *       每次启动都变且被持锁进程占用，读了也没意义）；</li>
     *   <li>含敏感信息/服务器专属的文件：server.properties（含 rcon.password 等凭据，
     *       绝不能分发给所有客户端）、ops.json、whitelist.json、banned-ips.json、
     *       banned-players.json、usercache.json（玩家隐私）、eula.txt —— 这些文件
     *       只对物理服务端有意义，客户端环境用不到。</li>
     * </ul>
     * 注 1：规范原文写 "./world/DIM"，实际 MC 目录为 DIM1（末地）/DIM-1（下界），按实际目录排除。
     * 注 2："world/" 前缀是 level-name=world 时的兜底；level-name 非默认值时由
     * P2PServerService 根据实际存档目录动态追加对应排除项（见 worldDataExclusions）。
     */
    private static final List<String> BUILTIN_EXCLUSIONS = List.of(
            "logs",
            "player-to-player",
            "world/region",
            "world/poi",
            "world/entities",
            "world/DIM-1",
            "world/DIM1",
            "session.lock",
            "server.properties",
            "ops.json",
            "whitelist.json",
            "banned-ips.json",
            "banned-players.json",
            "usercache.json",
            "eula.txt");

    /**
     * 哈希任务的最大并发数：min(4, CPU 核数)。
     * 取舍：io() 是无界缓存池，目录遍历远快于磁盘哈希，若每文件一个任务不限流，
     * 大型整合包（几千个文件）会瞬间创建几千个线程；而哈希是磁盘顺序读为主的负载，
     * 并发过高只会退化成磁盘随机读、拖慢总吞吐 —— 少量并发（机械盘 1 即够，
     * SSD 可吃到 4 路）已能填满磁盘带宽，故用信号量限到 4 与核数的较小者。
     */
    private static final int MAX_CONCURRENT_HASHES =
            Math.min(4, Runtime.getRuntime().availableProcessors());

    private EnvironmentScanner() {
    }

    /**
     * 扫描根目录，生成环境清单。
     *
     * @param root            扫描根（服务端根目录，或客户端的角色环境目录）；
     *                        不存在时返回空清单（客户端首次同步前目录还没建）
     * @param extraExclusions 追加排除项（来自 {@code GlobalConfig.nonEnvironmentPaths}，
     *                        服务端自定义的非环境文件；相对路径，正斜杠分隔）；可为 null
     * @return 清单（相对路径 → sha256 + 大小）
     */
    public static EnvironmentManifest scan(Path root, List<String> extraExclusions) {
        if (!Files.isDirectory(root)) {
            // 客户端第一次同步时本地环境目录尚不存在，按空清单处理（=全量下载）
            return new EnvironmentManifest(Map.of());
        }
        // 合并内置排除与服务端自定义排除；排除项统一规范化 + 小写
        // （Windows 文件系统大小写不敏感，配置写 Logs 也应命中 logs）
        List<String> exclusions = new ArrayList<>();
        for (String builtin : BUILTIN_EXCLUSIONS) {
            exclusions.add(normalize(builtin).toLowerCase(Locale.ROOT));
        }
        if (extraExclusions != null) {
            for (String extra : extraExclusions) {
                if (extra != null && !extra.isBlank()) {
                    exclusions.add(normalize(extra).toLowerCase(Locale.ROOT));
                }
            }
        }

        Path absoluteRoot = root.toAbsolutePath().normalize();
        // 并行哈希的结果容器：并发安全 + 天然有序（EnvironmentManifest 反正还会再排一次）
        Map<String, EnvironmentManifest.Entry> entries = new ConcurrentSkipListMap<>();
        List<CompletableFuture<Void>> hashTasks = new ArrayList<>();
        // 哈希并发限流（见 MAX_CONCURRENT_HASHES 注释的取舍说明）
        Semaphore hashPermits = new Semaphore(MAX_CONCURRENT_HASHES);

        try (Stream<Path> walk = Files.walk(absoluteRoot)) {
            walk.forEach(path -> {
                if (!Files.isRegularFile(path)) {
                    return; // 只收文件；目录本身不进清单（空目录不参与环境一致性）
                }
                String relative = normalize(absoluteRoot.relativize(path).toString());
                if (isExcluded(relative, exclusions)) {
                    return;
                }
                // 在遍历线程上先取许可再提交：io() 是 cached 无界池，每个排队任务都会
                // 新建线程，若在任务体内才取许可，几千个文件仍会瞬间建几千个线程；
                // 提交前取许可把"在飞"任务数（=占用线程数）硬限在 MAX_CONCURRENT_HASHES
                //（本方法整体阻塞、只在 IO 线程调用，遍历线程在这里等待是允许的）
                try {
                    hashPermits.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("环境目录扫描被中断: " + absoluteRoot, e);
                }
                hashTasks.add(CompletableFuture.runAsync(() -> {
                    try {
                        long size = Files.size(path);
                        String sha256 = Sha256.hexOfFile(path);
                        entries.put(relative, new EnvironmentManifest.Entry(sha256, size));
                    } catch (IOException e) {
                        // 单文件读失败（被锁定/瞬时删除）：记警告并跳过，不让整次扫描失败。
                        // 被跳过的文件不进清单 → 客户端也不会请求它，行为自洽
                        LOGGER.warn("环境文件哈希失败，已跳过: {} ({})", relative, e.toString());
                    } finally {
                        hashPermits.release(); // 无论成败都归还许可，避免遍历线程被卡死
                    }
                }, ThreadPools.io()));
            });
        } catch (IOException e) {
            // 根目录遍历失败属于环境级故障（权限/磁盘），向上抛给启动流程处理
            throw new IllegalStateException("环境目录扫描失败: " + absoluteRoot, e);
        }

        // 等全部哈希任务完成（本方法整体阻塞，见类 Javadoc 线程模型）
        CompletableFuture.allOf(hashTasks.toArray(new CompletableFuture[0])).join();

        EnvironmentManifest manifest = new EnvironmentManifest(entries);
        LOGGER.info("环境扫描完成: {} — {} 个文件, 全局哈希 {}",
                absoluteRoot, manifest.files().size(), manifest.globalHash());
        return manifest;
    }

    /**
     * 排除判断：相对路径本身等于排除项，或位于排除项目录之下（前缀 + "/"）。
     * 另外按规范补充：{@code *.tmp} 临时文件一律排除
     * （本模组自身的原子写中间文件与其他程序的临时文件都不属于稳定环境）。
     */
    private static boolean isExcluded(String relative, List<String> exclusions) {
        if (relative.toLowerCase(Locale.ROOT).endsWith(".tmp")) {
            return true;
        }
        String lower = relative.toLowerCase(Locale.ROOT);
        for (String exclusion : exclusions) {
            if (lower.equals(exclusion) || lower.startsWith(exclusion + "/")) {
                return true;
            }
        }
        return false;
    }

    /**
     * 路径规范化：反斜杠 → 正斜杠（Windows）、去掉开头的 "./" 与 "/"、
     * 去掉尾部斜杠（"logs/" 与 "logs" 等价）。<b>保留大小写</b> ——
     * 清单键要按原始大小写传给客户端落盘；排除比较时另行转小写。
     */
    private static String normalize(String path) {
        String p = path.replace('\\', '/');
        while (p.startsWith("./")) {
            p = p.substring(2);
        }
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        while (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }
}
