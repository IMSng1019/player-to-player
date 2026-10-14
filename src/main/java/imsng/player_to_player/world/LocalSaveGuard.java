package imsng.player_to_player.world;

import imsng.player_to_player.util.Quarantine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

/**
 * 本地存档的世界身份守门：复用已对齐的存档，或隔离旧存档并从服务端骨架重建。
 * <p>
 * 缺陷背景（{@code docs/plans/2026-10-14-world-identity-mismatch-corruption.md}）：世界文件夹键
 * 只有 {@code IP+世界名}。服务端换世界/重置世界/从备份还原后，客户端仍会走到这里；若不比对
 * 身份就直接"延续既有状态"，本地保存的旧世界 {@code region/*.mca} 会成为集成服务端的区块
 * 来源（服务端说"无数据"时回退到本地磁盘读），玩家看到旧地形；退出时这些旧区块又被整体
 * 上行覆盖服务端 MCA —— 双向静默损坏。
 *
 * <h2>为什么必须整体改名（勿简化成"删 level.dat"）</h2>
 * 重建分支是<b>合并式复制</b>（同名覆盖，不清理多余文件）：只删 {@code level.dat} 会让旧
 * {@code region/} 留在原目录，重建后照样被本地磁盘回退读命中。因此只要"现存目录没能通过
 * 身份校验"，就把<b>整个目录</b>改名隔离。
 * <p>
 * 这包括 {@code level.dat} 缺失但目录仍有旧 region 的情形 —— 那种目录本来就不是可用存档，
 * 却会被合并式复制补上一个正确的 {@code level.dat} 并从此"看起来已对齐"，从而永久放行
 * 旧区块读取。故本类的判据是"目录是否存在"，而不是"level.dat 是否存在"。
 *
 * <p><b>线程模型</b>：阻塞文件 IO，须在 io / 专用阻塞线程上调用（客户端在 join-flow 线程）。
 */
public final class LocalSaveGuard {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/local-world");

    private LocalSaveGuard() {
    }

    /**
     * 准备本地存档目录：身份一致则原样复用，否则隔离旧目录并从环境骨架重建。
     *
     * @param envWorldDir 服务端环境骨架目录（其 {@code level.dat} 是服务端本次的权威副本，
     *                    由环境同步写入）
     * @param saveDir     本地存档目录（{@code <世界文件夹>/primary/data/<世界名>}）
     * @param epochMillis 隔离目录命名用时间戳（由调用方提供，便于测试）
     * @return 已对齐的世界身份哈希；空串 = 服务端骨架给不出身份（此时旧存档一律被隔离，
     *         本地存档是骨架的全新副本，对齐关系仍成立）
     * @throws IOException 环境骨架缺 {@code level.dat}（环境同步未完成/服务端未把世界目录
     *                     纳入环境清单），或旧存档隔离失败（宁可中止切换，不可带旧数据继续）
     */
    public static String prepareSave(Path envWorldDir, Path saveDir, long epochMillis)
            throws IOException {
        Path envLevelDat = envWorldDir.resolve("level.dat");
        if (!Files.isRegularFile(envLevelDat)) {
            throw new IOException("环境目录缺少世界骨架（level.dat）: " + envWorldDir
                    + " —— 环境同步未完成或服务端未把世界目录纳入环境清单");
        }
        WorldIdentity.Snapshot server = WorldIdentity.snapshot(envLevelDat);
        if (!server.known()) {
            LOGGER.warn("环境骨架 level.dat 无法提取世界身份（缺 WorldGenSettings/RandomSeed），"
                    + "本次不复用既有本地存档: {}", envLevelDat);
        }
        Path localLevelDat = saveDir.resolve("level.dat");
        WorldIdentity.Snapshot local = Files.isRegularFile(localLevelDat)
                ? WorldIdentity.snapshot(localLevelDat)
                : WorldIdentity.Snapshot.unknown();

        // 客户端用**严格**判据（只用身份哈希）：误隔离的代价是"本地缓存重建"（非破坏性），
        // 误复用的代价是"旧世界区块被读入并上行覆盖服务端 MCA"。判错代价不对称，故取严。
        if (Files.isRegularFile(localLevelDat)
                && WorldIdentity.sameWorldStrict(server, local)) {
            LOGGER.info("本地存档世界身份校验通过，延续既有状态: {} (identity={})",
                    saveDir, server.shortId());
            return server.identityHash();
        }
        if (Files.isRegularFile(localLevelDat)
                && WorldIdentity.relaxedBySeed(server, local)) {
            // 种子相同、生成设置结构不同：多是 level.dat 被原版 DataFixer 跨版本重写。
            // 仍然隔离重建（本地缓存从服务端取回），只是明确告知这是"升级/同种子替换"，
            // 而不是"服务端换了世界"——两者的人工处置不同。
            LOGGER.warn("本地存档身份哈希与服务端骨架不同但世界种子相同: {} "
                            + "（升级重写的 level.dat，或同种子的另一份世界存档）—— "
                            + "仍按不一致处理并重建本地缓存: 服务端={} 本地={} 种子={}",
                    saveDir, server.shortId(), local.shortId(), local.seed().orElse(null));
        }

        if (Files.exists(saveDir)) {
            // 目录存在但没能通过校验：整体隔离（隔离而非删除 —— 旧存档可能还有人工价值）。
            String label = local.known() ? local.shortId()
                    : (Files.isRegularFile(localLevelDat) ? "unknown" : "no-level-dat");
            Path stale = Quarantine.quarantine(saveDir, label, epochMillis);
            LOGGER.warn("本地存档世界身份不一致（或无法确认），已隔离旧存档并重建："
                            + "服务端={} 本地={} 本地种子={} 旧存档 → {}",
                    server.shortId(), local.known() ? local.shortId() : "unknown",
                    local.seed().orElse(null), stale);
        }
        LOGGER.info("从环境骨架构建本地存档: {} → {} (identity={})", envWorldDir, saveDir,
                server.shortId());
        copySkeleton(envWorldDir, saveDir);
        return server.identityHash();
    }

    /** 把环境骨架整树复制进存档目录（同名覆盖；骨架按设计不含 region/poi/entities）。 */
    private static void copySkeleton(Path envWorldDir, Path saveDir) throws IOException {
        try (Stream<Path> walk = Files.walk(envWorldDir)) {
            for (Path source : (Iterable<Path>) walk::iterator) {
                Path target = saveDir.resolve(envWorldDir.relativize(source).toString());
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
        // session.lock 是运行期锁文件，跟着骨架复制过来会让引导误判"存档被占用"
        Files.deleteIfExists(saveDir.resolve("session.lock"));
    }
}
