package imsng.player_to_player.client.group;

import imsng.player_to_player.client.session.WorldSession;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.netproto.ControlConnection;
import net.minecraft.client.Minecraft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * 本地组世界启动器（主客户端，Phase 2；规范"主客户端……作为它这个组客户端的服务端"）。
 * <p>
 * 被指派为主客户端后（io 线程调用 {@link #launch}）：
 * <ol>
 *   <li><b>构建本地存档</b>：环境同步已把服务端的世界骨架（level.dat、playerdata、
 *       data/ 等 —— 服务端扫描时只排除了 region/poi/entities/DIM* 维度数据）带到
 *       {@code environment/primary/<世界名>/}；首次启动把它复制成
 *       {@code data/primary/<世界名>/} 存档。<b>只在存档不存在时复制</b>：
 *       本地存档是组世界的延续状态（时间/天气/玩家数据），重复加入不回滚 ——
 *       区块地形则不受影响，权威始终在服务端（申请门控每次都拉最新）；</li>
 *   <li><b>预置组运行时</b>（{@link GroupRuntime#arm}）：集成服务端一启动即被接管
 *       （申请门控、上行捕获、出生点跳过全部就位 —— arm 必须先于世界打开）；</li>
 *   <li><b>主线程切换</b>：编排性断开物理服务端 → 原版管线打开本地存档；</li>
 *   <li>接管完成回调里由 {@link GroupHost} 发布 LAN 端口并开始接待副客户端。</li>
 * </ol>
 * 任何一步失败：{@link GroupRuntime#disarm} + 会话拆除，玩家回到标题屏
 * （日志给出可操作的原因），不会留下半初始化状态。
 */
public final class LocalWorldLauncher {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/local-world");

    private LocalWorldLauncher() {
    }

    /**
     * 启动本地组世界（io 线程；阻塞做文件复制，主线程动作经 execute 投递）。
     *
     * @param conn      与物理服务端的控制连接（组运行时的申请/上行通道）
     * @param worldFolder 组世界文件夹（&lt;IP&gt;+&lt;世界名&gt;）
     * @param worldName 服务端世界名（HELLO_ACK 下发）
     * @param groupId   本组 ID（== 本客户端 clientId）
     */
    public static void launch(Minecraft minecraft, ControlConnection conn, Path worldFolder,
                              String worldName, UUID groupId) {
        NodeContext ctx = NodeContext.get();
        P2PPaths paths = ctx.paths();
        String saveName = P2PPaths.sanitize(worldName);
        Path savesRoot = paths.dataDir(worldFolder, ClientRole.PRIMARY);
        Path saveDir = savesRoot.resolve(saveName);
        Path envWorldDir = paths.environmentDir(worldFolder, ClientRole.PRIMARY).resolve(worldName);

        try {
            prepareSave(envWorldDir, saveDir);
        } catch (IOException e) {
            LOGGER.error("本地存档构建失败，无法作为主客户端启动（会话保持，可重进世界重试）", e);
            return;
        }

        // 预置组运行时：必须在世界打开前 —— MinecraftServerMixin 在 prepareLevels
        // 阶段就要读到 isArmedOrActive，ChunkMapMixin 在首个区块加载前要拿到申请客户端
        GroupRuntime.arm(new GroupRuntime.GroupPlan(groupId, conn,
                ctx.config().chunkClaimRetrySeconds, GroupHost::start));

        minecraft.execute(() -> WorldSwitcher.switchToLocalWorld(minecraft, savesRoot, saveName, () -> {
            // 打开失败/被玩家取消：撤销预置并拆除会话（半初始化状态不可留）
            GroupRuntime.disarm();
            WorldSession.teardownSession();
        }));
    }

    /**
     * 构建本地存档：saveDir 缺 level.dat 时从环境骨架整树复制。
     * <p>
     * 环境目录里的世界骨架不含 region/poi/entities（服务端扫描排除），复制量
     * 通常只有几 MB；已存在的存档不做覆盖（本地延续状态优先，见类 Javadoc）。
     */
    private static void prepareSave(Path envWorldDir, Path saveDir) throws IOException {
        if (Files.isRegularFile(saveDir.resolve("level.dat"))) {
            LOGGER.info("本地存档已存在，延续既有状态: {}", saveDir);
            return;
        }
        if (!Files.isRegularFile(envWorldDir.resolve("level.dat"))) {
            throw new IOException("环境目录缺少世界骨架（level.dat）: " + envWorldDir
                    + " —— 环境同步未完成或服务端未把世界目录纳入环境清单");
        }
        LOGGER.info("首次启动，从环境骨架构建本地存档: {} → {}", envWorldDir, saveDir);
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
