package imsng.player_to_player.client.group;

import imsng.player_to_player.client.session.WorldSession;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.PlayerStateFiles;
import imsng.player_to_player.netproto.ControlConnection;
import net.minecraft.client.Minecraft;
import net.minecraft.nbt.CompoundTag;
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
 * 被指派为主客户端前后分为两个阶段：
 * <ol>
 *   <li><b>切换前准备</b>：环境同步已把服务端的世界骨架（level.dat、playerdata、
 *       data/ 等 —— 服务端扫描时只排除了 region/poi/entities/DIM* 维度数据）带到
 *       {@code environment/primary/<世界名>/}；首次启动复制世界骨架，随后每次都用
 *       物理服务器权威玩家 NBT 覆盖本地 UUID 玩家文件，并清除 level.dat 内嵌玩家；</li>
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

    /** 已完成全部磁盘准备、可以直接在客户端主线程打开的本地世界。 */
    public record PreparedWorld(Path savesRoot, String saveName) {
    }

    /**
     * 在发送 ROLE_REQUEST 前准备本地主缓存（io 线程，可阻塞做文件 IO）。
     *
     * @param worldFolder 组世界文件夹（&lt;IP&gt;+&lt;世界名&gt;）
     * @param worldName 服务端世界名（HELLO_ACK 下发）
     * @param playerId  当前玩家 UUID
     * @param playerState 物理服务器实时下发并通过校验的权威玩家 NBT
     */
    public static PreparedWorld prepare(Path worldFolder, String worldName, UUID playerId,
                                        CompoundTag playerState) throws IOException {
        NodeContext ctx = NodeContext.get();
        P2PPaths paths = ctx.paths();
        String saveName = P2PPaths.sanitize(worldName);
        Path savesRoot = paths.dataDir(worldFolder, ClientRole.PRIMARY);
        Path saveDir = savesRoot.resolve(saveName);
        Path envWorldDir = paths.environmentDir(worldFolder, ClientRole.PRIMARY).resolve(worldName);

        prepareSave(envWorldDir, saveDir);
        PlayerStateFiles.installAuthoritative(saveDir, playerId, playerState);
        LOGGER.info("物理服务器权威玩家状态已安装到本地主缓存: {}", saveDir);
        return new PreparedWorld(savesRoot, saveName);
    }

    /**
     * 打开已经准备完成的本地组世界；本方法不再执行任何磁盘 IO。
     */
    public static void launchPrepared(Minecraft minecraft, ControlConnection conn,
                                      PreparedWorld world, UUID groupId) {
        NodeContext ctx = NodeContext.get();
        // 预置组运行时：必须在世界打开前 —— MinecraftServerMixin 在 prepareLevels
        // 阶段就要读到 isArmedOrActive，ChunkMapMixin 在首个区块加载前要拿到申请客户端
        GroupRuntime.arm(new GroupRuntime.GroupPlan(groupId, conn,
                ctx.config().chunkClaimRetrySeconds, GroupHost::start));

        minecraft.execute(() -> WorldSwitcher.switchToLocalWorld(
                minecraft, world.savesRoot(), world.saveName(), () -> {
            // 打开失败/被玩家取消：撤销预置并拆除会话（半初始化状态不可留）
            GroupRuntime.disarm();
            WorldSession.teardownSession();
        }));
    }

    /**
     * 构建本地存档：saveDir 缺 level.dat 时从环境骨架整树复制。
     * <p>
     * 环境目录里的世界骨架不含 region/poi/entities（服务端扫描排除），复制量
     * 通常只有几 MB；已存在的世界元数据不整体覆盖，玩家状态由 prepare 单独刷新。
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
