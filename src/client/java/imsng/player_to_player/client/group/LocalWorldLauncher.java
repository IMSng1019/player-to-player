package imsng.player_to_player.client.group;

import imsng.player_to_player.client.session.WorldSession;
import imsng.player_to_player.client.session.SwitchTelemetry;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.config.WorldConfig;
import imsng.player_to_player.core.ClientRole;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.PlayerStateFiles;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.world.LocalSaveGuard;
import imsng.player_to_player.world.WorldIdentity;
import imsng.player_to_player.runtime.WorkerProcess;
import imsng.player_to_player.client.env.EnvironmentPreflightClient;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.client.Minecraft;
import net.minecraft.nbt.CompoundTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

/**
 * 本地组世界启动器：准备权威存档和冻结环境，后台启动独立 Fabric 服务端 JVM。
 * <p>
 * 被指派为主客户端前后分为两个阶段：
 * <ol>
 *   <li><b>切换前准备</b>：环境同步已把服务端的世界骨架（level.dat、playerdata、
 *       data/ 等 —— 服务端扫描时只排除了 region/poi/entities/DIM* 维度数据）带到
 *       {@code environment/primary/<世界名>/}；首次启动复制世界骨架，随后每次都用
 *       物理服务器权威玩家 NBT 同时覆盖 UUID 玩家文件与 level.dat 内嵌玩家；</li>
 *   <li>由子进程在世界打开前预置区块申请门控、上行捕获与出生点禁用；</li>
 *   <li>父进程核对实际加载证明后，编排性断开旧游戏连接，走原版网络登录加入子服；</li>
 *   <li>{@link GroupHost} 将同组成员的 P2P 连接桥接到这个服务端，身份经 IPC 精确绑定。</li>
 * </ol>
 * 任何一步失败都会停止失败实例并拆除会话，玩家回到标题屏
 * （日志给出可操作的原因），不会留下半初始化状态。
 */
public final class LocalWorldLauncher {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/local-world");

    private LocalWorldLauncher() {
    }

    /**
     * 已完成全部磁盘准备、可以直接在客户端主线程打开的本地世界。
     *
     * @param worldIdentity 本地存档与服务端骨架已对齐的世界身份哈希（见
     *                      {@link WorldIdentity}）；空串 = 服务端 level.dat 无法给出身份，
     *                      此时本地存档是从骨架重建的全新副本，仍属"已对齐"，但不得
     *                      从旧的本地磁盘缓存回退读
     */
    public record PreparedWorld(Path savesRoot, String saveName, String worldIdentity,
                                Path worldFolder, Path environment, String worldPath) {
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
        Path environment = paths.environmentDir(worldFolder, ClientRole.PRIMARY);
        String worldPath = EnvironmentPreflightClient.worldPath();
        if (worldPath == null || worldPath.isBlank()) throw new IOException("环境预检未提供权威世界路径");
        Path envWorldDir = environment.resolve(worldPath).normalize();
        if (!envWorldDir.startsWith(environment.normalize())) throw new IOException("世界路径越过环境目录");

        String identity = prepareSave(envWorldDir, saveDir);
        imsng.player_to_player.env.WorldEnvironmentResources.sync(envWorldDir, saveDir);
        PlayerStateFiles.installAuthoritative(saveDir, playerId, playerState);
        LOGGER.info("物理服务器权威玩家状态已安装到本地主缓存: {} (世界身份={})",
                saveDir, WorldIdentity.shortId(identity));
        WorldConfig.updateWorldIdentity(paths.worldConfig(worldFolder), identity);
        SwitchTelemetry.mark("local_save_prepared");
        return new PreparedWorld(savesRoot, saveName, identity, worldFolder, environment, worldPath);
    }

    /**
     * 后台安装并启动已经准备完成的组世界；渲染线程只执行最终游戏连接切换。
     */
    public static void launchPrepared(Minecraft minecraft, ControlConnection conn,
                                      PreparedWorld world, UUID groupId) {
        // 独立JVM加载同步来的Fabric服务端环境；启动和IO不占渲染线程。
        // 子服READY之前保留当前游戏连接，失败时不会先把玩家移入半启动世界。
        ThreadPools.startBlocking("worker-launch", () -> {
            try {
                if (!WorldSession.isCurrentConnection(conn)) return;
                int gamePort = WorkerProcess.start(world.worldFolder(), world.environment(), world.worldPath(),
                        world.savesRoot(), world.saveName(), world.worldIdentity(), groupId, conn,
                        WorldSession.runtimeProperties(),
                        EnvironmentPreflightClient.prepared().environments().primaryManifest(),
                        EnvironmentPreflightClient.prepared().info().runtimeHash());
                if (!WorldSession.isCurrentConnection(conn)) { WorkerProcess.closeSession(); return; }
                GroupHost.startWorker(gamePort, conn, groupId);
                int bridgePort = WorkerProcess.openLocalPlayerBridge();
                minecraft.execute(() -> {
                    if (!WorldSession.isCurrentConnection(conn)) { WorkerProcess.closeSession(); return; }
                    WorldSwitcher.markTunnelPort(bridgePort);
                    WorldSwitcher.switchToTunnel(minecraft, bridgePort, NodeContext.get().playerName());
                });
            } catch (Exception error) {
                LOGGER.error("独立计算服务端启动失败", error);
                WorldSession.transitionFailed(conn, "独立计算服务端启动失败，请查看该世界runtime目录下的日志。");
            }
        });
    }

    /**
     * 构建本地存档：身份一致的既有存档直接复用，否则隔离旧存档并从环境骨架重建。
     * <p>
     * 判定与隔离逻辑在 {@link LocalSaveGuard}（src/main，可被 src/test 直接覆盖）——
     * 这里只负责把路径按 <b>P2PPaths</b> 口径解析出来。世界身份校验是数据完整性必需项，
     * 详见该类的类注释。
     *
     * @return 已对齐的世界身份哈希；空串 = 服务端 level.dat 无法给出身份
     */
    private static String prepareSave(Path envWorldDir, Path saveDir) throws IOException {
        return LocalSaveGuard.prepareSave(envWorldDir, saveDir, System.currentTimeMillis());
    }
}
