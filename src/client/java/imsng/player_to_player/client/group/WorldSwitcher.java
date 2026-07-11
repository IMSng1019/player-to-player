package imsng.player_to_player.client.group;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.ConnectScreen;
import net.minecraft.client.gui.screens.TitleScreen;
import net.minecraft.client.gui.screens.multiplayer.JoinMultiplayerScreen;
import net.minecraft.client.gui.screens.worldselection.WorldOpenFlows;
import net.minecraft.client.multiplayer.ServerData;
import net.minecraft.client.multiplayer.resolver.ServerAddress;
import net.minecraft.world.level.storage.LevelStorageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 世界切换协调器（Phase 2）：把玩家从物理服务端的世界"搬"进组世界。
 * <p>
 * 规范流程：客户端先正常加入物理服务端（Phase 1 会话：握手/环境同步/角色申请
 * 都经这条连接完成），随后按指派角色<b>编排性断开</b>并切换 ——
 * <ul>
 *   <li>主客户端 → 打开本地存档（集成服务端接管世界演算）；</li>
 *   <li>副客户端 → 连接 127.0.0.1 隧道口（经 P2P 进入主客户端的集成服务端）。</li>
 * </ul>
 * <b>切换旗标</b>：编排性断开同样会触发 Fabric 的 DISCONNECT 事件，
 * {@code WorldSession.onLeave} 以 {@link #consumeSwitchFlag} 识别并跳过会话拆除
 * （与服务端的控制连接必须活到组世界的整个生命周期）。旗标一次性消费，
 * 玩家之后真正退出组世界时的 DISCONNECT 走正常拆除。
 * <p>
 * <b>隧道地址登记</b>：副客户端经隧道连接时也会触发 JOIN 事件，
 * {@code WorldSession.onJoin} 以 {@link #isTunnelAddress} 识别并跳过新会话建立。
 * <p>
 * 两个 switch 方法必须在客户端主线程调用（内部 assert）；调用方从后台线程
 * 经 {@code minecraft.execute} 投递。
 */
public final class WorldSwitcher {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/switcher");

    /** 切换旗标（一次性）：置位后下一次 DISCONNECT 事件不拆会话。 */
    private static final AtomicBoolean SWITCHING = new AtomicBoolean(false);

    /** 当前隧道监听端口；0 = 无（volatile：JOIN 事件在主线程读，加入器在 io 线程写）。 */
    private static volatile int tunnelPort;

    private WorldSwitcher() {
    }

    // ------------------------------------------------------------ 旗标与登记

    /** 消费切换旗标（WorldSession.onLeave 调用）：返回 true 表示这是编排性断开。 */
    public static boolean consumeSwitchFlag() {
        return SWITCHING.getAndSet(false);
    }

    /** 登记副客户端隧道监听端口（SecondaryJoiner 建好隧道后调用；0 = 清除登记）。 */
    public static void markTunnelPort(int port) {
        tunnelPort = port;
    }

    /** 该服务器地址是否是本机隧道口（WorldSession.onJoin 的跳过判据）。 */
    public static boolean isTunnelAddress(String rawAddress) {
        int port = tunnelPort;
        if (port <= 0 || rawAddress == null) {
            return false;
        }
        String trimmed = rawAddress.trim();
        return trimmed.equals("127.0.0.1:" + port) || trimmed.equals("localhost:" + port);
    }

    // ------------------------------------------------------------ 切换动作

    /**
     * 切换到本地世界（主客户端；客户端主线程）。
     * <p>
     * {@code WorldOpenFlows.checkForBackupAndLoad} 走原版"打开单人存档"的完整
     * 管线（版本备份确认、实验性设置确认、数据包校验、集成服务端启动），
     * 与原版行为最大兼容 —— 存档根用组世界文件夹下的 data/primary（而非游戏
     * 默认 saves 目录），互不污染。
     *
     * @param savesRoot 存档根目录（LevelStorageSource 的根）
     * @param saveName  存档目录名
     * @param onFail    打开失败/玩家取消确认时的回退（渲染线程调用）
     */
    public static void switchToLocalWorld(Minecraft minecraft, Path savesRoot,
                                          String saveName, Runnable onFail) {
        if (!minecraft.isSameThread()) {
            throw new IllegalStateException("switchToLocalWorld 必须在客户端主线程调用");
        }
        LOGGER.info("编排性断开物理服务端，打开本地组世界: {}/{}", savesRoot, saveName);
        SWITCHING.set(true);
        if (minecraft.level != null) {
            minecraft.disconnect();
        }
        // 原版"选择世界界面打开存档"的同款入口（签名已 javap 核实）；
        // 失败回调里除业务回退外把玩家送回标题屏，避免卡在空屏
        new WorldOpenFlows(minecraft, LevelStorageSource.createDefault(savesRoot))
                .checkForBackupAndLoad(saveName, () -> {
                    LOGGER.error("本地组世界打开失败/被取消: {}", saveName);
                    minecraft.setScreen(new TitleScreen());
                    onFail.run();
                });
    }

    /**
     * 切换到隧道世界（副客户端；客户端主线程）：断开物理服务端，
     * 经 127.0.0.1 隧道口连接主客户端的集成服务端。
     *
     * @param port        隧道监听端口（已 {@link #markTunnelPort} 登记）
     * @param primaryName 主客户端玩家名（服务器列表项显示用）
     */
    public static void switchToTunnel(Minecraft minecraft, int port, String primaryName) {
        if (!minecraft.isSameThread()) {
            throw new IllegalStateException("switchToTunnel 必须在客户端主线程调用");
        }
        LOGGER.info("编排性断开物理服务端，经隧道加入主客户端 {} (127.0.0.1:{})", primaryName, port);
        SWITCHING.set(true);
        if (minecraft.level != null) {
            minecraft.disconnect();
        }
        ServerData data = new ServerData("P2P 组世界（主客户端: " + primaryName + "）",
                "127.0.0.1:" + port, ServerData.Type.OTHER);
        ConnectScreen.startConnecting(new JoinMultiplayerScreen(new TitleScreen()), minecraft,
                new ServerAddress("127.0.0.1", port), data, false);
    }
}
