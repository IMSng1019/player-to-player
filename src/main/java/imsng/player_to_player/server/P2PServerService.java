package imsng.player_to_player.server;

import imsng.player_to_player.compute.ComputeHandlers;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.EnvSyncServerHandlers;
import imsng.player_to_player.env.EnvironmentManifest;
import imsng.player_to_player.env.EnvironmentScanner;
import imsng.player_to_player.netproto.ControlServer;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.proxy.RelayCore;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.ChunkWriteback;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.registry.RegionFileProbe;
import imsng.player_to_player.registry.RegistryHandlers;
import imsng.player_to_player.util.ThreadPools;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 服务端主服务（{@code mode=server}，DESIGN.md 第 8 节）。
 * <p>
 * 规范"服务端"：装载 MC 服务端但不承担世界主线程运算（tick 挂起由 Mixin 门控），
 * 职责收敛为：区块注册表、玩家表、算力表、环境文件分发、打洞协助、日志收集、
 * （可选）兼任中转。本类在 SERVER_STARTED 时由 {@link imsng.player_to_player.core.P2PBootstrap}
 * 调用 {@link #start}，SERVER_STOPPING 时调用 {@link #stop}，完成上述子系统的组装：
 * <ol>
 *   <li>异步扫描环境清单（规范：服务端的哈希值在服务端启动时计算 —— 扫描含全量文件
 *       SHA-256，可能长达数十秒，绝不能阻塞 MC 启动线程，故丢给 IO 池并以
 *       volatile 字段发布，未就绪期间 HELLO_ACK 的 envReady=false）；</li>
 *   <li>区块注册表（本地 JSON 持久化 + 定期落盘）、玩家表、算力表；</li>
 *   <li>控制服务器（独立 TCP 端口，默认 25580）并挂接全部消息处理器；</li>
 *   <li>按配置兼任中转（规范：服务端可以选择自己是否作为中转服务端）。</li>
 * </ol>
 * <p>
 * 静态单例状态：volatile 保证可见性，start/stop 以类锁串行化，两者均幂等。
 */
public final class P2PServerService {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/server");

    // ---------------------------------------------------------- 运行期单例状态

    /** 服务是否已启动（volatile：stop 与 start 可能来自不同线程）。 */
    private static volatile boolean running;

    /** 环境清单：IO 线程扫描完成后发布；null = 扫描中（HELLO_ACK envReady=false）。 */
    private static volatile EnvironmentManifest environmentManifest;

    private static volatile ControlServer controlServer;
    private static volatile ChunkRegistry chunkRegistry;
    private static volatile PlayerTable playerTable;
    private static volatile ComputeTable computeTable;
    private static volatile GroupTable groupTable;
    /** 合并/分离协调器（Phase 3）。 */
    private static volatile MergeCoordinator mergeCoordinator;
    /** 兼任中转时的中转核心；不兼任则为 null。 */
    private static volatile RelayCore relayCore;

    private P2PServerService() {
    }

    // ------------------------------------------------------------------ 启动

    /**
     * 启动服务端主服务（SERVER_STARTED 事件回调；幂等）。
     *
     * @param server 已完成启动的 MC 服务器实例（世界目录/存档名此时已就绪）
     */
    public static synchronized void start(MinecraftServer server) {
        if (running) {
            return;
        }

        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        P2PPaths paths = ctx.paths();

        // a. 服务端根 = 游戏目录（环境文件定义基于它）；世界根 = 存档根目录（region 文件在其下）
        Path serverRoot = FabricLoader.getInstance().getGameDir();
        Path worldRoot = server.getWorldPath(LevelResource.ROOT);

        // b. 环境清单异步扫描：全量 SHA-256 属重 IO，规范要求启动时计算，
        //    但不能卡 MC 启动 —— 扫完通过 volatile 发布，之前 envReady=false。
        //    排除表 = 服务端自定义 + 实际存档目录的维度数据（见 worldDataExclusions）
        List<String> scanExclusions = new ArrayList<>();
        if (config.nonEnvironmentPaths != null) {
            scanExclusions.addAll(config.nonEnvironmentPaths); // GSON 反序列化可能为 null，防御
        }
        scanExclusions.addAll(worldDataExclusions(serverRoot, worldRoot));
        environmentManifest = null;
        ThreadPools.io().execute(() -> {
            try {
                EnvironmentManifest scanned =
                        EnvironmentScanner.scan(serverRoot, scanExclusions);
                environmentManifest = scanned;
                LOGGER.info("环境清单扫描完成: {} 个文件, 全局哈希 {}",
                        scanned.files().size(), scanned.globalHash());
            } catch (Exception e) {
                // 扫描失败不致命：环境同步不可用（envReady 恒 false），其余功能照常
                LOGGER.error("环境清单扫描失败，环境同步将不可用", e);
            }
        });

        // c. 区块注册表：持久化目录 + 世界 region 文件探测器（hasServerData 判定用）
        ChunkRegistry registry = new ChunkRegistry(paths.registryDir(), new RegionFileProbe(worldRoot));
        registry.load();            // 恢复上次运行的占用状态（服务端重启不丢注册表）
        registry.startAutoPersist(); // 定期落盘（scheduler 驱动，崩溃最多丢一个周期）
        PlayerTable players = new PlayerTable();
        ComputeTable computes = new ComputeTable();
        GroupTable groups = new GroupTable();

        // 世界名：HELLO_ACK 下发，客户端据此拼 <IP>+<世界名> 世界文件夹
        String worldName = server.getWorldData().getLevelName();

        // c/d. 控制服务器 + 全部消息处理器
        ControlServer control = new ControlServer(config.controlPort);
        control.on(MessageType.HELLO,
                new HelloHandler(config, computes, () -> environmentManifest, worldName));
        EnvSyncServerHandlers.register(control, serverRoot, () -> environmentManifest, config);
        // 区块数据面（Phase 2）：CHUNK_DATA_REQUEST 下发 / CHUNK_DATA_UPLOAD 实时上行；
        // 返回的写回接口交给 RegistryHandlers 处理 CHUNK_RELEASE 携带的最终数据
        ChunkWriteback writeback = ChunkDataHandlers.register(control, server, registry);
        RegistryHandlers.register(control, registry, players, writeback);
        ComputeHandlers.register(control, computes);
        LogCollector.register(control, paths.logsDir());
        P2PBrokerHandlers.register(control, config);
        // 角色指派（Phase 2）：环境同步完成的客户端按存档位置分主/副
        control.on(MessageType.ROLE_REQUEST,
                new RoleAssignHandler(server, config, registry, groups, computes));
        // 合并/分离协调器（Phase 3）：MERGE_REQUEST/PROGRESS + SPLIT_REQUEST
        MergeCoordinator merges = MergeCoordinator.register(
                control, config, registry, groups, computes, players);
        // 玩家数据面（Phase 3）：分离/合并前的玩家 NBT 上行与下发
        PlayerDataHandlers.register(control, server, groups);

        // e. 断连清理：按 peerId 清玩家表/算力表/组表/在线映射，并释放其组的全部区块占用。
        //    Phase 1/2 约定 groupId == 主客户端 clientId（NodeContext.groupId 注释同款约定），
        //    因此主客户端掉线时 releaseAll(peerId) 恰好释放其组占用的所有区块；
        //    副客户端的 peerId 不是任何 groupId，releaseAll 自然为空操作，无副作用。
        control.onDisconnect(conn -> {
            UUID peerId = conn.peerId();
            if (peerId == null) {
                return; // 未完成 HELLO 的连接没有任何登记，无需清理
            }
            // 竞态防护：客户端快速重连时 HelloHandler 会先登记新连接再关旧连接，
            // 旧连接的断连回调随后才触发 —— 此刻新会话已重新上报了玩家/算力/区块状态，
            // 若无条件按 peerId 全局清理会把新会话刚建立的状态清空（算力表被删、
            // 组占用的区块被 releaseAll 整组释放）。故先判断该 peerId 当前登记的
            // 连接是否仍是本连接：不是（已被新连接顶替）则只注销旧连接自身，跳过三项全局清理。
            boolean stillCurrent = HelloHandler.connectionOf(peerId) == conn;
            HelloHandler.unregister(peerId, conn);
            if (!stillCurrent) {
                LOGGER.info("客户端 {} 的旧连接断开，新连接已接管，跳过全局状态清理", peerId);
                return;
            }
            players.remove(peerId);
            computes.remove(peerId);
            registry.releaseAll(peerId);
            // 合并会话清理（Phase 3）：当事方掉线 → 中止其卷入的合并（A 继续运行）
            merges.abortInvolving(peerId);
            // 组表清理（Phase 2）：主客户端离线 → 整组解散；副客户端离线 → 摘成员
            groups.removeClient(peerId);
            LOGGER.info("客户端断开，已清理其状态: {}", peerId);
        });
        control.start();

        // f. 兼任中转：未指定外部中转地址且配置允许时，在本进程再起一个中转端口
        //   （规范：服务端可以选择自己是否作为中转服务端；复用 proxy 包的 RelayCore）
        RelayCore relay = null;
        String relayAddr = config.relayServerAddress == null ? "" : config.relayServerAddress.trim();
        if (config.serverActsAsRelay && relayAddr.isEmpty()) {
            relay = new RelayCore(config.relayPort);
            relay.start();
            LOGGER.info("服务端兼任中转 (relayPort={})", config.relayPort);
        }

        // 全部组件就绪后统一发布（volatile 写），stop() 据此收尾
        chunkRegistry = registry;
        playerTable = players;
        computeTable = computes;
        groupTable = groups;
        mergeCoordinator = merges;
        controlServer = control;
        relayCore = relay;
        running = true;

        LOGGER.info("P2P 服务端主服务已启动: controlPort={}, 世界={}", config.controlPort, worldName);
    }

    // ---------------------------------------------------------------- 工具

    /**
     * 根据实际存档根目录动态生成维度数据目录的排除项（region/poi/entities/DIM-1/DIM1）。
     * <p>
     * 背景：内置排除表只硬编码了 "world/" 前缀，但 server.properties 的 level-name
     * 可以不是 "world"（如 myworld），此时数 GB 的区块数据会被算入环境清单分发给
     * 所有客户端。这里用 {@code server.getWorldPath(LevelResource.ROOT)} 拿到的
     * 真实存档目录相对服务端根拼出排除项（EnvironmentScanner 保留 "world/" 兜底）。
     * 返回的相对路径统一规范化为正斜杠（Windows 下 relativize 产生反斜杠）。
     *
     * @param serverRoot 服务端根目录（环境清单的扫描根）
     * @param worldRoot  实际存档根目录（LevelResource.ROOT，形如 <root>/<level-name>/）
     * @return 追加到扫描排除表的相对路径列表；存档不在服务端根下（异常布局）时为空
     */
    private static List<String> worldDataExclusions(Path serverRoot, Path worldRoot) {
        List<String> exclusions = new ArrayList<>();
        Path normalizedServerRoot = serverRoot.toAbsolutePath().normalize();
        Path normalizedWorldRoot = worldRoot.toAbsolutePath().normalize();
        if (!normalizedWorldRoot.startsWith(normalizedServerRoot)) {
            // 存档目录不在服务端根下（自定义 universe 等布局）：本就不会被扫进清单，无需排除
            return exclusions;
        }
        // Windows 下 relativize 产生反斜杠，统一规范化为正斜杠（排除表约定正斜杠比较）
        String worldPrefix = normalizedServerRoot.relativize(normalizedWorldRoot)
                .toString().replace('\\', '/');
        if (worldPrefix.isEmpty()) {
            return exclusions; // 存档根 == 服务端根（理论不可能），防御性跳过
        }
        for (String dim : List.of("region", "poi", "entities", "DIM-1", "DIM1")) {
            exclusions.add(worldPrefix + "/" + dim);
        }
        return exclusions;
    }

    // ------------------------------------------------------------------ 停止

    /** 停止服务端主服务（SERVER_STOPPING 事件回调；幂等，各组件亦自身幂等）。 */
    public static synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;

        // 顺序：先停网络入口（不再有新请求），再持久化注册表，最后停中转
        ControlServer control = controlServer;
        controlServer = null;
        if (control != null) {
            control.stop();
        }

        ChunkRegistry registry = chunkRegistry;
        chunkRegistry = null;
        if (registry != null) {
            registry.shutdown(); // 停自动落盘并做最终 persist
        }

        RelayCore relay = relayCore;
        relayCore = null;
        if (relay != null) {
            relay.stop();
        }

        playerTable = null;
        computeTable = null;
        MergeCoordinator merges = mergeCoordinator;
        mergeCoordinator = null;
        if (merges != null) {
            merges.shutdown();
        }
        GroupTable groups = groupTable;
        groupTable = null;
        if (groups != null) {
            groups.clear();
        }
        environmentManifest = null;
        HelloHandler.clearAll(); // 清静态在线/NAT 映射，保证下次 start 干净

        LOGGER.info("P2P 服务端主服务已停止");
    }
}
