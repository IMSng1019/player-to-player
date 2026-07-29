package imsng.player_to_player.server;

import imsng.player_to_player.compute.ComputeHandlers;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.EnvSyncServerHandlers;
import imsng.player_to_player.env.EnvironmentSnapshotManager;
import imsng.player_to_player.netproto.ControlServer;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.proxy.RelayCore;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.JsonRegistryStore;
import imsng.player_to_player.registry.MysqlRegistryStore;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.registry.RegionFileProbe;
import imsng.player_to_player.registry.RegistryHandlers;
import imsng.player_to_player.registry.RegistryMutationJournal;
import imsng.player_to_player.registry.RegistryStore;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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

    /** 实时环境快照管理器；首轮不可变快照发布前 HELLO 返回 envReady=false。 */
    private static volatile EnvironmentSnapshotManager environmentSnapshots;

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

        // b. 版本化不可变环境快照：排除表 = 管理员配置 + 实际存档的维度数据。
        //    管理器后台构建首版，随后 WatchService 变化监听 + 60 秒校验实时更新；
        //    清单与文件内容均绑定同一 snapshotId，运行中的 level.dat 不再造成失配。
        List<String> scanExclusions = new ArrayList<>();
        if (config.nonEnvironmentPaths != null) {
            scanExclusions.addAll(config.nonEnvironmentPaths); // GSON 反序列化可能为 null，防御
        }
        scanExclusions.addAll(worldDataExclusions(serverRoot, worldRoot));
        EnvironmentSnapshotManager snapshots = new EnvironmentSnapshotManager(
                serverRoot, paths.environmentSnapshotsDir(), scanExclusions,
                config.envSnapshotRetentionMinutes);
        snapshots.start();

        // c. 区块注册表：后端选择（Phase 4：MySQL 可选，连接失败回退 JSON —— 注册表
        //    可用性优先于后端偏好）+ 世界 region 文件探测器（hasServerData 判定用）
        RegistryStore store = null;
        if (config.mysql != null && config.mysql.enabled) {
            try {
                store = MysqlRegistryStore.connect(config.mysql);
                LOGGER.info("区块注册表使用 MySQL 后端: {}", store.describe());
            } catch (Exception e) {
                LOGGER.error("MySQL 注册表后端连接失败，回退 JSON 文件后端", e);
            }
        }
        if (store == null) {
            store = new JsonRegistryStore(paths.registryDir());
        }
        ChunkRegistry registry = new ChunkRegistry(
                store, new RegionFileProbe(worldRoot),
                new RegistryMutationJournal(paths.registryMutationJournal()));
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
                new HelloHandler(config, computes, snapshots, worldName));
        EnvSyncServerHandlers.register(control, snapshots, config);
        // 区块数据面（Phase 2）：CHUNK_DATA_REQUEST 下发 / CHUNK_DATA_UPLOAD 实时上行；
        // 返回的写回接口交给 RegistryHandlers 处理 CHUNK_RELEASE 携带的最终数据
        ChunkDataHandlers chunkData = ChunkDataHandlers.register(control, server, registry);
        ComputeHandlers.register(control, computes);
        LogCollector.register(control, paths.logsDir());
        P2PBrokerHandlers.register(control, config);
        // 合并/分离协调器（Phase 3）：MERGE_REQUEST/PROGRESS + SPLIT_REQUEST
        MergeCoordinator merges = MergeCoordinator.register(
                control, config, registry, groups, computes, players);
        // 主客户端耐久退出：pending 元数据先落盘保护组占用，最终数据齐全并刷盘后
        // committed；客户端断线/服务端重启均可用同一 shutdownId 幂等恢复。
        ExitTransactionLedger exitLedger = new ExitTransactionLedger(
                new ExitTransactionStore(paths.exitTransactionsDir()));
        ExitPayloadStore exitPayloads = new ExitPayloadStore(
                paths.exitPayloadsDir(),
                (playerId, gzipNbt) -> PlayerDataFileStore.write(
                        server.getWorldPath(LevelResource.PLAYER_DATA_DIR),
                        playerId, gzipNbt),
                new ExitPayloadStore.ChunkAccess() {
                    @Override
                    public boolean validate(UUID groupId,
                                            imsng.player_to_player.registry.ChunkKey key,
                                            byte[] gzipNbt, boolean preserveExisting) {
                        return chunkData.validateExit(
                                groupId, key, gzipNbt, preserveExisting);
                    }

                    @Override
                    public boolean write(UUID groupId,
                                         imsng.player_to_player.registry.ChunkKey key,
                                         byte[] gzipNbt, boolean preserveExisting) {
                        return chunkData.writeExit(
                                groupId, key, gzipNbt, preserveExisting);
                    }

                    @Override
                    public void flush() {
                        chunkData.flushAll();
                    }
                });
        ExitTransactionHandlers exits = new ExitTransactionHandlers(
                exitLedger,
                (peerId, requested) -> {
                    if (peerId == null || !peerId.equals(requested.clientId())
                            || !peerId.equals(requested.groupId())) {
                        return null;
                    }
                    UUID currentPrimary = groups.primaryOf(requested.groupId());
                    boolean recoverOrphan = currentPrimary == null
                            && !groups.exists(requested.groupId());
                    if (!recoverOrphan && !peerId.equals(currentPrimary)) {
                        return null;
                    }
                    Set<UUID> currentMembers;
                    if (recoverOrphan) {
                        // 物理服重启后组表不可恢复。只允许主客户端自己的玩家文件，
                        // 防恶意清单借恢复路径覆盖任意其他玩家；副客户端沿用实时确认态。
                        currentMembers = Set.of(peerId);
                    } else {
                        currentMembers = groups.membersOf(requested.groupId());
                        if (currentMembers.isEmpty()
                                || !currentMembers.containsAll(requested.playerIds())) {
                            return null;
                        }
                    }
                    // 退出冻结与合并/分离不能并行；先中止涉及该组的预同步会话。
                    merges.abortInvolving(requested.groupId());
                    Set<imsng.player_to_player.registry.ChunkKey> currentClaims =
                            new HashSet<>(registry.freezeOrRecoverForExit(
                                    requested.groupId(), requested.chunks(), recoverOrphan));
                    return new imsng.player_to_player.group.ExitManifest(
                            requested.shutdownId(), requested.clientId(),
                            requested.groupId(), currentMembers, currentClaims);
                },
                new ExitTransactionHandlers.PendingProtection() {
                    @Override
                    public void protect(imsng.player_to_player.group.ExitManifest manifest) {
                        registry.restoreExitProtection(
                                manifest.groupId(), manifest.chunks());
                    }

                    @Override
                    public void cancelUnpersisted(
                            imsng.player_to_player.group.ExitManifest manifest) {
                        registry.cancelExitProtection(manifest.groupId());
                    }
                },
                exitPayloads,
                chunkData::flushAll,
                manifest -> {
                    registry.completeExitDurably(
                            manifest.groupId(), manifest.chunks());
                    groups.removeClient(manifest.clientId());
                },
                imsng.player_to_player.util.ThreadPools.io(),
                HelloHandler::withCurrentSession);
        exits.register(control);
        chunkData.setExitTransactions(exits);
        RegistryHandlers.register(
                control, registry, players, chunkData.releaseWriteback(), exits);
        // 玩家实时上行在 EXIT_BEGIN 后必须被拒绝，防旧 IO 任务覆盖最终玩家文件。
        PlayerDataHandlers.register(control, server, groups, exits);
        ConcurrentHashMap<UUID, CompletableFuture<Void>> disconnectCleanups =
                new ConcurrentHashMap<>();
        // 角色指派（Phase 2）：正常重进前先结束强杀遗留的 pending，保留实时数据。
        control.on(MessageType.ROLE_REQUEST,
                new RoleAssignHandler(server, config, registry, groups, computes,
                        clientId -> {
                            CompletableFuture<Void> cleanup =
                                    disconnectCleanups.get(clientId);
                            if (cleanup != null) {
                                cleanup.join();
                            }
                            exits.awaitPendingBegins(clientId);
                        },
                        exits::abandonPendingForRejoinNow));
        // 启动网络入口前恢复 pending 冻结，并补做 committed 的暂存应用/清理。
        // committed 是必须完成的耐久意图，磁盘暂时不可用时不能跳过后开放新会话。
        int recoveryAttempt = 0;
        while (true) {
            recoveryAttempt++;
            try {
                exits.recoverDurableState();
                break;
            } catch (Exception e) {
                LOGGER.error("恢复退出事务第 {} 次失败，1 秒后继续重试",
                        recoveryAttempt, e);
                sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }
        // 指令/聊天逐级路由（Phase 4）：COMMAND_RELAY 特例处理 + 泛化下放，CHAT_RELAY 全网分发
        CommandChatHandlers.register(control, server, groups, players);
        // 末影珍珠交接路由（Phase 4）：PEARL_HANDOFF 转发目标组 / PEARL_LANDED 回程抛出者组
        PearlHandlers.register(control, groups);

        // e. 断连清理：按 peerId 清玩家表/算力表/组表/在线映射，并释放其组的全部区块占用。
        //    Phase 1/2 约定 groupId == 主客户端 clientId（NodeContext.groupId 注释同款约定），
        //    因此主客户端掉线时 releaseAll(peerId) 恰好释放其组占用的所有区块；
        //    副客户端的 peerId 不是任何 groupId，releaseAll 自然为空操作，无副作用。
        control.onDisconnect(conn -> {
            UUID peerId = conn.peerId();
            if (peerId == null) {
                return; // 未完成 HELLO 的连接没有任何登记，无需清理
            }
            if (!running) {
                // 服务端整体停机时保留注册表快照，不能为每个被动断开的连接启动
                // 无限释放重试；下次启动由注册表日志与退出事务统一恢复。
                HelloHandler.unregister(peerId, conn);
                return;
            }
            // 先在会话锁外等待已经排队的 EXIT_BEGIN。BEGIN 任务需要同一会话锁，
            // 若在锁内等待会形成死锁；等待结束后再进锁重读 ledger，后来才排入的
            // BEGIN 要么先完成耐久保护，要么在旧会话摘除后因身份失效而被拒绝。
            exits.awaitPendingBegins(peerId);
            CompletableFuture<Void> cleanup = new CompletableFuture<>();
            java.util.concurrent.atomic.AtomicBoolean protectedExit =
                    new java.util.concurrent.atomic.AtomicBoolean();
            // 旧会话摘除、易被新 HELLO 覆盖的内存状态清理、以及断连清理 future
            // 发布必须处于同一临界区。这样替换连接的 ROLE_REQUEST 必然能观察并等待
            // 旧组完成 flush + 耐久释放，不会在发布空窗中创建随后被旧清理删除的新组。
            if (!HelloHandler.unregisterIfCurrent(peerId, conn, () -> {
                boolean protectedNow = exits.isProtectedNow(peerId);
                protectedExit.set(protectedNow);
                if (!protectedNow) {
                    // 区块冻结必须先于替换 HELLO 可见，否则新连接可在 cleanup
                    // future 已发布但冻结尚未执行的空窗中抢到随后会被旧清理释放的 claim。
                    registry.freezeForExit(peerId);
                    disconnectCleanups.put(peerId, cleanup);
                }
                players.remove(peerId);
                computes.remove(peerId);
            })) {
                LOGGER.info("客户端 {} 的旧连接断开，新连接已接管，跳过全局状态清理", peerId);
                return;
            }
            if (protectedExit.get()) {
                // EXIT_BEGIN 已耐久确认：断线只是退出传输中断，不得释放唯一写入所有权。
                merges.abortInvolving(peerId);
                LOGGER.info("主客户端 {} 处于耐久退出事务中，断线后保留组与区块占用",
                        peerId);
                return;
            }
            // 合并会话清理（Phase 3）：当事方掉线 → 中止其卷入的合并（A 继续运行）
            merges.abortInvolving(peerId);
            imsng.player_to_player.util.ThreadPools.io().execute(() -> {
                int attempt = 0;
                while (true) {
                    if (!running) {
                        cleanup.complete(null);
                        return;
                    }
                    attempt++;
                    try {
                        chunkData.flushAll();
                        int released = registry.releaseAllDurably(peerId);
                        groups.removeClient(peerId);
                        LOGGER.info("客户端断开清理完成: {}，耐久释放 {} 个区块",
                                peerId, released);
                        cleanup.complete(null);
                        return;
                    } catch (Exception e) {
                        LOGGER.warn("客户端 {} 断连清理第 {} 次失败，保持冻结并重试: {}",
                                peerId, attempt, e.toString());
                        sleepUninterruptibly(1, TimeUnit.SECONDS);
                    }
                }
            });
            cleanup.whenComplete((ignored, failure) ->
                    disconnectCleanups.remove(peerId, cleanup));
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
        environmentSnapshots = snapshots;
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

    private static void sleepUninterruptibly(long duration, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(duration);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(remaining);
            } catch (InterruptedException ignored) {
                // 异常断连清理只有完成耐久释放后才能结束。
            }
        }
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

        EnvironmentSnapshotManager snapshots = environmentSnapshots;
        environmentSnapshots = null;
        if (snapshots != null) {
            snapshots.stop();
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
        HelloHandler.clearAll(); // 清静态在线/NAT 映射，保证下次 start 干净

        LOGGER.info("P2P 服务端主服务已停止");
    }
}
