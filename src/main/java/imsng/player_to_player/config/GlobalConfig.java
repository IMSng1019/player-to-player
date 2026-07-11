package imsng.player_to_player.config;

import imsng.player_to_player.core.NodeMode;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 总配置文件（{@code player-to-player/config.json}）。
 * <p>
 * 规范要求：总配置文件中必须有一个选项表明模组运行模式（server / proxy_server / client）；
 * 初次加载时按运行环境自动生成默认值（服务器环境 → server，客户端环境 → client）。
 * <p>
 * 所有字段直接暴露为 public 供 GSON 序列化；读取方通过 {@link imsng.player_to_player.core.NodeContext}
 * 拿到本类实例，修改后调用 {@link #save(Path)} 落盘。
 */
public final class GlobalConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/config");

    // ------------------------------------------------------------------ 模式

    /** 运行模式：server（服务端）/ proxy_server（中转服务端）/ client（客户端）。 */
    public String mode = NodeMode.CLIENT.id();

    // ------------------------------------------------------------ 网络与中转

    /**
     * 中转服务端地址（"ip:端口" 或 "ip"，端口缺省用 {@link #relayPort}）。
     * 服务端模式下：指定后打洞协助与中转都由该地址承担；
     * 为空且 {@link #serverActsAsRelay} 为 true 时服务端自己兼任中转。
     */
    public String relayServerAddress = "";

    /**
     * 上级服务端地址（"ip:端口" 或 "ip"；proxy_server 模式使用）。
     * 规范：中转服务端需要设定自己的服务端 —— 中转端隶属于某个服务端，
     * 据此从服务端同步环境文件、上报中转状态。Phase 1 仅做配置占位，
     * 实际连接与环境同步在 Phase 2 落地（见 ProxyServerService）。
     */
    public String parentServerAddress = "";

    /** 服务端是否自己兼任中转服务端（规范：服务端可以选择自己是否作为中转）。 */
    public boolean serverActsAsRelay = true;

    /** 控制协议监听端口（服务端），独立于 MC 端口。 */
    public int controlPort = 25580;

    /** 中转服务监听端口（中转服务端 / 兼任中转的服务端）。 */
    public int relayPort = 25581;

    /** P2P UDP 本地端口，0 = 随机分配。 */
    public int p2pUdpPort = 0;

    // ------------------------------------------------------------ 算力与内存

    /**
     * 成为主客户端所需的最小剩余内存（字节）。
     * 规范：服务端可以设定剩余的内存大小，默认为 0.5 GB。
     */
    public long minFreeMemoryBytes = 512L * 1024 * 1024;

    /** 是否启用 Geekbench 非官方 API 查询单核算力（失败自动降级为本地跑分）。 */
    public boolean geekbenchLookupEnabled = true;

    // ------------------------------------------------------------ 环境文件

    /**
     * 服务端指定的"非环境文件"路径（相对服务端根目录，正斜杠分隔）。
     * 规范：服务端可以指定哪些文件夹或者文件为非环境文件。
     * 内置排除项（logs/、world/region 等）见 EnvironmentScanner，不需要写在这里。
     */
    public List<String> nonEnvironmentPaths = new ArrayList<>();

    /**
     * 环境文件传输分块大小（字节）。加载时被 clamp 到
     * [{@link #MIN_ENV_FILE_CHUNK_BYTES}, {@link #MAX_ENV_FILE_CHUNK_BYTES}]：
     * 超过协议最大帧长会让对端解帧器抛 TooLongFrameException 断连，且原因难排查。
     */
    public int envFileChunkBytes = 1024 * 1024;

    /** 分块大小下限（64 KB）：再小传输轮次过多，纯浪费往返。 */
    private static final int MIN_ENV_FILE_CHUNK_BYTES = 64 * 1024;

    /**
     * 分块大小上限：协议最大帧长减 64 KB 余量（帧头 + JSON 段 + 路径等元数据），
     * 保证 ENV_FILE_DATA 整帧不会超过对端 LengthFieldBasedFrameDecoder 的上限。
     */
    private static final int MAX_ENV_FILE_CHUNK_BYTES = Protocol.MAX_FRAME_BYTES - 64 * 1024;

    // ------------------------------------------------------------ 服务端行为

    /** 服务端模式下是否挂起世界 tick（核心特性；关闭则回退为原版行为，便于排障）。 */
    public boolean suspendWorldTick = true;

    /** 是否禁用出生点常加载区块（规范：装载了该模组的服务器不加载出生点常加载区块）。 */
    public boolean disableSpawnChunks = true;

    // ------------------------------------------------------------ 注册表后端

    /** MySQL 区块注册表后端（可选；默认用本地 JSON 持久化）。 */
    public MysqlConfig mysql = new MysqlConfig();

    public static final class MysqlConfig {
        public boolean enabled = false;
        public String jdbcUrl = "";
        public String username = "";
        public String password = "";
    }

    // ------------------------------------------------------------------ IO

    /**
     * 读取总配置；文件不存在（或损坏）时按默认值创建并落盘。
     *
     * @param path        配置文件路径
     * @param defaultMode 首次生成时的默认模式（按运行环境自动检测的结果）
     */
    public static GlobalConfig loadOrCreate(Path path, NodeMode defaultMode) {
        try {
            GlobalConfig loaded = JsonUtil.readFile(path, GlobalConfig.class);
            if (loaded != null) {
                loaded.sanitize();
                // 回写一次：老版本配置升级后补齐新增字段
                loaded.save(path);
                return loaded;
            }
        } catch (Exception e) {
            LOGGER.warn("总配置读取失败，使用默认值重建: {}", path, e);
        }
        GlobalConfig config = new GlobalConfig();
        config.mode = defaultMode.id();
        config.save(path);
        LOGGER.info("已生成默认总配置: {} (mode={})", path, config.mode);
        return config;
    }

    /**
     * 加载后校正非法配置值。目前只处理 envFileChunkBytes：
     * 配置过大时 ENV_FILE_DATA 帧会超过对端解帧上限，对端直接
     * TooLongFrameException 断连，环境同步反复失败且日志里看不出根因 ——
     * 在加载入口就 clamp 并告警，把问题暴露在配置层而不是网络层。
     */
    private void sanitize() {
        if (envFileChunkBytes < MIN_ENV_FILE_CHUNK_BYTES
                || envFileChunkBytes > MAX_ENV_FILE_CHUNK_BYTES) {
            int clamped = Math.max(MIN_ENV_FILE_CHUNK_BYTES,
                    Math.min(envFileChunkBytes, MAX_ENV_FILE_CHUNK_BYTES));
            LOGGER.warn("envFileChunkBytes={} 超出合法区间 [{}, {}]，已校正为 {}",
                    envFileChunkBytes, MIN_ENV_FILE_CHUNK_BYTES, MAX_ENV_FILE_CHUNK_BYTES, clamped);
            envFileChunkBytes = clamped;
        }
    }

    /** 落盘（原子写，避免半写损坏）。 */
    public void save(Path path) {
        try {
            JsonUtil.writeFileAtomic(path, this);
        } catch (IOException e) {
            LOGGER.error("总配置保存失败: {}", path, e);
        }
    }

    /** 解析后的节点模式（配置串非法时兜底为 client）。 */
    public NodeMode nodeMode() {
        return NodeMode.fromId(mode, NodeMode.CLIENT);
    }
}
