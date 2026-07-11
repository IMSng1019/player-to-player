package imsng.player_to_player.core;

import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.p2p.NatInfo;

import java.util.UUID;

/**
 * 节点运行期上下文（全局单例）。
 * <p>
 * 保存本节点在整个 P2P 体系中的当前状态：模式、客户端角色、身份标识、
 * 算力评分、NAT 探测结果、配置与目录等。所有子系统通过它读取共享状态。
 * <p>
 * 线程安全说明：字段全部为 volatile（引用替换原子可见），复合状态变更
 * （如主副切换）由上层状态机（GroupManager，Phase 2+）串行化，本类不加锁。
 */
public final class NodeContext {

    private static final NodeContext INSTANCE = new NodeContext();

    /** 节点模式（server / proxy_server / client），初始化后不再变化。 */
    private volatile NodeMode mode = NodeMode.CLIENT;
    /** 客户端运行期角色，随合并/分离动态切换；非 CLIENT 模式下恒为 UNASSIGNED。 */
    private volatile ClientRole clientRole = ClientRole.UNASSIGNED;
    /** 本客户端的唯一标识（使用玩家 UUID；服务端/中转端为随机 UUID）。 */
    private volatile UUID clientId;
    /** 本客户端所属组客户端的标识（约定为当前主客户端的 clientId）。 */
    private volatile UUID groupId;
    /** 本客户端玩家名（加入世界时由会话层写入；日志上传等落盘命名用）。 */
    private volatile String playerName;
    /** 本机算力评分（模组加载时异步测得，加入世界时上报服务端）。 */
    private volatile ComputeScore computeScore;
    /** NAT 探测结果（模组加载时异步测得）。 */
    private volatile NatInfo natInfo;
    /** 总配置（player-to-player/config.json）。 */
    private volatile GlobalConfig config;
    /** 目录结构管理器。 */
    private volatile P2PPaths paths;

    private NodeContext() {
    }

    public static NodeContext get() {
        return INSTANCE;
    }

    public NodeMode mode() {
        return mode;
    }

    public void setMode(NodeMode mode) {
        this.mode = mode;
    }

    public ClientRole clientRole() {
        return clientRole;
    }

    public void setClientRole(ClientRole clientRole) {
        this.clientRole = clientRole;
    }

    public UUID clientId() {
        return clientId;
    }

    public void setClientId(UUID clientId) {
        this.clientId = clientId;
    }

    public UUID groupId() {
        return groupId;
    }

    public void setGroupId(UUID groupId) {
        this.groupId = groupId;
    }

    public String playerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public ComputeScore computeScore() {
        return computeScore;
    }

    public void setComputeScore(ComputeScore computeScore) {
        this.computeScore = computeScore;
    }

    public NatInfo natInfo() {
        return natInfo;
    }

    public void setNatInfo(NatInfo natInfo) {
        this.natInfo = natInfo;
    }

    public GlobalConfig config() {
        return config;
    }

    public void setConfig(GlobalConfig config) {
        this.config = config;
    }

    public P2PPaths paths() {
        return paths;
    }

    public void setPaths(P2PPaths paths) {
        this.paths = paths;
    }

    /** 便捷判断：本节点是否为服务端模式。 */
    public boolean isServer() {
        return mode == NodeMode.SERVER;
    }

    /** 便捷判断：本节点是否为中转服务端模式。 */
    public boolean isProxyServer() {
        return mode == NodeMode.PROXY_SERVER;
    }

    /** 便捷判断：本节点是否为客户端模式。 */
    public boolean isClient() {
        return mode == NodeMode.CLIENT;
    }
}
