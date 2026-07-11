package imsng.player_to_player.core;

/**
 * 节点运行模式（配置级）。
 * <p>
 * 对应总配置文件 {@code player-to-player/config.json} 中的 {@code mode} 字段。
 * 首次加载时自动检测：服务端环境 → {@link #SERVER}，客户端环境 → {@link #CLIENT}；
 * {@link #PROXY_SERVER} 只能由管理员手动在配置中切换。
 * <p>
 * 注意与 {@link ClientRole} 区分：NodeMode 是"这台机器是什么"，
 * ClientRole 是"客户端此刻在组内担任什么角色"（可随合并/分离动态切换）。
 */
public enum NodeMode {
    /** 服务端：装载 MC 服务端但挂起世界 tick，负责区块注册表、环境分发、MCA 写回、玩家表。 */
    SERVER("server"),
    /** 中转服务端：辅助 P2P 打洞，打洞失败时充当中转；同样具备环境分发能力。 */
    PROXY_SERVER("proxy_server"),
    /** 客户端：玩家侧，运行期内再细分为主客户端 / 副客户端（见 {@link ClientRole}）。 */
    CLIENT("client");

    /** 配置文件中使用的字符串标识。 */
    private final String id;

    NodeMode(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    /**
     * 从配置字符串解析模式。
     *
     * @param id          配置中的 mode 字符串（大小写不敏感，容忍首尾空白）
     * @param fallback    解析失败时的兜底模式（通常为按环境自动检测的结果）
     */
    public static NodeMode fromId(String id, NodeMode fallback) {
        if (id == null) {
            return fallback;
        }
        String normalized = id.trim().toLowerCase();
        for (NodeMode mode : values()) {
            if (mode.id.equals(normalized)) {
                return mode;
            }
        }
        return fallback;
    }
}
