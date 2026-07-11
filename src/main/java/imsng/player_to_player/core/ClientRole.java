package imsng.player_to_player.core;

/**
 * 客户端在组客户端（一个主客户端 + 若干副客户端）内的运行期角色。
 * <p>
 * 角色不是静态的：合并（算力更强者当选新主）、分离（副客户端独立成主）、
 * 主客户端退出（组内重新算力分配）都会导致主副切换。
 * 因此环境文件需要主/副两套同时下载（见世界文件夹的 primary/ 与 secondary/ 子目录）。
 */
public enum ClientRole {
    /** 未定级：尚未加入世界，或正在等待服务端的角色指派。 */
    UNASSIGNED("unassigned"),
    /**
     * 主客户端：为本组已加载区块运行世界 tick（主线程），
     * 相当于联机模式中的房主 —— 同时运行"原本服务端"与"原本客户端"。
     */
    PRIMARY("primary"),
    /**
     * 副客户端：只负责渲染与输入，不承担主线程 tick；
     * Phase 3 起在后台保持影子实例热备，随时可接管为主客户端。
     */
    SECONDARY("secondary");

    private final String id;

    ClientRole(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static ClientRole fromId(String id, ClientRole fallback) {
        if (id == null) {
            return fallback;
        }
        String normalized = id.trim().toLowerCase();
        for (ClientRole role : values()) {
            if (role.id.equals(normalized)) {
                return role;
            }
        }
        return fallback;
    }
}
