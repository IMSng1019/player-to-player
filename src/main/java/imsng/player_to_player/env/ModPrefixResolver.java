package imsng.player_to_player.env;

import java.util.EnumSet;
import java.util.Set;

/**
 * mod 文件名前缀解析器（服务端分发用）。
 * <p>
 * 规范出处（player_to_player-prompt.txt "文件结构"段）：
 * <pre>
 *   server-        服务端运行读取
 *   proxy_server-  中转服务端运行读取
 *   server_client- 主客户端运行读取
 *   client-        副客户端运行读取
 * </pre>
 * 前缀可无序叠加（示例 {@code server-server_client-client-areas-hint-4.4.5.jar}
 * → 服务端、主客户端、副客户端读取）；无任何前缀 = 所有端都装。
 * <p>
 * <b>调用方注意（规范："主客户端和副客户端随时可以切换 所以下载环境文件时需要同时下载"）</b>：
 * 客户端不能只按当前角色下载 —— 必须分别以 {@link Target#PRIMARY_CLIENT} 与
 * {@link Target#SECONDARY_CLIENT} 各调用一次同步（写入 primary/environment 与
 * secondary/environment 两套目录），角色切换时才能立即用另一套环境启动。
 */
public final class ModPrefixResolver {

    /** 分发目标端（与四种节点角色一一对应）。 */
    public enum Target {
        /** 服务端（server- 前缀）。 */
        SERVER,
        /** 中转服务端（proxy_server- 前缀）。 */
        PROXY_SERVER,
        /** 主客户端（server_client- 前缀）。 */
        PRIMARY_CLIENT,
        /** 副客户端（client- 前缀）。 */
        SECONDARY_CLIENT
    }

    /**
     * 前缀 → 目标端的固定剥离顺序。
     * <b>按最长优先排列</b>（server_client- → proxy_server- → server- → client-）：
     * 短前缀（server-）放在含相同头部的长前缀之后尝试，杜绝任何"短前缀误吞
     * 长前缀头部"的可能，且对未来新增前缀（如 server_xxx-）天然安全。
     */
    private static final String[] PREFIXES = {
            "server_client-", "proxy_server-", "server-", "client-"};
    private static final Target[] PREFIX_TARGETS = {
            Target.PRIMARY_CLIENT, Target.PROXY_SERVER, Target.SERVER, Target.SECONDARY_CLIENT};

    private ModPrefixResolver() {
    }

    /**
     * 解析文件名适用的目标端集合。
     * <p>
     * 从文件名头部循环剥离前缀（按 {@link #PREFIXES} 的最长优先顺序逐个尝试），
     * 直到再剥不出任何前缀为止；剥出的前缀集合即适用端。
     * 一个前缀重复出现（如 "server-server-x.jar"）等价出现一次（EnumSet 去重）。
     *
     * @param fileName 纯文件名（不含路径），null/空返回"全部端"
     * @return 适用端集合；无任何前缀时返回包含全部四端的集合（规范：默认所有端都装）
     */
    public static Set<Target> targetsOf(String fileName) {
        EnumSet<Target> targets = EnumSet.noneOf(Target.class);
        String rest = fileName != null ? fileName : "";
        boolean stripped = true;
        while (stripped) {
            stripped = false;
            for (int i = 0; i < PREFIXES.length; i++) {
                if (rest.startsWith(PREFIXES[i])) {
                    targets.add(PREFIX_TARGETS[i]);
                    rest = rest.substring(PREFIXES[i].length());
                    stripped = true;
                    break; // 剥掉一个后从头再试（前缀无序叠加，任意排列都能剥净）
                }
            }
        }
        if (targets.isEmpty()) {
            // 无前缀 = 所有端（规范："如果没有前缀则默认所有端都需要装"）
            return EnumSet.allOf(Target.class);
        }
        return targets;
    }

    /** 便捷判断：该文件是否需要分发给指定目标端。 */
    public static boolean appliesTo(String fileName, Target target) {
        return targetsOf(fileName).contains(target);
    }
}
