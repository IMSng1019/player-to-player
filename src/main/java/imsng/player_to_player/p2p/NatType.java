package imsng.player_to_player.p2p;

/**
 * NAT 类型（经典 STUN 分类）。
 * <p>
 * 打洞可行性由双方 NAT 类型共同决定：双方均为锥形（FULL_CONE / RESTRICTED /
 * PORT_RESTRICTED）时打洞成功率高；一方 SYMMETRIC 时需要端口预测，
 * 双方 SYMMETRIC 基本只能走中转。
 */
public enum NatType {
    /** 公网直连（无 NAT），或已做端口映射。 */
    OPEN_INTERNET,
    /** 完全锥形：任意外部主机都可通过映射地址回包，打洞最容易。 */
    FULL_CONE,
    /** 受限锥形：仅本机通信过的外部 IP 可回包。 */
    RESTRICTED_CONE,
    /** 端口受限锥形：仅本机通信过的外部 IP:端口 可回包。 */
    PORT_RESTRICTED_CONE,
    /** 对称型：对不同目的地址映射不同公网端口，打洞需端口预测，成功率低。 */
    SYMMETRIC,
    /** UDP 被防火墙完全阻断，只能走中转。 */
    UDP_BLOCKED,
    /** 尚未探测或探测失败。 */
    UNKNOWN;

    /** 粗略判断与对端打洞是否有希望（用于提前决定是否直接走中转）。 */
    public boolean punchLikely(NatType peer) {
        if (this == UDP_BLOCKED || peer == UDP_BLOCKED) {
            return false;
        }
        // 双对称：不做端口预测的前提下视为不可打洞
        return !(this == SYMMETRIC && peer == SYMMETRIC);
    }
}
