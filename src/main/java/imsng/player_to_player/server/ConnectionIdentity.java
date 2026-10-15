package imsng.player_to_player.server;

import imsng.player_to_player.netproto.ControlConnection;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 控制连接的身份声明表（服务端）与"自洽性"判据。
 *
 * <h2>为什么需要它（复查 G-06 的收口）</h2>
 * {@code COMMAND_RELAY} 与 {@code PLAYER_POS_UPDATE} 里的 {@code playerUuid} 是
 * <b>客户端自报</b>的：主客户端集成服务端里 {@code ServerPlayer.getUUID()} 是权威的
 * （它自己世界里的玩家），但服务端只看得到 JSON。若不校验，任何已握手连接都能
 * 冒用 op 的 UUID 换取 permission、或以他人名义刷写玩家表坐标（后者在玩家表落库后
 * 会直接污染 {@code /tp} 落点）。
 * <p>
 * 判据是<b>同一连接自洽</b>：该 UUID（或玩家名）必须属于<b>这条连接所在组</b>的成员。
 * 依据是本项目既有约定 —— 玩家客户端的 {@code clientId == 该玩家的 MC 档案 UUID}
 * （{@code WorldSession.onJoin} 用 {@code client.getUser().getProfileId()} 构造 HELLO），
 * 因此"组成员集合"（{@link GroupTable#membersOf}）本身就是一份玩家 UUID 集合；
 * 主客户端替组内副客户端上报时，副客户端自己也在同一组里，天然通过。
 * <p>
 * <b>不要高估这层保证</b>：它只保证"报到的是本组的人"，挡不住同组内冒用（被改造的
 * 主客户端本来就能代表本组发言，也能冒充本组任一成员）。彻底收口需要控制信道的
 * 玩家级身份认证 —— 那是独立议题（见 DESIGN.md 第 7 节的同类取舍）。
 * <p>
 * 为什么还要保留玩家名判据：集成服务端的玩家 UUID 由该实例的认证模式决定
 * （离线/开发环境会派生成离线 UUID），此时与 HELLO 声明的档案 UUID 可能不一致；
 * 名字在两个世界里都是同一个，用它兜底可避免"严格 UUID 校验反而把正常上报拒掉"。
 * 名字也不匹配才拒。
 * <p>
 * 线程模型：声明表是 {@link ConcurrentHashMap}（写入在 Netty 线程的 HELLO 会话锁内，
 * 读取在 Netty/io 线程），判据只做集合查询，无锁无阻塞。
 */
public final class ConnectionIdentity {

    /** clientId → HELLO 自报的玩家名（断连即清）。 */
    private static final Map<UUID, String> DECLARED_NAMES = new ConcurrentHashMap<>();

    private ConnectionIdentity() {
    }

    /** 记录一次 HELLO 声明的玩家名（幂等覆盖）。 */
    static void declare(UUID clientId, String playerName) {
        if (clientId == null) {
            return;
        }
        if (playerName == null || playerName.isBlank()) {
            DECLARED_NAMES.remove(clientId);
        } else {
            DECLARED_NAMES.put(clientId, playerName);
        }
    }

    /** 该客户端完全离线时清除声明（重连会重新声明）。 */
    static void forget(UUID clientId) {
        if (clientId != null) {
            DECLARED_NAMES.remove(clientId);
        }
    }

    /** 服务停止时清空（保证 stop → start 幂等）。 */
    static void clearAll() {
        DECLARED_NAMES.clear();
    }

    /** 该连接 HELLO 时声明的玩家名；未声明返回 null。 */
    public static String nameOf(UUID clientId) {
        return clientId == null ? null : DECLARED_NAMES.get(clientId);
    }

    /**
     * 上报的玩家身份是否与"这条连接所在组"自洽。
     *
     * @param groups     组表（服务端权威成员视图）
     * @param clientId   连接身份（{@link ControlConnection#peerId()}）
     * @param playerUuid 上报的玩家 UUID（可为 null，仅按名字核对）
     * @param playerName 上报的玩家名（可为 null，仅按 UUID 核对）
     * @return true = 允许本次上报（UUID 命中组成员，或名字命中某成员声明的名字）
     */
    public static boolean isOwnGroupMember(GroupTable groups, UUID clientId,
                                           UUID playerUuid, String playerName) {
        if (groups == null || clientId == null) {
            return false;
        }
        UUID groupId = groups.groupOf(clientId);
        if (groupId == null) {
            return false; // 未入组的连接没有任何可代表的对象
        }
        Set<UUID> members = groups.membersOf(groupId);
        if (playerUuid != null && members.contains(playerUuid)) {
            return true;
        }
        if (playerName == null || playerName.isBlank()) {
            return false;
        }
        for (UUID member : members) {
            String declared = DECLARED_NAMES.get(member);
            if (declared != null && declared.equalsIgnoreCase(playerName)) {
                return true;
            }
        }
        return false;
    }
}
