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

    /** 一次 HELLO 的声明：玩家名 + 协议能力位。 */
    private record Declaration(String playerName, boolean commandRouting) {
    }

    /** clientId → HELLO 声明（断连即清）。 */
    private static final Map<UUID, Declaration> DECLARATIONS = new ConcurrentHashMap<>();

    private ConnectionIdentity() {
    }

    /**
     * 记录一次 HELLO 声明（幂等覆盖）。
     *
     * @param commandRouting 是否声明支持"指令全量上链"语义（新增能力位，加性协议）
     */
    static void declare(UUID clientId, String playerName, boolean commandRouting) {
        if (clientId == null) {
            return;
        }
        if (playerName == null || playerName.isBlank()) {
            if (!commandRouting) {
                DECLARATIONS.remove(clientId);
                return;
            }
            playerName = "?";
        }
        DECLARATIONS.put(clientId, new Declaration(playerName, commandRouting));
    }

    /** 该客户端完全离线时清除声明（重连会重新声明）。 */
    static void forget(UUID clientId) {
        if (clientId != null) {
            DECLARATIONS.remove(clientId);
        }
    }

    /** 服务停止时清空（保证 stop → start 幂等）。 */
    static void clearAll() {
        DECLARATIONS.clear();
    }

    /** 该连接 HELLO 时声明的玩家名；未声明返回 null。 */
    public static String nameOf(UUID clientId) {
        Declaration declaration = clientId == null ? null : DECLARATIONS.get(clientId);
        return declaration == null ? null : declaration.playerName();
    }

    /**
     * 该连接是否声明支持"指令全量上链"语义（HELLO 的 {@code commandRouting} 能力位）。
     * <p>
     * 混版本安全网：老客户端只会在本地执行失败后上送，且本地已经执行过 —— 新服务端
     * 若按新语义（全量上链）处理，发起组会被执行两次。因此新服务端只处理声明过该
     * 能力的连接的上送，老客户端的上送静默忽略（它自己的本地行为不变）。反方向由
     * 客户端看 HELLO_ACK 的同名字段决定是否拦截本地执行（缺省即不拦截 = 老行为）。
     */
    public static boolean supportsCommandRouting(UUID clientId) {
        Declaration declaration = clientId == null ? null : DECLARATIONS.get(clientId);
        return declaration != null && declaration.commandRouting();
    }

    /**
     * 上报的玩家身份是否与"这条连接所在组"自洽（保留判据；新代码请用
     * {@link #canonicalMemberId}，它同时给出归一化后的成员 ID）。
     */
    public static boolean isOwnGroupMember(GroupTable groups, UUID clientId,
                                           UUID playerUuid, String playerName) {
        return canonicalMemberId(groups, clientId, playerUuid, playerName) != null;
    }

    /**
     * 把上报的玩家身份归一到<b>组成员 ID</b>（组成员集合里存的就是玩家档案 UUID）。
     * <p>
     * 判据必须<b>配对</b>：一条上报只能是"某个具体成员"的，不允许把 A 的 UUID 与
     * B 的名字拼在一起用（2026-10-16 复查发现：早期的"UUID 命中 **或** 名字命中"
     * 两条独立分支可被拼接绕过 —— 组内任意成员用"外部 op 的 UUID + 自己的名字"就能
     * 让服务端按 op 的真实等级去执行 3/4 级指令，而 3/4 级指令会在物理服务端真实生效）。
     * <p>
     * 两条合法路径：
     * <ol>
     *   <li><b>档案 UUID 直配</b>：集成服保留了玩家档案 UUID 时（房主、在线模式），
     *       实体 UUID 就等于成员的 clientId；</li>
     *   <li><b>离线派生 UUID</b>：经隧道的副客户端在集成服里拿的是原版离线派生值
     *       {@code UUID.nameUUIDFromBytes("OfflinePlayer:" + 名字)}，此时要求
     *       "上报的派生 UUID 等于该成员名字的派生值" —— 名字兜底因此不再是万能钥匙，
     *       必须与派生规律一致才放行。</li>
     * </ol>
     *
     * @param groups     组表（服务端权威成员视图）
     * @param clientId   连接身份（{@link ControlConnection#peerId()}）
     * @param playerUuid 上报的玩家 UUID
     * @param playerName 上报的玩家名
     * @return 归一化后的组成员 ID；不属于本组返回 null
     */
    public static UUID canonicalMemberId(GroupTable groups, UUID clientId,
                                         UUID playerUuid, String playerName) {
        if (groups == null || clientId == null) {
            return null;
        }
        UUID groupId = groups.groupOf(clientId);
        if (groupId == null) {
            return null; // 未入组的连接没有任何可代表的对象
        }
        Set<UUID> members = groups.membersOf(groupId);
        if (playerUuid != null && members.contains(playerUuid)) {
            return playerUuid; // 路径 1：档案 UUID 直配
        }
        if (playerName == null || playerName.isBlank()) {
            return null;
        }
        UUID derived = offlineUuid(playerName);
        for (UUID member : members) {
            Declaration declaration = DECLARATIONS.get(member);
            String declared = declaration == null ? null : declaration.playerName();
            if (declared == null || !declared.equalsIgnoreCase(playerName)) {
                continue;
            }
            // 路径 2：名字与声明一致 **且** 上报 UUID 正是该名字的离线派生值
            //（不能只比名字：那样任何连接都能拿着别人的名字越过身份判据）
            if (playerUuid == null || playerUuid.equals(derived)
                    || playerUuid.equals(member)) {
                return member;
            }
        }
        return null;
    }

    /** 原版离线模式派生 UUID（{@code ServerLoginPacketListenerImpl} 同款算法）。 */
    public static UUID offlineUuid(String playerName) {
        return UUID.nameUUIDFromBytes(("OfflinePlayer:" + playerName).getBytes(
                java.nio.charset.StandardCharsets.UTF_8));
    }
}
