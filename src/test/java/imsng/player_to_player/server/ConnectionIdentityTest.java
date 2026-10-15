package imsng.player_to_player.server;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 身份自洽判据（复查 G-06 的收口）的边界测试。
 *
 * <p>要防的具体事故：任何已握手连接冒用 op 的 UUID 换取 permission、
 * 或以他人名义刷写玩家表坐标（玩家表落库后坐标会成为跨组 /tp 的落点）。
 * 判据本身只保证"报到的是本组的人"（同组内冒用挡不住），这里把边界钉死。</p>
 */
class ConnectionIdentityTest {

    private static final UUID HOST = UUID.randomUUID();
    private static final UUID SECONDARY = UUID.randomUUID();
    private static final UUID OUTSIDER = UUID.randomUUID();

    /** 建一个"主客户端 HOST + 副客户端 SECONDARY"的组表。 */
    private static GroupTable groupTable() {
        GroupTable groups = new GroupTable();
        groups.createGroup(HOST);
        groups.addSecondary(HOST, SECONDARY);
        return groups;
    }

    @Test
    @DisplayName("本组成员的 UUID 通过（主客户端替副客户端上报）")
    void acceptsOwnGroupMembers() {
        GroupTable groups = groupTable();
        ConnectionIdentity.declare(HOST, "Host", true);
        ConnectionIdentity.declare(SECONDARY, "Second", true);

        assertTrue(ConnectionIdentity.isOwnGroupMember(groups, HOST, HOST, "Host"));
        // 主客户端的连接代表本组上报副客户端 —— 链路要求必须通过
        assertTrue(ConnectionIdentity.isOwnGroupMember(groups, HOST, SECONDARY, "Second"));
        assertTrue(ConnectionIdentity.isOwnGroupMember(groups, SECONDARY, SECONDARY, "Second"));
    }

    @Test
    @DisplayName("组外玩家的 UUID/名字一律拒绝（冒用 op 换取权限的主路径）")
    void rejectsOutsiders() {
        GroupTable groups = groupTable();
        ConnectionIdentity.declare(HOST, "Host", true);

        assertFalse(ConnectionIdentity.isOwnGroupMember(groups, HOST, OUTSIDER, "Victim"));
        // UUID 冒充组外的人、名字也换成组外的人 → 拒
        assertFalse(ConnectionIdentity.isOwnGroupMember(
                groups, HOST, OUTSIDER, "NotInGroup"));
    }

    @Test
    @DisplayName("名字兜底：集成服玩家 UUID 与档案 UUID 不一致时仍能通过")
    void fallsBackToDeclaredName() {
        GroupTable groups = groupTable();
        ConnectionIdentity.declare(SECONDARY, "Second", true);

        // 离线/开发环境下集成服派生了离线 UUID：UUID 不匹配，但名字是本组声明的
        UUID offlineUuid = UUID.nameUUIDFromBytes("OfflinePlayer:Second".getBytes());
        assertTrue(ConnectionIdentity.isOwnGroupMember(
                groups, HOST, offlineUuid, "Second"));
        // 名字大小写不敏感（原版玩家名可比对的既有口径）
        assertTrue(ConnectionIdentity.isOwnGroupMember(groups, HOST, null, "second"));
        // 组内成员之外的名字依旧拒
        assertFalse(ConnectionIdentity.isOwnGroupMember(groups, HOST, offlineUuid, "Second?"));
    }

    @Test
    @DisplayName("配对判据：不得把 A 的 UUID 与 B 的名字拼起来用（跨成员拼接绕过）")
    void mixedUuidAndNameFromDifferentMembersIsRejected() {
        GroupTable groups = groupTable();
        ConnectionIdentity.declare(HOST, "Host", true);
        ConnectionIdentity.declare(SECONDARY, "Second", true);

        // 组内成员报"外部 op 的 UUID + 自己的名字"：uuid 不在组里、名字虽在组里，
        // 但两者不是同一个成员 ⇒ 必须拒绝（否则服务端会按那个 UUID 查 ops 名单，
        // 用 op 的真实等级去执行 3/4 级指令）
        UUID opUuid = UUID.randomUUID();
        assertFalse(ConnectionIdentity.isOwnGroupMember(groups, HOST, opUuid, "Host"));
        assertNull(ConnectionIdentity.canonicalMemberId(groups, HOST, opUuid, "Host"));

        // 同样地：自己的 UUID + 别人的名字也不成立（UUID 命中时名字不参与判断）
        assertNotNull(ConnectionIdentity.canonicalMemberId(groups, HOST, HOST, "Second"),
                "UUID 命中即认该成员（名字只作兜底，不参与比对）");
        assertEquals(HOST, ConnectionIdentity.canonicalMemberId(groups, HOST, HOST, "Second"));
    }

    @Test
    @DisplayName("离线派生 UUID 必须与名字的派生值一致才按名字兜底")
    void offlineDerivedUuidMustMatchName() {
        GroupTable groups = groupTable();
        ConnectionIdentity.declare(SECONDARY, "Second", true);

        UUID derived = ConnectionIdentity.offlineUuid("Second");
        assertEquals(SECONDARY,
                ConnectionIdentity.canonicalMemberId(groups, HOST, derived, "Second"));
        // 名字对但 UUID 是另一个玩家的派生值 ⇒ 拒绝
        UUID otherDerived = ConnectionIdentity.offlineUuid("SomeoneElse");
        assertNull(ConnectionIdentity.canonicalMemberId(groups, HOST, otherDerived, "Second"));
    }

    @Test
    @DisplayName("能力位：只有 HELLO 声明过 commandRouting 的连接才按新语义处理上送")
    void routingCapabilityBit() {
        GroupTable groups = groupTable();
        ConnectionIdentity.declare(HOST, "Host", true);
        ConnectionIdentity.declare(SECONDARY, "Second", false);
        assertTrue(ConnectionIdentity.supportsCommandRouting(HOST));
        assertFalse(ConnectionIdentity.supportsCommandRouting(SECONDARY),
                "老客户端未声明能力位：服务端必须忽略它的上送（它本地已执行过一次）");
        assertFalse(ConnectionIdentity.supportsCommandRouting(UUID.randomUUID()));
    }

    @Test
    @DisplayName("未入组/未知连接不通过（HELLO 之后、ROLE 之前）")
    void rejectsUnknownConnections() {
        GroupTable groups = groupTable();
        assertFalse(ConnectionIdentity.isOwnGroupMember(groups, OUTSIDER, OUTSIDER, "X"));
        assertFalse(ConnectionIdentity.isOwnGroupMember(null, HOST, HOST, "Host"));
        assertFalse(ConnectionIdentity.isOwnGroupMember(groups, null, HOST, "Host"));
    }

    @Test
    @DisplayName("断连清理后声明失效，重连需重新声明")
    void forgetClearsDeclaredName() {
        GroupTable groups = groupTable();
        ConnectionIdentity.declare(SECONDARY, "Second", true);
        UUID offlineUuid = UUID.nameUUIDFromBytes("OfflinePlayer:Second".getBytes());
        assertTrue(ConnectionIdentity.isOwnGroupMember(groups, HOST, offlineUuid, "Second"));

        ConnectionIdentity.forget(SECONDARY);
        assertFalse(ConnectionIdentity.isOwnGroupMember(groups, HOST, offlineUuid, "Second"));
        ConnectionIdentity.clearAll();
    }
}
