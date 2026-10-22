package imsng.player_to_player.group;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 权限名单与落权去重状态的纯 Java 部分。
 * <p>名单属于物理服务端会话，落权记录属于某一次玩家连接；两者的寿命不同。
 * 所有方法使用同一把锁，使 Netty 整表替换不会暴露“新组 ID + 旧名单”的中间状态。</p>
 */
final class LocalPermissionState {
    private UUID groupId;
    private Map<UUID, LocalPermissions.Member> members = Map.of();
    private record Applied(Object player, int level) { }
    private final Map<UUID, Applied> applied = new HashMap<>();

    synchronized void replace(UUID group, Map<UUID, LocalPermissions.Member> table) {
        groupId = group;
        members = Map.copyOf(table);
    }

    synchronized Integer levelFor(UUID group, UUID playerId, String playerName) {
        if (groupId == null || !groupId.equals(group)) {
            return null;
        }
        UUID memberId = memberIdFor(group, playerId, playerName);
        // 服务端发的是整表：从本组名单移除（包括空表）同样是撤权，不能保留旧 OP。
        return memberId != null ? members.get(memberId).level() : 0;
    }

    /** 将集成服离线UUID归一为控制协议档案UUID；未收到名单/重名时返回null，调用方延期。 */
    synchronized UUID memberIdFor(UUID group, UUID playerId, String playerName) {
        if (groupId == null || !groupId.equals(group)) return null;
        if (members.containsKey(playerId)) return playerId;
        // 开发/离线登录可能产生不同 UUID，只有权威名单内唯一同名时才能兜底。
        // 缺失名字的占位符不能视为有效身份，重复名字即使等级相同也拒绝授予。
        if (playerName == null || playerName.isBlank() || "?".equals(playerName)) {
            return null;
        }
        UUID matched = null;
        for (Map.Entry<UUID, LocalPermissions.Member> entry : members.entrySet()) {
            LocalPermissions.Member member = entry.getValue();
            if (member.name().equalsIgnoreCase(playerName)) {
                if (matched != null) {
                    return null;
                }
                matched = entry.getKey();
            }
        }
        return matched;
    }

    synchronized boolean needsApply(UUID playerId, Object player, int level) {
        Applied previous = applied.get(playerId);
        return previous == null || previous.player() != player || previous.level() != level;
    }

    synchronized void markApplied(UUID playerId, Object player, int level) {
        applied.put(playerId, new Applied(player, level));
    }

    synchronized void forget(UUID playerId, Object player) {
        Applied previous = applied.get(playerId);
        if (previous != null && previous.player() == player) {
            applied.remove(playerId);
        }
    }

    /** 宿主重启/主副切换只丢弃实例状态，保留可能早于新宿主到达的权威名单。 */
    synchronized void resetRuntime() {
        applied.clear();
    }

    /** 离开物理服务端会话才清空名单，防止跨世界继承权限。 */
    synchronized void resetSession() {
        groupId = null;
        members = Map.of();
        applied.clear();
    }
}
