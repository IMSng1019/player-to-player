package imsng.player_to_player.registry;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 普通断连耐久释放后留下的 ownership 恢复凭证。
 *
 * <p>只有服务端从权威注册表实际移除的完整区块集合才能生成本记录；客户端随后重连
 * 发送 EXIT_BEGIN 时必须与该集合严格一致，不能凭自报清单占用任意空闲区块。</p>
 */
public record DisconnectClaimRecovery(UUID recoveryId, UUID groupId,
                                      Set<ChunkKey> chunks,
                                      Set<UUID> members,
                                      long createdAtMillis) {

    public DisconnectClaimRecovery {
        Objects.requireNonNull(recoveryId, "recoveryId");
        Objects.requireNonNull(groupId, "groupId");
        if (chunks == null) {
            throw new IllegalArgumentException("断连恢复凭证区块清单为空");
        }
        // 空 ownership 也必须留下 tombstone：否则零区块主客户端断连后，服务端
        // 重启无法区分“确实没有区块”与“客户端试图伪造恢复清单”。
        chunks = Set.copyOf(chunks);
        if (members == null || members.isEmpty() || !members.contains(groupId)) {
            throw new IllegalArgumentException("断连恢复凭证组成员清单非法");
        }
        members = Set.copyOf(members);
        if (createdAtMillis <= 0) {
            throw new IllegalArgumentException("断连恢复凭证时间非法");
        }
    }
}
