package imsng.player_to_player.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 组客户端表（服务端，Phase 2；规范"组客户端：一个主客户端及其所属的副客户端"）。
 * <p>
 * 维护 组 → 主客户端 + 成员 的权威视图，供角色指派（{@link RoleAssignHandler}）
 * 与断连清理使用。<b>Phase 1/2 约定 groupId == 主客户端 clientId</b>（与
 * {@code NodeContext.groupId}、{@code ChunkRegistry.releaseAll} 的口径一致）；
 * 表结构仍显式存 primaryClientId 字段，为 Phase 3 合并时"主客户端迁移、
 * groupId 延续"预留演化空间。
 * <p>
 * 线程模型：写路径以内部锁串行化（组创建/解散/成员迁移是复合操作），
 * 读走 ConcurrentHashMap 无锁。
 */
public final class GroupTable {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/group-table");

    /** 组信息：主客户端 + 全部成员（含主客户端自身）。 */
    public record GroupInfo(UUID groupId, UUID primaryClientId, Set<UUID> members) {
    }

    /** groupId → 组信息。 */
    private final Map<UUID, GroupInfo> groups = new ConcurrentHashMap<>();
    /** clientId → 所属 groupId（反向索引）。 */
    private final Map<UUID, UUID> memberToGroup = new ConcurrentHashMap<>();

    /** 写路径互斥锁。 */
    private final Object lock = new Object();

    /**
     * 创建一个以该客户端为主客户端的新组（groupId == primaryClientId）。
     * 该客户端若已在其他组中，先移出（重复 ROLE_REQUEST / 快速重进的防御）。
     */
    public GroupInfo createGroup(UUID primaryClientId) {
        synchronized (lock) {
            removeClientLocked(primaryClientId);
            GroupInfo info = new GroupInfo(primaryClientId, primaryClientId,
                    ConcurrentHashMap.newKeySet());
            info.members().add(primaryClientId);
            groups.put(primaryClientId, info);
            memberToGroup.put(primaryClientId, primaryClientId);
            LOGGER.info("组已创建: groupId={} (主客户端)", primaryClientId);
            return info;
        }
    }

    /**
     * 把客户端加入既有组作为副客户端。
     *
     * @return false = 组不存在（调用方应改走建组路径）
     */
    public boolean addSecondary(UUID groupId, UUID clientId) {
        synchronized (lock) {
            GroupInfo info = groups.get(groupId);
            if (info == null) {
                return false;
            }
            removeClientLocked(clientId);
            // removeClientLocked 可能解散了 clientId 自己当主的空组，重查目标组仍在
            info = groups.get(groupId);
            if (info == null) {
                return false;
            }
            info.members().add(clientId);
            memberToGroup.put(clientId, groupId);
            LOGGER.info("副客户端 {} 加入组 {}", clientId, groupId);
            return true;
        }
    }

    /**
     * 客户端离线/退出时的清理：副客户端只摘成员；主客户端离线则整组解散
     * （Phase 2 无迁移 —— 组内副客户端的隧道会随主客户端下线自然断开，
     * 它们重新加入世界时走全新的角色指派；Phase 3 在此处接入算力再分配）。
     *
     * @return 若该客户端是主客户端，返回被解散的组 ID；否则返回 null
     */
    public UUID removeClient(UUID clientId) {
        synchronized (lock) {
            return removeClientLocked(clientId);
        }
    }

    /** 锁内清理实现（见 {@link #removeClient}）。 */
    private UUID removeClientLocked(UUID clientId) {
        UUID groupId = memberToGroup.remove(clientId);
        if (groupId == null) {
            return null;
        }
        GroupInfo info = groups.get(groupId);
        if (info == null) {
            return null;
        }
        if (info.primaryClientId().equals(clientId)) {
            // 主客户端离线：整组解散，成员反向索引一并清理
            groups.remove(groupId);
            for (UUID member : info.members()) {
                memberToGroup.remove(member, groupId);
            }
            LOGGER.info("组 {} 已解散（主客户端 {} 离线，{} 名成员）",
                    groupId, clientId, info.members().size());
            return groupId;
        }
        info.members().remove(clientId);
        LOGGER.info("副客户端 {} 离开组 {}", clientId, groupId);
        return null;
    }

    /** 查客户端所属组；无归属返回 null。 */
    public UUID groupOf(UUID clientId) {
        return memberToGroup.get(clientId);
    }

    /** 查组的主客户端；组不存在返回 null。 */
    public UUID primaryOf(UUID groupId) {
        GroupInfo info = groups.get(groupId);
        return info != null ? info.primaryClientId() : null;
    }

    /** 组成员快照（防御性拷贝）；组不存在返回空集。 */
    public Set<UUID> membersOf(UUID groupId) {
        GroupInfo info = groups.get(groupId);
        return info != null ? Set.copyOf(info.members()) : Set.of();
    }

    /**
     * 合并两组（Phase 3，规范"合并"）：groupA 与 groupB 的全部成员并入一个新组，
     * 主客户端为 newPrimary（算力分配的胜者）。<b>groupId 约定延续 Phase 1/2 口径
     * （groupId == 新主客户端 clientId）</b>——注册表迁移（{@code ChunkRegistry.migrateAll}）
     * 与断连清理（releaseAll(peerId)）都依赖该口径，不在本期打破。
     *
     * @return 合并后的组信息；任一组不存在或 newPrimary 不在两组成员内返回 null
     */
    public GroupInfo mergeGroups(UUID groupA, UUID groupB, UUID newPrimary) {
        synchronized (lock) {
            GroupInfo a = groups.get(groupA);
            GroupInfo b = groups.get(groupB);
            if (a == null || b == null) {
                return null;
            }
            if (!a.members().contains(newPrimary) && !b.members().contains(newPrimary)) {
                return null; // 新主必须来自合并双方（防状态漂移）
            }
            // 摘除两旧组（含反向索引），组建新组
            groups.remove(groupA);
            groups.remove(groupB);
            GroupInfo merged = new GroupInfo(newPrimary, newPrimary,
                    ConcurrentHashMap.newKeySet());
            merged.members().addAll(a.members());
            merged.members().addAll(b.members());
            groups.put(newPrimary, merged);
            for (UUID member : merged.members()) {
                memberToGroup.put(member, newPrimary);
            }
            LOGGER.info("组已合并: {} + {} → {} (主客户端 {}, {} 名成员)",
                    groupA, groupB, newPrimary, newPrimary, merged.members().size());
            return merged;
        }
    }

    /**
     * 组分离（Phase 3，规范"分离"）：把 departing 成员集从原组摘出，
     * 组成以 newPrimary 为主的新组（groupId == newPrimary）。
     * <p>
     * 单端分离是 departing 只有一个成员的特例。原组主客户端不允许被分走
     * （规范语义：分离的是"渲染区域无交集的副客户端"；主客户端迁移走合并流程）。
     *
     * @return 新组信息；原组不存在 / departing 含原组主 / newPrimary 不在 departing
     *         内时返回 null（调用方回 ERROR）
     */
    public GroupInfo splitGroup(UUID groupId, Set<UUID> departing, UUID newPrimary) {
        synchronized (lock) {
            GroupInfo info = groups.get(groupId);
            if (info == null || departing == null || departing.isEmpty()
                    || !departing.contains(newPrimary)
                    || departing.contains(info.primaryClientId())
                    || !info.members().containsAll(departing)) {
                return null;
            }
            info.members().removeAll(departing);
            GroupInfo fresh = new GroupInfo(newPrimary, newPrimary,
                    ConcurrentHashMap.newKeySet());
            fresh.members().addAll(departing);
            groups.put(newPrimary, fresh);
            for (UUID member : departing) {
                memberToGroup.put(member, newPrimary);
            }
            LOGGER.info("组已分离: {} 名成员离开组 {} 组成新组 {}（主客户端 {}）",
                    departing.size(), groupId, newPrimary, newPrimary);
            return fresh;
        }
    }

    /** 组是否存在。 */
    public boolean exists(UUID groupId) {
        return groups.containsKey(groupId);
    }

    /** 清空全表（服务停止时调用）。 */
    public void clear() {
        synchronized (lock) {
            groups.clear();
            memberToGroup.clear();
        }
    }
}
