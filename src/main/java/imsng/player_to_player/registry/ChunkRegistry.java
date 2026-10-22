package imsng.player_to_player.registry;

import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 区块注册表（服务端，DESIGN.md 第 5 节；规范"区块注册表：区块列表 / MySQL"）。
 * <p>
 * 维护 区块 → 占用组 的权威映射，保证同一区块同一时刻只被一个组客户端加载：
 * <ul>
 *   <li>{@link #tryClaim}：<b>原子地</b>检查目标区块 + 东西南北四邻（规范的"缓冲层"）
 *       是否被<b>其他组</b>占用，全部空闲（或属本组）才授予并登记目标区块；</li>
 *   <li>{@link #probe}：与 tryClaim 同一套检查但<b>不登记</b>（Phase 4 传送门预检 /
 *       末影珍珠交接判定用 —— 只想知道"能不能占"，不想真的占）；</li>
 *   <li>{@link #release}：单区块释放（仅占用组本人可释放）；</li>
 *   <li>{@link #releaseAll}：整组释放（主客户端掉线时服务端调用）。</li>
 * </ul>
 * <p>
 * <b>持久化</b>：经 {@link RegistryStore} 后端（本地 JSON 或 MySQL，Phase 4），
 * 定期落盘 + 关闭时落盘，服务端重启不丢占用状态。
 * <p>
 * <b>线程模型</b>：claim/release 来自 Netty 事件循环，落盘在 scheduler/调用线程 ——
 * 写路径以内部锁串行化（五格检查+登记必须原子），读走 ConcurrentHashMap 无锁。
 */
public final class ChunkRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/registry");

    /** 自动 checkpoint 周期（秒）；ACK 前变更已在 WAL，周期任务只负责压缩快照。 */
    private static final long AUTO_PERSIST_SECONDS = 30;

    /** 占用信息：占用组 + 授予时刻（诊断用）。 */
    public record ClaimInfo(UUID groupId, long claimedAtMillis) {
    }

    /**
     * 申请结果。granted=false 时 blockingChunk/blockingGroup 说明拒绝原因
     * （哪个区块被哪个组占着 —— 客户端据此决定对该组发起预连接，规范"预连接"入口）；
     * granted=true 时 hasServerData 指示该区块走服务端数据还是种子生成。
     */
    public record ClaimResult(boolean granted, ChunkKey blockingChunk, UUID blockingGroup,
                              boolean hasServerData) {

        static ClaimResult grantedResult(boolean hasServerData) {
            return new ClaimResult(true, null, null, hasServerData);
        }

        static ClaimResult rejected(ChunkKey blockingChunk, UUID blockingGroup) {
            return new ClaimResult(false, blockingChunk, blockingGroup, false);
        }
    }

    /** 区块 → 占用信息（读无锁；写经 claimLock 串行化）。 */
    private final Map<ChunkKey, ClaimInfo> claims = new ConcurrentHashMap<>();

    /** 已取得 EXIT_BEGIN 保护确认、等待最终提交的组。 */
    private final java.util.Set<UUID> exitFrozenGroups = ConcurrentHashMap.newKeySet();

    /**
     * 已成功提交的普通区块释放回执。其生命周期跨越 ACK 丢失和服务端重启；写入必须
     * 与对应 ownership 删除共用一条 {@link RegistryMutationJournal} 记录。
     */
    private final Map<UUID, ChunkReleaseReceipt> releaseReceipts = new HashMap<>();

    /** 普通断连实际释放的完整 ownership 清单；只用于同客户端退出重连恢复。 */
    private final Map<UUID, DisconnectClaimRecovery> disconnectRecoveries =
            new HashMap<>();

    /** 写路径互斥锁：五格检查 + 登记必须是不可分割的整体。 */
    private final Object claimLock = new Object();

    /** 后端全量快照保存互斥；防旧快照后完成并覆盖更新的退出事务状态。 */
    private final Object persistLock = new Object();

    /** 每次内存占用变更递增；用于判断是否仍有未保存的新状态。 */
    private final AtomicLong changeVersion = new AtomicLong();

    /** 已成功保存到后端的最高内存版本；与 changeVersion 不同即仍需落盘。 */
    private final AtomicLong persistedVersion = new AtomicLong();

    /** 持久化后端（本地 JSON / MySQL，Phase 4 可插拔）。 */
    private final RegistryStore store;

    /** region 文件探测器（hasServerData 判定）。 */
    private final RegionFileProbe probe;

    /** ACK 前 force 的先行变更日志；测试可使用 disabled 实现。 */
    private final RegistryMutationJournal journal;

    /** 自动落盘任务句柄；shutdown 时取消。 */
    private volatile ScheduledFuture<?> autoPersistTask;

    /** 仅发布已经耐久完成的释放/迁移事实；监听器不参与所有权判定或持久化成功条件。 */
    private volatile Consumer<UUID> ownershipChangeListener;

    public ChunkRegistry(RegistryStore store, RegionFileProbe probe) {
        this(store, probe, RegistryMutationJournal.disabled());
    }

    public ChunkRegistry(RegistryStore store, RegionFileProbe probe,
                         RegistryMutationJournal journal) {
        this.store = store;
        this.probe = probe;
        this.journal = journal != null
                ? journal : RegistryMutationJournal.disabled();
    }

    /** 监听旧占用组的归属变化；null 解除。生产监听器应只排队通知，不能阻塞写入路径。 */
    public void setOwnershipChangeListener(Consumer<UUID> listener) {
        ownershipChangeListener = listener;
    }

    /** 必须在 WAL 与内存修改均成功后、claimLock 外调用，通知失败不能逆转耐久结果。 */
    private void notifyOwnershipChanged(UUID previousGroup) {
        Consumer<UUID> listener = ownershipChangeListener;
        if (listener == null) {
            return;
        }
        try {
            listener.accept(previousGroup);
        } catch (RuntimeException failure) {
            LOGGER.warn("归属变更通知失败，客户端将按原重试周期重新申请: {}", previousGroup, failure);
        }
    }

    // ------------------------------------------------------------ 申请与释放

    /**
     * 申请加载区块：原子检查 目标 + 四邻 是否被其他组占用，全部空闲（或属本组）
     * 才授予（只登记目标区块本身；缓冲层不登记 —— 邻块仍可被本组后续申请）。
     */
    public ClaimResult tryClaim(ChunkKey key, UUID groupId) {
        synchronized (claimLock) {
            if (exitFrozenGroups.contains(groupId)) {
                return ClaimResult.rejected(key, groupId);
            }
            ClaimResult blocked = checkBlockedLocked(key, groupId);
            if (blocked != null) {
                return blocked;
            }
            // 授予：登记目标区块（重复申请刷新占用时间，幂等）
            journal.appendPut(key, groupId);
            claims.put(key, new ClaimInfo(groupId, System.currentTimeMillis()));
            markDirtyLocked();
        }
        // hasServerData 是磁盘探测，放锁外做（不占 claimLock，避免拖慢并发申请）
        return ClaimResult.grantedResult(probe != null && probe.hasServerData(key));
    }

    /**
     * 只读探测（Phase 4）：与 {@link #tryClaim} 同一套"目标 + 四邻"检查，
     * 但<b>不登记任何占用</b>。传送门预检与末影珍珠交接判定用 —— 若走 tryClaim，
     * 探测本身就会占下一个永远不会被加载（也就永远不会随卸载释放）的区块，泄漏占用。
     */
    public ClaimResult probe(ChunkKey key, UUID groupId) {
        synchronized (claimLock) {
            ClaimResult blocked = checkBlockedLocked(key, groupId);
            return blocked != null ? blocked : ClaimResult.grantedResult(false);
        }
    }

    /** 锁内的"目标 + 四邻"占用检查；被阻塞返回拒绝结果，可占返回 null。 */
    private ClaimResult checkBlockedLocked(ChunkKey key, UUID groupId) {
        // 目标区块本身：被其他组占用即拒绝
        ClaimInfo existing = claims.get(key);
        if (existing != null && !existing.groupId().equals(groupId)) {
            return ClaimResult.rejected(key, existing.groupId());
        }
        // 缓冲层（东西南北四邻）：任何一格被其他组占用即拒绝
        for (ChunkKey neighbor : key.neighbors4()) {
            ClaimInfo info = claims.get(neighbor);
            if (info != null && !info.groupId().equals(groupId)) {
                return ClaimResult.rejected(neighbor, info.groupId());
            }
        }
        return null;
    }

    /**
     * 释放单个区块（仅占用组本人可释放，防其他组恶意/误释放）。
     *
     * @return 是否真的释放了（false = 未被占用或占用者不是该组）
     */
    public boolean release(ChunkKey key, UUID groupId) {
        synchronized (claimLock) {
            if (exitFrozenGroups.contains(groupId)) {
                return false;
            }
            ClaimInfo existing = claims.get(key);
            if (existing == null || !existing.groupId().equals(groupId)) {
                return false;
            }
            journal.appendRemove(key, groupId);
            claims.remove(key);
            markDirtyLocked();
        }
        notifyOwnershipChanged(groupId);
        return true;
    }

    /**
     * 查询一次释放重试的既有成功回执。相同 releaseId 若绑定到不同请求，立即拒绝，
     * 不能把 UUID 误复用掩盖成另一份区块数据已经写入。
     */
    public ChunkReleaseReceipt findReleaseReceipt(
            UUID releaseId, ChunkKey key, UUID groupId,
            int payloadBytes, String payloadSha256) {
        synchronized (claimLock) {
            ChunkReleaseReceipt receipt = releaseReceipts.get(releaseId);
            if (receipt == null) {
                return null;
            }
            if (!receipt.matchesRequest(
                    key, groupId, payloadBytes, payloadSha256)) {
                throw new IllegalArgumentException(
                        "releaseId 已绑定到另一释放请求: " + releaseId);
            }
            return receipt;
        }
    }

    /** 在途计数器确认 confirmation 确实对应已经建立且属于该组的成功回执。 */
    boolean hasReleaseReceiptForGroup(UUID releaseId, UUID groupId) {
        synchronized (claimLock) {
            ChunkReleaseReceipt receipt = releaseReceipts.get(releaseId);
            return receipt != null && groupId.equals(receipt.groupId());
        }
    }

    /**
     * 在最终数据已经按区块锁写入并 flush 后，原子建立成功回执并删除 ownership。
     * WAL append 先于两项内存变更，因此进程在任意指令间崩溃，重启重放仍会同时看到
     * 二者；相同操作重放直接返回原回执，不再次删除后来取得的 ownership。
     *
     * @return 成功或既有回执；owner 不符/退出冻结时返回 null
     */
    public ChunkReleaseReceipt completeRelease(ChunkReleaseReceipt receipt) {
        if (receipt == null) {
            throw new IllegalArgumentException("释放回执不能为空");
        }
        synchronized (claimLock) {
            ChunkReleaseReceipt existing = findReleaseReceipt(
                    receipt.releaseId(), receipt.key(), receipt.groupId(),
                    receipt.payloadBytes(), receipt.payloadSha256());
            if (existing != null) {
                return existing;
            }
            if (exitFrozenGroups.contains(receipt.groupId())) {
                return null;
            }
            ClaimInfo owner = claims.get(receipt.key());
            if (owner == null || !receipt.groupId().equals(owner.groupId())) {
                return null;
            }
            journal.appendRelease(receipt);
            claims.remove(receipt.key());
            releaseReceipts.put(receipt.releaseId(), receipt);
            markDirtyLocked();
        }
        notifyOwnershipChanged(receipt.groupId());
        return receipt;
    }

    /**
     * 客户端收到正 CHUNK_RELEASE_ACK 后确认回执可回收。删除属于纯垃圾回收，不影响
     * ownership 正确性，因此只标脏并随 30 秒 checkpoint 批量持久化，避免每次卸载再做
     * 一次 fsync。checkpoint 前崩溃最多让已无害的回执暂时复活；重复确认仍直接成功。
     */
    public boolean confirmReleaseReceipt(UUID releaseId, UUID groupId) {
        if (releaseId == null || groupId == null) {
            return false;
        }
        synchronized (claimLock) {
            ChunkReleaseReceipt receipt = releaseReceipts.get(releaseId);
            if (receipt == null) {
                return true;
            }
            if (!groupId.equals(receipt.groupId())) {
                return false;
            }
            releaseReceipts.remove(releaseId);
            markDirtyLocked();
            return true;
        }
    }

    /** 释放某组占用的全部区块（主客户端掉线时调用）。返回释放的区块数。 */
    public int releaseAll(UUID groupId) {
        if (groupId == null) {
            return 0;
        }
        int released = 0;
        synchronized (claimLock) {
            if (exitFrozenGroups.contains(groupId)) {
                return 0;
            }
            boolean hasClaims = claims.values().stream()
                    .anyMatch(info -> groupId.equals(info.groupId()));
            boolean hasReceipts = releaseReceipts.values().stream()
                    .anyMatch(receipt -> groupId.equals(receipt.groupId()));
            boolean hasRecovery = disconnectRecoveries.containsKey(groupId);
            if (hasClaims || hasReceipts || hasRecovery) {
                journal.appendRemoveGroup(groupId);
            }
            var it = claims.entrySet().iterator();
            while (it.hasNext()) {
                if (groupId.equals(it.next().getValue().groupId())) {
                    it.remove();
                    released++;
                }
            }
            int removedReceipts = removeReceiptsForGroupLocked(groupId);
            boolean removedRecovery = disconnectRecoveries.remove(groupId) != null;
            if (released > 0 || removedReceipts > 0 || removedRecovery) {
                markDirtyLocked();
            }
        }
        if (released > 0) {
            LOGGER.info("已释放组 {} 的 {} 个区块占用", groupId, released);
            notifyOwnershipChanged(groupId);
        }
        return released;
    }

    /**
     * 异常断连使用的整组耐久释放。调用前可先冻结；WAL append 失败时不修改内存，
     * 调用方应继续重试，不能让其他组观察到未确认的释放。
     */
    public int releaseAllDurably(UUID groupId) {
        return releaseAllDurably(groupId,
                groupId != null ? Set.of(groupId) : Set.of());
    }

    /** 带服务端权威组成员快照的异常断连耐久释放。 */
    public int releaseAllDurably(UUID groupId, Set<UUID> groupMembers) {
        if (groupId == null) {
            return 0;
        }
        if (groupMembers == null || !groupMembers.contains(groupId)) {
            throw new IllegalArgumentException("断连释放组成员清单非法");
        }
        int released;
        synchronized (persistLock) {
            synchronized (claimLock) {
                exitFrozenGroups.add(groupId);
                Map<ChunkKey, ClaimInfo> removed = new HashMap<>();
                for (Map.Entry<ChunkKey, ClaimInfo> entry : claims.entrySet()) {
                    if (groupId.equals(entry.getValue().groupId())) {
                        removed.put(entry.getKey(), entry.getValue());
                    }
                }
                DisconnectClaimRecovery newRecovery = new DisconnectClaimRecovery(
                        UUID.randomUUID(), groupId, removed.keySet(), groupMembers,
                        System.currentTimeMillis());
                // 一条 force 同时证明“这些权威 claim 被本次断连释放”和
                // “仅该完整清单可被同客户端 EXIT_BEGIN 恢复”；空清单也要记录 tombstone。
                journal.appendDisconnectRelease(newRecovery);
                removed.keySet().forEach(claims::remove);
                int removedReceipts = removeReceiptsForGroupLocked(groupId);
                disconnectRecoveries.put(groupId, newRecovery);
                // 新 recovery 即使区块清单为空也是需要 checkpoint 的状态变更。
                markDirtyLocked();
                exitFrozenGroups.remove(groupId);
                released = removed.size();
            }
        }
        if (released > 0) {
            notifyOwnershipChanged(groupId);
        }
        return released;
    }

    /**
     * 原子冻结某组并返回权威占用清单。冻结后普通申请/释放/releaseAll 都不能改变
     * 该组，只有 {@link #completeExitDurably} 可以耐久解除并释放事务清单内区块。
     */
    public java.util.Set<ChunkKey> freezeForExit(UUID groupId) {
        if (groupId == null) {
            return Set.of();
        }
        synchronized (claimLock) {
            exitFrozenGroups.add(groupId);
            return java.util.Set.copyOf(claimsOf(groupId));
        }
    }

    /**
     * EXIT_BEGIN 的原子冻结与清单选择。在线组使用服务端当前占用；服务端重启后
     * 组表丢失时，可先验证客户端清单没有窃取其他组占用，再把该清单作为待恢复
     * 清单。实际补回/删除占用在 pending 元数据落盘后的 restoreExitProtection 完成。
     */
    public Set<ChunkKey> freezeOrRecoverForExit(UUID groupId,
                                                Set<ChunkKey> requestedChunks,
                                                boolean recoverOrphan) {
        return freezeOrRecoverForExit(groupId, requestedChunks,
                groupId != null ? Set.of(groupId) : Set.of(), recoverOrphan);
    }

    /** 带玩家成员清单校验的 EXIT_BEGIN 冻结/断连恢复入口。 */
    public Set<ChunkKey> freezeOrRecoverForExit(UUID groupId,
                                                Set<ChunkKey> requestedChunks,
                                                Set<UUID> requestedMembers,
                                                boolean recoverOrphan) {
        if (groupId == null || requestedChunks == null || requestedMembers == null) {
            throw new IllegalArgumentException("冻结退出清单所需参数不完整");
        }
        synchronized (claimLock) {
            if (recoverOrphan) {
                DisconnectClaimRecovery recovery = disconnectRecoveries.get(groupId);
                if (recovery == null) {
                    Set<ChunkKey> authoritativeClaims = Set.copyOf(claimsOf(groupId));
                    // 服务端可能在 EXIT_BEGIN 前重启：组表是内存态，但注册表 claims
                    // 已耐久。只允许“非空、完整一致、单成员主客户端”的受限恢复；
                    // 空清单必须有 tombstone，多成员身份则不能信任客户端自报。
                    if (!authoritativeClaims.isEmpty()
                            && authoritativeClaims.equals(requestedChunks)
                            && requestedMembers.equals(Set.of(groupId))) {
                        exitFrozenGroups.add(groupId);
                        return authoritativeClaims;
                    }
                    throw new IllegalStateException("孤儿组没有服务端断连释放凭证");
                }
                if (!recovery.chunks().equals(requestedChunks)) {
                    throw new IllegalStateException(
                            "孤儿组请求清单与服务端断连释放凭证不一致");
                }
                if (!recovery.members().equals(requestedMembers)) {
                    throw new IllegalStateException(
                            "孤儿组玩家清单与服务端断连释放凭证不一致");
                }
                // 只信任服务端实际释放并耐久记录的清单；仍需复核期间是否被其他组
                // 占用或形成四邻冲突，绝不能覆盖释放之后产生的新权威状态。
                for (ChunkKey requested : recovery.chunks()) {
                    ClaimInfo owner = claims.get(requested);
                    if (owner != null && !groupId.equals(owner.groupId())) {
                        throw new IllegalStateException(
                                "孤儿组请求包含其他组占用: "
                                        + requested.asString());
                    }
                    for (ChunkKey neighbor : requested.neighbors4()) {
                        ClaimInfo neighborOwner = claims.get(neighbor);
                        if (neighborOwner != null
                                && !groupId.equals(neighborOwner.groupId())) {
                            throw new IllegalStateException(
                                    "孤儿组请求清单存在四邻冲突: "
                                            + requested.asString() + " neighbor="
                                            + neighbor.asString());
                        }
                    }
                }
                exitFrozenGroups.add(groupId);
                return recovery.chunks();
            }
            exitFrozenGroups.add(groupId);
            return Set.copyOf(claimsOf(groupId));
        }
    }

    /**
     * 服务端重启后以已经耐久确认的 pending 清单恢复权威占用并重新冻结。
     * 事务清单比可能落后一个周期的注册表快照更新，因此会移除该组不在清单中的
     * 陈旧占用并补回清单中缺失的占用；若某清单区块已被另一组占用则拒绝启动，
     * 不能静默覆盖潜在的新状态。
     */
    public void restoreExitProtection(UUID groupId, Set<ChunkKey> manifestChunks) {
        if (groupId == null || manifestChunks == null) {
            throw new IllegalArgumentException("恢复退出保护所需参数不完整");
        }
        synchronized (persistLock) {
            synchronized (claimLock) {
                for (ChunkKey key : manifestChunks) {
                    ClaimInfo existing = claims.get(key);
                    if (existing != null && !groupId.equals(existing.groupId())) {
                        throw new IllegalStateException("退出事务区块已被其他组占用: "
                                + key.asString() + " owner=" + existing.groupId());
                    }
                }
                journal.appendReplaceGroup(groupId, manifestChunks);
                claims.entrySet().removeIf(entry -> groupId.equals(entry.getValue().groupId())
                        && !manifestChunks.contains(entry.getKey()));
                long now = System.currentTimeMillis();
                for (ChunkKey key : manifestChunks) {
                    claims.put(key, new ClaimInfo(groupId, now));
                }
                disconnectRecoveries.remove(groupId);
                exitFrozenGroups.add(groupId);
                markDirtyLocked();
            }
        }
    }

    public boolean isExitFrozen(UUID groupId) {
        return groupId != null && exitFrozenGroups.contains(groupId);
    }

    /** EXIT_BEGIN 元数据未能落盘时撤销本轮内存冻结，不修改任何占用。 */
    public void cancelExitProtection(UUID groupId) {
        if (groupId != null) {
            exitFrozenGroups.remove(groupId);
        }
    }

    /**
     * 客户端选择正常 ROLE 重进时耐久放弃旧断连退出恢复资格。
     * 一次 WAL force 后即可返回；全量辅助快照留给周期 checkpoint 异步收敛。
     */
    public boolean discardDisconnectRecoveryDurably(UUID groupId) {
        if (groupId == null) {
            return false;
        }
        synchronized (claimLock) {
            DisconnectClaimRecovery recovery = disconnectRecoveries.get(groupId);
            if (recovery == null) {
                return false;
            }
            journal.appendConsumeDisconnectRecovery(
                    groupId, recovery.recoveryId());
            if (disconnectRecoveries.remove(groupId, recovery)) {
                markDirtyLocked();
                return true;
            }
            return false;
        }
    }

    /**
     * 只清理该退出事务清单内的区块并在解除冻结前同步持久化。持久化失败时恢复
     * 原占用并保持冻结，调用方不得返回 committed ACK。
     */
    public int completeExitDurably(UUID groupId, Set<ChunkKey> manifestChunks) {
        if (groupId == null || manifestChunks == null) {
            return 0;
        }
        int released;
        synchronized (persistLock) {
            synchronized (claimLock) {
                exitFrozenGroups.add(groupId);
                journal.appendRemoveKeys(groupId, manifestChunks);
                Map<ChunkKey, ClaimInfo> removed = new HashMap<>();
                for (ChunkKey key : manifestChunks) {
                    ClaimInfo info = claims.get(key);
                    if (info != null && groupId.equals(info.groupId())) {
                        removed.put(key, info);
                        claims.remove(key);
                    }
                }
                int removedReceipts = removeReceiptsForGroupLocked(groupId);
                DisconnectClaimRecovery removedRecovery =
                        disconnectRecoveries.remove(groupId);
                if (!removed.isEmpty() || removedReceipts > 0
                        || removedRecovery != null) {
                    markDirtyLocked();
                }
                exitFrozenGroups.remove(groupId);
                released = removed.size();
            }
        }
        if (released > 0) {
            notifyOwnershipChanged(groupId);
        }
        return released;
    }

    /**
     * 整组迁移（Phase 3 合并）：把 from 组占用的<b>全部</b>区块原子地改挂到 to 组名下。
     * <p>
     * 与"释放再申请"相比，迁移不存在中间空窗 —— 释放瞬间第三组恰好申请到这些
     * 区块会让合并双方都拿不到；原子改挂则保证合并期间区块归属始终有主。
     *
     * @return 迁移的区块数
     */
    public int migrateAll(UUID from, UUID to) {
        if (from == null || to == null || from.equals(to)) {
            return 0;
        }
        int migrated = 0;
        synchronized (claimLock) {
            if (exitFrozenGroups.contains(from) || exitFrozenGroups.contains(to)) {
                return 0;
            }
            boolean hasClaims = claims.values().stream()
                    .anyMatch(info -> from.equals(info.groupId()));
            if (hasClaims) {
                journal.appendMigrateGroup(from, to);
            }
            for (Map.Entry<ChunkKey, ClaimInfo> e : claims.entrySet()) {
                if (from.equals(e.getValue().groupId())) {
                    e.setValue(new ClaimInfo(to, System.currentTimeMillis()));
                    migrated++;
                }
            }
            if (migrated > 0) {
                markDirtyLocked();
            }
        }
        if (migrated > 0) {
            LOGGER.info("区块占用整组迁移: {} → {} ({} 个区块)", from, to, migrated);
            notifyOwnershipChanged(from);
        }
        return migrated;
    }

    /**
     * 指定区块迁移（Phase 3 分离）：把列表中<b>仍属 from 组</b>的区块改挂到 to 组。
     * 不属 from 组的条目静默跳过（分离请求与实际占用之间允许弱一致）。
     *
     * @return 实际迁移的区块数
     */
    public int migrate(Collection<ChunkKey> keys, UUID from, UUID to) {
        if (keys == null || keys.isEmpty() || from == null || to == null || from.equals(to)) {
            return 0;
        }
        int migrated = 0;
        synchronized (claimLock) {
            if (exitFrozenGroups.contains(from) || exitFrozenGroups.contains(to)) {
                return 0;
            }
            Set<ChunkKey> actual = new java.util.HashSet<>();
            for (ChunkKey key : keys) {
                ClaimInfo info = claims.get(key);
                if (info != null && from.equals(info.groupId())) {
                    actual.add(key);
                }
            }
            if (!actual.isEmpty()) {
                journal.appendMigrateKeys(from, to, actual);
                for (ChunkKey key : actual) {
                    claims.put(key, new ClaimInfo(to, System.currentTimeMillis()));
                    migrated++;
                }
            }
            if (migrated > 0) {
                markDirtyLocked();
            }
        }
        if (migrated > 0) {
            LOGGER.info("区块占用定向迁移: {} → {} ({} / {} 个区块)", from, to, migrated, keys.size());
            notifyOwnershipChanged(from);
        }
        return migrated;
    }

    /** 某组当前占用的全部区块（快照；诊断与合并规模评估用）。 */
    public List<ChunkKey> claimsOf(UUID groupId) {
        List<ChunkKey> result = new ArrayList<>();
        if (groupId == null) {
            return result;
        }
        for (Map.Entry<ChunkKey, ClaimInfo> e : claims.entrySet()) {
            if (groupId.equals(e.getValue().groupId())) {
                result.add(e.getKey());
            }
        }
        return result;
    }

    /** 查询区块占用组；空闲返回 null。 */
    public UUID ownerOf(ChunkKey key) {
        ClaimInfo info = claims.get(key);
        return info != null ? info.groupId() : null;
    }

    /** 该区块在服务端存档中是否已有数据（探测器委托）。 */
    public boolean hasServerData(ChunkKey key) {
        return probe != null && probe.hasServerData(key);
    }

    // ------------------------------------------------------------ 持久化

    /**
     * 从持久化后端恢复上次运行的占用状态（服务端启动时调用一次）。
     * 坏数据由后端自行跳过（宁可丢一条记录也不能让服务端起不来）。
     */
    public void load() {
        boolean replayedJournal = journal.hasEntries();
        RegistryMutationJournal.AuxiliaryState auxiliary =
                journal.loadAuxiliaryState();
        RegistryMutationJournal.ReplayState recovered = journal.replayState(
                store.load(), auxiliary.releaseReceipts(),
                auxiliary.disconnectRecoveries());
        Map<ChunkKey, UUID> loaded = recovered.claims();
        long now = System.currentTimeMillis();
        synchronized (claimLock) {
            for (Map.Entry<ChunkKey, UUID> e : loaded.entrySet()) {
                claims.put(e.getKey(), new ClaimInfo(e.getValue(), now));
            }
            releaseReceipts.putAll(recovered.releaseReceipts());
            disconnectRecoveries.putAll(recovered.disconnectRecoveries());
        }
        if (replayedJournal) {
            changeVersion.incrementAndGet();
        }
        LOGGER.info("区块注册表已恢复 {} 条占用记录（后端: {}）", loaded.size(), store.describe());
    }

    /** 启动定期落盘（scheduler 驱动；幂等）。 */
    public synchronized void startAutoPersist() {
        if (autoPersistTask != null) {
            return;
        }
        autoPersistTask = ThreadPools.scheduler().scheduleWithFixedDelay(() -> {
            try {
                persistIfDirty();
            } catch (Exception e) {
                LOGGER.error("区块注册表定期落盘失败", e);
            }
        }, AUTO_PERSIST_SECONDS, AUTO_PERSIST_SECONDS, TimeUnit.SECONDS);
    }

    /** 停止定期落盘并做最终 persist（服务端停止时调用；幂等）。 */
    public synchronized void shutdown() {
        ScheduledFuture<?> task = autoPersistTask;
        autoPersistTask = null;
        if (task != null) {
            task.cancel(false);
        }
        persist();
        store.close();
    }

    /** 有变更才落盘（定期任务用）。 */
    private void persistIfDirty() {
        if (persistedVersion.get() != changeVersion.get()) {
            persist();
        }
    }

    /** 全量落盘：保存快照并清空日志期间阻止并发变更，避免删掉尚未进快照的新日志。 */
    public void persist() {
        synchronized (persistLock) {
            synchronized (claimLock) {
                long snapshotVersion = changeVersion.get();
                Map<ChunkKey, UUID> snapshot = claimsSnapshotLocked();
                Map<UUID, ChunkReleaseReceipt> receipts =
                        Map.copyOf(releaseReceipts);
                Map<UUID, DisconnectClaimRecovery> recoveries =
                        Map.copyOf(disconnectRecoveries);
                journal.appendCheckpoint(snapshot);
                store.save(snapshot);
                journal.saveAuxiliaryState(receipts, recoveries);
                journal.clear();
                persistedVersion.accumulateAndGet(snapshotVersion, Math::max);
            }
        }
    }

    private Map<ChunkKey, UUID> claimsSnapshotLocked() {
        Map<ChunkKey, UUID> snapshot = new HashMap<>();
        for (Map.Entry<ChunkKey, ClaimInfo> entry : claims.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().groupId());
        }
        return snapshot;
    }

    /** claimLock 内删除已终结组无法再确认的普通释放回执。 */
    private int removeReceiptsForGroupLocked(UUID groupId) {
        int before = releaseReceipts.size();
        releaseReceipts.values().removeIf(
                receipt -> groupId.equals(receipt.groupId()));
        return before - releaseReceipts.size();
    }

    private void markDirtyLocked() {
        changeVersion.incrementAndGet();
    }

    /** 当前占用总数（诊断/测试用）。 */
    public int size() {
        return claims.size();
    }
}
