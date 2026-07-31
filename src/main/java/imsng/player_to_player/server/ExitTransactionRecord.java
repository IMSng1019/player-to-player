package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;

import java.util.Objects;
import java.util.Set;

/**
 * 持久化的退出事务元数据；大块 NBT 由客户端在重连后幂等重放。
 * writtenChunksDeclared=false 表示记录尚未提交，或来自没有 writtenChunks 字段的旧格式。
 */
record ExitTransactionRecord(ExitManifest manifest, boolean committed,
                             Set<ChunkKey> writtenChunks,
                             boolean writtenChunksDeclared,
                             boolean dataApplied, boolean cleanupComplete,
                             boolean abandoned) {

    ExitTransactionRecord(ExitManifest manifest, boolean committed) {
        this(manifest, committed, Set.of(), false,
                false, false, false);
    }

    ExitTransactionRecord(ExitManifest manifest, boolean committed,
                          Set<ChunkKey> writtenChunks) {
        this(manifest, committed, writtenChunks, true,
                false, false, false);
    }

    ExitTransactionRecord {
        Objects.requireNonNull(manifest, "manifest");
        writtenChunks = Set.copyOf(
                Objects.requireNonNull(writtenChunks, "writtenChunks"));
        if (!manifest.chunks().containsAll(writtenChunks)) {
            throw new IllegalArgumentException("written 区块必须属于 ownership 清单");
        }
        if (!writtenChunksDeclared && !writtenChunks.isEmpty()) {
            throw new IllegalArgumentException("未声明 writtenChunks 时集合必须为空");
        }
        if (writtenChunksDeclared && !committed) {
            throw new IllegalArgumentException("pending 事务不能提前固定 writtenChunks");
        }
        if (cleanupComplete && !committed) {
            throw new IllegalArgumentException("未提交事务不能标记为已清理");
        }
        if (dataApplied && (!committed || abandoned)) {
            throw new IllegalArgumentException("只有非放弃的 committed 事务可标记数据已应用");
        }
        if (abandoned && !committed) {
            throw new IllegalArgumentException("放弃事务必须处于终态");
        }
        if (cleanupComplete && !abandoned && !dataApplied) {
            throw new IllegalArgumentException("正式数据未应用的事务不能完成清理");
        }
    }

    /** 缺少新字段的旧 committed 事务必须继续按 data/preserve marker 规则恢复。 */
    boolean legacyDisposition() {
        return committed && !abandoned && !writtenChunksDeclared;
    }
}
