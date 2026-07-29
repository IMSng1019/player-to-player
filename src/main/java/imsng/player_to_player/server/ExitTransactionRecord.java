package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitManifest;

import java.util.Objects;

/** 持久化的退出事务元数据；大块 NBT 由客户端在重连后幂等重放。 */
record ExitTransactionRecord(ExitManifest manifest, boolean committed,
                             boolean dataApplied, boolean cleanupComplete,
                             boolean abandoned) {

    ExitTransactionRecord(ExitManifest manifest, boolean committed) {
        this(manifest, committed, false, false, false);
    }

    ExitTransactionRecord {
        Objects.requireNonNull(manifest, "manifest");
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
}
