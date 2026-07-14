package imsng.player_to_player.env;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 已发布的不可变环境快照。
 * <p>
 * 清单对象本身不可变，{@code snapshotId} 等于完整清单的全局哈希；唯一可变状态是
 * 最后访问时间，它只用于旧版本回收，不参与内容一致性判定。
 */
public final class EnvironmentSnapshot {

    private final String snapshotId;
    private final EnvironmentManifest manifest;
    private final long createdAtMillis;
    private final AtomicLong lastAccessMillis;

    public EnvironmentSnapshot(EnvironmentManifest manifest, long createdAtMillis) {
        this(manifest.globalHash(), manifest, createdAtMillis, createdAtMillis);
    }

    public EnvironmentSnapshot(String snapshotId, EnvironmentManifest manifest,
                               long createdAtMillis, long lastAccessMillis) {
        if (snapshotId == null || snapshotId.isBlank()) {
            throw new IllegalArgumentException("snapshotId 不能为空");
        }
        if (manifest == null || !snapshotId.equals(manifest.globalHash())) {
            throw new IllegalArgumentException("snapshotId 必须等于完整清单全局哈希");
        }
        this.snapshotId = snapshotId;
        this.manifest = manifest;
        this.createdAtMillis = createdAtMillis;
        this.lastAccessMillis = new AtomicLong(Math.max(createdAtMillis, lastAccessMillis));
    }

    public String snapshotId() {
        return snapshotId;
    }

    public EnvironmentManifest manifest() {
        return manifest;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    public long lastAccessMillis() {
        return lastAccessMillis.get();
    }

    /** 网络线程读取清单或文件时刷新访问时间，保证慢速下载期间快照不会被回收。 */
    public void touch() {
        lastAccessMillis.set(System.currentTimeMillis());
    }
}
