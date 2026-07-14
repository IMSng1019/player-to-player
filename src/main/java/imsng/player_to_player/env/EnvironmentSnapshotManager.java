package imsng.player_to_player.env;

import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * 环境快照实时管理器。
 * <p>
 * WatchService 线程只负责收事件和标记 dirty，哈希、复制与 manifest 落盘全部转交
 * IO 线程池。构建使用“dirty + building”单飞状态机：任意时刻最多一轮重活；构建
 * 期间的新变化不会丢失，而是在当前轮结束后立即补建下一版。
 */
public final class EnvironmentSnapshotManager {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env-snapshot");

    private final Path sourceRoot;
    private final EnvironmentPathPolicy pathPolicy;
    private final EnvironmentSnapshotStore store;
    private final long debounceMillis;
    private final long reconciliationMillis;
    private final long retentionMillis;

    private final Object stateLock = new Object();
    private final Map<String, EnvironmentSnapshot> snapshots = new ConcurrentHashMap<>();
    private final Map<WatchKey, Path> watchDirectories = new ConcurrentHashMap<>();

    private volatile EnvironmentSnapshot current;
    private volatile boolean running;
    private boolean dirty;
    private boolean building;
    private boolean maintenanceRunning;
    private int sourceUpdateDepth;
    private ScheduledFuture<?> debounceTask;
    private ScheduledFuture<?> maintenanceTask;
    private WatchService watchService;
    private Thread watchThread;
    private volatile String publishedSourceFingerprint;

    /** 生产构造：1 秒防抖、60 秒校验，旧快照保留时长由配置传入。 */
    public EnvironmentSnapshotManager(Path sourceRoot, Path repositoryRoot,
                                      List<String> extraExclusions,
                                      int retentionMinutes) {
        this(sourceRoot, repositoryRoot, extraExclusions,
                Duration.ofSeconds(1), Duration.ofSeconds(60),
                Duration.ofMinutes(Math.max(10, retentionMinutes)));
    }

    /** 包内构造允许测试缩短时间，不向生产调用方暴露易误配的毫秒级参数。 */
    EnvironmentSnapshotManager(Path sourceRoot, Path repositoryRoot,
                               List<String> extraExclusions,
                               Duration debounce, Duration reconciliation,
                               Duration retention) {
        this.sourceRoot = sourceRoot.toAbsolutePath().normalize();
        this.pathPolicy = EnvironmentPathPolicy.create(extraExclusions);
        this.store = new EnvironmentSnapshotStore(
                this.sourceRoot, repositoryRoot, extraExclusions);
        this.debounceMillis = Math.max(0L, debounce.toMillis());
        this.reconciliationMillis = Math.max(1L, reconciliation.toMillis());
        this.retentionMillis = Math.max(1L, retention.toMillis());
    }

    /** 启动监听并异步构建首份快照；幂等。 */
    public void start() {
        synchronized (stateLock) {
            if (running) {
                return;
            }
            running = true;
            dirty = true;
        }
        try {
            Files.createDirectories(sourceRoot);
            snapshots.putAll(store.loadSnapshots());
            watchService = FileSystems.getDefault().newWatchService();
            registerAllDirectories(sourceRoot);
            watchThread = ThreadPools.namedFactory("p2p-env-watch")
                    .newThread(this::watchLoop);
            watchThread.start();
            scheduleBuild(0L);
            maintenanceTask = ThreadPools.scheduler().scheduleWithFixedDelay(
                    this::runMaintenance,
                    reconciliationMillis, reconciliationMillis, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            stop();
            throw new IllegalStateException("环境快照管理器启动失败: " + sourceRoot, e);
        }
    }

    /** 当前已原子发布的完整快照；首轮构建完成前为 null。 */
    public EnvironmentSnapshot current() {
        EnvironmentSnapshot snapshot = current;
        if (snapshot != null) {
            snapshot.touch();
        }
        return snapshot;
    }

    /** 按 ID 查找仍在保留期内的快照。 */
    public EnvironmentSnapshot find(String snapshotId) {
        if (snapshotId == null || snapshotId.isBlank()) {
            return null;
        }
        synchronized (stateLock) {
            EnvironmentSnapshot snapshot = snapshots.get(snapshotId);
            if (snapshot != null) {
                snapshot.touch();
            }
            return snapshot;
        }
    }

    public EnvironmentManifest.Entry entry(EnvironmentSnapshot snapshot, String path)
            throws IOException {
        return store.entry(snapshot, path);
    }

    public byte[] readChunk(EnvironmentSnapshot snapshot, String path,
                            long offset, int maxBytes) throws IOException {
        return store.readChunk(snapshot, path, offset, maxBytes);
    }

    /** 外部组件可主动请求刷新；多次调用会被防抖合并。 */
    public void requestRefresh() {
        synchronized (stateLock) {
            if (!running) {
                return;
            }
            dirty = true;
            if (building || maintenanceRunning || sourceUpdateDepth > 0) {
                return;
            }
            scheduleBuildLocked(debounceMillis);
        }
    }

    /**
     * 开始一轮外部批量更新（中转端从上级同步时使用）。本方法在 IO 线程调用，可等待
     * 当前构建/维护结束；门控期间 WatchService 仍记录 dirty，但不会发布中间状态。
     */
    public void beginSourceUpdate() throws InterruptedException {
        synchronized (stateLock) {
            while (running && (building || maintenanceRunning)) {
                stateLock.wait();
            }
            sourceUpdateDepth++;
            if (debounceTask != null) {
                debounceTask.cancel(false);
                debounceTask = null;
            }
        }
    }

    /**
     * 结束外部批量更新。
     *
     * @param publish true 表示整轮更新成功，立即构建；false 表示失败，丢弃本轮变化标记，
     *                继续提供上一份不可变快照，等待下一次完整同步
     */
    public void endSourceUpdate(boolean publish) {
        synchronized (stateLock) {
            if (sourceUpdateDepth <= 0) {
                throw new IllegalStateException("endSourceUpdate 调用次数超过 beginSourceUpdate");
            }
            sourceUpdateDepth--;
            if (sourceUpdateDepth == 0) {
                dirty = publish;
                if (running && publish) {
                    scheduleBuildLocked(0L);
                }
                stateLock.notifyAll();
            }
        }
    }

    /** 停止监听和待执行任务；正在运行的构建结束后会因 running=false 放弃发布。 */
    public void stop() {
        WatchService watcher;
        synchronized (stateLock) {
            if (!running && watchService == null) {
                return;
            }
            running = false;
            dirty = false;
            if (debounceTask != null) {
                debounceTask.cancel(false);
                debounceTask = null;
            }
            if (maintenanceTask != null) {
                maintenanceTask.cancel(false);
                maintenanceTask = null;
            }
            watcher = watchService;
            watchService = null;
            stateLock.notifyAll();
        }
        if (watcher != null) {
            try {
                watcher.close();
            } catch (IOException e) {
                LOGGER.debug("关闭环境目录监听器失败", e);
            }
        }
        watchDirectories.clear();
    }

    private void scheduleBuild(long delayMillis) {
        synchronized (stateLock) {
            if (running) {
                scheduleBuildLocked(delayMillis);
            }
        }
    }

    private void scheduleBuildLocked(long delayMillis) {
        if (debounceTask != null) {
            debounceTask.cancel(false);
        }
        debounceTask = ThreadPools.scheduler().schedule(
                this::beginBuild, delayMillis, TimeUnit.MILLISECONDS);
    }

    private void beginBuild() {
        synchronized (stateLock) {
            debounceTask = null;
            if (!running || building || maintenanceRunning
                    || sourceUpdateDepth > 0 || !dirty) {
                return;
            }
            dirty = false;
            building = true;
        }
        ThreadPools.io().execute(this::buildOnce);
    }

    private void buildOnce() {
        try {
            String fingerprintBefore = store.sourceFingerprint();
            EnvironmentSnapshot built = store.buildSnapshot();
            String fingerprintAfter = store.sourceFingerprint();
            if (running) {
                snapshots.put(built.snapshotId(), built);
                current = built;
                publishedSourceFingerprint = fingerprintAfter;
                LOGGER.info("环境快照已发布: id={} 文件数={}",
                        built.snapshotId(), built.manifest().files().size());
                if (!fingerprintBefore.equals(fingerprintAfter)) {
                    // 构建窗口内源目录又变化：当前版本仍然内部一致，但需立即追赶最新状态。
                    requestRefresh();
                }
            }
        } catch (Exception e) {
            // 失败时绝不清空 current：已经发布的上一版仍可继续服务正在加入的客户端。
            LOGGER.error("环境快照构建失败，继续保留上一有效版本", e);
        } finally {
            synchronized (stateLock) {
                building = false;
                stateLock.notifyAll();
                if (running && dirty) {
                    scheduleBuildLocked(0L);
                }
            }
        }
    }

    /** 60 秒兜底校验 + 旧版本/孤儿 Blob 回收。 */
    private void runMaintenance() {
        synchronized (stateLock) {
            if (!running || building || maintenanceRunning || sourceUpdateDepth > 0) {
                return;
            }
            maintenanceRunning = true;
        }
        try {
            String observed = store.sourceFingerprint();
            String published = publishedSourceFingerprint;
            if (published == null || !published.equals(observed)) {
                requestRefresh();
            }
            cleanupExpiredSnapshots();
        } catch (Exception e) {
            if (running) {
                LOGGER.warn("环境快照周期校验/回收失败，下个周期重试", e);
            }
        } finally {
            synchronized (stateLock) {
                maintenanceRunning = false;
                stateLock.notifyAll();
                if (running && dirty && !building) {
                    scheduleBuildLocked(0L);
                }
            }
        }
    }

    private void cleanupExpiredSnapshots() throws IOException {
        long cutoff = System.currentTimeMillis() - retentionMillis;
        List<String> expired = new ArrayList<>();
        List<EnvironmentSnapshot> retained;
        synchronized (stateLock) {
            EnvironmentSnapshot active = current;
            for (EnvironmentSnapshot snapshot : snapshots.values()) {
                if (snapshot != active && snapshot.lastAccessMillis() < cutoff) {
                    expired.add(snapshot.snapshotId());
                }
            }
            for (String snapshotId : expired) {
                snapshots.remove(snapshotId);
            }
            retained = List.copyOf(snapshots.values());
        }

        for (String snapshotId : expired) {
            store.deleteSnapshotManifest(snapshotId);
            LOGGER.info("已回收过期环境快照: {}", snapshotId);
        }
        store.deleteUnreferencedBlobs(retained);
        store.cleanupStaging(cutoff);
    }

    private void watchLoop() {
        while (running) {
            try {
                WatchKey key = watchService.take();
                Path directory = watchDirectories.get(key);
                if (directory == null) {
                    key.reset();
                    continue;
                }
                boolean relevantChange = false;
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        relevantChange = true;
                        continue;
                    }
                    Object context = event.context();
                    if (!(context instanceof Path childName)) {
                        continue;
                    }
                    Path changed = directory.resolve(childName);
                    String relative = pathPolicy.normalize(
                            sourceRoot.relativize(changed).toString());
                    if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE
                            && Files.isDirectory(changed) && pathPolicy.includes(relative)) {
                        registerAllDirectories(changed);
                    }
                    if (pathPolicy.includes(relative)) {
                        relevantChange = true;
                    }
                }
                if (!key.reset()) {
                    watchDirectories.remove(key);
                }
                if (relevantChange) {
                    requestRefresh();
                }
            } catch (ClosedWatchServiceException ignored) {
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (running) {
                    LOGGER.warn("处理环境目录变化事件失败，将等待后续事件/周期校验", e);
                }
            }
        }
    }

    private void registerAllDirectories(Path start) throws IOException {
        WatchService watcher = watchService;
        if (watcher == null || !Files.isDirectory(start)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(start)) {
            for (Path directory : walk.filter(Files::isDirectory).toList()) {
                String relative = pathPolicy.normalize(
                        sourceRoot.relativize(directory).toString());
                if (!relative.isEmpty() && !pathPolicy.includes(relative)) {
                    continue;
                }
                WatchKey key = directory.register(watcher,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_DELETE);
                watchDirectories.put(key, directory);
            }
        }
    }
}
