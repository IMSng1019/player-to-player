package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 区块上行服务（主客户端侧，Phase 2；规范"服务端需要实时计算区块的更改更新
 * 本地区块文件"——数据源就是主客户端的实时上行）。
 * <p>
 * 数据来源：{@code mixin.ChunkStorageMixin} 在集成服务端<b>每次把区块写盘</b>
 * （自动存盘 / 卸载存盘 / 停服 saveAll）时捕获 (ChunkKey, CompoundTag)，调用
 * {@link #enqueue}。选择"存盘时上行"而非"每 tick 上行"：MC 自身的 unsaved 脏标记
 * 已经天然去抖，上行频率与原版写盘频率一致，带宽可控且不额外拖慢 tick。
 *
 * <h2>队列设计</h2>
 * <ul>
 *   <li><b>按区块合并</b>（LinkedHashMap，同键覆盖）：存盘风暴里同一区块的多次
 *       写只留最新一份 —— 上行永远是幂等的"最终态覆盖"，丢中间版本无损；</li>
 *   <li><b>有界</b>（{@value #MAX_PENDING_CHUNKS} 个区块）：超限丢最旧（该区块
 *       数据仍在本地存档里，下次存盘会再次入队），绝不反压集成服务端主线程；</li>
 *   <li><b>单工作线程</b>（io 池）串行序列化 + 发送：NBT gzip 压缩是 CPU 活，
 *       enqueue（主线程调用）只做 map put。</li>
 * </ul>
 * 过滤：只上行仍在占用中的区块（{@link ChunkClaimClient#isClaimed}）——释放
 * （最终数据已随 CHUNK_RELEASE 上行）之后的迟到存盘直接丢弃。
 */
public final class ChunkUploadService {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/chunk-upload");

    /** 待上行队列的区块数上限（合并后；视距内区块量级为数百）。 */
    private static final int MAX_PENDING_CHUNKS = 8192;

    /** 实时单帧大小上限；超限快照保留到正常退出时经分片事务提交。 */
    private static final int MAX_COMPRESSED_BYTES = Protocol.MAX_FRAME_BYTES - 64 * 1024;

    private final ControlConnection conn;
    private final UUID groupId;
    private final Executor executor;
    private final Consumer<Runnable> retryScheduler;
    private final Predicate<ChunkKey> claimedPredicate;

    /** 待上行：key → 最新 NBT（LinkedHashMap 保序 + 同键覆盖；以自身为锁）。 */
    private final LinkedHashMap<ChunkKey, CompoundTag> queue = new LinkedHashMap<>();

    /** 每个活跃区块最近一次本地存盘快照；退出事务补齐最终数据用。 */
    private final Map<ChunkKey, CompoundTag> latestSnapshots = new java.util.HashMap<>();

    /** 工作线程是否在跑（enqueue 只在队列从空变非空时投递一次任务）。 */
    private boolean workerRunning;

    private volatile boolean shutdown;

    public ChunkUploadService(ControlConnection conn, UUID groupId) {
        this(conn, groupId, ThreadPools.io(),
                task -> ThreadPools.scheduler().schedule(task, 2, TimeUnit.SECONDS),
                key -> {
                    ChunkClaimClient claims = GroupRuntime.claims();
                    return claims != null && claims.isClaimed(key);
                });
    }

    ChunkUploadService(ControlConnection conn, UUID groupId, Executor executor,
                       Consumer<Runnable> retryScheduler,
                       Predicate<ChunkKey> claimedPredicate) {
        this.conn = conn;
        this.groupId = groupId;
        this.executor = executor;
        this.retryScheduler = retryScheduler;
        this.claimedPredicate = claimedPredicate;
    }

    /**
     * 区块 NBT 入队上行（集成服务端主线程调用；只做 map 操作，微秒级）。
     * tag 是 ChunkSerializer.write 刚生成的新对象，此后无人再改，跨线程只读安全。
     */
    public void enqueue(ChunkKey key, CompoundTag tag) {
        if (shutdown) {
            return;
        }
        // 预同步分接（Phase 3）：合并/分离的发送端在此拿到快照后的全部增量
        // （无消费者时是一次空列表遍历，可忽略）
        PresyncTaps.offer(key, tag);
        synchronized (queue) {
            CompoundTag snapshot = tag.copy();
            queue.put(key, snapshot);
            latestSnapshots.put(key, snapshot);
            if (queue.size() > MAX_PENDING_CHUNKS) {
                // 丢最旧：该区块数据仍在本地存档，下次存盘会重新入队
                Map.Entry<ChunkKey, CompoundTag> oldest = queue.entrySet().iterator().next();
                LOGGER.warn("上行队列超限，暂弃最旧区块（本地存档仍有，后续存盘会补传）: {}",
                        oldest.getKey().asString());
                queue.remove(oldest.getKey());
            }
            if (!workerRunning) {
                workerRunning = true;
                executor.execute(this::drainLoop);
            }
        }
    }

    /** 工作线程：串行取队首 → gzip → CHUNK_DATA_UPLOAD，直到队空。 */
    private void drainLoop() {
        while (true) {
            ChunkKey key;
            CompoundTag tag;
            synchronized (queue) {
                Map.Entry<ChunkKey, CompoundTag> head =
                        queue.isEmpty() ? null : queue.entrySet().iterator().next();
                if (head == null || shutdown) {
                    workerRunning = false;
                    return;
                }
                key = head.getKey();
                tag = head.getValue();
            }
            try {
                // 释放后迟到的存盘：最终数据已随 CHUNK_RELEASE 上行，这里直接丢弃
                if (!claimedPredicate.test(key)) {
                    removeIfCurrent(key, tag);
                    synchronized (queue) {
                        latestSnapshots.remove(key);
                    }
                    continue;
                }
                byte[] gzip = compress(tag);
                if (gzip == null || gzip.length > MAX_COMPRESSED_BYTES) {
                    // 保留 latestSnapshots 供退出事务分片提交，但移除当前实时队首，
                    // 否则 while 循环会反复处理同一不可发送项形成 100% CPU 死循环。
                    LOGGER.warn("区块 NBT 压缩后超过实时帧长上限，留待退出分片提交: {}",
                            key.asString());
                    removeIfCurrent(key, tag);
                    continue;
                }
                JsonObject json = new JsonObject();
                json.addProperty("dimension", key.dimension());
                json.addProperty("x", key.x());
                json.addProperty("z", key.z());
                json.addProperty("groupId", groupId.toString());
                ControlMessage response = conn.request(ControlMessage.of(
                                MessageType.CHUNK_DATA_UPLOAD, json, gzip))
                        .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (response.type() != MessageType.CHUNK_DATA_UPLOAD
                        || !imsng.player_to_player.util.JsonUtil.getBoolean(
                        response.json(), "written", false)) {
                    throw new IllegalStateException("区块上行未获写入确认");
                }
                removeIfCurrent(key, tag);
                LOGGER.debug("区块上行: {} ({} 字节压缩)", key.asString(), gzip.length);
            } catch (Exception e) {
                LOGGER.warn("区块上行失败，保留最新状态等待重试: {}", key.asString(), e);
                synchronized (queue) {
                    workerRunning = false;
                }
                retryScheduler.accept(() -> {
                    synchronized (queue) {
                        if (!workerRunning && !queue.isEmpty() && !shutdown) {
                            workerRunning = true;
                            executor.execute(this::drainLoop);
                        }
                    }
                });
                return;
            }
        }
    }

    private void removeIfCurrent(ChunkKey key, CompoundTag tag) {
        synchronized (queue) {
            if (queue.get(key) == tag) {
                queue.remove(key);
            }
        }
    }

    /** 取得最近一次本地存盘快照的副本；没有捕获过返回 null。 */
    public CompoundTag latestSnapshot(ChunkKey key) {
        synchronized (queue) {
            CompoundTag tag = latestSnapshots.get(key);
            return tag != null ? tag.copy() : null;
        }
    }

    /**
     * 同步压缩一份区块 NBT（供 CHUNK_RELEASE 最终数据复用；io/事件线程调用）。
     *
     * @return gzip 字节；失败返回 null
     */
    public static byte[] compress(CompoundTag tag) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(64 * 1024);
            NbtIo.writeCompressed(tag, bos);
            return bos.toByteArray();
        } catch (Exception e) {
            LOGGER.warn("区块 NBT 压缩失败", e);
            return null;
        }
    }

    /** 关停：丢弃残余队列（停服路径的最终数据由 CHUNK_UNLOAD 释放流程负责）。 */
    public void shutdown() {
        shutdown = true;
        synchronized (queue) {
            queue.clear();
            latestSnapshots.clear();
        }
    }
}
