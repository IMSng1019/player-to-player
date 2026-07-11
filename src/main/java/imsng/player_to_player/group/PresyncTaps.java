package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * 区块写入分接器（Phase 3，规范"增量追赶"的数据源）。
 * <p>
 * {@code ChunkUploadService.enqueue} 是主客户端集成服务端"区块发生了值得写盘的
 * 变更"的唯一汇聚点（ChunkStorageMixin 捕获）。预同步期间（合并让出方 A /
 * 分离中的原主），发送端把自己挂到本分接器上，即可零侵入地拿到快照期之后的
 * <b>全部增量</b> —— 与上行服务共享同一份 NBT（不可变快照），无需复制。
 * <p>
 * 无消费者时（绝大多数时间）开销为一次空列表遍历，可忽略。
 * <p>
 * 线程模型：offer 在集成服务端主线程（enqueue 调用点），消费者<b>不得阻塞</b>
 * （只做过滤 + 入队，重活转 io 池）；挂接/摘除可来自任意线程（COW 列表）。
 */
public final class PresyncTaps {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/presync-tap");

    /** 活跃消费者（合并/分离各自的预同步发送端；通常 0~2 个）。 */
    private static final List<BiConsumer<ChunkKey, CompoundTag>> CONSUMERS =
            new CopyOnWriteArrayList<>();

    private PresyncTaps() {
    }

    /** 挂接一个增量消费者（预同步开始时）。 */
    public static void add(BiConsumer<ChunkKey, CompoundTag> consumer) {
        CONSUMERS.add(consumer);
    }

    /** 摘除消费者（预同步结束/中止时；幂等）。 */
    public static void remove(BiConsumer<ChunkKey, CompoundTag> consumer) {
        CONSUMERS.remove(consumer);
    }

    /** 分发一次区块写入（ChunkUploadService.enqueue 调用；主线程，消费者不得阻塞）。 */
    static void offer(ChunkKey key, CompoundTag tag) {
        for (BiConsumer<ChunkKey, CompoundTag> consumer : CONSUMERS) {
            try {
                consumer.accept(key, tag);
            } catch (Exception e) {
                LOGGER.error("预同步分接器消费者异常: {}", key.asString(), e);
            }
        }
    }
}
