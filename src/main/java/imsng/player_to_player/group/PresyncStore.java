package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 预同步暂存库（Phase 3，规范"继承：……将原来两个主客户端已有的文件综合到新的
 * 组客户端再向服务端申请需要加载的区块"）。
 * <p>
 * 合并时让出方 A 把区块状态经 P2P 直接流式发给接管方 B（{@code PresyncReceiver}
 * 写入本库），<b>不占服务端下行带宽</b>；B 的集成服务端随后加载这些区块时，
 * {@code ChunkMapMixin} 的申请门控在"授予"之后<b>优先消费暂存数据</b>——
 * 它与服务端存档同源（A 的存盘上行与 P2P 镜像来自同一份 NBT）且省一次
 * CHUNK_DATA_REQUEST 往返。消费即移除（一次性），未被消费的条目随合并会话
 * 结束整体清空。
 * <p>
 * <b>容量上界</b>：{@value #MAX_STAGED_CHUNKS} 个区块（超限丢弃新条目并告警 ——
 * 服务端存档仍有同样数据，B 侧回退拉取路径，只损失带宽不损失数据）。
 * <p>
 * 线程模型：写入方是预同步接收线程（io 池），消费方是 B 集成服务端的区块
 * 加载链（Netty/io/主线程皆可能）——ConcurrentHashMap 单键原子操作，无外部锁。
 */
public final class PresyncStore {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/presync-store");

    /** 暂存区块数上限：两组视距并集的量级为数千，超限说明流程异常。 */
    private static final int MAX_STAGED_CHUNKS = 4096;

    /** 暂存表：区块 → 反序列化后的 NBT（接收线程已完成解压与校验）。 */
    private static final Map<ChunkKey, CompoundTag> STAGED = new ConcurrentHashMap<>();

    private PresyncStore() {
    }

    /** 暂存一个区块（预同步接收线程；同键覆盖 —— 增量追赶天然取最新）。 */
    public static void put(ChunkKey key, CompoundTag tag) {
        if (STAGED.size() >= MAX_STAGED_CHUNKS && !STAGED.containsKey(key)) {
            LOGGER.warn("预同步暂存超限（{}），丢弃 {}（B 将回退服务端拉取，无数据损失）",
                    MAX_STAGED_CHUNKS, key.asString());
            return;
        }
        STAGED.put(key, tag);
    }

    /** 消费一个暂存区块（加载门控调用；消费即移除）。无暂存返回 null。 */
    public static CompoundTag take(ChunkKey key) {
        return STAGED.remove(key);
    }

    /** 当前暂存量（诊断/进度日志用）。 */
    public static int size() {
        return STAGED.size();
    }

    /** 整体清空（合并会话结束/中止、世界会话拆除时调用；幂等）。 */
    public static void clear() {
        int n = STAGED.size();
        STAGED.clear();
        if (n > 0) {
            LOGGER.info("预同步暂存已清空: {} 个未消费区块", n);
        }
    }
}
