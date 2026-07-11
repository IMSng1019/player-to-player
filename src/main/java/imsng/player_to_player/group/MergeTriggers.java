package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * 合并触发桥（Phase 3，src/main 与 src/client 之间的静态桥梁，
 * 模式与 {@link GroupRuntime} 一致）。
 * <p>
 * 规范"预连接"的入口是<b>区块申请被拒</b>：{@link ChunkClaimClient} 收到
 * CHUNK_CLAIM_RESPONSE(granted=false) 时携带 blockingGroup —— 该信号经本类
 * 转发给 client 源集的 MergeClient（它持有 Minecraft/世界切换能力），由其决定
 * 是否向服务端发 MERGE_REQUEST。src/main 不能反向依赖 client 源集，故用
 * 可挂接的静态消费者解耦。
 * <p>
 * <b>抑制窗口</b>：分离（split）刚完成时，新主客户端的首轮区块申请可能仍撞上
 * 原组尚未释放完的区块 —— 若立刻触发合并会把刚分出去的组又并回来，形成
 * 分离↔合并震荡。{@link #suppressFor} 设置一段静默期，期间触发信号只记日志。
 * <p>
 * 线程模型：回调在 Netty 事件循环/scheduler 线程上触发，消费者不得阻塞
 * （MergeClient 内部自行转 io 池）；字段 volatile，挂接/清除与触发可来自不同线程。
 */
public final class MergeTriggers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/merge-trigger");

    /** 触发消费者：(阻塞组 groupId, 被阻塞区块)；null = 未挂接（Phase 2 行为：仅重试）。 */
    private static volatile BiConsumer<UUID, ChunkKey> consumer;

    /** 抑制截止时刻（epoch millis）；之前的触发信号全部忽略。 */
    private static volatile long suppressUntilMillis;

    private MergeTriggers() {
    }

    /** 挂接触发消费者（MergeClient 在世界会话建立时调用；null 清除）。 */
    public static void setConsumer(BiConsumer<UUID, ChunkKey> c) {
        consumer = c;
    }

    /** 设置抑制窗口（分离完成后调用，单位毫秒）。 */
    public static void suppressFor(long millis) {
        suppressUntilMillis = System.currentTimeMillis() + millis;
    }

    /** 区块申请被拒时由 {@link ChunkClaimClient} 调用（Netty 线程，不得阻塞）。 */
    static void onClaimBlocked(UUID blockingGroup, ChunkKey blockedChunk) {
        if (blockingGroup == null) {
            return;
        }
        if (System.currentTimeMillis() < suppressUntilMillis) {
            LOGGER.debug("合并触发被抑制窗口忽略: 阻塞组={} 区块={}",
                    blockingGroup, blockedChunk.asString());
            return;
        }
        BiConsumer<UUID, ChunkKey> c = consumer;
        if (c != null) {
            try {
                c.accept(blockingGroup, blockedChunk);
            } catch (Exception e) {
                LOGGER.error("合并触发消费者异常", e);
            }
        }
    }
}
