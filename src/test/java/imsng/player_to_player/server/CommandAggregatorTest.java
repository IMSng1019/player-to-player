package imsng.player_to_player.server;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 广播执行回执聚合的规范口径锁定（决策 4）：
 * "如果被执行 则将执行端的消息返回发出指令的端"；
 * "在指令被传递完所有的端都报错后则返回本地的报错信息"。
 */
class CommandAggregatorTest {

    private final List<String> replies = new ArrayList<>();

    private CommandAggregator aggregator(int total) {
        return new CommandAggregator(total, replies::add);
    }

    @Test
    @DisplayName("首个成功即应答，后续结果不再改变回执")
    void firstSuccessWins() {
        CommandAggregator aggregator = aggregator(2);
        aggregator.record(false, false, "别的组报错");
        assertEquals(0, replies.size(), "还有目标没回报，不能提前应答");
        aggregator.record(true, true, "执行端消息 A");
        assertEquals(List.of("执行端消息 A"), replies);
        aggregator.record(false, true, "执行端消息 B");
        assertEquals(List.of("执行端消息 A"), replies, "只回一条");
    }

    @Test
    @DisplayName("全部失败：优先回发起组那一次的报错（规范\"本地报错\"）")
    void allFailedPrefersOriginFailure() {
        CommandAggregator aggregator = aggregator(3);
        aggregator.record(false, false, "他组报错一");
        aggregator.record(true, false, "本地的报错信息");
        assertEquals(0, replies.size());
        aggregator.record(false, false, "他组报错二");
        assertEquals(List.of("本地的报错信息"), replies);
    }

    @Test
    @DisplayName("发起组不在目标集合时，回第一个失败端的文案")
    void allFailedWithoutOrigin() {
        CommandAggregator aggregator = aggregator(2);
        aggregator.record(false, false, "持有组的报错");
        aggregator.record(false, false, "");
        assertEquals(List.of("持有组的报错"), replies);
    }

    @Test
    @DisplayName("全失败且无任何文案：给兜底说明，不能静默")
    void allFailedWithoutText() {
        CommandAggregator aggregator = aggregator(1);
        aggregator.record(false, false, "");
        assertEquals(List.of(CommandAggregator.NOTHING_EXECUTED), replies);
    }

    @Test
    @DisplayName("超时/重复回报不改变结论（幂等）")
    void duplicateAndLateReportsAreIdempotent() {
        List<String> local = new ArrayList<>();
        CommandAggregator aggregator = new CommandAggregator(2, local::add);
        aggregator.record(true, false, "本地的报错信息");
        aggregator.record(true, false, "本地的报错信息"); // 迟到重复
        aggregator.record(false, false, "他组报错");
        assertEquals(List.of("本地的报错信息"), local);

        List<String> second = new ArrayList<>();
        CommandAggregator succeeded = new CommandAggregator(1, second::add);
        succeeded.record(true, true, "ok");
        succeeded.record(true, false, "迟到的失败");
        assertEquals(1, second.size(), "成功已定案后不再追加");
        assertTrue(second.contains("ok"));
    }
}
