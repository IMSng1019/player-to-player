package imsng.player_to_player.server;

import java.util.function.Consumer;

/**
 * 广播执行的应答聚合（服务端；规范"报错提示规则"与"如果被执行 则将执行端的消息
 * 返回发出指令的端"）。
 *
 * <p>规则（2026-10-15 决策 4：反馈在服务端聚合，发起者只收规范规定的那两条）：</p>
 * <ol>
 *   <li><b>首个成功即应答</b>：把该执行端的消息回给发起者，后续结果一律忽略
 *       —— 快速收敛，且不让一个卡住的组拖住整条回执；</li>
 *   <li><b>全部失败才应答</b>：优先回发起组自己那一次的报错文案（即规范说的
 *       "本地的报错信息"—— 发起组也在执行集合里，它的失败文案就是原版本地报错），
 *       否则回第一个失败端的文案，都没有则回一句兜底说明。</li>
 * </ol>
 *
 * <p>超时与断连由调用方折算成"失败"后再记录（{@code request()} 的 future 会以
 * 异常完成），所以这里不需要定时器。</p>
 *
 * <p>线程模型：{@code record} 会在多条连接的事件循环上并发回调，
 * 全部状态由 {@code synchronized} 串行化（临界区只有几次字符串比较与一次发送）。</p>
 */
public final class CommandAggregator {

    /** 兜底文案：所有目标都失败且没有任何报错文本时。 */
    static final String NOTHING_EXECUTED = "指令未在任何组执行成功";

    private final int total;
    private final Consumer<String> reply;

    private int remaining;
    private boolean replied;
    private String originFailure;
    private String firstFailure;

    /**
     * @param total 目标数（每个目标恰好记录一次）
     * @param reply 应答回调（只被调用一次；参数是给发起者看的文案，可为空串）
     */
    public CommandAggregator(int total, Consumer<String> reply) {
        this.total = Math.max(1, total);
        this.remaining = this.total;
        this.reply = reply;
    }

    /**
     * 记录一个目标的执行结果。
     *
     * @param origin   该目标是否为发起组自己那一次
     * @param executed 是否执行成功
     * @param message  执行端文案（成功消息 / 报错文本，可为空）
     */
    public synchronized void record(boolean origin, boolean executed, String message) {
        if (remaining <= 0) {
            return; // 幂等：重复回报/迟到回报不再计数
        }
        remaining--;
        String text = message == null ? "" : message;
        if (executed) {
            if (!replied) {
                replied = true;
                reply.accept(text);
            }
            return;
        }
        if (origin) {
            if (originFailure == null) {
                originFailure = text;
            }
        } else if (firstFailure == null) {
            firstFailure = text;
        }
        if (remaining == 0 && !replied) {
            replied = true;
            reply.accept(bestFailure());
        }
    }

    /** 全部失败时的文案选择（发起组的报错优先）。 */
    private String bestFailure() {
        if (originFailure != null && !originFailure.isEmpty()) {
            return originFailure;
        }
        if (firstFailure != null && !firstFailure.isEmpty()) {
            return firstFailure;
        }
        return NOTHING_EXECUTED;
    }
}
