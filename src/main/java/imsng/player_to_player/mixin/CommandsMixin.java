package imsng.player_to_player.mixin;

import com.mojang.brigadier.ParseResults;
import imsng.player_to_player.group.CommandRelayClient;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.ModifyVariable;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 指令执行探针（Phase 4，规范"服务器的指令处理"的客户端半边入口）。
 * <p>
 * 注入 {@code Commands.performCommand(ParseResults, String)}（1.20.4 所有玩家
 * 指令的汇聚点：签名/无签名聊天指令、{@code performPrefixedCommand} 均经此执行；
 * 数据包函数走 {@code FunctionCommand} 的独立执行链，不经过这里 —— 签名已用
 * javap 核实）：
 * <ul>
 *   <li><b>HEAD（@ModifyVariable）</b>：把 parseResults 换成挂了结果回调探针的
 *       版本（{@code Commands.mapSource} 原版工具方法，仅包装指令源，解析树
 *       不动）—— 探针旁听每个叶子命令的成败；</li>
 *   <li><b>RETURN（@Inject）</b>：结算探针 —— 1.20.4 的指令执行队列在
 *       performCommand 内同步跑完（{@code executeCommandInContext}），RETURN 时
 *       成败已定；全叶子失败或解析失败（回调从未发火）→ 上送服务端逐级路由。</li>
 * </ul>
 * 门控/限速/异常吞没全部在 {@link CommandRelayClient} 内：未接管场景（物理
 * 服务端、单人游戏）压 SKIP 直通，行为零差异；探针自身异常绝不影响原版执行。
 * 两注入点经线程局部栈严格配对（performCommand 可因 /execute 数据包等重入）。
 */
@Mixin(Commands.class)
public abstract class CommandsMixin {

    /** HEAD：换上挂探针回调的指令源（不符合条件时原样放行）。 */
    @ModifyVariable(
            method = "performCommand(Lcom/mojang/brigadier/ParseResults;Ljava/lang/String;)V",
            at = @At("HEAD"), argsOnly = true, ordinal = 0)
    private ParseResults<CommandSourceStack> p2p$attachProbe(
            ParseResults<CommandSourceStack> value,
            ParseResults<CommandSourceStack> parseResults, String command) {
        return CommandRelayClient.onPerformCommand(value, command);
    }

    /** RETURN（含 catch 后的正常返回）：弹出探针结算，失败则上送。 */
    @Inject(
            method = "performCommand(Lcom/mojang/brigadier/ParseResults;Ljava/lang/String;)V",
            at = @At("RETURN"))
    private void p2p$settleProbe(ParseResults<CommandSourceStack> parseResults,
                                 String command, CallbackInfo ci) {
        CommandRelayClient.afterPerformCommand();
    }
}
