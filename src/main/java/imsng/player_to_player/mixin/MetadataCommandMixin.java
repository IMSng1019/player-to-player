package imsng.player_to_player.mixin;

import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.mojang.brigadier.ResultConsumer;
import com.mojang.brigadier.context.CommandContext;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.metadata.MetadataCommands;
import imsng.player_to_player.metadata.MetadataService;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.execution.tasks.ExecuteCommand;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;

/** 叶命令执行收口，覆盖聊天、控制台、RCON、命令方块和函数的相同原版调用点。 */
@Mixin(ExecuteCommand.class)
public abstract class MetadataCommandMixin {
    @WrapOperation(method = "execute(Lnet/minecraft/commands/ExecutionCommandSource;Lnet/minecraft/commands/execution/ExecutionContext;Lnet/minecraft/commands/execution/Frame;)V",
            at = @At(value = "INVOKE", target = "Lcom/mojang/brigadier/context/ContextChain;runExecutable(Lcom/mojang/brigadier/context/CommandContext;Ljava/lang/Object;Lcom/mojang/brigadier/ResultConsumer;Z)I", remap = false))
    private int player_to_player$globalCommand(CommandContext<?> context, Object source,
                                               ResultConsumer<Object> consumer, boolean forked,
                                               Operation<Integer> original) {
        if (imsng.player_to_player.metadata.MetadataRuntime.isApplying()
                || !(source instanceof CommandSourceStack stack) || !MetadataCommands.isGlobal(MetadataCommands.leaf(context))) {
            return original.call(context, source, consumer, forked);
        }
        if (GroupRuntime.isManagedServer(stack.getServer())) {
            if (MetadataCommands.isLocalClockQuery(MetadataCommands.leaf(context)))
                return original.call(context, source, consumer, forked);
            return MetadataCommands.forward(context, stack);
        }
        // 原版ResultConsumer和反馈可能有副作用（execute store success），都在durable提交后发布。
        var feedback = new imsng.player_to_player.group.CommandFeedback();
        boolean[] completed = {false};
        boolean[] success = {false};
        int[] value = {0};
        var buffered = stack.withSource(feedback).withCallback((ok, result) -> {
            completed[0] = true; success[0] = ok; value[0] = result;
        });
        ResultConsumer<Object> bufferedConsumer = (executed, ok, result) -> {
            completed[0] = true; success[0] = ok; value[0] = result;
        };
        @SuppressWarnings("unchecked")
        CommandContext<Object> bufferedContext = ((CommandContext<Object>) context).copyFor(buffered);
        Integer bossResult = MetadataService.tryBossPlayers(bufferedContext, buffered);
        int result = bossResult == null ? original.call(context, buffered, bufferedConsumer, forked) : bossResult;
        // 中心的原版入口先产生字段结果，再同步耐久提交，之后才离开命令回调。
        MetadataService.afterLocalCommand(stack.getServer());
        if (!feedback.joined().isEmpty()) stack.sendSystemMessage(net.minecraft.network.chat.Component.literal(feedback.joined()));
        if (completed[0]) stack.callback().onResult(success[0], value[0]);
        return result;
    }
}
