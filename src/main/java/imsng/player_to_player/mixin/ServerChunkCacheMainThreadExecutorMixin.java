package imsng.player_to_player.mixin;

import imsng.player_to_player.group.GroupControlTasks;
import net.minecraft.server.level.ServerChunkCache;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * 同步区块等待期间也推进预同步主线程采集，解除所有权申请与合并之间的循环等待。
 * 目标是 1.20.4 的包私有内部类，因此使用 targets；outer 字段 field_18810 已按本地
 * Mojang→intermediary 映射核验，仍参与正常 remap，不能把整个 Mixin 设为 remap=false。
 * 这里只轮询专用控制队列；普通服务器指令/玩家输入继续遵循原版线程与 tick 顺序。
 */
@Mixin(targets = "net.minecraft.server.level.ServerChunkCache$MainThreadExecutor")
public abstract class ServerChunkCacheMainThreadExecutorMixin {
    @Shadow
    @Final
    private ServerChunkCache field_18810;

    @Inject(method = "pollTask()Z", at = @At("HEAD"), cancellable = true)
    private void player_to_player$pollMigrationControlTasks(CallbackInfoReturnable<Boolean> cir) {
        if (GroupControlTasks.poll(field_18810.getLevel().getServer())) {
            cir.setReturnValue(true);
        }
    }
}
