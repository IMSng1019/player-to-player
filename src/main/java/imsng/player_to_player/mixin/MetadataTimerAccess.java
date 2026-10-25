package imsng.player_to_player.mixin;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.world.level.timers.TimerQueue;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Invoker;

/** 恢复原版计时器时复用其回调注册表及校验规则，绝不把函数当作玩家指令执行。 */
@Mixin(TimerQueue.class)
public interface MetadataTimerAccess {
    @Invoker("loadEvent")
    void player_to_player$loadEvent(CompoundTag event);
}
