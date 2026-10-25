package imsng.player_to_player.mixin;

import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.llamalad7.mixinextras.injector.v2.WrapWithCondition;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.metadata.MetadataRuntime;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.players.SleepStatus;
import net.minecraft.world.level.GameRules;
import net.minecraft.world.level.dimension.end.EndDragonFight;
import net.minecraft.world.level.timers.TimerQueue;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;

/**
 * 计算端保留本组 Time、方块/流体计划 tick 和全部模拟，只关闭重复的全局状态推进。
 * 天气仅屏蔽计时器分支，保留原版雨量平滑与玩家通知；不能简单取消整个天气方法。
 */
@Mixin(ServerLevel.class)
public abstract class MetadataGlobalTickMixin {
    @WrapWithCondition(method = "tickTime", at = @At(value = "INVOKE",
            target = "Lnet/minecraft/server/level/ServerLevel;setDayTime(J)V"))
    private boolean player_to_player$globalDayTime(ServerLevel level, long value) {
        return !GroupRuntime.isManagedLevel(level);
    }

    @WrapWithCondition(method = "tickTime", at = @At(value = "INVOKE",
            target = "Lnet/minecraft/world/level/timers/TimerQueue;tick(Ljava/lang/Object;J)V"))
    private boolean player_to_player$globalTimers(TimerQueue<?> queue, Object server, long time) {
        return !GroupRuntime.isManagedServer((MinecraftServer) server);
    }

    @WrapOperation(method = "advanceWeatherCycle", at = @At(value = "INVOKE",
            target = "Lnet/minecraft/world/level/GameRules;getBoolean(Lnet/minecraft/world/level/GameRules$Key;)Z"))
    private boolean player_to_player$globalWeather(GameRules rules, GameRules.Key<GameRules.BooleanValue> key,
                                                  Operation<Boolean> original) {
        return !GroupRuntime.isManagedLevel((ServerLevel) (Object) this) && original.call(rules, key);
    }

    @WrapOperation(method = "tick(Ljava/util/function/BooleanSupplier;)V", at = @At(value = "INVOKE",
            target = "Lnet/minecraft/server/players/SleepStatus;areEnoughSleeping(I)Z"))
    private boolean player_to_player$globalSleep(SleepStatus status, int percentage, Operation<Boolean> original) {
        return !GroupRuntime.isManagedLevel((ServerLevel) (Object) this) && original.call(status, percentage);
    }

    @WrapWithCondition(method = "tick(Ljava/util/function/BooleanSupplier;)V", at = @At(value = "INVOKE",
            target = "Lnet/minecraft/world/level/dimension/end/EndDragonFight;tick()V"))
    private boolean player_to_player$dragonOwner(EndDragonFight fight) {
        ServerLevel level = (ServerLevel) (Object) this;
        return !GroupRuntime.isManagedLevel(level) || MetadataRuntime.hasDragonAuthority(level.getServer());
    }
}
