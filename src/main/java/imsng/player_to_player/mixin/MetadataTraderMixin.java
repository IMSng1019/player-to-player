package imsng.player_to_player.mixin;

import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.metadata.MetadataRuntime;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.npc.WanderingTraderSpawner;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/** 每个计算端都有生成器对象，但机会只能由协调端推进，收到唯一派发后才执行原版选点。 */
@Mixin(WanderingTraderSpawner.class)
public abstract class MetadataTraderMixin {
    @Inject(method = "tick", at = @At("HEAD"), cancellable = true)
    private void player_to_player$onlyDispatchedAttempts(ServerLevel level, boolean spawnEnemies,
                                                        boolean spawnFriendlies, CallbackInfoReturnable<Integer> cir) {
        if (GroupRuntime.isManagedLevel(level)) cir.setReturnValue(0);
    }

    @WrapOperation(method = "spawn", at = @At(value = "INVOKE",
            target = "Lnet/minecraft/server/level/ServerLevel;getRandomPlayer()Lnet/minecraft/server/level/ServerPlayer;"))
    private ServerPlayer player_to_player$selectedGlobalPlayer(ServerLevel level, Operation<ServerPlayer> original) {
        return MetadataRuntime.traderTarget() != null
                ? level.getServer().getPlayerList().getPlayer(MetadataRuntime.traderTarget()) : original.call(level);
    }
}
