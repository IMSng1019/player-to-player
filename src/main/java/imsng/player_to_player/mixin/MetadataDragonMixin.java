package imsng.player_to_player.mixin;

import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.metadata.MetadataRuntime;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.dimension.end.EndDragonFight;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** 水晶交互、实体回调也能直接改变战斗状态，因此除 tick 外还必须保护这些入口。 */
@Mixin(EndDragonFight.class)
public abstract class MetadataDragonMixin {
    @Shadow @Final private ServerLevel level;

    @Inject(method = {"tryRespawn", "setDragonKilled", "onCrystalDestroyed", "updateDragon"},
            at = @At("HEAD"), cancellable = true)
    private void player_to_player$guardBattleOwner(CallbackInfo ci) {
        if (GroupRuntime.isManagedLevel(level) && !MetadataRuntime.hasDragonAuthority(level.getServer())) ci.cancel();
    }
}
