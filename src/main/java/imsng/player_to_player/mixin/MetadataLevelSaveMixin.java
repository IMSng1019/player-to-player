package imsng.player_to_player.mixin;

import imsng.player_to_player.metadata.MetadataRuntime;
import net.minecraft.core.RegistryAccess;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.world.level.storage.PrimaryLevelData;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/** 防止原版保存丢弃未知字段，或将未耐久确认的中心状态写进 level.dat。 */
@Mixin(PrimaryLevelData.class)
public abstract class MetadataLevelSaveMixin {
    @Inject(method = "createTag", at = @At("RETURN"), cancellable = true)
    private void player_to_player$saveCommittedFields(RegistryAccess registry, CompoundTag player,
                                                     CallbackInfoReturnable<CompoundTag> cir) {
        cir.setReturnValue(MetadataRuntime.dataForVanillaSave((PrimaryLevelData) (Object) this, cir.getReturnValue()));
    }
}
