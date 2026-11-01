package imsng.player_to_player.mixin;

import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.PlayerDirectory;
import net.minecraft.commands.CommandSourceStack;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.util.Collection;

/** 服务端自定义补全也读取同一目录；只对受管组世界生效，不修改原版选择器执行。 */
@Mixin(CommandSourceStack.class)
public abstract class CommandSourceSuggestionsMixin {
    @Inject(method = "getOnlinePlayerNames", at = @At("RETURN"), cancellable = true)
    private void player_to_player$includeGlobalNames(CallbackInfoReturnable<Collection<String>> cir) {
        if (GroupRuntime.isManagedServer(((CommandSourceStack) (Object) this).getServer())) {
            cir.setReturnValue(PlayerDirectory.SESSION.namesIncluding(cir.getReturnValue()));
        }
    }
}
