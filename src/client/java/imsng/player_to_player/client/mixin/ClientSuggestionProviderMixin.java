package imsng.player_to_player.client.mixin;

import imsng.player_to_player.group.PlayerDirectory;
import net.minecraft.client.multiplayer.ClientSuggestionProvider;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.util.Collection;

/** /tp 等实体参数在客户端本地补全，必须给此入口提供跨组名字。 */
@Mixin(ClientSuggestionProvider.class)
public abstract class ClientSuggestionProviderMixin {
    @Inject(method = "getOnlinePlayerNames", at = @At("RETURN"), cancellable = true)
    private void player_to_player$includeGlobalNames(CallbackInfoReturnable<Collection<String>> cir) {
        cir.setReturnValue(PlayerDirectory.SESSION.namesIncluding(cir.getReturnValue()));
    }
}
