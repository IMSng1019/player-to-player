package imsng.player_to_player.client.mixin;

import imsng.player_to_player.client.session.ClientExitCoordinator;
import net.minecraft.client.gui.screens.PauseScreen;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** 在原版 ClientLevel.disconnect() 之前建立主客户端耐久退出屏障。 */
@Mixin(PauseScreen.class)
public abstract class PauseScreenExitMixin {

    @Inject(method = "onDisconnect()V", at = @At("HEAD"), cancellable = true)
    private void player_to_player$guardPauseDisconnect(CallbackInfo ci) {
        if (ClientExitCoordinator.interceptPauseDisconnect(
                (PauseScreen) (Object) this)) {
            ci.cancel();
        }
    }
}
