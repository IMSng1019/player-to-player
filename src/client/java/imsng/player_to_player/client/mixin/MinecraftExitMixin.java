package imsng.player_to_player.client.mixin;

import imsng.player_to_player.client.session.ClientExitCoordinator;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.Screen;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 在原版真正断开本地玩家或把 running 置 false 之前建立耐久退出屏障。
 * 旁路旗标由 ClientExitCoordinator 一次性消费，避免事务完成后递归拦截自身调用。
 */
@Mixin(Minecraft.class)
public abstract class MinecraftExitMixin {

    @Inject(
            method = "disconnect(Lnet/minecraft/client/gui/screens/Screen;)V",
            at = @At("HEAD"),
            cancellable = true
    )
    private void player_to_player$guardDisconnect(Screen targetScreen, CallbackInfo ci) {
        if (ClientExitCoordinator.interceptDisconnect(
                (Minecraft) (Object) this, targetScreen)) {
            ci.cancel();
        }
    }

    @Inject(method = "stop()V", at = @At("HEAD"), cancellable = true)
    private void player_to_player$guardStop(CallbackInfo ci) {
        if (ClientExitCoordinator.interceptStop((Minecraft) (Object) this)) {
            ci.cancel();
        }
    }
}

