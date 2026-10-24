package imsng.player_to_player.client.mixin;

import imsng.player_to_player.client.env.EnvironmentPreflightClient;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.ConnectScreen;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.client.multiplayer.ServerData;
import net.minecraft.client.multiplayer.resolver.ServerAddress;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** 保存原版连接参数用于下载后的原样重连；普通服务器连接行为不变。 */
@Mixin(ConnectScreen.class)
public abstract class ConnectScreenMixin {
    @Inject(method = "startConnecting", at = @At("HEAD"), cancellable = true)
    private static void player_to_player$rememberConnect(Screen parent, Minecraft client,
            ServerAddress address, ServerData data, boolean quickPlay, CallbackInfo ci) {
        if (!EnvironmentPreflightClient.onConnect(parent, client, address, data, quickPlay)) ci.cancel();
    }
}
