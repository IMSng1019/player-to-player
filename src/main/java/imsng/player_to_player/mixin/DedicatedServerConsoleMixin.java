package imsng.player_to_player.mixin;

import imsng.player_to_player.server.CommandChatHandlers;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.server.dedicated.DedicatedServer;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * 只拦专用服务器真实控制台/RCON入口，避免通用 Commands 拦截把玩家、函数和内部转发再次路由。
 * 目标签名已按 1.20.4 字节码核对；管理指令返回原版执行，组世界指令才异步转发。
 */
@Mixin(DedicatedServer.class)
public abstract class DedicatedServerConsoleMixin {
    @Redirect(method = "handleConsoleInputs", at = @At(value = "INVOKE", target =
            "Lnet/minecraft/commands/Commands;performPrefixedCommand(Lnet/minecraft/commands/CommandSourceStack;Ljava/lang/String;)V"))
    private void p2p$routeConsoleInput(Commands commands, CommandSourceStack source, String command) {
        if (!CommandChatHandlers.routeConsoleInput(source, command)) {
            commands.performPrefixedCommand(source, command);
        }
    }

    @Inject(method = "runCommand", at = @At("HEAD"), cancellable = true)
    private void p2p$routeRconInput(String command, CallbackInfoReturnable<String> callback) {
        String response = CommandChatHandlers.routeRconInput((DedicatedServer) (Object) this, command);
        if (response != null) {
            callback.setReturnValue(response);
        }
    }
}
