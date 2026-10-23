package imsng.player_to_player.mixin;

import com.mojang.authlib.GameProfile;
import imsng.player_to_player.runtime.WorkerBootstrap;
import imsng.player_to_player.runtime.WorkerIdentityTable;
import net.minecraft.network.Connection;
import net.minecraft.network.chat.Component;
import net.minecraft.network.protocol.login.ServerboundHelloPacket;
import net.minecraft.server.network.ServerLoginPacketListenerImpl;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.ModifyArg;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 独立子服仅接受父进程提前经认证IPC登记的回环TCP连接。将离线profile替换为经过
 * P2P身份认证的真实玩家UUID，随后原版/Fabric登录、配置及注册表握手全部照常执行。
 */
@Mixin(ServerLoginPacketListenerImpl.class)
public abstract class WorkerLoginMixin {
    @Shadow @Final private Connection connection;
    @Shadow public abstract void disconnect(Component reason);
    @Unique private WorkerIdentityTable.Identity player_to_player$workerIdentity;

    @Inject(method = "handleHello", at = @At("HEAD"), cancellable = true)
    private void player_to_player$requireAuthorizedSocket(ServerboundHelloPacket packet, CallbackInfo ci) {
        if (!WorkerBootstrap.isWorker()) return;
        player_to_player$workerIdentity = WorkerBootstrap.identities().consume(connection.getRemoteAddress(), packet.name());
        if (player_to_player$workerIdentity == null) {
            disconnect(Component.literal("该游戏连接未通过 P2P 组身份授权"));
            ci.cancel();
        }
    }

    @ModifyArg(method = "handleHello", at = @At(value = "INVOKE",
            target = "Lnet/minecraft/server/network/ServerLoginPacketListenerImpl;startClientVerification(Lcom/mojang/authlib/GameProfile;)V"), index = 0)
    private GameProfile player_to_player$canonicalProfile(GameProfile original) {
        if (!WorkerBootstrap.isWorker()) return original;
        WorkerIdentityTable.Identity identity = player_to_player$workerIdentity;
        if (identity == null) throw new IllegalStateException("子服登录缺少已验证身份");
        return new GameProfile(identity.playerId(), identity.playerName());
    }
}
