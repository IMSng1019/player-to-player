package imsng.player_to_player.client.mixin;

import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.client.multiplayer.ServerData;
import net.minecraft.network.Connection;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

/** 只读取本次登录连接及服务器数据，不依赖尚未出现的 Play handler。 */
@Mixin(ClientHandshakePacketListenerImpl.class)
public interface ClientLoginAccess {
    @Accessor("connection") Connection player_to_player$connection();
    @Accessor("serverData") ServerData player_to_player$serverData();
}
