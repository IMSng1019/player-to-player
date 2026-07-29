package imsng.player_to_player.client.mixin;

import net.minecraft.client.gui.screens.PauseScreen;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Invoker;

/** 退出保护确认后重放暂停菜单完整原版退出动作。 */
@Mixin(PauseScreen.class)
public interface PauseScreenAccessor {

    @Invoker("onDisconnect")
    void player_to_player$invokeOnDisconnect();
}
