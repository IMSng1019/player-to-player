package imsng.player_to_player.mixin;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.CustomSpawner;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;
import org.spongepowered.asm.mixin.gen.Invoker;

import java.util.List;

/** 仅暴露原版轻量控制入口；协调端不会借此推进区块或实体。 */
@Mixin(ServerLevel.class)
public interface MetadataLevelAccess {
    @Invoker("advanceWeatherCycle")
    void player_to_player$advanceWeatherCycle();

    @Invoker("wakeUpAllPlayers")
    void player_to_player$wakeUpAllPlayers();

    @Accessor("customSpawners")
    List<CustomSpawner> player_to_player$customSpawners();
}
