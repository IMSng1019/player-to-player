package imsng.player_to_player.mixin;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.npc.WanderingTraderSpawner;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;
import org.spongepowered.asm.mixin.gen.Invoker;

/** 存档字段之外生成器还保存三个缓存，权威同步必须同时更新它们。 */
@Mixin(WanderingTraderSpawner.class)
public interface MetadataTraderAccess {
    @Accessor("spawnDelay") void player_to_player$spawnDelay(int value);
    @Accessor("spawnChance") void player_to_player$spawnChance(int value);
    @Accessor("tickDelay") void player_to_player$tickDelay(int value);
    @Invoker("spawn") boolean player_to_player$spawn(ServerLevel level);
}
