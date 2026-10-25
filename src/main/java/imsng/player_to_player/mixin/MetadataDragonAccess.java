package imsng.player_to_player.mixin;

import net.minecraft.server.level.ServerBossEvent;
import net.minecraft.world.level.dimension.end.EndDragonFight;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

/** 撤销模拟权时显式取消旧龙血条，避免只读镜像残留在线玩家订阅。 */
@Mixin(EndDragonFight.class)
public interface MetadataDragonAccess {
    @Accessor("dragonEvent") ServerBossEvent player_to_player$dragonEvent();
}
