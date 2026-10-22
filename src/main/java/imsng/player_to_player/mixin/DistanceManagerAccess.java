package imsng.player_to_player.mixin;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import net.minecraft.server.level.DistanceManager;
import net.minecraft.server.level.Ticket;
import net.minecraft.util.SortedArraySet;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

/** 分离预检只读原版票据；只能在服务器主线程遍历，禁止修改返回的内部集合。 */
@Mixin(DistanceManager.class)
public interface DistanceManagerAccess {
    @Accessor("tickets")
    Long2ObjectOpenHashMap<SortedArraySet<Ticket<?>>> player_to_player$tickets();
}
