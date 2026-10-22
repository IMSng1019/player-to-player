package imsng.player_to_player.mixin;

import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import net.minecraft.server.level.ChunkHolder;
import net.minecraft.server.level.ChunkMap;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

/**
 * 分离的内存排空证明：所有权释放后，仍须等待原版更新表与待卸载表均移除旧 holder。
 * 这些访问器仅供服务器主线程只读检查，绝不绕过原版保存/实体卸载去删除 holder。
 */
@Mixin(ChunkMap.class)
public interface ChunkMapSplitAccess {
    @Accessor("updatingChunkMap")
    Long2ObjectLinkedOpenHashMap<ChunkHolder> player_to_player$updatingChunks();

    @Accessor("pendingUnloads")
    Long2ObjectLinkedOpenHashMap<ChunkHolder> player_to_player$pendingUnloads();
}
