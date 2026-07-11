package imsng.player_to_player.mixin;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.level.ChunkMap;
import net.minecraft.server.level.ServerLevel;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;
import org.spongepowered.asm.mixin.gen.Invoker;

/**
 * {@link ChunkMap} 的访问器/调用器（Phase 2 区块申请门控与上行捕获用）。
 * <p>
 * 两个成员均已用 javap 对 1.20.4 mojmap 核实：
 * <ul>
 *   <li>{@code level}：package-private final ServerLevel —— 上行捕获 Mixin 注入在
 *       父类 {@link net.minecraft.world.level.chunk.storage.ChunkStorage} 上，
 *       拿到的 this 需回查所属维度做"是否被组运行时接管"判定；</li>
 *   <li>{@code upgradeChunkTag(CompoundTag)}：private 单参桥接（readChunk 的
 *       升级 lambda 目标），内部补 DataVersion 升级、旧结构 legacy 处理与
 *       blending 上下文 —— 从服务端拉回的区块 NBT 可能是旧版本存档写下的，
 *       必须走与原版磁盘读取完全相同的升级管线再交给 ChunkSerializer.read。
 *       按 (CompoundTag)CompoundTag 描述符匹配，与父类公开的四参重载无歧义。</li>
 * </ul>
 */
@Mixin(ChunkMap.class)
public interface ChunkMapAccess {

    @Accessor("level")
    ServerLevel player_to_player$level();

    @Invoker("upgradeChunkTag")
    CompoundTag player_to_player$upgradeChunkTag(CompoundTag tag);
}
