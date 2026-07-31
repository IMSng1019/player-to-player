package imsng.player_to_player.mixin;

import net.minecraft.server.level.ChunkHolder;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Invoker;

import java.util.concurrent.CompletableFuture;

/** 暴露 ChunkHolder 的原版存盘依赖入口，供预卸载 ACK 屏障复用。 */
@Mixin(ChunkHolder.class)
public interface ChunkHolderSaveDependencyAccess {

    /**
     * 调用受保护的 {@code addSaveDependency(String, CompletableFuture<?>)}。
     * 该 API 会把依赖并入 holder.chunkToSave，原版 scheduleUnload 自然等待它完成。
     */
    @Invoker("addSaveDependency")
    void player_to_player$addSaveDependency(
            String reason, CompletableFuture<?> dependency);
}
