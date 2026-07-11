package imsng.player_to_player.mixin;

import imsng.player_to_player.group.ChunkUploadService;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.level.ChunkMap;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.storage.ChunkStorage;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 区块上行捕获（Phase 2，规范"服务端需要实时计算区块的更改更新本地区块文件"
 * ——更改的来源即主客户端集成服务端的实时上行）。
 * <p>
 * <b>注入点选择理由</b>：{@code ChunkStorage.write(ChunkPos, CompoundTag)} 是区块
 * NBT 落盘的<b>唯一咽喉</b>——自动存盘（saveChunkIfNeeded）、卸载存盘
 * （scheduleUnload → save）、停服 saveAllChunks 殊途同归都经 {@code ChunkMap.save}
 * 调到这里（ChunkMap 继承 ChunkStorage 且不覆写 write，javap 已核实签名）。
 * 在 HEAD 旁挂（不取消：本地照常写盘，本地存档兼作完整备份）把同一份 NBT 交给
 * {@link ChunkUploadService} 合并入队，即可保证"凡是原版认为值得写盘的区块变更，
 * 必然同步上行"，与原版脏标记（unsaved）语义完全对齐，无需自行发明去抖逻辑。
 * <p>
 * <b>门控</b>：instanceof 判定 this 是 ChunkMap（排除其他 ChunkStorage 用法）
 * + {@link GroupRuntime#isManagedLevel}（只有被组运行时接管的集成服务端才上行；
 * 物理服务端上 ChunkDataHandlers 写回时同样路过本注入点，因非接管维度而直接放行，
 * 不会产生"上行回声"）。热路径开销：一次 instanceof + 一次 volatile 读。
 * <p>
 * <b>线程</b>：write 在服务器主线程被调（save 内），enqueue 只做 map put（微秒级），
 * gzip 压缩与网络发送在上行服务的 io 工作线程完成，不拖慢主线程。
 */
@Mixin(ChunkStorage.class)
public abstract class ChunkStorageMixin {

    @Inject(method = "write", at = @At("HEAD"))
    private void player_to_player$captureChunkWrite(ChunkPos pos, CompoundTag tag, CallbackInfo ci) {
        // 只关心 ChunkMap 的区块写（ChunkStorage 也被其他存储场景实例化）
        if (!((Object) this instanceof ChunkMap)) {
            return;
        }
        ServerLevel level = ((ChunkMapAccess) this).player_to_player$level();
        if (!GroupRuntime.isManagedLevel(level)) {
            return; // 非组接管：物理服务端写回 / 单人游戏，均放行不上行
        }
        ChunkUploadService uploads = GroupRuntime.uploads();
        if (uploads == null || tag == null) {
            return;
        }
        uploads.enqueue(new ChunkKey(level.dimension().location().toString(), pos.x, pos.z), tag);
    }
}
