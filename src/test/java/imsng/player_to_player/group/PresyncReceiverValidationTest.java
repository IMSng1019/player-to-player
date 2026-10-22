package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 接收失败不得继续发送尾部成功 ACK；测试直接覆盖实际解码/暂存入口。 */
class PresyncReceiverValidationTest {
    @AfterEach void clean() { PresyncStore.clear(); }

    @Test
    void corruptOrMisplacedChunkFailsInsteadOfSilentlyAcknowledging() {
        PresyncReceiver receiver = new PresyncReceiver(null, null, "test", phase -> { });
        assertThrows(java.io.IOException.class,
                () -> receiver.stageChunk("minecraft:overworld;1;2", new byte[]{1, 2, 3}));
        CompoundTag tag = new CompoundTag();
        tag.putInt("xPos", 5); tag.putInt("zPos", 2);
        assertThrows(java.io.IOException.class,
                () -> receiver.stageChunk("minecraft:overworld;1;2", ChunkUploadService.compress(tag)));
        assertEquals(0, PresyncStore.size());
    }

    @Test
    void capacityOverflowFailsWithoutReplacingExistingRecords() {
        for (int x = 0; x < 4096; x++)
            PresyncStore.put(new ChunkKey("minecraft:overworld", x, 0), new CompoundTag());
        assertThrows(IllegalStateException.class, () ->
                PresyncStore.put(new ChunkKey("minecraft:overworld", 4096, 0), new CompoundTag()));
        assertEquals(4096, PresyncStore.size());
    }

    @Test
    void cancelledReceiverCannotRefillClearedStore() {
        PresyncReceiver receiver = new PresyncReceiver(null, null, "test", phase -> { });
        receiver.cancel();
        PresyncStore.clear();
        CompoundTag tag = new CompoundTag();
        tag.putInt("xPos", 1); tag.putInt("zPos", 2);
        assertThrows(java.io.IOException.class, () -> receiver.stageChunk(
                "minecraft:overworld;1;2", ChunkUploadService.compress(tag)));
        assertEquals(0, PresyncStore.size());
    }
}
