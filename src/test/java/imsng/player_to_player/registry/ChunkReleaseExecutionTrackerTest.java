package imsng.player_to_player.registry;

import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkReleaseExecutionTrackerTest {

    private static final UUID GROUP =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final UUID RELEASE_ID =
            UUID.fromString("129c3954-0249-45da-b98f-739dc5dab118");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", 21, -8);

    @Test
    void confirmationWaitsForEveryAlreadyAcceptedRetryBeforeDeletingReceipt() {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        ChunkReleaseExecutionTracker tracker =
                new ChunkReleaseExecutionTracker();
        assertTrue(registry.tryClaim(KEY, GROUP).granted());

        // A 先到服务端但仍在 IO 队列，客户端超时后发送 B；两者都必须先登记在途。
        assertTrue(tracker.begin(RELEASE_ID, GROUP));
        assertTrue(tracker.begin(RELEASE_ID, GROUP));

        // cached IO pool 允许 B 先执行并建立成功回执。
        ChunkReleaseReceipt receipt = new ChunkReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0]), false);
        assertEquals(receipt, registry.completeRelease(receipt));
        tracker.finish(RELEASE_ID, GROUP, registry); // B 完成，A 仍在途

        // B 的 ACK 到达客户端后发 confirmation；A 未排空前只能记录确认意图。
        assertTrue(tracker.confirm(RELEASE_ID, GROUP, registry));
        assertEquals(receipt, registry.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0])));

        // 本地已经允许卸载，同组随后重新取得同坐标的新 ownership。
        assertTrue(registry.tryClaim(KEY, GROUP).granted());

        // 旧 A 终于执行时仍必须命中旧回执，绝不能走首次写回/释放新 claim。
        assertEquals(receipt, registry.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0])));
        tracker.finish(RELEASE_ID, GROUP, registry);

        assertEquals(GROUP, registry.ownerOf(KEY));
        assertNull(registry.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0])));
    }

    private static final class MemoryStore implements RegistryStore {
        private Map<ChunkKey, UUID> claims = new HashMap<>();

        @Override
        public Map<ChunkKey, UUID> load() {
            return new HashMap<>(claims);
        }

        @Override
        public void save(Map<ChunkKey, UUID> snapshot) {
            claims = new HashMap<>(snapshot);
        }

        @Override
        public String describe() {
            return "memory";
        }
    }
}
