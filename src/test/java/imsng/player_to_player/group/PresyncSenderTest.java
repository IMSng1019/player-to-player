package imsng.player_to_player.group;

import imsng.player_to_player.p2p.P2PTransport;
import imsng.player_to_player.p2p.ReliableChannel;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/** 用真实可靠流与接收器验证尾部顺序；未生成的边界区块不能成为迁移前置条件。 */
class PresyncSenderTest {
    @TempDir Path players;

    @AfterEach void clean() {
        PresyncStore.clear();
    }

    @Test
    void finalMemoryStateOverridesOldSaveAndIncludesNewClaimsWithoutWaitingForUnknownChunks() throws Exception {
        ChunkKey loaded = new ChunkKey("minecraft:overworld", 26, 27);
        ChunkKey unknown = new ChunkKey("minecraft:overworld", 25, 27);
        ChunkKey newlyLoaded = new ChunkKey("minecraft:overworld", 27, 27);
        TestWorld world = new TestWorld();
        world.chunks.put(loaded, tag(loaded, 1));
        world.chunks.put(unknown, null); // 已获准但尚未生成；没有可读取的世界对象。
        try (Pair pair = new Pair()) {
            var received = receive(pair);
            PresyncSender sender = new PresyncSender(pair.a, world, "tail-test", phase -> {
                if (phase.equals("snapshot_done")) {
                    world.chunks.put(loaded, tag(loaded, 2));
                    world.chunks.put(newlyLoaded, tag(newlyLoaded, 3));
                }
                if (phase.equals("caught_up")) {
                    // 旧存盘消息比内存修改更旧，不能放到最终内存快照之后覆盖它。
                    PresyncTaps.offer(loaded, tag(loaded, 1));
                }
            });
            sender.run();
            received.get(5, TimeUnit.SECONDS);

            assertTrue(world.frozen, "成功后保持冻结，直到权威 COMMIT/ABORT");
            assertEquals(2, PresyncStore.take(loaded).getInt("revision"));
            assertEquals(3, PresyncStore.take(newlyLoaded).getInt("revision"));
            assertNull(PresyncStore.take(unknown), "不得为未知区块制造空 NBT");
            assertTrue(java.nio.file.Files.exists(players.resolve(world.playerId + ".dat")));
        }
    }

    @Test
    void unloadAfterInitialSnapshotStillSendsTheLastSavedRevision() throws Exception {
        ChunkKey key = new ChunkKey("minecraft:overworld", 4, 5);
        TestWorld world = new TestWorld();
        world.chunks.put(key, tag(key, 1));
        try (Pair pair = new Pair()) {
            var received = receive(pair);
            PresyncSender sender = new PresyncSender(pair.a, world, "unload-tail", phase -> {
                if (phase.equals("snapshot_done")) {
                    // A 在初始快照后保存 v2 并释放该块；B 仍暂存 v1，必须得到最后更新。
                    PresyncTaps.offer(key, tag(key, 2));
                    world.chunks.remove(key);
                }
            });
            sender.run();
            received.get(5, TimeUnit.SECONDS);
            assertEquals(2, PresyncStore.take(key).getInt("revision"));
        }
    }

    @Test
    void failureReadingLoadedTailAbortsInsteadOfAcknowledgingIncompleteState() throws Exception {
        TestWorld world = new TestWorld();
        ChunkKey key = new ChunkKey("minecraft:overworld", 1, 2);
        world.chunks.put(key, tag(key, 1));
        world.failFrozenCapture = true;
        try (Pair pair = new Pair()) {
            var received = receive(pair);
            PresyncSender sender = new PresyncSender(pair.a, world, "failed-tail", ignored -> { });
            assertThrows(Exception.class, sender::run);
            assertFalse(world.frozen, "提交前失败须恢复原组");
            assertFalse(received.isDone(), "不得发送 TAIL_DONE 让接收器提前确认");
            pair.b.abort();
            assertThrows(Exception.class, () -> received.get(5, TimeUnit.SECONDS));
        }
    }

    private CompletableFuture<Void> receive(Pair pair) {
        return CompletableFuture.runAsync(() -> {
            try {
                new PresyncReceiver(pair.b, players, "receiver", ignored -> { }).run();
            } catch (Exception error) {
                throw new CompletionException(error);
            }
        });
    }

    private static CompoundTag tag(ChunkKey key, int revision) {
        CompoundTag tag = new CompoundTag();
        tag.putInt("xPos", key.x());
        tag.putInt("zPos", key.z());
        tag.putInt("revision", revision);
        return tag;
    }

    private static final class TestWorld implements PresyncSender.SnapshotSource {
        final Map<ChunkKey, CompoundTag> chunks = new LinkedHashMap<>();
        final String playerId = UUID.randomUUID().toString();
        boolean frozen;
        boolean failFrozenCapture;
        public Executor executor() { return Runnable::run; }
        public Set<ChunkKey> claimedChunks() { return Set.copyOf(chunks.keySet()); }
        public CompoundTag captureChunk(ChunkKey key) {
            if (frozen && failFrozenCapture) throw new IllegalStateException("模拟最终序列化失败");
            return chunks.get(key);
        }
        public Map<String, CompoundTag> capturePlayers() { return Map.of(playerId, new CompoundTag()); }
        public void setFrozen(boolean value) { frozen = value; }
    }

    private static final class Pair implements AutoCloseable {
        final Wire first = new Wire(), second = new Wire();
        final ReliableChannel a, b;
        Pair() {
            first.peer = second;
            second.peer = first;
            a = new ReliableChannel(first, "sender-test");
            b = new ReliableChannel(second, "receiver-test");
        }
        public void close() {
            a.abort(); b.abort();
            first.worker.shutdownNow(); second.worker.shutdownNow();
        }
    }

    private static final class Wire implements P2PTransport {
        final ExecutorService worker = Executors.newSingleThreadExecutor();
        volatile Consumer<byte[]> receiver;
        volatile boolean open = true;
        Wire peer;
        public void send(byte[] bytes) {
            if (open && peer.open) peer.worker.execute(() -> {
                Consumer<byte[]> target = peer.receiver;
                if (target != null && peer.open) target.accept(bytes);
            });
        }
        public void setReceiver(Consumer<byte[]> target) { receiver = target; }
        public boolean isOpen() { return open; }
        public void close() { open = false; }
        public String describe() { return "memory"; }
    }
}
