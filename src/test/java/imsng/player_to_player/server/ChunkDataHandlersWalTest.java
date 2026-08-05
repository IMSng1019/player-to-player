package imsng.player_to_player.server;

import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkReleaseReceipt;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.RegistryStore;
import imsng.player_to_player.registry.ChunkWriteback;
import imsng.player_to_player.util.Sha256;
import net.minecraft.SharedConstants;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkDataHandlersWalTest {

    private static final UUID OWNER =
            UUID.fromString("8fd8f9de-6b13-4a85-a0b8-3e3f1e1cbf42");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", 2, -4);

    @TempDir
    Path temporary;

    @Test
    void repeatedContentAfterInterveningWriteIsANewWalEvent() throws Exception {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        DurableChunkJournal journal = new DurableChunkJournal(temporary.resolve("wal"));
        try {
            SharedConstants.tryDetectVersion();
            assertTrue(registry.tryClaim(KEY, OWNER).granted());
            HandlerRegistry routes = (type, handler) -> {
            };
            ChunkDataHandlers handlers = ChunkDataHandlers.register(
                    routes, null, registry, journal);
            Method append = ChunkDataHandlers.class.getDeclaredMethod(
                    "appendDurableSingle", UUID.class, ChunkKey.class,
                    byte[].class, int.class);
            append.setAccessible(true);
            byte[] stateA = chunk(1);
            byte[] stateB = chunk(2);

            assertTrue((boolean) append.invoke(
                    handlers, OWNER, KEY, stateA, Integer.MAX_VALUE));
            assertTrue((boolean) append.invoke(
                    handlers, OWNER, KEY, stateB, Integer.MAX_VALUE));
            assertTrue((boolean) append.invoke(
                    handlers, OWNER, KEY, stateA, Integer.MAX_VALUE));

            assertArrayEquals(stateA, journal.latest(KEY).orElseThrow().payload());
            assertEquals(3, journal.batchCount(),
                    "A→B→A 是三个写入事件，第三个 A 不能命中第一次 A 的幂等键");
        } finally {
            journal.close();
            registry.shutdown();
        }
    }

    @Test
    void finalWalWriteAndOwnershipReleaseExcludeLateRealtimeBatch() throws Exception {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        DurableChunkJournal journal = new DurableChunkJournal(temporary.resolve("race-wal"));
        try {
            SharedConstants.tryDetectVersion();
            assertTrue(registry.tryClaim(KEY, OWNER).granted());
            HandlerRegistry routes = (type, handler) -> {
            };
            ChunkDataHandlers handlers = ChunkDataHandlers.register(
                    routes, null, registry, journal);
            Method appendBatch = ChunkDataHandlers.class.getDeclaredMethod(
                    "appendValidatedBatch", UUID.class, ChunkDeltaBatch.class);
            appendBatch.setAccessible(true);

            byte[] finalState = chunk(10);
            byte[] lateRealtimeState = chunk(9);
            UUID releaseId = UUID.randomUUID();
            CountDownLatch commitEntered = new CountDownLatch(1);
            CountDownLatch allowCommit = new CountDownLatch(1);
            CountDownLatch lateStarted = new CountDownLatch(1);
            CountDownLatch lateFinished = new CountDownLatch(1);

            ChunkWriteback.ReleaseTransaction transaction =
                    new ChunkWriteback.ReleaseTransaction() {
                        @Override
                        public ChunkWriteback.ReleaseResult replay() {
                            return null;
                        }

                        @Override
                        public ChunkWriteback.ReleaseResult commit(
                                boolean finalDataWritten) {
                            commitEntered.countDown();
                            await(allowCommit);
                            ChunkReleaseReceipt receipt = registry.completeRelease(
                                    new ChunkReleaseReceipt(
                                            releaseId, KEY, OWNER,
                                            finalState.length,
                                            Sha256.hex(finalState),
                                            finalDataWritten));
                            return new ChunkWriteback.ReleaseResult(
                                    finalDataWritten, receipt != null);
                        }
                    };

            CompletableFuture<ChunkWriteback.ReleaseResult> release =
                    CompletableFuture.supplyAsync(() -> handlers.finalizeAndRelease(
                            KEY, OWNER, finalState, transaction));
            assertTrue(commitEntered.await(5, TimeUnit.SECONDS));

            ChunkDeltaBatch lateBatch = new ChunkDeltaBatch(
                    UUID.randomUUID(), 0, 0,
                    List.of(ChunkDelta.of(KEY, 1, lateRealtimeState)));
            CompletableFuture<Boolean> lateWrite = CompletableFuture.supplyAsync(() -> {
                lateStarted.countDown();
                try {
                    return invokeBatch(appendBatch, handlers, lateBatch);
                } finally {
                    lateFinished.countDown();
                }
            });

            boolean completedBeforeOwnershipCommit;
            try {
                assertTrue(lateStarted.await(5, TimeUnit.SECONDS));
                completedBeforeOwnershipCommit =
                        lateFinished.await(250, TimeUnit.MILLISECONDS);
            } finally {
                allowCommit.countDown();
            }

            ChunkWriteback.ReleaseResult result = release.get(5, TimeUnit.SECONDS);
            assertTrue(result.finalDataWritten());
            assertTrue(result.released());
            assertTrue(!completedBeforeOwnershipCommit,
                    "运行期批次不得插入最终 WAL 写入与 ownership 提交之间");
            assertTrue(!lateWrite.get(5, TimeUnit.SECONDS),
                    "ownership 删除后迟到批次必须被拒绝");
            assertArrayEquals(finalState,
                    journal.latest(KEY).orElseThrow().payload());
        } finally {
            journal.close();
            registry.shutdown();
        }
    }

    private static boolean invokeBatch(Method appendBatch,
                                       ChunkDataHandlers handlers,
                                       ChunkDeltaBatch batch) {
        try {
            appendBatch.invoke(handlers, OWNER, batch);
            return true;
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof IllegalStateException) {
                return false;
            }
            throw new RuntimeException(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("等待并发测试条件超时");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("并发测试被中断", e);
        }
    }

    private static byte[] chunk(int value) throws Exception {
        CompoundTag tag = new CompoundTag();
        tag.putInt("xPos", KEY.x());
        tag.putInt("zPos", KEY.z());
        tag.putInt("value", value);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        NbtIo.writeCompressed(tag, out);
        return out.toByteArray();
    }

    private static final class MemoryStore implements RegistryStore {
        private final Map<ChunkKey, UUID> values = new HashMap<>();

        @Override
        public Map<ChunkKey, UUID> load() {
            return Map.copyOf(values);
        }

        @Override
        public void save(Map<ChunkKey, UUID> snapshot) {
            values.clear();
            values.putAll(snapshot);
        }

        @Override
        public String describe() {
            return "memory";
        }
    }
}
