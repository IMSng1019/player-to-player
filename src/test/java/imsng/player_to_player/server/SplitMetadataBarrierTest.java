package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.registry.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 分离屏障必须先拒绝不安全/过期元数据，再改变任何组表或区块预约。 */
class SplitMetadataBarrierTest {
    private final UUID source = UUID.randomUUID(), member = UUID.randomUUID(), split = UUID.randomUUID();
    private final ChunkKey chunk = new ChunkKey("minecraft:overworld", 0, 0);
    private final GroupTable groups = new GroupTable();
    private final ChunkRegistry registry = new ChunkRegistry(null, null);
    private final Map<UUID, FakeConnection> connections = new HashMap<>();
    private final AtomicReference<SplitCoordinator.MetadataFence> fence = new AtomicReference<>(
            new SplitCoordinator.MetadataFence("world", 3, 20, true));
    private int fenceReads;
    private boolean invalidateAtMutation;
    private final SplitCoordinator coordinator;

    SplitMetadataBarrierTest() {
        groups.createGroup(source); groups.addSecondary(source, member);
        connections.put(source, new FakeConnection(source)); connections.put(member, new FakeConnection(member));
        coordinator = new SplitCoordinator(new GlobalConfig(), registry, groups, new ComputeTable(),
                new PlayerTable(), connections::get, ignored -> { }, (group, followers) -> { }, ignored -> {
                    fenceReads++;
                    return invalidateAtMutation && fenceReads >= 3
                            ? new SplitCoordinator.MetadataFence("world", 4, 20, true) : fence.get();
                });
    }

    @AfterEach void closeCoordinator() { coordinator.shutdown(); }

    @Test void unsafePreparationDoesNotPauseMembersOrReserveTheRegion() {
        fence.set(new SplitCoordinator.MetadataFence("world", 3, 20, false));
        send("prepare", "world", 3, 20);
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertTrue(connections.get(member).messages.isEmpty());
        assertEquals(source, groups.groupOf(member));
        assertTrue(registry.tryClaim(chunk, source).granted());
    }

    @Test void changedTermCannotCommitEvenWhenSourceAlreadyAppliedTheNewTerm() {
        send("prepare", "world", 3, 20);
        fence.set(new SplitCoordinator.MetadataFence("world", 4, 22, true));
        send("commit", "world", 4, 22);
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertEquals(source, groups.groupOf(member));
        assertFalse(registry.tryClaim(chunk, source).granted(), "失败提交保留预约直到源端完成取消");
        send("cancel", "world", 4, 22);
        assertEquals("cancelled", connections.get(source).last().json().get("phase").getAsString());
        assertTrue(registry.tryClaim(chunk, source).granted());
    }

    @Test void commitMustHaveAppliedAtLeastThePreparationVersion() {
        send("prepare", "world", 3, 19);
        assertEquals("prepared", connections.get(source).last().json().get("phase").getAsString());
        send("commit", "world", 3, 19);
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertEquals(source, groups.groupOf(member));
        send("commit", "world", 3, 20);
        assertEquals("committed", connections.get(source).last().json().get("phase").getAsString());
        assertEquals(member, groups.groupOf(member));
    }

    @Test void newlyUnsafeStateCannotCommitOrPublishNewPrimary() {
        send("prepare", "world", 3, 20);
        fence.set(new SplitCoordinator.MetadataFence("world", 3, 21, false));
        send("commit", "world", 3, 21);
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertEquals(source, groups.groupOf(member));
        assertTrue(connections.get(member).messages.stream().noneMatch(message -> message.type() == MessageType.ROLE_ASSIGN));
    }

    @Test void barrierIsRecheckedInsideTheRegistryMutationBeforeChangingGroups() {
        send("prepare", "world", 3, 20);
        invalidateAtMutation = true;
        send("commit", "world", 3, 20);
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertEquals(source, groups.groupOf(member));
        assertTrue(coordinator.involvesGroup(source));
        assertFalse(registry.tryClaim(chunk, source).granted());
    }

    @Test void wrongWorldAndFutureVersionAreRejectedBeforePreparation() {
        send("prepare", "other", 3, 20);
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        send("prepare", "world", 3, 21);
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertTrue(connections.get(member).messages.isEmpty());
    }

    private void send(String phase, String world, long term, long version) {
        JsonObject request = new JsonObject();
        request.addProperty("phase", phase); request.addProperty("splitId", split.toString());
        request.addProperty("groupId", source.toString()); request.addProperty("metadataWorldId", world);
        request.addProperty("metadataTerm", term); request.addProperty("metadataVersion", version);
        JsonArray members = new JsonArray(); members.add(member.toString()); request.add("departingClientIds", members);
        JsonArray regions = new JsonArray(); regions.add(new SplitRegion(chunk.dimension(), -1, 1, -1, 1).toJson());
        request.add("regions", regions);
        coordinator.handle(connections.get(source), ControlMessage.of(MessageType.SPLIT_REQUEST, request));
    }

    private static final class FakeConnection implements ControlConnection {
        private final UUID peer;
        private final List<ControlMessage> messages = new ArrayList<>();
        FakeConnection(UUID peer) { this.peer = peer; }
        ControlMessage last() { return messages.get(messages.size() - 1); }
        public void send(ControlMessage message) { messages.add(message); }
        public CompletableFuture<ControlMessage> request(ControlMessage message) { throw new UnsupportedOperationException(); }
        public SocketAddress remoteAddress() { return null; }
        public UUID peerId() { return peer; }
        public void setPeerId(UUID value) { throw new UnsupportedOperationException(); }
        public boolean isOpen() { return true; }
        public void close() { }
    }
}
