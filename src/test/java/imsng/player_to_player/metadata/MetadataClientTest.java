package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.world.WorldIdentity;
import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import static org.junit.jupiter.api.Assertions.*;

class MetadataClientTest {
    private static MetadataSnapshot snapshot(long seed) {
        CompoundTag data = new CompoundTag(); CompoundTag generation = new CompoundTag(); generation.putLong("seed", seed);
        data.put("WorldGenSettings", generation); CompoundTag root = new CompoundTag(); root.put("Data", data);
        return new MetadataSnapshot(WorldIdentity.ofLevelData(root).orElseThrow(), 3, 2, data);
    }
    @Test void freshSnapshotRequestChecksWorldIdentityAndKeepsNbtTypes() throws Exception {
        MetadataSnapshot snapshot = snapshot(44);
        var connection = new ReplyConnection(ControlMessage.of(MessageType.METADATA_SNAPSHOT, new JsonObject(),
                MetadataWire.encode(MetadataWire.snapshotToTag(snapshot))));
        assertEquals(snapshot, MetadataClient.requestSnapshot(connection, snapshot.worldId()).join());
        assertEquals(snapshot.worldId(), connection.request.json().get("worldId").getAsString());
        assertThrows(CompletionException.class, () -> MetadataClient.requestSnapshot(connection, snapshot(45).worldId()).join());
    }
    @Test void downloadedSnapshotDoesNotOpenSimulationAndDisconnectCannotRestoreReadiness() throws Exception {
        MetadataClient.initialize(snapshot(88));
        assertFalse(MetadataClient.isSimulationReady());
        assertEquals(2, MetadataClient.snapshot().term());
        MetadataClient.disconnected();
        assertFalse(MetadataClient.isSimulationReady());
    }
    @Test void rejectsEnvelopeIdentityThatDoesNotMatchGenerationData() throws Exception {
        MetadataSnapshot inconsistent = new MetadataSnapshot("other-world", 3, 2, snapshot(44).data());
        var connection = new ReplyConnection(ControlMessage.of(MessageType.METADATA_SNAPSHOT, new JsonObject(),
                MetadataWire.encode(MetadataWire.snapshotToTag(inconsistent))));
        assertThrows(CompletionException.class, () -> MetadataClient.requestSnapshot(connection, "").join());
    }
    private static final class ReplyConnection implements ControlConnection {
        private final ControlMessage reply; ControlMessage request;
        ReplyConnection(ControlMessage reply) { this.reply = reply; }
        public void send(ControlMessage message) { }
        public CompletableFuture<ControlMessage> request(ControlMessage message) { request = message; return CompletableFuture.completedFuture(reply); }
        public SocketAddress remoteAddress() { return null; }
        public UUID peerId() { return null; }
        public void setPeerId(UUID id) { }
        public boolean isOpen() { return true; }
        public void close() { }
    }
}
