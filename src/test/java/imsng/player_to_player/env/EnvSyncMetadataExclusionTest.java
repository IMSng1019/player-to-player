package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class EnvSyncMetadataExclusionTest {
    @TempDir Path directory;

    @Test void rejectsDefaultWorldMetadataBeforeRequestingAnyFile() throws Exception {
        reject("world", "world/level.dat", false);
    }
    @Test void rejectsActualWorldBackupFromProxyBeforeInstallingOldSnapshot() throws Exception {
        reject("saves/custom-world", "SAVES/CUSTOM-WORLD/LEVEL.DAT_OLD", true);
    }
    private void reject(String world, String file, boolean explicitPath) throws Exception {
        var connection = new OldSnapshot(file);
        Path target = directory.resolve("cache"); Files.createDirectories(target);
        Files.writeString(target.resolve("existing.txt"), "previous-safe-environment");
        var client = explicitPath ? new EnvSyncClient(connection, new GlobalConfig(), world)
                : new EnvSyncClient(connection, new GlobalConfig());
        assertThrows(Exception.class, () -> client.syncTo(target, null).get(5, TimeUnit.SECONDS));
        assertEquals(0, connection.downloads);
        assertEquals("previous-safe-environment", Files.readString(target.resolve("existing.txt")));
        assertFalse(Files.exists(target.resolve(file)));
    }
    private static final class OldSnapshot implements ControlConnection {
        final EnvironmentManifest manifest; int downloads;
        OldSnapshot(String file) {
            manifest = new EnvironmentManifest(Map.of(file, new EnvironmentManifest.Entry(Sha256.hex(new byte[]{1}), 1)));
        }
        public CompletableFuture<ControlMessage> request(ControlMessage request) {
            if (request.type() == MessageType.ENV_MANIFEST_REQUEST) {
                JsonObject reply = new JsonObject(); reply.addProperty("snapshotId", manifest.globalHash());
                reply.addProperty("filteredHash", manifest.globalHash()); reply.add("manifest", manifest.toJson());
                return CompletableFuture.completedFuture(request.reply(MessageType.ENV_MANIFEST, reply, null));
            }
            downloads++;
            return CompletableFuture.failedFuture(new IllegalStateException("已排除的元数据不能进入下载阶段"));
        }
        public void send(ControlMessage message) { }
        public SocketAddress remoteAddress() { return null; }
        public UUID peerId() { return null; }
        public void setPeerId(UUID id) { }
        public boolean isOpen() { return true; }
        public void close() { }
    }
}
