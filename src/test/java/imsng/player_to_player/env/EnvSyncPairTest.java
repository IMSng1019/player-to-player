package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static imsng.player_to_player.env.ModPrefixResolver.Target.PRIMARY_CLIENT;
import static imsng.player_to_player.env.ModPrefixResolver.Target.SECONDARY_CLIENT;
import static org.junit.jupiter.api.Assertions.*;

/** 双角色同步使用真实文件读写；内存连接只代替网络传输并记录实际下载请求。 */
class EnvSyncPairTest {

    @TempDir
    Path tempDir;

    @Test
    void sharedContentDownloadsOnceWhileRoleFilesRemainIndependent() throws Exception {
        PairConnection connection = new PairConnection(
                Map.of("config/shared.txt", "shared", "mods/primary.jar", "primary"),
                Map.of("config/renamed.txt", "shared", "mods/secondary.jar", "secondary"));
        Path primary = tempDir.resolve("primary");
        Path secondary = tempDir.resolve("secondary");

        EnvSyncClient.PairResult result = new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary, secondary).get(5, TimeUnit.SECONDS);

        assertEquals(connection.manifests.get(PRIMARY_CLIENT).globalHash(), result.primaryHash());
        assertEquals(connection.manifests.get(SECONDARY_CLIENT).globalHash(), result.secondaryHash());
        assertEquals(List.of(PRIMARY_CLIENT, SECONDARY_CLIENT), connection.manifestRequests);
        assertEquals(3, connection.fileRequests.size());
        assertFalse(connection.fileRequests.contains("SECONDARY_CLIENT:config/renamed.txt"));
        assertEquals("shared", Files.readString(secondary.resolve("config/renamed.txt")));
        assertFalse(Files.exists(primary.resolve("mods/secondary.jar")));
        assertFalse(Files.exists(secondary.resolve("mods/primary.jar")));

        // 配置必须是独立文件：一个角色运行时改写配置，不能同步污染另一角色。
        Files.writeString(secondary.resolve("config/renamed.txt"), "changed");
        assertEquals("shared", Files.readString(primary.resolve("config/shared.txt")));
        assertFalse(Files.isSameFile(primary.resolve("config/shared.txt"),
                secondary.resolve("config/renamed.txt")));
    }

    @Test
    void matchingSecondaryFileCanSeedPrimaryWithoutDownload() throws Exception {
        PairConnection connection = new PairConnection(
                Map.of("config/main.txt", "shared"), Map.of("config/copy.txt", "shared"));
        Path primary = tempDir.resolve("primary");
        Path secondary = tempDir.resolve("secondary");
        write(secondary.resolve("config/copy.txt"), "shared");

        new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary, secondary).get(5, TimeUnit.SECONDS);

        assertTrue(connection.fileRequests.isEmpty());
        assertEquals("shared", Files.readString(primary.resolve("config/main.txt")));
    }

    @Test
    void unlistedSecondaryFileIsNotAReuseSource() throws Exception {
        PairConnection connection = new PairConnection(Map.of("config/main.txt", "shared"), Map.of());
        Path primary = tempDir.resolve("primary");
        Path secondary = tempDir.resolve("secondary");
        write(secondary.resolve("config/old-world.txt"), "shared");

        new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary, secondary).get(5, TimeUnit.SECONDS);

        assertEquals(List.of("PRIMARY_CLIENT:config/main.txt"), connection.fileRequests);
        assertFalse(Files.exists(secondary.resolve("config/old-world.txt")));
    }

    @Test
    void changedPublishedDirectoryDoesNotCorruptValidatedStagingReuse() throws Exception {
        PairConnection connection = new PairConnection(
                Map.of("config/shared.txt", "shared"),
                Map.of("config/a-first.txt", "unique", "config/shared.txt", "shared"));
        Path primary = tempDir.resolve("primary");
        Path secondary = tempDir.resolve("secondary");
        connection.beforeFile = key -> {
            if (key.equals("SECONDARY_CLIENT:config/a-first.txt")) {
                // 模拟下载期间旧环境被模组改写；副角色仍可复用已验证的主角色暂存内容。
                write(primary.resolve("config/shared.txt"), "broken");
            }
        };

        new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary, secondary).get(5, TimeUnit.SECONDS);

        assertEquals(2, connection.fileRequests.size());
        assertEquals("shared", Files.readString(primary.resolve("config/shared.txt")));
        assertEquals("shared", Files.readString(secondary.resolve("config/shared.txt")));
        try (var files = Files.walk(secondary)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }

    @Test
    void aNewPairDoesNotReuseAnotherWorldDirectory() throws Exception {
        PairConnection connection = new PairConnection(
                Map.of("config/shared.txt", "shared"), Map.of("config/shared.txt", "shared"));
        EnvSyncClient client = new EnvSyncClient(connection, new GlobalConfig());
        client.syncPair(tempDir.resolve("world-a/primary"), tempDir.resolve("world-a/secondary"))
                .get(5, TimeUnit.SECONDS);
        client.syncPair(tempDir.resolve("world-b/primary"), tempDir.resolve("world-b/secondary"))
                .get(5, TimeUnit.SECONDS);

        assertEquals(2, connection.fileRequests.size());
    }

    @Test
    void reuseCannotBypassDestinationTraversalCheck() throws Exception {
        PairConnection connection = new PairConnection(
                Map.of("config/shared.txt", "shared"), Map.of("../escaped.txt", "shared"));

        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(tempDir.resolve("primary"), tempDir.resolve("secondary"))
                .get(5, TimeUnit.SECONDS));

        assertFalse(Files.exists(tempDir.resolve("escaped.txt")));
    }

    @Test
    void expiredSecondarySnapshotRefetchesBothManifests() throws Exception {
        PairConnection connection = new PairConnection(
                Map.of("config/shared.txt", "shared"),
                Map.of("config/shared.txt", "shared", "config/unique.txt", "unique"));
        connection.expireSecondaryOnce = true;

        new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(tempDir.resolve("primary"), tempDir.resolve("secondary"))
                .get(5, TimeUnit.SECONDS);

        assertEquals(List.of(PRIMARY_CLIENT, SECONDARY_CLIENT, PRIMARY_CLIENT, SECONDARY_CLIENT),
                connection.manifestRequests);
        assertEquals(List.of("v1", "v1", "v2", "v2"),
                connection.requestedSnapshots);
        assertEquals("unique", Files.readString(tempDir.resolve("secondary/config/unique.txt")));
    }

    @Test
    void rejectsOverlappingRoleDirectoriesBeforeNetworkRequests() {
        PairConnection connection = new PairConnection(Map.of(), Map.of());

        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(tempDir.resolve("environment"), tempDir.resolve("environment/secondary"))
                .get(5, TimeUnit.SECONDS));

        assertTrue(connection.manifestRequests.isEmpty());
    }

    @Test
    void separateSyncToCallsKeepTheirExistingFullViewContract() throws Exception {
        PairConnection connection = new PairConnection(Map.of("config/full.txt", "full"), Map.of());
        EnvSyncClient client = new EnvSyncClient(connection, new GlobalConfig());
        String hash = client.syncTo(tempDir.resolve("proxy"), null).get(5, TimeUnit.SECONDS);

        assertEquals(connection.manifests.get(PRIMARY_CLIENT).globalHash(), hash);
        assertEquals("full", Files.readString(tempDir.resolve("proxy/config/full.txt")));
    }

    private static void write(Path file, String contents) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, contents);
    }

    @Test
    void matchingSymbolicLinkMustNotCountAsIndependentRoleFile() throws Exception {
        PairConnection connection = new PairConnection(
                Map.of("config/shared.txt", "shared"), Map.of("config/shared.txt", "shared"));
        Path primary = tempDir.resolve("primary"), secondary = tempDir.resolve("secondary");
        Path original = primary.resolve("config/shared.txt"), linked = secondary.resolve("config/shared.txt");
        write(original, "shared");
        Files.createDirectories(linked.getParent());
        try { Files.createSymbolicLink(linked, original); }
        catch (java.io.IOException | UnsupportedOperationException error) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "当前Windows账户不能创建符号链接");
        }
        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary, secondary).get(5, TimeUnit.SECONDS));
        assertEquals("shared", Files.readString(original));
    }

    @Test
    void newDirectoryDoesNotRetainAnUnlistedPredictableTemporaryPath() throws Exception {
        PairConnection connection = new PairConnection(Map.of("config/shared.txt", "shared"), Map.of());
        Path primary = tempDir.resolve("primary");
        Path oldTemporary = primary.resolve("config/shared.txt.p2pdl.tmp");
        write(oldTemporary, "must remain untouched");
        new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary, tempDir.resolve("secondary")).get(5, TimeUnit.SECONDS);
        assertFalse(Files.exists(oldTemporary));
    }

    @FunctionalInterface
    private interface BeforeFile {
        void accept(String key) throws Exception;
    }

    private static final class PairConnection implements ControlConnection {
        private final Map<ModPrefixResolver.Target, Map<String, String>> contents = new HashMap<>();
        private final Map<ModPrefixResolver.Target, EnvironmentManifest> manifests = new HashMap<>();
        private final List<ModPrefixResolver.Target> manifestRequests = new ArrayList<>();
        private final List<String> fileRequests = new ArrayList<>();
        private final List<String> requestedSnapshots = new ArrayList<>();
        private boolean expireSecondaryOnce;
        private boolean secondaryExpired;
        private BeforeFile beforeFile = key -> { };

        private PairConnection(Map<String, String> primary, Map<String, String> secondary) {
            contents.put(PRIMARY_CLIENT, primary);
            contents.put(SECONDARY_CLIENT, secondary);
            contents.forEach((target, files) -> {
                Map<String, EnvironmentManifest.Entry> entries = new HashMap<>();
                files.forEach((path, value) -> {
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    entries.put(path, new EnvironmentManifest.Entry(Sha256.hex(bytes), bytes.length));
                });
                manifests.put(target, new EnvironmentManifest(entries));
            });
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            try {
                JsonObject json = new JsonObject();
                if (message.type() == MessageType.ENV_MANIFEST_REQUEST) {
                    var target = message.json().has("target")
                            ? ModPrefixResolver.Target.valueOf(message.json().get("target").getAsString())
                            : PRIMARY_CLIENT;
                    manifestRequests.add(target);
                    EnvironmentManifest manifest = manifests.get(target);
                    json.addProperty("snapshotId", secondaryExpired ? "v2" : "v1");
                    json.addProperty("filteredHash", manifest.globalHash());
                    json.add("manifest", manifest.toJson());
                    return CompletableFuture.completedFuture(ControlMessage.of(MessageType.ENV_MANIFEST, json));
                }
                assertEquals(MessageType.ENV_FILE_REQUEST, message.type());
                String snapshotId = message.json().get("snapshotId").getAsString();
                requestedSnapshots.add(snapshotId);
                String path = message.json().get("path").getAsString();
                var target = contents.get(PRIMARY_CLIENT).containsKey(path) ? PRIMARY_CLIENT : SECONDARY_CLIENT;
                assertEquals(0, message.json().get("offset").getAsLong());
                String key = target.name() + ":" + path;
                fileRequests.add(key);
                beforeFile.accept(key);
                if (target == SECONDARY_CLIENT && expireSecondaryOnce && !secondaryExpired) {
                    secondaryExpired = true;
                    json.addProperty("code", "snapshot_not_found");
                    return CompletableFuture.completedFuture(ControlMessage.of(MessageType.ERROR, json));
                }
                byte[] payload = contents.get(target).get(path).getBytes(StandardCharsets.UTF_8);
                json.addProperty("snapshotId", snapshotId);
                json.addProperty("path", path);
                json.addProperty("offset", 0);
                json.addProperty("total", payload.length);
                json.addProperty("last", true);
                return CompletableFuture.completedFuture(ControlMessage.of(MessageType.ENV_FILE_DATA, json, payload));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override public void send(ControlMessage message) { }
        @Override public SocketAddress remoteAddress() { return null; }
        @Override public UUID peerId() { return null; }
        @Override public void setPeerId(UUID peerId) { }
        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }
}
