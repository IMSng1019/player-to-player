package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.Sha256;
import imsng.player_to_player.util.JsonUtil;
import com.google.gson.JsonArray;
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

import static org.junit.jupiter.api.Assertions.*;

/** 用真实目录验证双角色发布边界；内存连接只替代网络传输。 */
class EnvSyncTransactionTest {
    @Test
    void canceledOldDownloadCannotOverwriteNewerCompletedPair() throws Exception {
        SnapshotConnection old = new SnapshotConnection();
        old.delaySecondary = new CompletableFuture<>();
        old.secondaryRequested = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean stillOld = new java.util.concurrent.atomic.AtomicBoolean(true);
        var first = new EnvSyncClient(old, new GlobalConfig())
                .syncPair(primary(), secondary(), "v1", stillOld::get);
        assertTrue(old.secondaryRequested.await(5, TimeUnit.SECONDS));
        stillOld.set(false);
        SnapshotConnection next = new SnapshotConnection();
        next.current = "v2";
        new EnvSyncClient(next, new GlobalConfig()).syncPair(primary(), secondary(), "v2", () -> true)
                .get(5, TimeUnit.SECONDS);
        old.delaySecondary.complete(null);
        assertThrows(Exception.class, () -> first.get(5, TimeUnit.SECONDS));
        assertEquals("v2-primary", Files.readString(primary().resolve("config/primary.txt")));
        assertEquals("v2-secondary", Files.readString(secondary().resolve("config/secondary.txt")));
    }
    @TempDir Path tempDir;

    @Test
    void failedSecondDirectoryInstallRollsBackAlreadyInstalledPrimary() throws Exception {
        write(primary().resolve("config/primary.txt"), "old-primary");
        write(secondary().resolve("config/secondary.txt"), "old-secondary");
        byte[] updated = "new".getBytes(StandardCharsets.UTF_8);
        EnvironmentManifest manifest = new EnvironmentManifest(Map.of("value.txt",
                new EnvironmentManifest.Entry(Sha256.hex(updated), updated.length)));
        // 故障只注入目录移动系统调用；日志、第一份安装和回滚仍使用真实文件系统。
        try (var transaction = new EnvironmentDirectoryTransaction((source, target) -> {
            if (source.equals(secondary())) throw new java.io.IOException("模拟副端目录被占用");
            Files.move(source, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        })) {
            write(transaction.stage(primary(), manifest).resolve("value.txt"), "new");
            write(transaction.stage(secondary(), manifest).resolve("value.txt"), "new");
            assertThrows(java.io.IOException.class, transaction::commit);
            assertEquals("old-primary", Files.readString(primary().resolve("config/primary.txt")));
            assertEquals("old-secondary", Files.readString(secondary().resolve("config/secondary.txt")));
        }
    }

    @Test
    void tamperedRecoveryJournalCannotRedirectTargetsOutsideThisWorld() throws Exception {
        Path outside = tempDir.resolve("outside");
        write(outside.resolve("keep.txt"), "untouched");
        String transactionId = UUID.randomUUID().toString();
        JsonObject journal = new JsonObject();
        journal.addProperty("transactionId", transactionId);
        journal.addProperty("status", "PREPARED");
        JsonArray entries = new JsonArray();
        for (int index = 0; index < 2; index++) {
            JsonObject entry = new JsonObject();
            entry.addProperty("target", (index == 0 ? outside : secondary()).toAbsolutePath().normalize().toString());
            entry.addProperty("staging", ".p2p-env-stage-missing-" + index);
            entry.addProperty("backup", ".p2p-env-backup-" + transactionId + "-" + index);
            entry.addProperty("hadOriginal", false);
            entries.add(entry);
        }
        journal.add("directories", entries);
        String digest = Sha256.hex((primary().toAbsolutePath().normalize() + "\n"
                + secondary().toAbsolutePath().normalize()).getBytes(StandardCharsets.UTF_8));
        JsonUtil.writeFileAtomic(tempDir.resolve(".p2p-env-install-" + digest + ".json"), journal);
        SnapshotConnection connection = new SnapshotConnection();
        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary(), secondary()).get(5, TimeUnit.SECONDS));
        assertEquals("untouched", Files.readString(outside.resolve("keep.txt")));
        assertTrue(connection.manifestVersions.isEmpty());
    }

    @Test
    void nextSyncRecoversInterruptedPairPublicationBeforeAnyNetworkRequest() throws Exception {
        Path primary = primary(), secondary = secondary();
        write(primary.resolve("config/primary.txt"), "half-new");
        write(secondary.resolve("config/secondary.txt"), "old-secondary");
        String transactionId = UUID.randomUUID().toString();
        Path backup = primary.resolveSibling(".p2p-env-backup-" + transactionId + "-0");
        write(backup.resolve("config/primary.txt"), "old-primary");
        Path primaryStage = primary.resolveSibling(".p2p-env-stage-interrupted-primary");
        Path secondaryStage = secondary.resolveSibling(".p2p-env-stage-interrupted-secondary");
        write(secondaryStage.resolve("config/secondary.txt"), "new-secondary");
        JsonObject journal = new JsonObject();
        journal.addProperty("transactionId", transactionId);
        journal.addProperty("status", "PREPARED");
        JsonArray entries = new JsonArray();
        for (int index = 0; index < 2; index++) {
            Path target = index == 0 ? primary : secondary;
            JsonObject entry = new JsonObject();
            entry.addProperty("target", target.toAbsolutePath().normalize().toString());
            entry.addProperty("staging", (index == 0 ? primaryStage : secondaryStage).getFileName().toString());
            entry.addProperty("backup", ".p2p-env-backup-" + transactionId + "-" + index);
            entry.addProperty("hadOriginal", true);
            entries.add(entry);
        }
        journal.add("directories", entries);
        String digest = Sha256.hex((primary.toAbsolutePath().normalize() + "\n"
                + secondary.toAbsolutePath().normalize()).getBytes(StandardCharsets.UTF_8));
        Path journalPath = tempDir.resolve(".p2p-env-install-" + digest + ".json");
        JsonUtil.writeFileAtomic(journalPath, journal);
        SnapshotConnection connection = new SnapshotConnection();
        // 故意让网络准备失败：恢复必须发生在网络请求之前，而且不能再次污染旧版本。
        connection.corruptManifestHash = true;
        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary, secondary).get(5, TimeUnit.SECONDS));
        assertEquals("old-primary", Files.readString(primary.resolve("config/primary.txt")));
        assertEquals("old-secondary", Files.readString(secondary.resolve("config/secondary.txt")));
        assertFalse(Files.exists(backup));
        assertFalse(Files.exists(secondaryStage));
        assertFalse(Files.exists(journalPath));
    }

    @Test
    void pairPinsSecondaryManifestToPrimaryVersionEvenWhenServerAdvances() throws Exception {
        SnapshotConnection connection = new SnapshotConnection();
        connection.advanceAfterFirstManifest = true;
        new EnvSyncClient(connection, new GlobalConfig()).syncPair(primary(), secondary())
                .get(5, TimeUnit.SECONDS);
        assertEquals("v1-primary", Files.readString(primary().resolve("config/primary.txt")));
        assertEquals("v1-secondary", Files.readString(secondary().resolve("config/secondary.txt")));
        assertEquals(List.of("latest", "v1"), connection.manifestVersions);
    }

    @Test
    void successRemovesUnlistedFilesIncludingFilesExcludedBySourceScanner() throws Exception {
        write(primary().resolve("config/removed.txt"), "old");
        write(primary().resolve("config/stale.tmp"), "old");
        write(secondary().resolve("mods/removed.jar"), "old");
        new EnvSyncClient(new SnapshotConnection(), new GlobalConfig()).syncPair(primary(), secondary())
                .get(5, TimeUnit.SECONDS);
        assertFalse(Files.exists(primary().resolve("config/removed.txt")));
        assertFalse(Files.exists(primary().resolve("config/stale.tmp")));
        assertFalse(Files.exists(secondary().resolve("mods/removed.jar")));
    }

    @Test
    void failedSecondaryDownloadKeepsBothPreviouslyPublishedDirectories() throws Exception {
        write(primary().resolve("config/primary.txt"), "old-primary");
        write(secondary().resolve("config/secondary.txt"), "old-secondary");
        write(primary().resolve("keep.txt"), "preserved");
        SnapshotConnection connection = new SnapshotConnection();
        connection.failSecondaryDownload = true;
        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary(), secondary()).get(5, TimeUnit.SECONDS));
        assertEquals("old-primary", Files.readString(primary().resolve("config/primary.txt")));
        assertEquals("old-secondary", Files.readString(secondary().resolve("config/secondary.txt")));
        assertEquals("preserved", Files.readString(primary().resolve("keep.txt")));
    }

    @Test
    void incorrectFilteredHashFailsBeforeDownloadingOrPublishing() throws Exception {
        write(primary().resolve("keep.txt"), "old");
        SnapshotConnection connection = new SnapshotConnection();
        connection.corruptManifestHash = true;
        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary(), secondary()).get(5, TimeUnit.SECONDS));
        assertTrue(connection.fileVersions.isEmpty());
        assertEquals("old", Files.readString(primary().resolve("keep.txt")));
    }

    @Test
    void expiredSecondaryRestartsTheWholePairBeforePublication() throws Exception {
        SnapshotConnection connection = new SnapshotConnection();
        connection.expireSecondaryOnce = true;
        new EnvSyncClient(connection, new GlobalConfig()).syncPair(primary(), secondary())
                .get(5, TimeUnit.SECONDS);
        assertEquals("v2-primary", Files.readString(primary().resolve("config/primary.txt")));
        assertEquals("v2-secondary", Files.readString(secondary().resolve("config/secondary.txt")));
        assertEquals(List.of("latest", "v1", "latest", "v2"), connection.manifestVersions);
    }

    @Test
    void explicitlyRequestedVersionDoesNotAdvanceWhenItExpires() throws Exception {
        SnapshotConnection connection = new SnapshotConnection();
        connection.expireSecondaryOnce = true;
        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary(), secondary(), "v1").get(5, TimeUnit.SECONDS));
        assertEquals(List.of("v1", "v1"), connection.manifestVersions);
        assertFalse(Files.exists(primary()));
        assertFalse(Files.exists(secondary()));
    }

    @Test
    void resultCarriesOneVersionAndBothValidatedManifests() throws Exception {
        SnapshotConnection connection = new SnapshotConnection();
        EnvSyncClient.PairResult result = new EnvSyncClient(connection, new GlobalConfig())
                .syncPair(primary(), secondary(), "v1").get(5, TimeUnit.SECONDS);
        assertEquals("v1", result.snapshotId());
        assertEquals(result.primaryManifest().globalHash(), result.primaryHash());
        assertEquals(result.secondaryManifest().globalHash(), result.secondaryHash());
    }

    @Test
    void singleDirectorySyncAlsoPreservesOldFilesWhenAnyDownloadFails() throws Exception {
        write(primary().resolve("config/primary.txt"), "old-primary");
        write(primary().resolve("config/secondary.txt"), "old-secondary");
        SnapshotConnection connection = new SnapshotConnection();
        connection.failSecondaryDownload = true;
        assertThrows(Exception.class, () -> new EnvSyncClient(connection, new GlobalConfig())
                .syncTo(primary(), null).get(5, TimeUnit.SECONDS));
        assertEquals("old-primary", Files.readString(primary().resolve("config/primary.txt")));
        assertEquals("old-secondary", Files.readString(primary().resolve("config/secondary.txt")));
    }

    private Path primary() { return tempDir.resolve("primary/environment"); }
    private Path secondary() { return tempDir.resolve("secondary/environment"); }
    private static void write(Path path, String value) throws Exception {
        Files.createDirectories(path.getParent());
        Files.writeString(path, value);
    }

    private static final class SnapshotConnection implements ControlConnection {
        private final List<String> manifestVersions = new ArrayList<>();
        private final List<String> fileVersions = new ArrayList<>();
        private String current = "v1";
        private boolean advanceAfterFirstManifest;
        private boolean failSecondaryDownload;
        private boolean corruptManifestHash;
        private boolean expireSecondaryOnce;
        private CompletableFuture<Void> delaySecondary;
        private java.util.concurrent.CountDownLatch secondaryRequested;

        @Override public CompletableFuture<ControlMessage> request(ControlMessage message) {
            JsonObject json = new JsonObject();
            if (message.type() == MessageType.ENV_MANIFEST_REQUEST) {
                String requested = message.json().has("snapshotId")
                        ? message.json().get("snapshotId").getAsString() : "latest";
                manifestVersions.add(requested);
                String version = requested.equals("latest") ? current : requested;
                String role = message.json().has("target")
                        ? message.json().get("target").getAsString() : "FULL";
                Map<String, EnvironmentManifest.Entry> entries = new HashMap<>();
                if (!role.equals("SECONDARY_CLIENT")) entries.put("config/primary.txt", entry(version + "-primary"));
                if (!role.equals("PRIMARY_CLIENT")) entries.put("config/secondary.txt", entry(version + "-secondary"));
                EnvironmentManifest manifest = new EnvironmentManifest(entries);
                json.addProperty("snapshotId", version);
                json.addProperty("filteredHash", corruptManifestHash ? "wrong" : manifest.globalHash());
                json.add("manifest", manifest.toJson());
                if (advanceAfterFirstManifest) current = "v2";
                return completed(MessageType.ENV_MANIFEST, json, null);
            }
            String version = message.json().get("snapshotId").getAsString();
            String path = message.json().get("path").getAsString();
            fileVersions.add(version);
            if (path.equals("config/secondary.txt") && (failSecondaryDownload || expireSecondaryOnce)) {
                json.addProperty("code", expireSecondaryOnce ? "snapshot_not_found" : "read_failed");
                expireSecondaryOnce = false;
                current = "v2";
                return completed(MessageType.ERROR, json, null);
            }
            byte[] bytes = (version + (path.endsWith("primary.txt") ? "-primary" : "-secondary"))
                    .getBytes(StandardCharsets.UTF_8);
            json.addProperty("snapshotId", version);
            json.addProperty("path", path);
            json.addProperty("offset", 0);
            json.addProperty("total", bytes.length);
            json.addProperty("last", true);
            if (path.equals("config/secondary.txt") && delaySecondary != null) {
                secondaryRequested.countDown();
                return delaySecondary.thenApply(ignored -> ControlMessage.of(MessageType.ENV_FILE_DATA, json, bytes));
            }
            return completed(MessageType.ENV_FILE_DATA, json, bytes);
        }

        private CompletableFuture<ControlMessage> completed(MessageType type, JsonObject json, byte[] bytes) {
            return CompletableFuture.completedFuture(ControlMessage.of(type, json, bytes));
        }
        private EnvironmentManifest.Entry entry(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            return new EnvironmentManifest.Entry(Sha256.hex(bytes), bytes.length);
        }
        @Override public void send(ControlMessage message) { }
        @Override public SocketAddress remoteAddress() { return null; }
        @Override public UUID peerId() { return null; }
        @Override public void setPeerId(UUID peerId) { }
        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }
}
