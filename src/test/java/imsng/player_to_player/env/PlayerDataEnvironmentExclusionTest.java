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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 玩家存档已有独立耐久通道，保存背包/位置不应触发整个模组环境重新构建。 */
class PlayerDataEnvironmentExclusionTest {
    @TempDir
    Path directory;

    @Test
    void defaultWorldPlayerSaveDoesNotChangeEnvironmentVersion() throws Exception {
        assertPlayerSaveDoesNotChangeSnapshot("world", List.of());
    }

    @Test
    void actualWorldPlayerSaveDoesNotChangeEnvironmentVersion() throws Exception {
        assertPlayerSaveDoesNotChangeSnapshot("saves/actual-world",
                EnvironmentPathPolicy.worldMetadataExclusions("SAVES\\ACTUAL-WORLD/"));
    }

    @Test
    void centralServiceUsesActualWorldPlayerDirectory() throws Exception {
        var policy = EnvironmentPathPolicy.create(centralExclusions(
                directory, directory.resolve("saves/actual-world")));
        assertFalse(policy.includes("saves/actual-world/playerdata/player.dat"));
        assertTrue(policy.includes("saves/actual-world/serverconfig/config.json"));
        assertTrue(policy.includes("saves/actual-world/stats/player.json"));
    }

    @Test
    void centralServicePreservesWorldAtSourceRootWithoutAnEmptyPrefixError() throws Exception {
        var policy = EnvironmentPathPolicy.create(centralExclusions(directory, directory));
        assertFalse(policy.includes("playerdata/player.dat_old"));
        assertFalse(policy.includes("level.dat"));
        assertTrue(policy.includes("serverconfig/config.json"));
        assertTrue(policy.includes("advancements/player.json"));
    }

    @Test
    void downloaderRejectsDefaultWorldPlayerDataBeforeDownloading() throws Exception {
        assertDownloadRejected("world", "world/playerdata/player.dat", false);
    }

    @Test
    void downloaderRejectsActualWorldBackupBeforeDownloading() throws Exception {
        assertDownloadRejected("saves/actual-world",
                "SAVES/ACTUAL-WORLD/PLAYERDATA/BACKUPS/PLAYER.DAT_OLD", true);
    }

    @Test
    void oldCustomWorldSnapshotCannotServePlayerDataAfterPolicyUpgrade() throws Exception {
        Path source = directory.resolve("source");
        Path repository = directory.resolve("repository");
        String playerPath = "saves/actual-world/playerdata/player.dat";
        write(source.resolve(playerPath), "old-player-position");
        // 模拟旧中转端未应用实际世界目录排除规则时已经发布的快照。
        EnvironmentSnapshot old = new EnvironmentSnapshotStore(source, repository, List.of())
                .buildSnapshot();
        var upgraded = new EnvironmentSnapshotStore(source, repository,
                EnvironmentPathPolicy.worldMetadataExclusions("saves/actual-world"));

        assertTrue(upgraded.loadSnapshots().isEmpty(), "升级后旧snapshotId不得继续下发玩家存档");
        assertThrows(java.io.IOException.class, () -> upgraded.readAllBytes(old, playerPath));
        assertTrue(upgraded.buildSnapshot().manifest().files().isEmpty());
    }

    private void assertPlayerSaveDoesNotChangeSnapshot(String world, List<String> exclusions)
            throws Exception {
        Path source = directory.resolve("source");
        Path repository = directory.resolve("repository");
        write(source.resolve("mods/example.jar"), "unchanged-mod");
        write(source.resolve(world + "/playerdata/player.dat"), "position-before");
        write(source.resolve(world + "/playerdata/player.dat_old"), "backup-before");
        // 排除整个玩家数据目录，包括备份工具建立的子目录和非标准扩展名。
        write(source.resolve(world + "/playerdata/backups/player.save"), "archive-before");
        write(source.resolve(world + "/stats/player.json"), "statistics-still-owned-by-existing-path");
        write(source.resolve(world + "/advancements/player.json"), "advancements-still-owned-by-existing-path");
        write(source.resolve(world + "/serverconfig/settings.json"), "required-mod-config");
        write(source.resolve(world + "/datapacks/example/pack.mcmeta"), "required-data-pack");
        write(source.resolve(world + "/playerdatabase/readme.txt"), "unrelated-prefix");
        var store = new EnvironmentSnapshotStore(source, repository, exclusions);
        EnvironmentSnapshot before = store.buildSnapshot();
        String fingerprintBefore = store.sourceFingerprint();

        write(source.resolve(world + "/playerdata/player.dat"), "position-and-inventory-after");
        write(source.resolve(world + "/playerdata/player.dat_old"), "position-before");
        write(source.resolve(world + "/playerdata/backups/player.save"), "archive-after");
        EnvironmentSnapshot after = store.buildSnapshot();

        assertEquals(before.snapshotId(), after.snapshotId(), "仅玩家状态改变不能产生新环境版本");
        assertEquals(fingerprintBefore, store.sourceFingerprint(), "玩家状态不应触发周期校验重建");
        assertEquals(Set.of("mods/example.jar", world + "/stats/player.json",
                world + "/advancements/player.json", world + "/serverconfig/settings.json",
                world + "/datapacks/example/pack.mcmeta", world + "/playerdatabase/readme.txt"),
                after.manifest().files().keySet(), "没有独立数据通道的文件和模组环境不能一并排除");
    }

    private void assertDownloadRejected(String world, String playerPath, boolean actualWorld)
            throws Exception {
        OldPlayerSnapshot connection = new OldPlayerSnapshot(playerPath);
        Path target = directory.resolve("target");
        write(target.resolve("existing.txt"), "previous-safe-environment");
        var client = actualWorld ? new EnvSyncClient(connection, new GlobalConfig(), world)
                : new EnvSyncClient(connection, new GlobalConfig());

        assertThrows(Exception.class, () -> client.syncTo(target, null).get(5, TimeUnit.SECONDS));
        assertEquals(0, connection.downloads, "在清单阶段拒绝旧快照，不能先下载玩家文件");
        assertEquals("previous-safe-environment", Files.readString(target.resolve("existing.txt")));
        assertFalse(Files.exists(target.resolve(playerPath)));
    }

    private static void write(Path file, String content) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    @SuppressWarnings("unchecked")
    private static List<String> centralExclusions(Path source, Path world) throws Exception {
        // 覆盖中心服务真正使用的拼接入口，不为测试放开生产生命周期类的API。
        var method = imsng.player_to_player.server.P2PServerService.class
                .getDeclaredMethod("worldDataExclusions", Path.class, Path.class);
        method.setAccessible(true);
        return (List<String>) method.invoke(null, source, world);
    }

    /** 仅替代网络边界：真实EnvSyncClient负责清单哈希校验、路径判定和目录事务。 */
    private static final class OldPlayerSnapshot implements ControlConnection {
        private final EnvironmentManifest manifest;
        private int downloads;

        private OldPlayerSnapshot(String file) {
            manifest = new EnvironmentManifest(Map.of(file,
                    new EnvironmentManifest.Entry(Sha256.hex(new byte[]{1}), 1)));
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage request) {
            if (request.type() == MessageType.ENV_MANIFEST_REQUEST) {
                JsonObject reply = new JsonObject();
                reply.addProperty("snapshotId", manifest.globalHash());
                reply.addProperty("filteredHash", manifest.globalHash());
                reply.add("manifest", manifest.toJson());
                return CompletableFuture.completedFuture(request.reply(MessageType.ENV_MANIFEST, reply, null));
            }
            downloads++;
            return CompletableFuture.failedFuture(new IllegalStateException("玩家数据不能进入环境下载阶段"));
        }

        @Override public void send(ControlMessage message) { }
        @Override public SocketAddress remoteAddress() { return null; }
        @Override public UUID peerId() { return null; }
        @Override public void setPeerId(UUID id) { }
        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }
}
