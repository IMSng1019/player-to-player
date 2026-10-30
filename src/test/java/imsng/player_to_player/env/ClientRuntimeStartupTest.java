package imsng.player_to_player.env;

import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ClientRuntimeStartupTest {
    @TempDir Path directory;

    @Test void firstIsolatedLaunchNeedsStartupEvidenceEvenForAnEmptyEnvironment() {
        assertFalse(ClientRuntimeStartup.matches("a".repeat(64), new EnvironmentManifest(Map.of()), "world"));
    }

    @Test void addedAndDeletedConfigurationChangesRuntimeIdentityWithoutChangingMods() throws Exception {
        Path config = directory.resolve("config/rules.txt");
        Files.createDirectories(config.getParent());
        Files.writeString(config, "rules");
        var empty = new EnvironmentManifest(Map.of());
        var configured = new EnvironmentManifest(Map.of("config/rules.txt",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config))));
        var startup = ClientRuntimeStartup.capture(directory, empty);
        assertFalse(startup.matches(configured, "world"));
        assertNotEquals(EnvironmentRuntimeIdentity.hash(empty, "world"), EnvironmentRuntimeIdentity.hash(configured, "world"));
        assertEquals(EnvironmentRuntimeIdentity.hash(empty, "world"),
                EnvironmentRuntimeIdentity.hash(new EnvironmentManifest(Map.of()), "world"));
    }

    @Test void changedConfigurationBeforeStartupCannotPassWithOnlyVersionFlag() throws Exception {
        Path config = directory.resolve("config/game.json");
        Files.createDirectories(config.getParent());
        Files.writeString(config, "required");
        var required = new EnvironmentManifest(Map.of("config/game.json",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config))));
        Files.writeString(config, "edited");
        var startup = ClientRuntimeStartup.capture(directory, required);
        assertFalse(startup.matches(required, "world"));
    }

    @Test void snapshotRepresentsStartupBytesInsteadOfLaterDiskChanges() throws Exception {
        Path config = directory.resolve("game.json");
        Files.writeString(config, "loaded");
        var required = new EnvironmentManifest(Map.of("game.json",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config))));
        var startup = ClientRuntimeStartup.capture(directory, required);
        Files.writeString(config, "changed-after-loading");
        assertTrue(startup.matches(required, "world"));
        var changed = new EnvironmentManifest(Map.of("game.json",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config))));
        assertFalse(startup.matches(changed, "world"));
    }

    @Test void localOptionsInAnOlderManifestCannotForceAnotherEnvironmentRestart() throws Exception {
        Path config = directory.resolve("config/game.json");
        Files.createDirectories(config.getParent());
        Files.writeString(config, "server-rules");
        var rules = new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config));
        var required = new EnvironmentManifest(Map.of("config/game.json", rules,
                "options.txt", new EnvironmentManifest.Entry("a".repeat(64), 12),
                "optionsof.txt", new EnvironmentManifest.Entry("b".repeat(64), 13)));
        // 旧服的环境清单可能仍携带个人选项；本地保留值与其不同并不意味着模组环境未加载。
        Files.writeString(directory.resolve("options.txt"), "lang:zh_cn\nsoundCategory_master:0.1\n");
        var startup = ClientRuntimeStartup.capture(directory, required);
        assertTrue(startup.matches(required, "world"));
        // 真实玩法配置仍参与启动证据，不能借保留本地选项绕过服务端配置校验。
        var changed = new EnvironmentManifest(Map.of("config/game.json",
                new EnvironmentManifest.Entry("c".repeat(64), 20)));
        assertFalse(startup.matches(changed, "world"));
    }
}
