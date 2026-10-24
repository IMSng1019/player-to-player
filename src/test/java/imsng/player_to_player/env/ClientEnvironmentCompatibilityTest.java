package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import imsng.player_to_player.util.Sha256;
import static org.junit.jupiter.api.Assertions.*;

class ClientEnvironmentCompatibilityTest {
    @TempDir Path root;

    @Test void addingOrRemovingUniversalModsRequiresANewClientProcess() throws Exception {
        Path oldJar = mod("old.jar", "oldmod", "*", "1");
        Path addedJar = mod("added.jar", "addedmod", "*", "1");
        var oldEntry = new EnvironmentManifest.Entry(Sha256.hexOfFile(oldJar), Files.size(oldJar));
        var addedEntry = new EnvironmentManifest.Entry(Sha256.hexOfFile(addedJar), Files.size(addedJar));
        var loaded = Map.of("oldmod", new ClientEnvironmentCompatibility.LoadedMod("oldmod", "1", "*", oldEntry.sha256()));
        assertTrue(ClientEnvironmentCompatibility.check(root, new EnvironmentManifest(Map.of("mods/old.jar", oldEntry)), loaded).isEmpty());
        assertFalse(ClientEnvironmentCompatibility.check(root, new EnvironmentManifest(Map.of(
                "mods/old.jar", oldEntry, "mods/added.jar", addedEntry)), loaded).isEmpty());
        assertFalse(ClientEnvironmentCompatibility.check(root, new EnvironmentManifest(Map.of()), loaded).isEmpty());
    }

    @Test void matchingFilenameIsInsufficientWhenLoadedBytesDiffer() throws Exception {
        Path jar = mod("game.jar", "game", "*", "1");
        var manifest = new EnvironmentManifest(Map.of("mods/game.jar",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(jar), Files.size(jar))));
        var loaded = Map.of("game", new ClientEnvironmentCompatibility.LoadedMod("game", "1", "*", "b".repeat(64)));
        assertFalse(ClientEnvironmentCompatibility.check(root, manifest, loaded).isEmpty());
        var exact = Map.of("game", new ClientEnvironmentCompatibility.LoadedMod("game", "1", "*", Sha256.hexOfFile(jar)));
        assertTrue(ClientEnvironmentCompatibility.check(root, manifest, exact).isEmpty());
    }

    @Test void serverOnlyJarIsNotARequiredClientModButExtraUniversalModIsRejected() throws Exception {
        Path jar = mod("server.jar", "servermod", "server", "1");
        var manifest = new EnvironmentManifest(Map.of("mods/server.jar",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(jar), Files.size(jar))));
        assertTrue(ClientEnvironmentCompatibility.check(root, manifest, Map.of()).isEmpty());
        var cosmetic = new ClientEnvironmentCompatibility.LoadedMod("cosmetic", "1", "client", "a".repeat(64));
        assertTrue(ClientEnvironmentCompatibility.check(root, manifest, Map.of("cosmetic", cosmetic)).isEmpty());
        var extra = new ClientEnvironmentCompatibility.LoadedMod("extra", "1", "*", "a".repeat(64));
        assertFalse(ClientEnvironmentCompatibility.check(root, manifest, Map.of("extra", extra)).isEmpty());
    }

    @Test void nestedCandidateCanSatisfySameJarAndItsModulesAreNotExtraTopLevelMods() throws Exception {
        Path jar = mod("nested.jar", "nested", "*", "1");
        var manifest = new EnvironmentManifest(Map.of("mods/nested.jar",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(jar), Files.size(jar))));
        var nested = new ClientEnvironmentCompatibility.LoadedMod("nested", "1", "*", Sha256.hexOfFile(jar), true);
        var apiModule = new ClientEnvironmentCompatibility.LoadedMod("api_module", "1", "*", "b".repeat(64), true);
        assertTrue(ClientEnvironmentCompatibility.check(root, manifest, Map.of("nested", nested, "api_module", apiModule)).isEmpty());
    }

    @Test void dedicatedServerRequiresServerModsAndIgnoresClientOnlyMods() throws Exception {
        Path server = mod("server.jar", "serverlogic", "server", "1");
        Path client = mod("client.jar", "cosmetic", "client", "1");
        var manifest = new EnvironmentManifest(Map.of("mods/server.jar",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(server), Files.size(server)), "mods/client.jar",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(client), Files.size(client))));
        assertFalse(ClientEnvironmentCompatibility.check(root, manifest, Map.of(), "server").isEmpty());
        var loaded = new ClientEnvironmentCompatibility.LoadedMod("serverlogic", "1", "server", Sha256.hexOfFile(server));
        assertTrue(ClientEnvironmentCompatibility.check(root, manifest, Map.of("serverlogic", loaded), "server").isEmpty());
    }

    private Path mod(String name, String id, String environment, String version) throws Exception {
        Path jar = root.resolve("mods").resolve(name);
        Files.createDirectories(jar.getParent());
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(jar))) {
            zip.putNextEntry(new ZipEntry("fabric.mod.json"));
            zip.write(("{\"id\":\"" + id + "\",\"version\":\"" + version
                    + "\",\"environment\":\"" + environment + "\"}").getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return jar;
    }
}
