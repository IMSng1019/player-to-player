package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ClientLocalSettingsTest {
    @TempDir Path root;

    @Test void firstInstallationInheritsPersonalOptionsWithoutCopyingSharedConfigOrAccounts() throws Exception {
        Path launcher = Files.createDirectories(root.resolve("launcher"));
        String options = "lang:zh_cn\nsoundCategory_master:0.15\nkey_key.forward:key.keyboard.up\n"
                + "onboardAccessibility:false\nmoddedOption:preserved\n";
        Files.writeString(launcher.resolve("options.txt"), options);
        Files.writeString(launcher.resolve("optionsof.txt"), "ofRenderRegions:true\n");
        Files.createDirectories(launcher.resolve("config"));
        Files.writeString(launcher.resolve("config/rules.json"), "launcher rules");
        Files.writeString(launcher.resolve("launcher_accounts.json"), "private");
        Path world = root.resolve("world-a");
        Path game = runtime(world, "first");

        ClientLocalSettings.prepare(world, launcher, game);

        assertEquals(options, Files.readString(game.resolve("options.txt")));
        assertEquals(options, Files.readString(world.resolve("client-settings/options.txt")));
        assertEquals(options, Files.readString(launcher.resolve("options.txt")));
        assertEquals("ofRenderRegions:true\n", Files.readString(game.resolve("optionsof.txt")));
        assertFalse(Files.exists(game.resolve("config/rules.json")));
        assertFalse(Files.exists(game.resolve("launcher_accounts.json")));
    }

    @Test void reinstallKeepsTheLatestOptionsOfTheSameServer() throws Exception {
        Path world = root.resolve("world-a");
        Path previous = runtime(world, "previous");
        Files.createDirectories(world.resolve("client-settings"));
        Files.writeString(world.resolve("client-settings/options.txt"), "soundCategory_master:0.5");
        Files.writeString(previous.resolve("options.txt"), "lang:zh_cn\nsoundCategory_master:0.1");
        Path next = runtime(world, "next");
        // 即使旧版服务端曾把个人选项作为环境下发，也只能使用本机偏好。
        Files.writeString(next.resolve("options.txt"), "lang:en_us\nsoundCategory_master:1.0");

        ClientLocalSettings.prepare(world, previous, next);

        assertEquals("lang:zh_cn\nsoundCategory_master:0.1", Files.readString(next.resolve("options.txt")));
        assertEquals(Files.readString(previous.resolve("options.txt")),
                Files.readString(world.resolve("client-settings/options.txt")));
    }

    @Test void normalExitSavesServerPreferencesForTheNextLauncherSession() throws Exception {
        Path world = root.resolve("world-a");
        Path previous = runtime(world, "previous");
        Files.writeString(previous.resolve("options.txt"), "lang:zh_cn\nsoundCategory_master:0.2");
        assertTrue(ClientLocalSettings.saveCurrent(previous));
        Path launcher = Files.createDirectories(root.resolve("launcher"));
        Files.writeString(launcher.resolve("options.txt"), "lang:en_us\nsoundCategory_master:0.7");
        Path next = runtime(world, "next");

        ClientLocalSettings.prepare(world, launcher, next);

        assertEquals("lang:zh_cn\nsoundCategory_master:0.2", Files.readString(next.resolve("options.txt")));
        assertEquals("lang:en_us\nsoundCategory_master:0.7", Files.readString(launcher.resolve("options.txt")));
    }

    @Test void switchingServersDoesNotOverwriteAnExistingServerProfile() throws Exception {
        Path firstWorld = root.resolve("world-a");
        Path firstGame = runtime(firstWorld, "first");
        Files.writeString(firstGame.resolve("options.txt"), "lang:zh_cn\nsoundCategory_master:0.1");
        ClientLocalSettings.saveCurrent(firstGame);
        Path secondWorld = root.resolve("world-b");
        Path secondGame = runtime(secondWorld, "first");
        Files.writeString(secondGame.resolve("options.txt"), "lang:en_us\nsoundCategory_master:0.3");
        ClientLocalSettings.saveCurrent(secondGame);
        Path next = runtime(secondWorld, "next");

        ClientLocalSettings.prepare(secondWorld, firstGame, next);

        assertEquals("lang:en_us\nsoundCategory_master:0.3", Files.readString(next.resolve("options.txt")));
        assertEquals("lang:zh_cn\nsoundCategory_master:0.1",
                Files.readString(firstWorld.resolve("client-settings/options.txt")));
    }

    @Test void identicalEnvironmentDoesNotMakeAnotherServersRuntimeReusable() throws Exception {
        Path firstWorld = root.resolve("world-a");
        Path secondWorld = root.resolve("world-b");
        Path firstGame = runtime(firstWorld, "same-environment-hash");
        Path secondGame = runtime(secondWorld, "same-environment-hash");
        assertTrue(ClientLocalSettings.belongsToWorld(firstWorld, firstGame));
        assertTrue(ClientLocalSettings.belongsToWorld(secondWorld, secondGame));
        assertFalse(ClientLocalSettings.belongsToWorld(secondWorld, firstGame));
        assertFalse(ClientLocalSettings.belongsToWorld(firstWorld, root.resolve("launcher")));
    }

    @Test void unboundGameDirectoriesCannotCreateServerProfiles() throws Exception {
        Path ordinary = Files.createDirectories(root.resolve("launcher/game"));
        Files.writeString(ordinary.resolve("options.txt"), "lang:zh_cn");
        assertFalse(ClientLocalSettings.saveCurrent(ordinary));

        Path missingRecord = Files.createDirectories(root.resolve("world/client-runtime/unknown/game"));
        Files.writeString(missingRecord.resolve("options.txt"), "lang:zh_cn");
        assertFalse(ClientLocalSettings.saveCurrent(missingRecord));
        assertFalse(Files.exists(root.resolve("world/client-settings")));
    }

    @Test void missingLocalOptionsDoNotFallBackToServerSuppliedOptions() throws Exception {
        Path launcher = Files.createDirectories(root.resolve("launcher"));
        Path world = root.resolve("world-a");
        Path next = runtime(world, "first");
        Files.writeString(next.resolve("options.txt"), "lang:untrusted");

        ClientLocalSettings.prepare(world, launcher, next);

        assertFalse(Files.exists(next.resolve("options.txt")));
        assertFalse(Files.exists(world.resolve("client-settings/options.txt")));
    }

    @Test void cannotInstallPersonalSettingsIntoTheOriginalLauncher() throws Exception {
        Path launcher = Files.createDirectories(root.resolve("launcher"));
        Files.writeString(launcher.resolve("options.txt"), "original");
        assertThrows(IOException.class, () -> ClientLocalSettings.prepare(root.resolve("world"), launcher, launcher));
        assertEquals("original", Files.readString(launcher.resolve("options.txt")));
    }

    @Test void oversizedOptionsCannotReplaceAnExistingProfile() throws Exception {
        Path world = root.resolve("world-a");
        Path game = runtime(world, "first");
        Files.createDirectories(world.resolve("client-settings"));
        Files.writeString(world.resolve("client-settings/options.txt"), "saved");
        Files.write(game.resolve("options.txt"), new byte[4 * 1024 * 1024 + 1]);

        assertThrows(IOException.class, () -> ClientLocalSettings.saveCurrent(game));
        assertEquals("saved", Files.readString(world.resolve("client-settings/options.txt")));
    }

    @Test void symbolicLinksCannotRedirectPreferenceWrites() throws Exception {
        Path world = root.resolve("world-a");
        Path game = runtime(world, "first");
        Files.writeString(game.resolve("options.txt"), "local");
        Path outside = Files.createDirectories(root.resolve("outside"));
        Files.writeString(outside.resolve("options.txt"), "untouched");
        try {
            Files.createSymbolicLink(world.resolve("client-settings"), outside);
        } catch (IOException | UnsupportedOperationException | SecurityException unsupported) {
            assumeTrue(false, "当前平台不允许测试创建符号链接");
        }

        assertThrows(IOException.class, () -> ClientLocalSettings.saveCurrent(game));
        assertEquals("untouched", Files.readString(outside.resolve("options.txt")));
    }

    @Test void onlyRootPersonalSettingsAreLocal() {
        assertTrue(ClientLocalSettings.isLocalPath("OPTIONS.TXT"));
        assertTrue(ClientLocalSettings.isLocalPath("optionsof.txt"));
        assertTrue(ClientLocalSettings.isLocalPath("optionsshaders.txt"));
        assertFalse(ClientLocalSettings.isLocalPath("config/options.txt"));
        assertFalse(ClientLocalSettings.isLocalPath("servers.dat"));
        assertFalse(ClientLocalSettings.isLocalPath("../options.txt"));
    }

    private Path runtime(Path world, String instance) throws IOException {
        Path game = Files.createDirectories(world.resolve("client-runtime").resolve(instance).resolve("game"));
        Files.writeString(game.getParent().resolve(ClientRuntimeStartup.MANIFEST_FILE), "{}");
        return game;
    }
}
