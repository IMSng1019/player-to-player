package imsng.player_to_player.env;

import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class EnvironmentUploadStoreTest {
    @TempDir Path root;

    @Test
    void staleWriterCannotOverwriteAnotherGroupsChange() throws Exception {
        Path file = root.resolve("config/game.json");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "new-authoritative");
        assertEquals("conflict", apply("config/game.json", Sha256.hex("old"), "mine"));
        assertEquals("new-authoritative", Files.readString(file));
    }

    @Test
    void conditionalUpdateAndRetryAreIdempotent() throws Exception {
        Path file = root.resolve("config/game.json");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "before");
        assertEquals("applied", apply("config/game.json", Sha256.hex("before"), "after"));
        assertEquals("unchanged", apply("config/game.json", Sha256.hex("before"), "after"));
        assertEquals("after", Files.readString(file));
    }

    @Test
    void newFilesAndDeletionStillRequireMatchingBase() throws Exception {
        assertEquals("applied", apply("config/new.json", "", "created"));
        assertEquals("conflict", apply("config/new.json", "", "replace"));
        assertEquals("applied", apply("config/new.json", Sha256.hex("created"), null));
        assertFalse(Files.exists(root.resolve("config/new.json")));
    }

    @Test
    void rejectsTraversalExecutablesAndMutableWorldState() throws Exception {
        assertEquals("forbidden", apply("config/../server.properties", "", "override"));
        assertEquals("forbidden", apply("mods/evil.jar", "", "jar"));
        assertEquals("forbidden", apply("world/level.dat", "", "old-world"));
        assertEquals("forbidden", apply("../escape.json", "", "escape"));
    }

    private String apply(String path, String base, String replacement) throws Exception {
        Class<?> type = assertDoesNotThrow(() -> Class.forName(
                "imsng.player_to_player.env.EnvironmentUploadStore"));
        return (String) type.getMethod("apply", Path.class, List.class, String.class,
                String.class, byte[].class).invoke(null, root, List.of("config"), path, base,
                replacement == null ? null : replacement.getBytes(StandardCharsets.UTF_8));
    }
}
