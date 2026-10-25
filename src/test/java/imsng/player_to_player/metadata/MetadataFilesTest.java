package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import imsng.player_to_player.world.WorldIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class MetadataFilesTest {
    @TempDir Path directory;
    private static CompoundTag data(long seed, long day, long time) {
        CompoundTag value = new CompoundTag();
        CompoundTag generation = new CompoundTag(); generation.putLong("seed", seed);
        value.put("WorldGenSettings", generation); value.putLong("DayTime", day); value.putLong("Time", time);
        return value;
    }
    private static MetadataSnapshot snapshot(CompoundTag data) {
        CompoundTag root = new CompoundTag(); root.put("Data", data);
        return new MetadataSnapshot(WorldIdentity.ofLevelData(root).orElseThrow(), 4, 2, data);
    }
    private void write(Path world, CompoundTag data) throws Exception {
        Files.createDirectories(world); CompoundTag root = new CompoundTag(); root.put("Data", data);
        NbtIo.writeCompressed(root, world.resolve("level.dat"));
    }

    @Test void installsWithoutEnvironmentLevelDatAndPreservesIndependentPlayerAndSimulationClock() throws Exception {
        Path environment = directory.resolve("environment"); Files.createDirectories(environment);
        Path save = directory.resolve("save");
        CompoundTag old = data(42, 10, 900);
        CompoundTag player = new CompoundTag(); player.putString("inventory", "valuable"); old.put("Player", player);
        old.putString("deletedField", "stale"); write(save, old);
        CompoundTag authoritative = data(42, 7000, 80); authoritative.putString("modField", "retained");
        authoritative.putString("__p2pCommandReceipt", "internal-authority-ledger");
        MetadataFiles.prepareSave(environment, save, snapshot(authoritative), 1234);
        CompoundTag result = NbtIo.readCompressed(save.resolve("level.dat"), NbtAccounter.unlimitedHeap()).getCompound("Data");
        assertEquals(7000, result.getLong("DayTime")); assertEquals(900, result.getLong("Time"));
        assertEquals(player, result.getCompound("Player")); assertEquals("retained", result.getString("modField"));
        assertFalse(result.contains("deletedField"));
        assertFalse(result.contains("__p2pCommandReceipt"));
    }

    @Test void differentWorldQuarantinesEntireCacheBeforeInstallingSnapshot() throws Exception {
        Path environment = directory.resolve("environment"); Files.createDirectories(environment);
        Path save = directory.resolve("save"); write(save, data(1, 0, 99));
        Files.createDirectories(save.resolve("region")); Files.writeString(save.resolve("region/old.mca"), "old-chunks");
        MetadataFiles.prepareSave(environment, save, snapshot(data(2, 50, 5)), 1234);
        assertFalse(Files.exists(save.resolve("region/old.mca")));
        assertEquals(snapshot(data(2, 50, 5)).worldId(), WorldIdentity.ofLevelDatFile(save.resolve("level.dat")).orElseThrow());
        try (var files = Files.list(directory)) { assertTrue(files.anyMatch(path -> path.getFileName().toString().startsWith("save.stale-"))); }
    }

    @Test void refusesSnapshotWhoseGenerationIdentityDoesNotMatchEnvelope() {
        MetadataSnapshot invalid = new MetadataSnapshot("wrong-world", 0, 1, data(42, 0, 0));
        assertThrows(IOException.class, () -> MetadataFiles.prepareSave(directory.resolve("env"), directory.resolve("save"), invalid, 1234));
        assertFalse(Files.exists(directory.resolve("save")));
    }
}
