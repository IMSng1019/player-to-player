package imsng.player_to_player.world;

import net.minecraft.SharedConstants;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.StringTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 世界身份哈希的行为覆盖（缺陷背景见
 * {@code docs/plans/2026-10-14-world-identity-mismatch-corruption.md}）：
 * <ul>
 *   <li>同内容不同插入顺序 → 同身份（规范化排序生效）；</li>
 *   <li>种子/生成器设置不同 → 身份不同（换世界可识别）；</li>
 *   <li>每局都变的字段（内嵌玩家、时间、DataVersion）不参与身份（防"每次加入都误判"）；</li>
 *   <li>缺 WorldGenSettings 的旧存档回退 RandomSeed；两者都没有 → 无法确定身份；</li>
 *   <li>文件级：损坏/缺失 level.dat 一律返回空（调用方须按"无法确认同源"处理）。</li>
 * </ul>
 */
class WorldIdentityTest {

    @TempDir
    Path temporary;

    @Test
    void sameWorldGenSettingsWithDifferentInsertionOrderGiveSameIdentity() {
        CompoundTag first = levelData(13579L, true);
        CompoundTag second = levelData(13579L, false);
        assertEquals(WorldIdentity.ofLevelData(first), WorldIdentity.ofLevelData(second),
                "复合标签内部是无序表，身份必须与键的插入顺序无关（否则同世界会被判成换世界）");
    }

    @Test
    void differentSeedGivesDifferentIdentity() {
        assertNotEquals(WorldIdentity.ofLevelData(levelData(1L, true)),
                WorldIdentity.ofLevelData(levelData(2L, true)),
                "种子不同必须是不同身份：这是发现'服务端已换世界'的唯一依据");
    }

    @Test
    void differentGeneratorSettingsGiveDifferentIdentity() {
        CompoundTag a = levelData(7L, true);
        CompoundTag b = levelData(7L, true);
        b.getCompound("Data").getCompound("WorldGenSettings")
                .getCompound("dimensions").getCompound("minecraft:overworld")
                .getCompound("generator").putString("settings", "minecraft:amplified");
        assertNotEquals(WorldIdentity.ofLevelData(a), WorldIdentity.ofLevelData(b),
                "同种子不同生成器配置是两个不同世界，不能被当作同源");
    }

    @Test
    void volatileFieldsDoNotAffectIdentity() {
        CompoundTag before = levelData(42L, true);
        CompoundTag after = levelData(42L, true);
        CompoundTag data = after.getCompound("Data");
        data.putInt("DataVersion", 9999);           // 版本升级会改
        data.putLong("LastPlayed", 123456789L);      // 每局都变
        data.putLong("Time", 98765L);                // 每局都变
        CompoundTag player = new CompoundTag();
        player.putInt("XpLevel", 30);
        data.put("Player", player);                  // PlayerStateFiles 每次加入都会改写
        assertEquals(WorldIdentity.ofLevelData(before), WorldIdentity.ofLevelData(after),
                "易变字段参与身份会导致每次加入都误判为换世界，进而无谓隔离玩家存档");
    }

    @Test
    void dimensionListOrderMatters() {
        CompoundTag a = levelData(5L, true);
        CompoundTag b = levelData(5L, true);
        ListTag list = new ListTag();
        list.add(StringTag.valueOf("minecraft:overworld"));
        list.add(StringTag.valueOf("minecraft:the_nether"));
        b.getCompound("Data").getCompound("WorldGenSettings").put("dimension_order", list);
        assertNotEquals(WorldIdentity.ofLevelData(a), WorldIdentity.ofLevelData(b),
                "列表是有序结构，其内容差异必须进入身份");
    }

    @Test
    void fallsBackToRandomSeedForLegacyLevelData() {
        CompoundTag legacy = new CompoundTag();
        CompoundTag data = new CompoundTag();
        data.putLong("RandomSeed", 7462156823368393586L);
        legacy.put("Data", data);
        Optional<String> identity = WorldIdentity.ofLevelData(legacy);
        assertTrue(identity.isPresent(), "缺 WorldGenSettings 的旧存档应回退 RandomSeed 而非放弃判定");

        CompoundTag otherSeed = new CompoundTag();
        CompoundTag otherData = new CompoundTag();
        otherData.putLong("RandomSeed", 3614799650234757351L);
        otherSeed.put("Data", otherData);
        assertNotEquals(identity, WorldIdentity.ofLevelData(otherSeed),
                "回退路径同样必须能区分种子");
    }

    @Test
    void returnsEmptyWhenIdentitySubsetMissing() {
        CompoundTag root = new CompoundTag();
        root.put("Data", new CompoundTag());
        assertTrue(WorldIdentity.ofLevelData(root).isEmpty(),
                "没有 WorldGenSettings 也没有 RandomSeed 时必须返回'无法确定'，不得退化成常量身份");
        assertTrue(WorldIdentity.ofLevelData(new CompoundTag()).isEmpty(),
                "缺 Data 复合标签同样按无法确定处理");
        assertTrue(WorldIdentity.ofLevelData(null).isEmpty(), "null 按无法确定处理");
    }

    @Test
    void readsIdentityFromLevelDatFile() throws Exception {
        SharedConstants.tryDetectVersion();
        Path levelDat = temporary.resolve("level.dat");
        NbtIo.writeCompressed(levelData(88L, true), levelDat);
        Optional<String> fromFile = WorldIdentity.ofLevelDatFile(levelDat);
        assertTrue(fromFile.isPresent(), "正常 level.dat 必须能读出身份");
        assertEquals(WorldIdentity.ofLevelData(levelData(88L, true)), fromFile,
                "文件路径与内存路径必须给出一致身份");
    }

    @Test
    void missingOrCorruptLevelDatYieldsNoIdentity() throws Exception {
        assertTrue(WorldIdentity.ofLevelDatFile(temporary.resolve("不存在.dat")).isEmpty(),
                "文件缺失按无法确定处理");
        assertTrue(WorldIdentity.ofLevelDatFile(null).isEmpty(), "null 路径按无法确定处理");

        Path corrupt = temporary.resolve("broken.dat");
        Files.writeString(corrupt, "这不是 gzip NBT");
        assertTrue(WorldIdentity.ofLevelDatFile(corrupt).isEmpty(),
                "损坏文件按无法确定处理：调用方据此禁止复用旧本地存档");
    }

    @Test
    void shortIdIsStablePrefixAndHandlesUnknown() {
        String identity = WorldIdentity.ofLevelData(levelData(3L, true)).orElseThrow();
        assertEquals(identity.substring(0, 8), WorldIdentity.shortId(identity),
                "短标识只取前缀，便于日志与隔离目录名对照");
        assertEquals("unknown", WorldIdentity.shortId(""));
        assertEquals("unknown", WorldIdentity.shortId(null));
        assertFalse(WorldIdentity.shortId("abc").isEmpty());
    }

    /** 构造一份 level.dat 形态的根标签；{@code seedFirst} 控制键插入顺序。 */
    private static CompoundTag levelData(long seed, boolean seedFirst) {
        CompoundTag generator = new CompoundTag();
        generator.putString("type", "minecraft:noise");
        generator.putString("settings", "minecraft:overworld");
        CompoundTag overworld = new CompoundTag();
        overworld.putString("type", "minecraft:overworld");
        overworld.put("generator", generator);
        CompoundTag dimensions = new CompoundTag();
        dimensions.put("minecraft:overworld", overworld);

        CompoundTag worldGenSettings = new CompoundTag();
        if (seedFirst) {
            worldGenSettings.putLong("seed", seed);
            worldGenSettings.putByte("generate_features", (byte) 1);
            worldGenSettings.put("dimensions", dimensions);
        } else {
            worldGenSettings.put("dimensions", dimensions);
            worldGenSettings.putByte("generate_features", (byte) 1);
            worldGenSettings.putLong("seed", seed);
        }

        CompoundTag data = new CompoundTag();
        data.put("WorldGenSettings", worldGenSettings);
        data.putInt("DataVersion", 3700);

        CompoundTag root = new CompoundTag();
        root.put("Data", data);
        return root;
    }
}
