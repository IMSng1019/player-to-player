package imsng.player_to_player.world;

import net.minecraft.SharedConstants;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地存档世界身份守门的回归测试（对应 {@code docs/plans/2026-10-14-world-identity-mismatch-corruption.md}
 * §8-1 标注"最重要"的用例，也是那次数据损坏缺陷的唯一自动化守卫）。
 * <p>
 * 关键点：隔离必须<b>整体改名</b>。半隔离（只删/只换 level.dat、把旧 region 留在原目录）
 * 会让旧区块继续被门控的本地磁盘回退读命中，缺陷原样复发 —— 而重建是"合并式复制"，
 * 不会清理多余文件。
 */
class LocalSaveGuardTest {

    @TempDir
    Path temporary;

    @Test
    void matchingIdentityReusesExistingSaveAndReturnsIdentity() throws Exception {
        Path env = writeLevel(skeleton(7462156823368393586L));
        Path save = temporary.resolve("data/world");
        writeLevelFile(save, levelData(7462156823368393586L));
        Path oldRegion = save.resolve("region/r.0.0.mca");
        Files.createDirectories(oldRegion.getParent());
        Files.writeString(oldRegion, "玩家自己的区块缓存");

        String identity = LocalSaveGuard.prepareSave(env, save, 1_700_000_000_000L);

        assertFalse(identity.isEmpty(), "身份一致时必须返回已对齐的身份哈希");
        assertEquals(7462156823368393586L, WorldIdentity.seedOf(
                WorldIdentityTestSupport.read(save.resolve("level.dat"))).orElseThrow());
        assertTrue(Files.exists(oldRegion), "同世界必须复用既有存档（继承既有区块，不重新下载）");
        assertNoQuarantine(temporary.resolve("data"));
    }

    @Test
    void changedIdentityQuarantinesWholeSaveAndRebuildsFromSkeleton() throws Exception {
        Path env = writeLevel(skeleton(7462156823368393586L)); // 服务端：新世界
        Path save = temporary.resolve("data/world");
        writeLevelFile(save, levelData(3614799650234757351L)); // 本地：旧世界
        Path oldRegion = save.resolve("region/r.0.0.mca");
        Files.createDirectories(oldRegion.getParent());
        Files.writeString(oldRegion, "旧世界的建筑");

        String identity = LocalSaveGuard.prepareSave(env, save, 1_700_000_000_000L);

        assertEquals(7462156823368393586L,
                WorldIdentity.seedOf(WorldIdentityTestSupport.read(save.resolve("level.dat")))
                        .orElseThrow(),
                "重建后的 level.dat 必须来自服务端骨架（否则集成的服务端会按旧种子生成地形）");
        assertFalse(Files.exists(save.resolve("region")),
                "旧 region 绝不能留在原目录：合并式复制不清理多余文件，留下就会被本地磁盘回退读命中");
        assertFalse(identity.isEmpty());
        Path stale = onlyQuarantine(temporary.resolve("data"));
        assertTrue(Files.exists(stale.resolve("region/r.0.0.mca")),
                "隔离必须是改名保留（旧存档可能还有人工价值），不能删除");
        assertEquals("旧世界的建筑", Files.readString(stale.resolve("region/r.0.0.mca")));
    }

    @Test
    void saveDirWithoutLevelDatButWithStaleRegionIsQuarantinedToo() throws Exception {
        // 这是"只删 level.dat"或"从不含 level.dat 的备份恢复"造成的状态：目录里只剩旧
        // region。若只看 level.dat 是否存在就跳过校验，合并式重建会补上一个正确的
        // level.dat，让这个目录从此"看起来已对齐"，旧区块被永久放行。
        Path env = writeLevel(skeleton(7462156823368393586L));
        Path save = temporary.resolve("data/world");
        Path oldRegion = save.resolve("region/r.0.0.mca");
        Files.createDirectories(oldRegion.getParent());
        Files.writeString(oldRegion, "旧世界区块但 level.dat 被删了");

        LocalSaveGuard.prepareSave(env, save, 1_700_000_000_000L);

        assertFalse(Files.exists(save.resolve("region")),
                "level.dat 缺失的目录同样不可信：必须整体隔离，不能让旧 region 被重建掩盖");
        Path stale = onlyQuarantine(temporary.resolve("data"));
        assertEquals("旧世界区块但 level.dat 被删了",
                Files.readString(stale.resolve("region/r.0.0.mca")));
    }

    @Test
    void dataFixerRewriteOfSameWorldIsNotTreatedAsWorldChange() throws Exception {
        // 跨版本升级时原版会把旧形态的 level.dat 迁移成新形态：生成设置的结构哈希变了，
        // 但种子没变 —— 世界没换。误隔离会丢掉未物化到 MCA 的耐久区块，故必须判为同世界。
        Path env = writeLevel(skeleton(42L));                 // 迁移后的新形态
        Path save = temporary.resolve("data/world");
        writeLevelFile(save, legacyLevelData(42L));           // 迁移前的旧形态（RandomSeed）
        Path oldRegion = save.resolve("region/r.0.0.mca");
        Files.createDirectories(oldRegion.getParent());
        Files.writeString(oldRegion, "升级前的区块");

        LocalSaveGuard.prepareSave(env, save, 1_700_000_000_000L);

        assertTrue(Files.exists(oldRegion), "同种子（仅 level.dat 结构被升级重写）必须复用本地存档");
        assertNoQuarantine(temporary.resolve("data"));
    }

    @Test
    void missingSkeletonIsRejectedWithoutTouchingLocalSave() throws Exception {
        Path env = temporary.resolve("environment/world");
        Files.createDirectories(env); // 没有 level.dat：环境同步未完成/服务端未把世界纳入清单
        Path save = temporary.resolve("data/world");
        writeLevelFile(save, levelData(1L));

        assertThrows(IOException.class,
                () -> LocalSaveGuard.prepareSave(env, save, 1_700_000_000_000L),
                "缺骨架必须中止切换（玩家留在物理服），不能带着旧存档继续");
        assertTrue(Files.exists(save.resolve("level.dat")), "中止时不得改动本地存档");
    }

    @Test
    void unreadableLocalLevelDatIsQuarantined() throws Exception {
        Path env = writeLevel(skeleton(7L));
        Path save = temporary.resolve("data/world");
        Files.createDirectories(save);
        Files.writeString(save.resolve("level.dat"), "这不是 gzip NBT");

        LocalSaveGuard.prepareSave(env, save, 1_700_000_000_000L);

        Path stale = onlyQuarantine(temporary.resolve("data"));
        assertTrue(Files.readString(stale.resolve("level.dat")).contains("这不是 gzip NBT"),
                "损坏的本地存档同样按'无法确认同源'处理并隔离保留");
    }

    // ------------------------------------------------------------ 夹具

    /** 服务端环境骨架目录（含世界 level.dat）。 */
    private Path writeLevel(CompoundTag tag) throws IOException {
        Path dir = temporary.resolve("environment/world");
        writeLevelFile(dir, tag);
        return dir;
    }

    private static void writeLevelFile(Path dir, CompoundTag tag) throws IOException {
        SharedConstants.tryDetectVersion();
        Files.createDirectories(dir);
        NbtIo.writeCompressed(tag, dir.resolve("level.dat"));
    }

    private static CompoundTag skeleton(long seed) {
        return levelData(seed);
    }

    /** 新形态：Data.WorldGenSettings（含 seed）。 */
    private static CompoundTag levelData(long seed) {
        CompoundTag worldGenSettings = new CompoundTag();
        worldGenSettings.putLong("seed", seed);
        worldGenSettings.putByte("generate_features", (byte) 1);
        CompoundTag data = new CompoundTag();
        data.put("WorldGenSettings", worldGenSettings);
        CompoundTag root = new CompoundTag();
        root.put("Data", data);
        return root;
    }

    /** 旧形态：只有 Data.RandomSeed（模拟版本升级前的存档）。 */
    private static CompoundTag legacyLevelData(long seed) {
        CompoundTag data = new CompoundTag();
        data.putLong("RandomSeed", seed);
        data.putLong("LastPlayed", 1234567890L);
        CompoundTag root = new CompoundTag();
        root.put("Data", data);
        return root;
    }

    private static void assertNoQuarantine(Path parent) throws IOException {
        try (Stream<Path> list = Files.list(parent)) {
            List<String> stale = list.map(p -> p.getFileName().toString())
                    .filter(name -> name.contains(".stale-")).toList();
            assertTrue(stale.isEmpty(), "不该产生隔离目录，实际: " + stale);
        }
    }

    private static Path onlyQuarantine(Path parent) throws IOException {
        try (Stream<Path> list = Files.list(parent)) {
            List<Path> stale = list.filter(p -> p.getFileName().toString().contains(".stale-"))
                    .toList();
            assertEquals(1, stale.size(), "应恰好产生一个隔离目录，实际: " + stale);
            return stale.get(0);
        }
    }

    /** 断言辅助：读取 level.dat 根标签（放在独立类里避免与测试共享可变状态）。 */
    static final class WorldIdentityTestSupport {
        private WorldIdentityTestSupport() {
        }

        static CompoundTag read(Path file) throws IOException {
            return NbtIo.readCompressed(file, net.minecraft.nbt.NbtAccounter.unlimitedHeap());
        }
    }

    @Test
    void quarantinedNameCarriesOldIdentityShortId() throws Exception {
        Path env = writeLevel(skeleton(7462156823368393586L));
        Path save = temporary.resolve("data/world");
        writeLevelFile(save, levelData(3614799650234757351L));
        String localIdentity = WorldIdentity.snapshot(save.resolve("level.dat"))
                .identityHash();

        LocalSaveGuard.prepareSave(env, save, 1_700_000_000_000L);

        Path stale = onlyQuarantine(temporary.resolve("data"));
        assertNotNull(localIdentity);
        assertTrue(stale.getFileName().toString()
                        .startsWith("world.stale-" + WorldIdentity.shortId(localIdentity) + "-"),
                "隔离目录名要带上旧世界身份短标识，便于人工核对: " + stale.getFileName());
    }
}
