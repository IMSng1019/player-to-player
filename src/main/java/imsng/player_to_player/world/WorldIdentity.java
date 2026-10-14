package imsng.player_to_player.world;

import imsng.player_to_player.util.Sha256;
import net.minecraft.nbt.ByteArrayTag;
import net.minecraft.nbt.ByteTag;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.FloatTag;
import net.minecraft.nbt.IntArrayTag;
import net.minecraft.nbt.IntTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.LongArrayTag;
import net.minecraft.nbt.LongTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.ShortTag;
import net.minecraft.nbt.StringTag;
import net.minecraft.nbt.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 世界身份：由一个存档 {@code level.dat} 的 <b>世界生成身份子集</b>算出的 SHA-256 短标识。
 * <p>
 * 存在意义（缺陷背景见 {@code docs/plans/2026-10-14-world-identity-mismatch-corruption.md}）：
 * 世界文件夹键只有 {@code IP+世界名}，服务端换世界/重置世界后客户端仍会复用同一份本地
 * {@code data/<世界名>/}，于是玩家看到的是旧世界、退出时又把旧区块上行覆盖服务端 MCA。
 * 本类给出"这两份 level.dat 是不是同一个世界"的可比对依据。
 *
 * <h2>身份输入为什么是这两个字段</h2>
 * <ul>
 *   <li>{@code Data.WorldGenSettings} —— 种子、{@code generate_features}、奖励箱与三个维度的
 *       生成器设置全部在其中，是"同一世界"的充分依据；</li>
 *   <li>旧存档缺失该复合标签时回退 {@code Data.RandomSeed}（+ 可选的 {@code Data.Generator}）。</li>
 * </ul>
 * <b>刻意不包含</b> {@code Data.DataVersion}、{@code LastPlayed}、{@code Time}、内嵌玩家等：
 * 前者随版本升级变化、后三者每局都变（{@code PlayerStateFiles.installAuthoritative} 每次加入
 * 都会改写 {@code Data.Player}），把它们算进身份会导致"每次加入都误判为换世界"。
 *
 * <h2>为什么不用整文件哈希</h2>
 * {@code level.dat} 是 gzip 压缩 + NBT，同一份逻辑内容在不同写入路径下字节并不相同；
 * 而 {@code CompoundTag} 内部是 {@code HashMap}，直接 {@code toString()} 的键序不可控。
 * 因此本类把身份子集递归规范化成<b>键名排序</b>的确定性字符串再哈希 —— 内容等价即哈希相等。
 *
 * <p><b>线程模型</b>：纯函数 + 只读文件访问，可在任意线程调用（调用方通常是 io 线程）。
 */
public final class WorldIdentity {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/world-identity");

    /**
     * 身份输入口径版本。改动身份子集/编码方式时必须递增：旧身份哈希与新口径不可比对，
     * 消费方（客户端隔离、服务端状态隔离）按"口径不同则不复用旧结论"处理。
     */
    public static final String FORMAT_VERSION = "v1";

    /** level.dat 根下的世界元数据复合标签。 */
    private static final String DATA = "Data";
    private static final String WORLD_GEN_SETTINGS = "WorldGenSettings";
    private static final String RANDOM_SEED = "RandomSeed";
    private static final String GENERATOR = "Generator";

    /** 单份 level.dat 解压后的读取上限（防解压炸弹；正常 level.dat 远小于此）。 */
    private static final long MAX_LEVEL_DAT_BYTES = 64L * 1024 * 1024;

    /** 短标识长度（日志与隔离目录名用；8 位十六进制足够区分人工可辨的世界）。 */
    private static final int SHORT_ID_LENGTH = 8;

    private WorldIdentity() {
    }

    /**
     * 世界身份视图：身份哈希 + 世界种子。
     * <p>
     * 为什么要带种子：身份哈希的输入是 {@code WorldGenSettings} 的<b>结构</b>，而
     * {@code level.dat} 是原版自己会在存盘时整体重写的文件 —— 跨版本升级时 DataFixer
     * 会把旧形态（{@code RandomSeed}/{@code Generator}）迁移成新形态
     * （{@code WorldGenSettings}），结构哈希必然变化而世界没换。种子是两侧都稳定的锚点，
     * 用来把这类"结构变了、世界没变"的情形与"真的换世界"区分开。
     *
     * @param identityHash 身份哈希；空串 = 无法确定
     * @param seed         世界种子；空 = 该存档没有可用的种子字段
     */
    public record Snapshot(String identityHash, java.util.Optional<Long> seed) {

        /** 无法确定的身份视图（文件缺失/损坏/缺少身份字段）。 */
        public static Snapshot unknown() {
            return new Snapshot("", java.util.Optional.empty());
        }

        /** 是否给出了可用的身份哈希。 */
        public boolean known() {
            return identityHash != null && !identityHash.isEmpty();
        }

        /** 短标识（日志/隔离目录名用）。 */
        public String shortId() {
            return WorldIdentity.shortId(identityHash);
        }
    }

    /**
     * 两份身份视图是否属于同一个世界。
     * <p>
     * 判定顺序：① 身份哈希相等 → 同世界；② 否则两侧种子都已知且相等 → 同世界
     * （版本升级重写生成设置结构但世界未换）；③ 其余（含任一侧无法确定）→ <b>不是</b>同世界。
     * <p>
     * <b>已知边界</b>：同种子不同生成器配置（如 amplification）会被判为同世界 —— 这是
     * 为了让"跨版本升级"不误判而付出的代价；"同种子重建世界"同理识别不了（那需要世界实例
     * UUID，属计划中的 P1）。
     */
    public static boolean sameWorld(Snapshot first, Snapshot second) {
        if (first == null || second == null) {
            return false;
        }
        if (first.known() && second.known()
                && first.identityHash().equals(second.identityHash())) {
            return true;
        }
        return first.seed().isPresent() && second.seed().isPresent()
                && first.seed().get().equals(second.seed().get());
    }

    /** 读取 level.dat 文件并给出身份视图；文件缺失/损坏时给出 {@link Snapshot#unknown()}。 */
    public static Snapshot snapshot(Path levelDat) {
        if (levelDat == null || !Files.isRegularFile(levelDat)) {
            return Snapshot.unknown();
        }
        try {
            CompoundTag root = NbtIo.readCompressed(levelDat,
                    NbtAccounter.create(MAX_LEVEL_DAT_BYTES));
            return snapshot(root);
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("读取 level.dat 提取世界身份失败（按无法确定处理）: {}", levelDat, e);
            return Snapshot.unknown();
        }
    }

    /** 从一个已解析的 level.dat 根标签给出身份视图。 */
    public static Snapshot snapshot(CompoundTag root) {
        return new Snapshot(ofLevelData(root).orElse(""), seedOf(root));
    }

    /**
     * 提取世界种子：优先 {@code Data.WorldGenSettings.seed}，回退 {@code Data.RandomSeed}。
     *
     * @return 种子；两个字段都不存在时为空
     */
    public static java.util.Optional<Long> seedOf(CompoundTag root) {
        if (root == null || !root.contains(DATA, Tag.TAG_COMPOUND)) {
            return java.util.Optional.empty();
        }
        CompoundTag data = root.getCompound(DATA);
        if (data.contains(WORLD_GEN_SETTINGS, Tag.TAG_COMPOUND)
                && data.getCompound(WORLD_GEN_SETTINGS).contains("seed", Tag.TAG_LONG)) {
            return java.util.Optional.of(
                    data.getCompound(WORLD_GEN_SETTINGS).getLong("seed"));
        }
        if (data.contains(RANDOM_SEED, Tag.TAG_LONG)) {
            return java.util.Optional.of(data.getLong(RANDOM_SEED));
        }
        return java.util.Optional.empty();
    }

    /**
     * 从一个已解析的 level.dat 根标签提取世界身份哈希。
     *
     * @return 身份哈希（小写十六进制 SHA-256）；无法确定身份时返回 {@link Optional#empty()}
     *         —— 只有既无 {@code WorldGenSettings} 又无 {@code RandomSeed} 的畸形存档才会如此
     */
    public static Optional<String> ofLevelData(CompoundTag root) {
        if (root == null || !root.contains(DATA, Tag.TAG_COMPOUND)) {
            return Optional.empty();
        }
        CompoundTag data = root.getCompound(DATA);
        StringBuilder input = new StringBuilder(FORMAT_VERSION);
        if (data.contains(WORLD_GEN_SETTINGS, Tag.TAG_COMPOUND)) {
            input.append("|wgs=");
            appendCanonical(data.get(WORLD_GEN_SETTINGS), input);
        } else {
            boolean any = false;
            if (data.contains(RANDOM_SEED, Tag.TAG_LONG)) {
                input.append("|rs=").append(data.getLong(RANDOM_SEED));
                any = true;
            }
            if (data.contains(GENERATOR, Tag.TAG_COMPOUND)) {
                input.append("|gen=");
                appendCanonical(data.get(GENERATOR), input);
                any = true;
            }
            if (!any) {
                return Optional.empty();
            }
        }
        return Optional.of(Sha256.hex(input.toString()));
    }

    /**
     * 读取一个 {@code level.dat} 文件并提取世界身份。
     * 文件不存在、损坏或缺少身份字段一律返回 {@link Optional#empty()}（调用方须按
     * "无法确认同源"处理，不得按"同源"处理）。
     */
    public static Optional<String> ofLevelDatFile(Path levelDat) {
        if (levelDat == null || !Files.isRegularFile(levelDat)) {
            return Optional.empty();
        }
        try {
            CompoundTag root = NbtIo.readCompressed(levelDat,
                    NbtAccounter.create(MAX_LEVEL_DAT_BYTES));
            return ofLevelData(root);
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("读取 level.dat 提取世界身份失败（按无法确认处理）: {}", levelDat, e);
            return Optional.empty();
        }
    }

    /** 身份哈希的短标识（日志/隔离目录名用）；输入为空时返回 "unknown"。 */
    public static String shortId(String identityHash) {
        if (identityHash == null || identityHash.isBlank()) {
            return "unknown";
        }
        return identityHash.length() <= SHORT_ID_LENGTH
                ? identityHash
                : identityHash.substring(0, SHORT_ID_LENGTH);
    }

    /**
     * 把 NBT 标签递归写成确定性字符串：复合标签<b>按键名排序</b>、列表按顺序、
     * 数值用各自类型的精确十进制（避免 double 走字符串化丢精度）。
     */
    private static void appendCanonical(Tag tag, StringBuilder out) {
        if (tag == null) {
            out.append("null");
            return;
        }
        switch (tag.getId()) {
            case Tag.TAG_COMPOUND -> {
                CompoundTag compound = (CompoundTag) tag;
                List<String> keys = new ArrayList<>(compound.getAllKeys());
                keys.sort(Comparator.naturalOrder());
                out.append('{');
                for (String key : keys) {
                    out.append(key).append('=');
                    appendCanonical(compound.get(key), out);
                    out.append(';');
                }
                out.append('}');
            }
            case Tag.TAG_LIST -> {
                ListTag list = (ListTag) tag;
                out.append('[');
                for (int i = 0; i < list.size(); i++) {
                    appendCanonical(list.get(i), out);
                    out.append(',');
                }
                out.append(']');
            }
            case Tag.TAG_BYTE -> out.append(((ByteTag) tag).getAsByte());
            case Tag.TAG_SHORT -> out.append(((ShortTag) tag).getAsShort());
            case Tag.TAG_INT -> out.append(((IntTag) tag).getAsInt());
            case Tag.TAG_LONG -> out.append(((LongTag) tag).getAsLong());
            case Tag.TAG_FLOAT -> out.append(((FloatTag) tag).getAsFloat());
            case Tag.TAG_DOUBLE -> out.append(((DoubleTag) tag).getAsDouble());
            case Tag.TAG_STRING -> {
                StringTag string = (StringTag) tag;
                out.append('"').append(string.getAsString().replace("\\", "\\\\")
                        .replace("\"", "\\\"")).append('"');
            }
            case Tag.TAG_BYTE_ARRAY -> out.append(java.util.Arrays.toString(
                    ((ByteArrayTag) tag).getAsByteArray()));
            case Tag.TAG_INT_ARRAY -> out.append(java.util.Arrays.toString(
                    ((IntArrayTag) tag).getAsIntArray()));
            case Tag.TAG_LONG_ARRAY -> out.append(java.util.Arrays.toString(
                    ((LongArrayTag) tag).getAsLongArray()));
            default -> out.append(tag.getAsString());
        }
    }
}
