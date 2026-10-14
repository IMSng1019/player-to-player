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
