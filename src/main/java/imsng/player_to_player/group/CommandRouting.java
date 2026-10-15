package imsng.player_to_player.group;

import com.mojang.brigadier.ParseResults;
import com.mojang.brigadier.context.CommandContextBuilder;
import com.mojang.brigadier.context.ParsedArgument;
import com.mojang.brigadier.context.ParsedCommandNode;
import com.mojang.brigadier.tree.ArgumentCommandNode;
import com.mojang.brigadier.tree.CommandNode;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.arguments.DimensionArgument;
import net.minecraft.commands.arguments.EntityArgument;
import net.minecraft.commands.arguments.coordinates.BlockPosArgument;
import net.minecraft.commands.arguments.coordinates.ColumnPosArgument;
import net.minecraft.commands.arguments.coordinates.Coordinates;
import net.minecraft.commands.arguments.coordinates.Vec2Argument;
import net.minecraft.commands.arguments.coordinates.Vec3Argument;
import net.minecraft.commands.arguments.selector.EntitySelector;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.core.BlockPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 指令分类（主/副客户端与物理服务端<b>共用</b>）：一条指令是否引用"执行上下文"，
 * 以及它引用了哪些方块坐标。这是 2026-10-15 定稿口径里两条守则的判定核心。
 *
 * <h2>形状规则（决策 2）</h2>
 * 凡引用执行上下文的指令 —— {@code @s}/{@code @p}/{@code @r}、相对/局部坐标
 * （{@code ~}/{@code ^}）、以及<b>缺省位置或缺省目标</b>（如 {@code /summon zombie}、
 * {@code /gamemode creative}、裸 {@code /kill} —— 目标默认就是执行者本人）——
 * <b>只在发起组执行</b>，且指令源用发起玩家实体，原版语义（谁执行、以谁为中心）完整保留。
 * 其余指令在所有组各执行一次（用户口径："一条指令的语义 = 全网各执行一次"）。
 *
 * <h2>归属预检（决策 1，挂死守则）</h2>
 * 方块类指令携带的坐标参数被解析成方块坐标交给调用方查区块注册表：
 * <b>只有真正持有这些区块的组</b>才执行，其余组直接跳过。理由是实测的挂死机制 ——
 * 被接管的集成服务端里 {@code Level.getBlockState} 等路径会在主线程
 * {@code managedBlock + join} 等待区块加载，而<b>本组不持有、被其他组占用</b>的区块
 * 其加载 future 永远悬置（门控无限重试）⇒ 主线程<b>永久忙等</b>（集成服没有 watchdog）。
 * 组内玩家视野里的区块本来就有 PLAYER 票（原版照常加），所以"执行到别人的区块"
 * 不是理论情形，而是随时可复现的冻结。
 *
 * <h2>判定来源：解析树，不维护指令表</h2>
 * 判定完全从 Brigadier 解析结果自动得出（用户口径"不用维护白名单"）：
 * <ul>
 *   <li>坐标参数：{@code Coordinates} 的 {@code isXRelative/isYRelative/isZRelative}
 *       （{@code ~} 与 {@code ^} 两种形式都为 true，已用 javap 核实）→ 上下文；</li>
 *   <li>选择器参数：{@code @s}（{@code EntitySelector.isSelfSelector()}）与
 *       {@code @p}/{@code @r}/带距离或坐标过滤的选择器（按实参原文判定，
 *       {@code EntitySelector} 的位置字段是私有的）→ 上下文；
 *       具名玩家与 {@code @a}/{@code @e}（无位置过滤）不依赖源，可全网各执行一次；</li>
 *   <li>缺省参数：解析路径的末节点挂着<b>未被消费</b>的位置类/实体类参数
 *       ⇒ 该指令会用执行者自己的位置或实体（如 {@code /summon zombie}、
 *       {@code /gamemode creative}）→ 上下文；</li>
 *   <li>维度重定向（{@code /execute in <维度> …}）：绝对坐标属于哪个维度无法从
 *       本地解析判定 → 一律按上下文处理（保守）。</li>
 * </ul>
 *
 * <h2>线程与调用面</h2>
 * 纯函数、无状态、无阻塞；服务端在 Netty/io 线程用物理服解析器调用，接收端在
 * 集成服主线程用自己的解析器调用。两侧的解析树必须来自同一套指令注册
 * （模组环境同步保证），判定结论因此一致。
 */
public final class CommandRouting {

    /** 分类结果：是否引用执行上下文；引用的绝对方块坐标（可能为空）。 */
    public record Analysis(boolean contextBound, List<BlockPos> blockPositions) {

        /** 无坐标、不引用上下文的常量（入口异常时的保守回退）。 */
        static final Analysis BROADCAST = new Analysis(false, List.of());

        /**
         * 引用的方块坐标所覆盖的<b>区块范围</b>（含两端）；无坐标返回 null。
         * <p>
         * 为什么看范围而不是逐点：{@code /fill}、{@code /clone} 只声明角点，
         * <b>内部方块不属于任何实参</b>，而执行时会逐个遍历 —— 只查角点会漏掉
         * "角点在本组、内部在他组"的形态，那正是主线程永久忙等的最短路径。
         * 保守取所有实参坐标的包围盒，宁可多拒（失败方向是"不执行"）也不放过。
         */
        public ChunkRange chunkRange() {
            if (blockPositions.isEmpty()) {
                return null;
            }
            int minX = Integer.MAX_VALUE;
            int minZ = Integer.MAX_VALUE;
            int maxX = Integer.MIN_VALUE;
            int maxZ = Integer.MIN_VALUE;
            for (BlockPos pos : blockPositions) {
                int x = pos.getX() >> 4;
                int z = pos.getZ() >> 4;
                minX = Math.min(minX, x);
                minZ = Math.min(minZ, z);
                maxX = Math.max(maxX, x);
                maxZ = Math.max(maxZ, z);
            }
            return new ChunkRange(minX, minZ, maxX, maxZ);
        }
    }

    /** 区块范围（含两端，单位=区块坐标）。 */
    public record ChunkRange(int minX, int minZ, int maxX, int maxZ) {

        /** 范围内区块数（长方体展开）。 */
        public long count() {
            return (long) (maxX - minX + 1) * (maxZ - minZ + 1);
        }
    }

    /**
     * 单条指令允许覆盖的区块数上限（超出即拒绝执行）。
     * <p>
     * 防的是"用一个巨大的包围盒指令把预检变成 DoS"（{@code /fill 0 0 0 100000 100 100000}
     * 之类的形态原版自己也会因方块数上限拒绝，但预检发生在原版之前）。
     * 8192 个区块 ≈ 90×90 区块，远超正常玩法（原版 /fill 上限 32768 方块 ≈ 32 区块），
     * 不会误伤任何合理指令。
     */
    public static final long MAX_ROUTING_CHUNKS = 8192L;

    private CommandRouting() {
    }

    // ------------------------------------------------------ 归属复核（接收端用，纯函数）

    /**
     * 解析转发帧的 {@code ownedChunks} 字段（服务端核过的"持有区块"清单）。
     * <p>
     * 入站数据不可信：字段缺失/不是数组/条目非法一律跳过（跳过不等于放行 ——
     * 有问题的条目本身就不构成"需要复核的持有区块"）。
     *
     * @return 可复核的区块键列表（可能为空）
     */
    public static List<ChunkKey> parseChunkKeys(com.google.gson.JsonElement raw) {
        if (raw == null || !raw.isJsonArray()) {
            return List.of();
        }
        List<ChunkKey> keys = new ArrayList<>();
        for (com.google.gson.JsonElement element : raw.getAsJsonArray()) {
            if (!element.isJsonPrimitive()) {
                continue;
            }
            try {
                keys.add(ChunkKey.parse(element.getAsString()));
            } catch (RuntimeException e) {
                // 非法键跳过
            }
        }
        return List.copyOf(keys);
    }

    /**
     * 接收端归属复核：服务端核过的持有区块必须<b>仍然</b>由本组持有。
     * <p>
     * 复核的是"服务端预检 → 实际执行"之间的归属变化（合并/分离/退出事务期间会发生）。
     * 不通过则调用方<b>不执行</b>：集成服主线程一旦碰触"本组不持有"的区块，加载
     * future 会永远悬置，主线程在 {@code managedBlock + join} 里永久忙等。
     *
     * @param keys    服务端核过的持有区块
     * @param claimed 本组是否仍持有该区块
     * @return null = 放行；否则为拒绝文案
     */
    public static String verifyOwnedChunks(List<ChunkKey> keys,
                                           java.util.function.Predicate<ChunkKey> claimed) {
        if (keys == null || keys.isEmpty()) {
            return null; // 无坐标指令，或"无人持有 → 发起组按需申请"：无需复核
        }
        for (ChunkKey key : keys) {
            if (!claimed.test(key)) {
                return "目标区块当前不由本组持有（" + key.x() + "," + key.z() + "），已跳过执行";
            }
        }
        return null;
    }

    /**
     * 分类一条已解析的指令。
     *
     * @param parse 解析结果；为 null 时返回"不引用上下文、无坐标"（调用方按广播处理）
     */
    public static Analysis analyze(ParseResults<CommandSourceStack> parse) {
        if (parse == null || parse.getContext() == null) {
            return Analysis.BROADCAST;
        }
        boolean contextBound = false;
        List<BlockPos> positions = new ArrayList<>(2);
        try {
            String input = parse.getReader() != null ? parse.getReader().getString() : "";
            CommandSourceStack source = parse.getContext().getSource();
            // 逐层走上下文链：/execute … run <子指令> 的内层参数在子上下文里
            for (CommandContextBuilder<CommandSourceStack> builder = parse.getContext();
                 builder != null; builder = builder.getChild()) {
                Map<String, ParsedArgument<CommandSourceStack, ?>> arguments =
                        builder.getArguments();
                for (ParsedCommandNode<CommandSourceStack> parsed : builder.getNodes()) {
                    if (!(parsed.getNode() instanceof ArgumentCommandNode<?, ?> node)) {
                        continue;
                    }
                    if (node.getType() instanceof DimensionArgument) {
                        contextBound = true; // 维度重定向：坐标归属不可本地判定
                        continue;
                    }
                    ParsedArgument<CommandSourceStack, ?> argument =
                            arguments.get(node.getName());
                    if (argument == null) {
                        continue; // 该参数没有实参（可选未给）
                    }
                    Object value = argument.getResult();
                    if (value instanceof Coordinates coordinates) {
                        if (isRelative(coordinates)) {
                            contextBound = true; // ~ 或 ^：以执行者位置/朝向为原点
                        }
                        if (isPositionKind(node.getType()) && source != null) {
                            // 位置类实参一律结算为方块坐标：绝对坐标与执行者无关；
                            // 相对/局部坐标用**发起者上报的位置与朝向**解析（服务端构造的
                            // 解析源就锚在那里）—— 这样"包围盒"对 /execute positioned
                            // 里包着的绝对坐标、以及 ~ ~ ~ 形态都成立，越界才能被拦下。
                            positions.add(coordinates.getBlockPos(source));
                        }
                        // 其余（旋转类：RotationArgument 的朝向、绝对旋转）与位置无关，忽略
                    } else if (value instanceof EntitySelector selector) {
                        if (isContextSelector(selector, rangeText(input, argument))) {
                            contextBound = true;
                        }
                    }
                }
                contextBound |= hasDefaultedContextArgument(builder);
            }
        } catch (RuntimeException e) {
            // 分类失败绝不能影响指令本身的执行：保守按"上下文指令"处理（只落发起组）
            return new Analysis(true, List.of());
        }
        return new Analysis(contextBound, List.copyOf(positions));
    }

    /** 相对/局部坐标（{@code ~}/{@code ^}）：两种形式在 Coordinates 上都为 relative。 */
    private static boolean isRelative(Coordinates coordinates) {
        return coordinates.isXRelative() || coordinates.isYRelative() || coordinates.isZRelative();
    }

    /**
     * 位置类参数类型（它们的绝对形态可以直接结算为方块坐标）。
     * <p>
     * {@link Vec2Argument} 也计入：{@code /spreadplayers <center> …} 的中心是 XZ 坐标
     * （把"散点区域"当成无坐标指令广播给所有组，会让每个组在别人领地里取样读方块 →
     * 主线程永久忙等）。注意原版的<b>旋转</b>用的是 {@link RotationArgument}（已 javap
     * 核实 {@code /tp} 的 rotation 是 RotationArgument），不会被这里捞进来。
     */
    private static boolean isPositionKind(Object type) {
        return type instanceof Vec3Argument || type instanceof BlockPosArgument
                || type instanceof ColumnPosArgument || type instanceof Vec2Argument;
    }

    /**
     * 选择器是否引用执行上下文。
     * <p>
     * {@code @s} 用原版 API 判定；{@code @p}/{@code @r}（"最近/随机"必须知道源位置）
     * 与带距离/坐标过滤的选择器按实参原文判定；具名玩家与 {@code @a}/{@code @e}
     * 不依赖源，交给所有组各解析一次（既是用户口径，也复现"目标不在该组的那些组
     * 报错、什么也不做"的兜底）。
     */
    private static boolean isContextSelector(EntitySelector selector, String text) {
        if (selector.isSelfSelector()) {
            return true; // @s
        }
        if (text == null) {
            return false;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        if (!lower.startsWith("@")) {
            return false; // 具名玩家：绝对目标
        }
        if (lower.startsWith("@p") || lower.startsWith("@r")) {
            return true; // 最近/随机：以源位置排序
        }
        return lower.contains("distance=") || lower.contains("x=") || lower.contains("y=")
                || lower.contains("z=") || lower.contains("dx=") || lower.contains("dy=")
                || lower.contains("dz=") || lower.contains("sort=nearest")
                || lower.contains("sort=furthest");
    }

    /**
     * 末节点是否存在<b>未被消费</b>的位置类/实体类子参数。
     * <p>
     * 命中即"该指令会使用执行者自己的位置或实体"：例如 {@code /summon zombie}
     * （可选 [pos] 未给 → 用源位置）、{@code /gamemode creative}（可选 [target] 未给
     * → 目标是执行者本人）、裸 {@code /kill}、{@code /clear}、{@code /tp Bob}
     * （未给 destination → 传送到执行者身边）。这些在别的组里会静默落到该组出生点
     * 或选中别人，所以必须只留在发起组。
     */
    private static boolean hasDefaultedContextArgument(
            CommandContextBuilder<CommandSourceStack> builder) {
        List<ParsedCommandNode<CommandSourceStack>> nodes = builder.getNodes();
        if (nodes.isEmpty()) {
            return false;
        }
        CommandNode<CommandSourceStack> leaf = nodes.get(nodes.size() - 1).getNode();
        for (CommandNode<CommandSourceStack> child : leaf.getChildren()) {
            if (!(child instanceof ArgumentCommandNode<?, ?> argument)) {
                continue;
            }
            if (builder.getArguments().containsKey(child.getName())) {
                continue; // 已给实参：位置/目标由实参决定，另行判定
            }
            if (!isDefaultableContextKind(argument.getType())) {
                continue;
            }
            if (isDefaultedFromSibling(builder, argument.getType())) {
                continue; // 缺省值取自同指令的另一个同类型实参（如 /forceload add <from> 的 to=from）
            }
            return true;
        }
        return false;
    }

    /**
     * 该缺省参数是否"由同指令的另一个同类型实参补齐"（与执行者位置无关）。
     * <p>
     * 典型：{@code /forceload add <from> [<to>]} —— 只给 from 时 to 取 from
     * （javap 核实的原版实现），末节点的 to 子参数未被消费。若按"缺省位置=执行者位置"
     * 处理，这条指令会只留在发起组：发起组设了票据却认领不到别人持有的区块，
     * 区块永远加载不出来（且仍回原版"Added N chunk(s)"，静默失效）。
     */
    private static boolean isDefaultedFromSibling(
            CommandContextBuilder<CommandSourceStack> builder, Object childType) {
        Class<?> childClass = childType.getClass();
        for (ParsedCommandNode<CommandSourceStack> parsed : builder.getNodes()) {
            if (!(parsed.getNode() instanceof ArgumentCommandNode<?, ?> node)) {
                continue;
            }
            if (node.getType().getClass() != childClass) {
                continue;
            }
            // 同类型实参已给 ⇒ 缺省值按"取该实参"处理（位置仍由它决定，另行提取坐标）。
            // 只认同类型：/spawnpoint <targets> [<pos>] 这种"缺省位置真的取执行者位置"
            // 的形态不会被误放行（targets 是 EntityArgument，与 pos 的 Vec3Argument 不同类）。
            if (builder.getArguments().containsKey(node.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 可以"缺省"为执行上下文的参数类型：位置类（缺省 → 用执行者位置）与实体类
     * （缺省 → 就是执行者本人）。
     * <p>
     * 刻意<b>不含</b>旋转类（{@code RotationArgument}/{@code Vec2Argument}）：
     * 缺省旋转只影响"朝向"这一细节（{@code /tp Alice 1 2 3} 的朝向继承执行者），
     * 把它算成上下文会让"给别的组的玩家设定绝对落点"这类指令完全不可用，
     * 代价明显大于收益。
     */
    private static boolean isDefaultableContextKind(Object type) {
        return isPositionKind(type) || type instanceof EntityArgument;
    }

    /** 取实参原文（选择器原文判定用）。 */
    private static String rangeText(String input, ParsedArgument<CommandSourceStack, ?> argument) {
        var range = argument.getRange();
        if (range == null || range.isEmpty() || input.isEmpty()) {
            return null;
        }
        int start = Math.max(0, range.getStart());
        int end = Math.min(input.length(), range.getEnd());
        return start >= end ? null : input.substring(start, end);
    }
}
