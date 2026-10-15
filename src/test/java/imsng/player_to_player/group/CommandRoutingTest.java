package imsng.player_to_player.group;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.ParseResults;
import net.minecraft.SharedConstants;
import net.minecraft.commands.CommandBuildContext;
import net.minecraft.commands.CommandSource;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.core.BlockPos;
import net.minecraft.core.HolderLookup;
import net.minecraft.data.registries.VanillaRegistries;
import net.minecraft.network.chat.Component;
import net.minecraft.server.Bootstrap;
import net.minecraft.world.flag.FeatureFlags;
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 指令分类器（形状规则 + 归属预检的判定核心）对<b>真实原版指令树</b>的行为锁定。
 *
 * <p>用 {@code new Commands(CommandSelection.ALL, …)} 建出与运行期同构的解析树，
 * 逐条断言"是否引用执行上下文"与"引用的方块坐标"。这两条判据直接决定：
 * ① 哪些指令只留在发起组（用发起玩家实体当源，保原版语义）；
 * ② 哪些指令只在真正持有相关区块的组执行（挂死守则：集成服主线程对
 * 非本组区块的加载会永久忙等）。</p>
 *
 * <p>解析源是"没有世界"的构造：原版 {@code requires} 判据只读权限位
 * （{@code hasPermission}），坐标/选择器解析不碰世界，故 null 世界足够。</p>
 */
class CommandRoutingTest {

    private static CommandDispatcher<CommandSourceStack> dispatcher;

    @BeforeAll
    static void bootstrap() {
        SharedConstants.tryDetectVersion();
        Bootstrap.bootStrap();
        HolderLookup.Provider registries = VanillaRegistries.createLookup();
        CommandBuildContext buildContext = CommandBuildContext.simple(
                registries, FeatureFlags.DEFAULT_FLAGS);
        Commands commands = new Commands(Commands.CommandSelection.ALL, buildContext);
        dispatcher = commands.getDispatcher();
    }

    private static CommandRouting.Analysis analyze(String command, int permission) {
        CommandSourceStack source = new CommandSourceStack(CommandSource.NULL,
                Vec3.ZERO, Vec2.ZERO, null, permission, "tester",
                Component.literal("tester"), null, null);
        ParseResults<CommandSourceStack> parse = dispatcher.parse(command, source);
        assertTrue(parse.getExceptions().isEmpty(),
                "测试夹具必须能解析: /" + command + " → " + parse.getExceptions());
        return CommandRouting.analyze(parse);
    }

    // ------------------------------------------------------------ 上下文类（只落发起组）

    @Test
    @DisplayName("相对/局部坐标：~ 与 ^ 都按上下文处理")
    void relativeCoordinatesAreContext() {
        assertTrue(analyze("setblock ~ ~1 ~ stone", 4).contextBound());
        assertTrue(analyze("setblock ^ ^ ^ stone", 4).contextBound());
        assertTrue(analyze("fill ~ ~ ~ ~10 ~10 ~10 stone", 4).contextBound());
        assertTrue(analyze("execute positioned 0 64 0 run setblock ~ ~ ~ stone", 4)
                .contextBound());
    }

    @Test
    @DisplayName("@s/@p/@r 与带距离过滤的选择器：源实体/源位置语义，只落发起组")
    void sourceSelectorsAreContext() {
        assertTrue(analyze("give @s diamond", 4).contextBound());
        assertTrue(analyze("give @p diamond", 4).contextBound());
        assertTrue(analyze("effect give @r speed 10", 4).contextBound());
        assertTrue(analyze("kill @e[distance=..5]", 4).contextBound());
        assertTrue(analyze("execute as @s run say hi", 4).contextBound());
    }

    @Test
    @DisplayName("缺省位置/缺省目标：目标或落点默认就是执行者本人，只落发起组")
    void defaultedContextArgumentsAreContext() {
        assertTrue(analyze("summon zombie", 4).contextBound(), "/summon zombie 落在执行者位置");
        assertTrue(analyze("gamemode creative", 4).contextBound(), "F3+F4 的指令形态");
        assertTrue(analyze("kill", 4).contextBound(), "裸 /kill 的目标是执行者");
        assertTrue(analyze("clear", 4).contextBound());
        assertTrue(analyze("setworldspawn", 4).contextBound());
        assertTrue(analyze("spawnpoint", 4).contextBound());
    }

    @Test
    @DisplayName("/tp <目标> 的简写形态：判为可广播，由 /tp 特例与\"虚拟源 entity=null\"双兜底")
    void teleportShortFormIsHandledBySafetyNets() {
        // /tp <目的地> 的语义是"把执行者送到目的地" —— 按狭义判据它引用执行者，
        // 但原版树里它表现为 destination 实体参数（既非缺省位置也非源选择器）。
        // 双重兜底使其结论无害：
        //   ① /tp <玩家名> 由服务端特例按玩家表解析（与跨组 tp 同一条实现）；
        //   ② 非发起组的执行以 entity=null 的虚拟源进行 ⇒ 原版直接报
        //      "No entity was found"，不会把任何玩家搬到该组出生点。
        assertFalse(analyze("tp Alice", 4).contextBound());
        // 给出落点的形态是绝对坐标，交由持有该区块的组执行
        CommandRouting.Analysis explicit = analyze("tp Alice 1 2 3", 4);
        assertFalse(explicit.contextBound());
        assertEquals(List.of(new BlockPos(1, 2, 3)), explicit.blockPositions());
        // 以执行者为目标/相对坐标的形态仍是上下文
        assertTrue(analyze("tp @s Alice", 4).contextBound());
        assertTrue(analyze("tp Alice ~ ~ ~", 4).contextBound());
    }

    @Test
    @DisplayName("维度重定向：绝对坐标的归属维度无法本地判定，保守按上下文")
    void dimensionRedirectIsContext() {
        assertTrue(analyze("execute in minecraft:the_nether run setblock 0 64 0 stone", 4)
                .contextBound());
    }

    // ------------------------------------------------------------ 可广播类（全网各执行一次）

    @Test
    @DisplayName("世界状态类：无坐标、无源依赖 → 所有组各执行一次")
    void worldStateCommandsAreBroadcast() {
        assertFalse(analyze("time set day", 4).contextBound());
        assertFalse(analyze("weather clear", 4).contextBound());
        assertFalse(analyze("gamerule doDaylightCycle false", 4).contextBound());
        assertFalse(analyze("difficulty peaceful", 4).contextBound());
        assertFalse(analyze("list", 0).contextBound());
        assertFalse(analyze("say hello", 4).contextBound());
    }

    @Test
    @DisplayName("绝对坐标：坐标值不依赖执行者，可在任意组结算 + 提取方块坐标")
    void absolutePositionsAreExtracted() {
        CommandRouting.Analysis set = analyze("setblock 1 2 3 stone", 4);
        assertFalse(set.contextBound());
        assertEquals(List.of(new BlockPos(1, 2, 3)), set.blockPositions());

        CommandRouting.Analysis fill = analyze("fill 0 0 0 10 20 30 minecraft:stone", 4);
        assertFalse(fill.contextBound());
        assertEquals(2, fill.blockPositions().size());
        assertEquals(new BlockPos(0, 0, 0), fill.blockPositions().get(0));
        assertEquals(new BlockPos(10, 20, 30), fill.blockPositions().get(1));

        CommandRouting.Analysis clone = analyze("clone 0 0 0 1 1 1 2 2 2", 4);
        assertFalse(clone.contextBound());
        assertEquals(3, clone.blockPositions().size());

        CommandRouting.Analysis execute = analyze(
                "execute if block 0 64 0 minecraft:stone run say hi", 4);
        assertFalse(execute.contextBound());
        assertEquals(List.of(new BlockPos(0, 64, 0)), execute.blockPositions());
    }

    @Test
    @DisplayName("具名目标与无位置过滤的 @a/@e：不依赖源，交各组各解析一次")
    void absoluteTargetsAreBroadcast() {
        assertFalse(analyze("give Alice diamond", 4).contextBound());
        assertFalse(analyze("gamemode creative Alice", 4).contextBound());
        assertFalse(analyze("give @a diamond", 4).contextBound());
        assertFalse(analyze("kill @e[type=minecraft:zombie]", 4).contextBound());
        assertFalse(analyze("summon zombie 100 64 100", 4).contextBound());
    }

    @Test
    @DisplayName("归属预检的覆盖范围：按坐标包围盒展开到区块（/fill 的内部区块不能漏）")
    void chunkRangeCoversInteriorChunks() {
        // 200>>4 = 12：从 (0,0) 到 (12,12) 共 13×13 = 169 个区块
        CommandRouting.ChunkRange fill = analyze("fill 0 0 0 200 0 200 minecraft:stone", 4)
                .chunkRange();
        assertEquals(0, fill.minX());
        assertEquals(12, fill.maxX());
        assertEquals(169L, fill.count(), "/fill 只声明两个角点，内部区块必须一并覆盖");

        // 负坐标的算术右移：-1>>4 = -1
        CommandRouting.ChunkRange single = analyze("setblock 16 64 -1 minecraft:stone", 4)
                .chunkRange();
        assertEquals(1, single.minX());
        assertEquals(-1, single.minZ());
        assertEquals(1L, single.count());

        // 跨 10 个区块的 clone：从 (0,0) 到 (10,10) 共 121 个区块
        CommandRouting.ChunkRange clone = analyze("clone 0 0 0 15 15 15 160 64 160", 4)
                .chunkRange();
        assertEquals(121L, clone.count());

        // 无坐标指令没有范围可查
        assertNull(analyze("time set day", 4).chunkRange());
    }

    @Test
    @DisplayName("缺省参数由同指令实参补齐时不按上下文处理（/forceload add <from> 的 to=from）")
    void siblingDefaultedArgumentsAreNotContext() {
        CommandRouting.Analysis add = analyze("forceload add 100 -200", 4);
        assertFalse(add.contextBound(),
                "to 未给时原版取 from（与执行者位置无关），误判为上下文会让票据只设在发起组");
        assertEquals(List.of(new BlockPos(100, 0, -200)), add.blockPositions());

        // 真正的"缺省取执行者位置"仍必须是上下文（回归保护）
        assertTrue(analyze("spawnpoint Alice", 4).contextBound(),
                "targets 是 EntityArgument、pos 是 Vec3Argument —— 不同类，不得按兄弟实参补齐");
        assertTrue(analyze("setworldspawn", 4).contextBound());
    }

    @Test
    @DisplayName("XZ 中心（Vec2）计入坐标，旋转（RotationArgument）不计入")
    void vec2CenterCountsRotationDoesNot() {
        CommandRouting.Analysis spread = analyze("spreadplayers 500 500 5 10 false @a", 4);
        assertFalse(spread.contextBound());
        assertFalse(spread.blockPositions().isEmpty(),
                "散点的中心必须计入，否则会被广播给所有组在别人领地里取样读方块");
        assertEquals(500 >> 4, spread.blockPositions().get(0).getX() >> 4);

        CommandRouting.Analysis tp = analyze("tp Alice 1 2 3 90 0", 4);
        assertEquals(List.of(new BlockPos(1, 2, 3)), tp.blockPositions(),
                "朝向不是位置：RotationArgument 不得被当成方块坐标");
    }

    @Test
    @DisplayName("黑盒与偏移量：/function 按上下文；/particle 的 delta 不计入坐标")
    void blackBoxAndOffsetArguments() {
        // 函数体是黑盒：广播会让其他组用"出生点 + entity=null"的虚拟源执行函数体，
        // 里面的 ~/^/@s 全落错地方（在他组出生点复刻一遍建筑）
        assertTrue(analyze("function mypack:build_hut", 4).contextBound());
        assertTrue(analyze("schedule function mypack:tick 1t", 4).contextBound());

        // /particle 的 delta 是偏移量：并入包围盒会误拒合法指令或路由到错误的组
        CommandRouting.Analysis particle =
                analyze("particle flame 100 64 100 0.5 0.5 0.5 0.01 20", 4);
        assertFalse(particle.contextBound());
        assertEquals(List.of(new BlockPos(100, 64, 100)), particle.blockPositions());
    }

    // ------------------------------------------------------------ 边界

    @Test
    @DisplayName("解析失败/空输入不抛异常（合法性由原版 validateParseResults 判定）")
    void malformedInputDoesNotThrow() {
        ParseResults<CommandSourceStack> broken = dispatcher.parse("nosuchcommand a b c",
                new CommandSourceStack(CommandSource.NULL, Vec3.ZERO, Vec2.ZERO, null, 4,
                        "tester", Component.literal("tester"), null, null));
        // 判据是原版那一套（reader 还有剩余输入即非法），不能只看 exceptions 表：
        // 未知指令的解析结果是 exceptions={} + canRead=true
        assertTrue(broken.getReader().canRead());
        assertThrows(com.mojang.brigadier.exceptions.CommandSyntaxException.class,
                () -> Commands.validateParseResults(broken));
        assertFalse(CommandRouting.analyze(broken).contextBound());
        assertTrue(CommandRouting.analyze(null).blockPositions().isEmpty());
        // 无权限的指令同样以"解析不通过"表现：与真人报错文案一致
        ParseResults<CommandSourceStack> denied = dispatcher.parse("gamemode creative",
                new CommandSourceStack(CommandSource.NULL, Vec3.ZERO, Vec2.ZERO, null, 0,
                        "tester", Component.literal("tester"), null, null));
        assertTrue(denied.getReader().canRead());
        assertThrows(com.mojang.brigadier.exceptions.CommandSyntaxException.class,
                () -> Commands.validateParseResults(denied));
    }
}
