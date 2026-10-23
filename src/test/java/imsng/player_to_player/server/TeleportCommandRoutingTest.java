package imsng.player_to_player.server;

import com.mojang.brigadier.CommandDispatcher;
import imsng.player_to_player.registry.PlayerTable;
import net.minecraft.SharedConstants;
import net.minecraft.commands.CommandBuildContext;
import net.minecraft.commands.CommandSource;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.data.registries.VanillaRegistries;
import net.minecraft.network.chat.Component;
import net.minecraft.server.Bootstrap;
import net.minecraft.world.flag.FeatureFlags;
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 在真实原版解析树上验证游戏入口的 TP 规划，防止“解析通过但被通用广播错误分发”。 */
class TeleportCommandRoutingTest {
    private static final String OVERWORLD = "minecraft:overworld";
    private static final String NETHER = "minecraft:the_nether";
    private static final UUID ALICE = UUID.randomUUID();
    private static final UUID BOB = UUID.randomUUID();
    private static final UUID GROUP_A = UUID.randomUUID();
    private static final UUID GROUP_B = UUID.randomUUID();
    private static CommandDispatcher<CommandSourceStack> dispatcher;

    @BeforeAll
    static void bootstrap() {
        SharedConstants.tryDetectVersion();
        Bootstrap.bootStrap();
        var build = CommandBuildContext.simple(VanillaRegistries.createLookup(), FeatureFlags.DEFAULT_FLAGS);
        dispatcher = new Commands(Commands.CommandSelection.ALL, build).getDispatcher();
    }

    private static CommandSourceStack source(int permission) {
        return new CommandSourceStack(CommandSource.NULL,
                new Vec3(100, 64, 200), new Vec2(30, 90), null, permission, "Alice",
                Component.literal("Alice"), null, null);
    }

    private TeleportCommandRouting.Plan plan(String command) {
        return plan(command, 3000, 70, 4000);
    }

    private TeleportCommandRouting.Plan plan(String command, double x, double y, double z) {
        var source = source(2);
        var parse = dispatcher.parse(command, source);
        assertDoesNotThrow(() -> Commands.validateParseResults(parse));
        Map<String, PlayerTable.PlayerEntry> players = Map.of(
                "Alice", new PlayerTable.PlayerEntry(ALICE, "Alice", OVERWORLD, 100, 64, 200,
                        GROUP_A, "primary"),
                "Bob", new PlayerTable.PlayerEntry(BOB, "Bob", NETHER, x, y, z,
                        GROUP_B, "secondary"));
        var plan = TeleportCommandRouting.plan(parse, source, OVERWORLD, "Alice", players::get,
                id -> ALICE.equals(id) ? GROUP_A : BOB.equals(id) ? GROUP_B : null);
        if (plan != null && plan.rejection() == null) {
            assertDoesNotThrow(() -> Commands.validateParseResults(dispatcher.parse(plan.command(), source)));
        }
        return plan;
    }

    @Test
    void implicitPlayerDestinationResolvesDifferentGroupAndDimension() {
        var plan = plan("tp Bob");
        assertEquals(GROUP_A, plan.groupId());
        assertEquals(NETHER, plan.dimension());
        assertEquals("execute in minecraft:the_nether run teleport Alice 3000.0 70.0 4000.0",
                plan.command());
    }

    @Test
    void explicitMovedPlayerRunsOnTheirGroupEvenWhenSenderIsElsewhere() {
        var plan = plan("teleport Bob Alice");
        assertEquals(GROUP_B, plan.groupId());
        assertEquals(OVERWORLD, plan.dimension());
        assertTrue(plan.command().startsWith("execute in minecraft:overworld run teleport Bob"));
        assertTrue(plan.command().endsWith("100.0 64.0 200.0"));
    }

    @Test
    void selfSelectorAndExplicitNameShareSameDestinationResolution() {
        assertEquals(plan("tp Alice Bob"), plan("tp @s Bob"));
        assertEquals(plan("tp Bob Alice"), plan("tp Bob @s"));
    }

    @Test
    void relativeCoordinatesUseSenderPositionForRemoteMovedPlayer() {
        var plan = plan("tp Bob ~1 ~2 ~-3");
        assertEquals(GROUP_B, plan.groupId());
        assertEquals("execute in minecraft:overworld run teleport Bob 101.0 66.0 197.0",
                plan.command());
    }

    @Test
    void implicitCoordinatesKeepTargetRotationAndExplicitRotationUsesRealSource() {
        // 原版 Vec3Argument 对 X/Z 整数字面量使用方块中心，展开后必须保留这半格。
        assertEquals("execute in minecraft:overworld run teleport Alice 1000.5 70.0 2000.5",
                plan("tp 1000 70 2000").command());
        assertEquals("execute in minecraft:overworld run teleport Bob 1000.5 70.0 2000.5 100.0 25.0",
                plan("tp Bob 1000 70 2000 ~10 ~-5").command());
    }

    @Test
    void localCoordinatesUsePitchAndYawInVanillaOrder() {
        String[] fields = plan("tp Bob ^ ^ ^2").command().split(" ");
        assertEquals(100 - Math.sqrt(3), Double.parseDouble(fields[6]), 0.0001);
        assertEquals(63, Double.parseDouble(fields[7]), 0.0001);
        assertEquals(200, Double.parseDouble(fields[8]), 0.0001);
    }

    @Test
    void missingPlayersAndInvalidDestinationCoordinatesAreRejected() {
        assertNotNull(plan("tp Missing Bob").rejection());
        assertNotNull(plan("tp Alice Missing").rejection());
        assertNotNull(plan("tp Bob", Double.NaN, 70, 0).rejection());
        assertNotNull(plan("tp Bob", 30_000_001, 70, 0).rejection());
        assertNotNull(plan("tp 30000001 70 0").rejection());
    }

    @Test
    void unsupportedSelectorsAndNestedExecutionKeepOriginalRouting() {
        assertNull(plan("tp @a Bob"));
        assertNull(plan("execute as @s run tp Bob"));
        assertNull(plan("gamemode creative"));
    }

    @Test
    void entityUuidTargetsKeepOriginalEntityResolution() {
        String entity = "7b7f531e-aac8-4b6a-8195-dfd4fd92153c";
        assertNull(plan("tp " + entity + " 100 70 200"));
        assertNull(plan("tp Alice " + entity));
    }

    @Test
    void vanillaPermissionValidationStillRejectsTeleportBeforePlanning() {
        assertThrows(Exception.class, () -> Commands.validateParseResults(
                dispatcher.parse("tp Bob", source(0))));
    }
}
