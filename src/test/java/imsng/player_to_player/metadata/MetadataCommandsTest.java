package imsng.player_to_player.metadata;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MetadataCommandsTest {
    @Test void wrappedGlobalLeafIsRecognizedBeforeGroupBroadcast() {
        var dispatcher = new com.mojang.brigadier.CommandDispatcher<net.minecraft.commands.CommandSourceStack>();
        dispatcher.register(com.mojang.brigadier.builder.LiteralArgumentBuilder.<net.minecraft.commands.CommandSourceStack>literal("time")
                .then(com.mojang.brigadier.builder.LiteralArgumentBuilder.<net.minecraft.commands.CommandSourceStack>literal("add")
                        .then(com.mojang.brigadier.builder.RequiredArgumentBuilder.<net.minecraft.commands.CommandSourceStack, Integer>argument(
                                "amount", com.mojang.brigadier.arguments.IntegerArgumentType.integer()).executes(context -> 1))));
        dispatcher.register(com.mojang.brigadier.builder.LiteralArgumentBuilder.<net.minecraft.commands.CommandSourceStack>literal("execute")
                .then(com.mojang.brigadier.builder.LiteralArgumentBuilder.<net.minecraft.commands.CommandSourceStack>literal("run").redirect(dispatcher.getRoot())));
        assertTrue(MetadataCommands.containsGlobal(dispatcher.parse("execute run time add 1", null)));
        assertFalse(MetadataCommands.containsGlobal(dispatcher.parse("execute run unknown", null)));
    }

    @Test void globalCommandsAreExecutedOnceAndDoNotCaptureUnrelatedText() {
        assertTrue(MetadataCommands.isGlobal("/time add 20"));
        assertTrue(MetadataCommands.isGlobal("gamerule doDaylightCycle false"));
        assertTrue(MetadataCommands.isGlobal("schedule function test:tick 40t"));
        assertTrue(MetadataCommands.isGlobal("bossbar remove test:boss"));
        assertFalse(MetadataCommands.isGlobal("say time add 20"));
        assertFalse(MetadataCommands.isGlobal("execute as @a run time add 20"));
        assertTrue(MetadataCommands.isLocalClockQuery("/time query gametime"));
        assertFalse(MetadataCommands.isLocalClockQuery("time query daytime"));
    }
}
