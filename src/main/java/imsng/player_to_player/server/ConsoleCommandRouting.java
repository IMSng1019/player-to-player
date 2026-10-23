package imsng.player_to_player.server;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.ParseResults;
import com.mojang.brigadier.context.CommandContextBuilder;
import com.mojang.brigadier.context.ParsedArgument;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.PlayerTable;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/** 控制台命令的纯路由计划：没有网络副作用，使用原版解析树和真实源结算参数。 */
final class ConsoleCommandRouting {
    enum Kind { LOCAL, FORWARD, REJECT }

    record Decision(Kind kind, UUID groupId, String command, String dimension,
                    List<ChunkKey> ownedChunks, String message) {
        static Decision local() {
            return new Decision(Kind.LOCAL, null, null, null, List.of(), "");
        }

        static Decision reject(String reason) {
            return new Decision(Kind.REJECT, null, null, null, List.of(), reason);
        }
    }

    static Decision plan(String command, CommandDispatcher<CommandSourceStack> dispatcher,
                         CommandSourceStack source, String dimension,
                         Function<String, PlayerTable.PlayerEntry> playerLookup,
                         Function<UUID, UUID> groupLookup,
                         Function<ChunkKey, UUID> ownerLookup) {
        String normalized = command.startsWith("/") ? command.substring(1) : command;
        ParseResults<CommandSourceStack> parse = dispatcher.parse(normalized, source);
        try {
            Commands.validateParseResults(parse);
        } catch (CommandSyntaxException invalid) {
            return Decision.local(); // 解析/权限错误仍由原版给出原始报错。
        }
        if (!isGroupExecutable(normalized, dispatcher, source)) {
            return Decision.local(); // op/deop/stop 等管理指令绝不能投递给组客户端。
        }
        String root = normalized.split("\\s+", 2)[0];
        if ("help".equals(root) || "seed".equals(root)) {
            // 指令树帮助和世界种子在物理服仍是权威、且不依赖组世界演算。
            // 不能把“2级可解析”直接等同于“必须转发”，否则会破坏原有控制台查询。
            return Decision.local();
        }
        if (!"gamemode".equals(root) && !"tp".equals(root) && !"teleport".equals(root)) {
            return Decision.reject("当前控制台跨组路由支持 gamemode <模式> <玩家> 和 tp <玩家> <目的玩家或坐标>；"
                    + "其他世界指令尚不能保持完整执行语义，未执行");
        }
        TeleportCommandRouting.Plan teleport = TeleportCommandRouting.plan(parse, source,
                dimension, null, playerLookup, groupLookup);
        if (teleport != null) {
            return teleport.rejection() != null ? Decision.reject(teleport.rejection())
                    : new Decision(Kind.FORWARD, teleport.groupId(), teleport.command(),
                            teleport.dimension(), List.of(), "");
        }
        String rawTarget = text(parse, "target");
        if (rawTarget == null || rawTarget.startsWith("@")) {
            return Decision.reject("控制台跨组选择器尚不支持，请使用具体玩家名，未执行");
        }
        PlayerTable.PlayerEntry target = playerLookup.apply(rawTarget);
        UUID targetGroup = target == null ? null : groupLookup.apply(target.playerId());
        if (targetGroup == null) {
            return Decision.reject("目标玩家不在线或尚未完成组分配: " + rawTarget);
        }
        return new Decision(Kind.FORWARD, targetGroup,
                "gamemode " + text(parse, "gamemode") + " " + target.playerName(),
                dimension, List.of(), "");
    }

    /** 与玩家上链复用同一分类口径：以两级权限重新解析，免维护管理命令白名单。 */
    static boolean isGroupExecutable(String command, CommandDispatcher<CommandSourceStack> dispatcher,
                                     CommandSourceStack source) {
        try {
            Commands.validateParseResults(dispatcher.parse(command, source.withPermission(2)));
            return true;
        } catch (CommandSyntaxException denied) {
            return false;
        }
    }

    private static ParsedArgument<CommandSourceStack, ?> argument(
            ParseResults<CommandSourceStack> parse, String name) {
        for (CommandContextBuilder<CommandSourceStack> builder = parse.getContext();
             builder != null; builder = builder.getChild()) {
            ParsedArgument<CommandSourceStack, ?> argument = builder.getArguments().get(name);
            if (argument != null) {
                return argument;
            }
        }
        return null;
    }

    private static String text(ParseResults<CommandSourceStack> parse, String name) {
        ParsedArgument<CommandSourceStack, ?> argument = argument(parse, name);
        return argument == null ? null : argument.getRange().get(parse.getReader().getString());
    }

}
