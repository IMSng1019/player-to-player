package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import com.mojang.brigadier.context.CommandContext;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.network.chat.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 元数据指令在已完成 execute 上下文转换的叶节点处收口。
 * 发送的是叶指令和已解析的坐标/朝向，中心只执行一次；函数、命令方块也经过此入口。
 */
public final class MetadataCommands {
    private static final Set<String> GLOBAL = Set.of("time", "weather", "gamerule", "difficulty",
            "defaultgamemode", "setworldspawn", "worldborder", "bossbar", "schedule");
    private MetadataCommands() { }

    public static boolean isGlobal(String command) {
        String text = command.stripLeading();
        if (text.startsWith("/")) text = text.substring(1);
        int end = 0;
        while (end < text.length() && !Character.isWhitespace(text.charAt(end))) end++;
        return GLOBAL.contains(text.substring(0, end));
    }

    public static boolean isLocalClockQuery(String command) {
        return command.strip().matches("/?time\\s+query\\s+gametime");
    }

    public static String leaf(CommandContext<?> context) {
        if (context.getNodes().isEmpty()) return "";
        int start = context.getNodes().get(0).getRange().getStart();
        return context.getInput().substring(start);
    }

    /** execute包装并不改变叶命令的全局归属，禁止旧路由把相同叶变更广播执行。 */
    public static boolean containsGlobal(com.mojang.brigadier.ParseResults<CommandSourceStack> parse) {
        for (var context = parse.getContext(); context != null; context = context.getChild()) {
            if (!context.getNodes().isEmpty()) {
                var first = context.getNodes().get(0).getNode();
                if (first instanceof com.mojang.brigadier.tree.LiteralCommandNode<?> && GLOBAL.contains(first.getName())) return true;
            }
        }
        return false;
    }

    /** 把最终上下文中的~/^坐标在源世界求值，眼睛锚点等不能靠中心的空实体重建。 */
    public static String resolvedLeaf(CommandContext<?> context, CommandSourceStack source) {
        int start = context.getNodes().get(0).getRange().getStart();
        StringBuilder command = new StringBuilder(leaf(context));
        // 从后向前替换，避免前面的替换改变后续原始Brigadier范围。
        var nodes = context.getNodes();
        for (int i = nodes.size() - 1; i >= 0; i--) {
            var parsed = nodes.get(i);
            if (!(parsed.getNode() instanceof com.mojang.brigadier.tree.ArgumentCommandNode<?, ?> argument)) continue;
            Object type = argument.getType();
            if (!(type instanceof net.minecraft.commands.arguments.coordinates.BlockPosArgument)
                    && !(type instanceof net.minecraft.commands.arguments.coordinates.Vec2Argument)) continue;
            var coordinates = context.getArgument(argument.getName(), net.minecraft.commands.arguments.coordinates.Coordinates.class);
            String absolute;
            if (type instanceof net.minecraft.commands.arguments.coordinates.BlockPosArgument) {
                var position = coordinates.getBlockPos(source);
                absolute = position.getX() + " " + position.getY() + " " + position.getZ();
            } else {
                var position = coordinates.getPosition(source);
                absolute = position.x + " " + position.z;
            }
            command.replace(parsed.getRange().getStart() - start, parsed.getRange().getEnd() - start, absolute);
        }
        return command.toString();
    }

    /** 普通玩家聊天已由旧路由在中心执行；此处补齐函数和程序化入口。 */
    public static int forward(CommandContext<?> context, CommandSourceStack source) {
        var snapshot = MetadataClient.snapshot();
        var connection = GroupRuntime.conn();
        if (snapshot == null || connection == null || !connection.isOpen()
                || !MetadataClient.isSimulationReady()) {
            source.sendFailure(Component.literal("世界元数据尚未确认，指令未执行"));
            source.callback().onResult(false, 0);
            return 0;
        }
        JsonObject request = new JsonObject();
        request.addProperty("action", "command");
        request.addProperty("worldId", snapshot.worldId());
        request.addProperty("term", snapshot.term());
        request.addProperty("baseVersion", snapshot.version());
        request.addProperty("operationId", UUID.randomUUID().toString());
        request.addProperty("command", resolvedLeaf(context, source));
        request.addProperty("dimension", source.getLevel().dimension().location().toString());
        request.addProperty("x", source.getPosition().x);
        request.addProperty("y", source.getPosition().y);
        request.addProperty("z", source.getPosition().z);
        request.addProperty("xRot", source.getRotation().x);
        request.addProperty("yRot", source.getRotation().y);
        request.addProperty("source", source.getTextName());
        if (source.getPlayer() != null) request.addProperty("playerUuid", source.getPlayer().getUUID().toString());
        request.addProperty("permission", source.hasPermission(2) ? 2 : 0);
        try {
            // 标签/分数/范围等选择器依赖发起世界的真实实体，不能到空的中心世界重新解析。
            var bossPlayers = MetadataBossPlayers.resolveLocal(context, source);
            if (bossPlayers != null) {
                com.google.gson.JsonArray ids = new com.google.gson.JsonArray();
                bossPlayers.forEach(id -> ids.add(id.toString())); request.add("bossPlayers", ids);
            }
            // 此响应只依赖中心的轻量命令提交，不等待本组 ACK，避免跨线程互等。
            var reply = connection.request(ControlMessage.of(MessageType.METADATA_CHANGE, request))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            String message = reply.json().has("message") ? reply.json().get("message").getAsString() : "";
            if (!message.isEmpty()) source.sendSystemMessage(Component.literal(message));
            int result = reply.json().has("result") ? reply.json().get("result").getAsInt() : 0;
            source.callback().onResult(imsng.player_to_player.util.JsonUtil.getBoolean(reply.json(), "success", false), result);
            return result;
        } catch (Exception error) {
            source.sendFailure(Component.literal("全局指令结果未确认；不会自动重放: " + error.getMessage()));
            source.callback().onResult(false, 0);
            return 0;
        }
    }
}
