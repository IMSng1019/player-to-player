package imsng.player_to_player.server;

import com.mojang.brigadier.ParseResults;
import com.mojang.brigadier.context.CommandContextBuilder;
import com.mojang.brigadier.context.ParsedArgument;
import imsng.player_to_player.registry.PlayerTable;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.arguments.coordinates.Coordinates;
import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.world.level.Level;
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.function.Function;

/**
 * 玩家、控制台与 RCON 共用的直接 TP 规划。
 *
 * <p>传送操作必须在“被传送玩家当前所在组”执行，目的区块不决定执行组。
 * 否则向所有组广播 {@code tp Alice Bob} 时，每个组都缺少另一侧的玩家实体；
 * 将目的地按普通方块写入做归属检查又会在进入加载/合并流程前直接拒绝传送。
 * 此处在权威玩家表上解析目的地并固定相对坐标的源，再让组世界沿原版传送路径
 * 移动玩家，后续区块认领、预同步、合并仍由现有加载门控负责。</p>
 */
final class TeleportCommandRouting {
    record Plan(UUID groupId, String command, String dimension, String rejection) {
        static Plan reject(String message) {
            return new Plan(null, null, null, message);
        }
    }

    private TeleportCommandRouting() {
    }

    /**
     * 输入必须先通过原版权限与语法校验。null 表示并非直接 TP，或玩家使用了仍需
     * 原版实体选择器解释的形态，调用方继续原有通用路由；控制台没有隐式玩家。
     */
    static Plan plan(ParseResults<CommandSourceStack> parse, CommandSourceStack source,
                     String sourceDimension, String implicitPlayerName,
                     Function<String, PlayerTable.PlayerEntry> playerLookup,
                     Function<UUID, UUID> groupLookup) {
        String command = parse.getReader().getString();
        String root = command.split("\\s+", 2)[0];
        if (!"tp".equals(root) && !"teleport".equals(root)) {
            return null; // execute 内嵌 TP 不能脱离它的上下文单独重写。
        }
        String rawTarget = text(parse, "targets");
        if (rawTarget == null || "@s".equals(rawTarget)) {
            rawTarget = implicitPlayerName;
        }
        if (rawTarget == null) {
            return Plan.reject("控制台没有玩家实体，请显式指定被操作的玩家名");
        }
        if (implicitPlayerName != null && isEntityUuid(rawTarget)) {
            return null; // UUID 可以指向生物等非玩家实体，仍交原版实体解析路径处理。
        }

        if (rawTarget.startsWith("@")) {
            return implicitPlayerName != null ? null
                    : Plan.reject("控制台跨组选择器尚不支持，请使用具体玩家名，未执行");
        }
        PlayerTable.PlayerEntry target = playerLookup.apply(rawTarget);
        UUID targetGroup = target == null ? null : groupLookup.apply(target.playerId());
        if (targetGroup == null) {
            return Plan.reject("目标玩家不在线或尚未完成组分配: " + rawTarget);
        }
        if (argument(parse, "facingEntity") != null || argument(parse, "facingLocation") != null) {
            return implicitPlayerName != null ? null
                    : Plan.reject("控制台跨组 tp facing 暂不支持，请使用显式坐标与朝向，未执行");
        }
        ParsedArgument<CommandSourceStack, ?> location = argument(parse, "location");
        if (location == null) {
            String destinationName = text(parse, "destination");
            if ("@s".equals(destinationName)) {
                destinationName = implicitPlayerName;
            }
            if (implicitPlayerName != null && isEntityUuid(destinationName)) {
                return null;
            }

            if (destinationName == null || destinationName.startsWith("@")) {
                return implicitPlayerName != null ? null
                        : Plan.reject("请使用具体目的玩家名或明确的目的坐标，未执行");
            }
            PlayerTable.PlayerEntry destination = playerLookup.apply(destinationName);
            UUID destinationGroup = destination == null ? null : groupLookup.apply(destination.playerId());
            if (destinationGroup == null) {
                return Plan.reject("目的玩家不在线或尚未完成组分配: " + destinationName);
            }
            if (targetGroup.equals(destinationGroup)) {
                // 同组仍让原版从当前实体读取位置、维度和朝向，不引入 1Hz 玩家表的延迟。
                return new Plan(targetGroup, "teleport " + target.playerName() + " "
                        + destination.playerName(), sourceDimension, null);
            }
            Vec3 position = new Vec3(destination.x(), destination.y(), destination.z());
            if (!validPosition(position) || ResourceLocation.tryParse(destination.dimension()) == null) {
                return Plan.reject("目的玩家位置或维度非法，未执行");
            }
            // 玩家表尚无朝向，跨组到玩家延续已有 /tp <玩家> 的口径：保留被传送者朝向。
            // 原版坐标 TP 未给 rotation 时读取目标实体自身朝向（不是虚拟源的朝向）；
            // 明确给出绝对坐标，也不会让 execute in 的下界坐标缩放污染落点。
            String forwarded = "execute in " + destination.dimension() + " run teleport "
                    + target.playerName() + " " + position(position);
            return new Plan(targetGroup, forwarded, destination.dimension(), null);
        }
        Vec3 position = ((Coordinates) location.getResult()).getPosition(source);
        if (!validPosition(position) || ResourceLocation.tryParse(sourceDimension) == null) {
            return Plan.reject("传送坐标或维度非法，未执行");
        }
        String forwarded = "execute in " + sourceDimension + " run teleport "
                + target.playerName() + " " + position(position);
        ParsedArgument<CommandSourceStack, ?> rotation = argument(parse, "rotation");
        if (rotation != null) {
            // 仅显式相对朝向在真实发起源上结算；未给 rotation 时保持原版“保留目标朝向”。
            // 原版 Vec2 是 pitch/yaw，指令文本输入顺序则为 yaw/pitch。
            Vec2 angles = ((Coordinates) rotation.getResult()).getRotation(source);
            if (!Float.isFinite(angles.x) || !Float.isFinite(angles.y)) {
                return Plan.reject("传送朝向非法，未执行");
            }
            forwarded += " " + number(angles.y) + " " + number(angles.x);
        }
        return new Plan(targetGroup, forwarded, sourceDimension, null);
    }

    private static boolean validPosition(Vec3 position) {
        return Double.isFinite(position.x) && Double.isFinite(position.y) && Double.isFinite(position.z)
                && Level.isInSpawnableBounds(BlockPos.containing(position));
    }

    private static boolean isEntityUuid(String text) {
        if (text == null) return false;
        try {
            UUID.fromString(text);
            return true;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }



    private static String position(Vec3 position) {
        return number(position.x) + " " + number(position.y) + " " + number(position.z);
    }

    /** Brigadier 数值读取不接受 E 记法，必须输出普通十进制。 */
    private static String number(double value) {
        return BigDecimal.valueOf(value).toPlainString();
    }

    private static ParsedArgument<CommandSourceStack, ?> argument(
            ParseResults<CommandSourceStack> parse, String name) {
        for (CommandContextBuilder<CommandSourceStack> builder = parse.getContext();
             builder != null; builder = builder.getChild()) {
            ParsedArgument<CommandSourceStack, ?> argument = builder.getArguments().get(name);
            if (argument != null) return argument;
        }
        return null;
    }

    private static String text(ParseResults<CommandSourceStack> parse, String name) {
        ParsedArgument<CommandSourceStack, ?> argument = argument(parse, name);
        return argument == null ? null : argument.getRange().get(parse.getReader().getString());
    }
}
