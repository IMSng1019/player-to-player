package imsng.player_to_player.metadata;

import com.mojang.brigadier.context.CommandContext;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import imsng.player_to_player.registry.PlayerTable;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.arguments.selector.EntitySelector;
import net.minecraft.nbt.NbtUtils;
import net.minecraft.nbt.Tag;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.ComponentUtils;
import net.minecraft.server.bossevents.CustomBossEvent;
import net.minecraft.server.commands.BossBarCommands;
import net.minecraft.server.level.ServerPlayer;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 中心通常没有远端玩家的 ServerPlayer 对象，原版 bossbar set players 的实体集合不能
 * 直接代表全世界玩家。此适配器只替换该叶命令的名单解析/保存；血条定义仍由原版管理。
 */
public final class MetadataBossPlayers {
    private static final Pattern SET_PLAYERS = Pattern.compile("^/?bossbar\\s+set\\s+\\S+\\s+players(?:\\s+(.*?))?\\s*$");
    private MetadataBossPlayers() { }

    private static String targets(CommandContext<?> context) {
        var matcher = SET_PLAYERS.matcher(MetadataCommands.leaf(context));
        return matcher.matches() ? Objects.requireNonNullElse(matcher.group(1), "").trim() : null;
    }

    /**
     * 原执行世界拥有实体、记分板和模组选择器上下文，复杂选择器在这里按原版解析成 UUID。
     * 无过滤的 @a、名字和 @s 留给中心玩家表解析，避免全局 @a 被截成发起组的本地名单。
     */
    public static List<UUID> resolveLocal(CommandContext<?> context, CommandSourceStack source) throws CommandSyntaxException {
        String target = targets(context);
        if (target == null || !needsLocalSelector(target)) return null;
        EntitySelector selector = context.getArgument("targets", EntitySelector.class);
        return selector.findPlayers(source).stream().map(ServerPlayer::getUUID).toList();
    }

    private static boolean needsLocalSelector(String target) {
        return target.startsWith("@") && !target.equals("@a") && !target.equals("@s");
    }

    public static Integer execute(CommandContext<?> context, CommandSourceStack source,
                                   PlayerTable players, UUID originPlayer) {
        return execute(context, source, players, originPlayer, null);
    }

    /** null 表示不是本适配器的叶命令，调用方继续原版处理。已处理错误返回0并保留具体反馈。 */
    @SuppressWarnings("unchecked")
    public static Integer execute(CommandContext<?> context, CommandSourceStack source,
                                   PlayerTable players, UUID originPlayer, List<UUID> resolved) {
        String target = targets(context);
        if (target == null) return null;
        try {
            if (!source.hasPermission(2)) throw new IllegalArgumentException("没有修改全局 Boss 血条的权限");
            List<UUID> selected = resolved == null ? resolveTargets(target, players, originPlayer)
                    : validateResolved(resolved, players);
            CustomBossEvent event = BossBarCommands.getBossBar((CommandContext<CommandSourceStack>) context);
            if (!replacePlayers(event, selected, source.getServer().getPlayerList().getPlayers())) {
                source.sendFailure(Component.translatable("commands.bossbar.set.players.unchanged"));
                source.callback().onResult(false, 0);
                return 0;
            }
            if (selected.isEmpty()) {
                source.sendSuccess(() -> Component.translatable("commands.bossbar.set.players.success.none", event.getDisplayName()), true);
            } else {
                source.sendSuccess(() -> Component.translatable("commands.bossbar.set.players.success.some", event.getDisplayName(),
                        selected.size(), ComponentUtils.formatList(selected, id -> {
                            var player = players.get(id);
                            return Component.literal(player == null ? id.toString() : player.playerName());
                        })), true);
            }
            // 清空名单是成功且返回0，不能用 result>0 猜成功状态；此处替代原版叶执行
            // 回调，调用方命中适配器后不得再重复通知 callback。
            source.callback().onResult(true, selected.size());
            return selected.size();
        } catch (CommandSyntaxException error) {
            source.sendFailure(Component.literal(error.getRawMessage().getString()));
        } catch (IllegalArgumentException error) {
            source.sendFailure(Component.literal(error.getMessage()));
        }
        source.callback().onResult(false, 0);
        return 0;
    }

    /** 只用权威在线玩家表即可完整表达的选择；没有足够上下文时明确失败，禁止静默清空。 */
    static List<UUID> resolveTargets(String target, PlayerTable players, UUID originPlayer) {
        if (target.isEmpty()) return List.of();
        if (target.equals("@a")) return players.snapshot().keySet().stream().sorted().toList();
        if (target.equals("@s")) {
            if (originPlayer == null || players.get(originPlayer) == null)
                throw new IllegalArgumentException("原执行上下文没有在线玩家，@s 无法选择 Boss 血条玩家");
            return List.of(originPlayer);
        }
        if (target.startsWith("@"))
            throw new IllegalArgumentException("此 Boss 血条选择器需要在原执行世界解析；中心控制台目前仅支持 @a、@s、玩家名和 UUID");
        var player = players.byName(target);
        if (player == null) {
            try { player = players.get(UUID.fromString(target)); }
            catch (IllegalArgumentException ignored) { }
        }
        if (player == null) throw new IllegalArgumentException("找不到在线玩家: " + target);
        return List.of(player.playerId());
    }

    private static List<UUID> validateResolved(List<UUID> resolved, PlayerTable players) {
        LinkedHashSet<UUID> selected = new LinkedHashSet<>();
        for (UUID id : resolved) {
            if (id == null || players.get(id) == null) throw new IllegalArgumentException("选择的玩家已不在线，Boss 血条名单未修改");
            selected.add(id);
        }
        return List.copyOf(selected);
    }

    /** UUID集合比较包括暂时不在中心服的成员，重试相同命令不会清空并重加可见条。 */
    static boolean replacePlayers(CustomBossEvent event, Collection<UUID> targets, Collection<ServerPlayer> localPlayers) {
        Set<UUID> before = new HashSet<>();
        for (Tag tag : event.save().getList("Players", Tag.TAG_INT_ARRAY)) before.add(NbtUtils.loadUUID(tag));
        Set<UUID> desired = new LinkedHashSet<>(targets);
        if (before.equals(desired)) return false;
        event.removeAllPlayers();
        desired.forEach(event::addOfflinePlayer);
        localPlayers.forEach(event::onPlayerConnect);
        return true;
    }
}
