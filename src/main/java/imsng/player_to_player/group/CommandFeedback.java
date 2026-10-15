package imsng.player_to_player.group;

import net.minecraft.commands.CommandSource;
import net.minecraft.network.chat.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 收集指令反馈文本的 {@link CommandSource}（双侧共用）。
 *
 * <p>全量上链后，一条指令的可见输出只有一个来源：服务端聚合后的回执
 * （规范"如果被执行 则将执行端的消息返回发出指令的端"）。因此执行端
 * <b>不把反馈直接发给玩家或控制台</b>，而是收集成文本回传：</p>
 * <ul>
 *   <li>转发执行（{@code origin=false}，虚拟源）——回传给发起玩家；</li>
 *   <li>发起组执行（{@code origin=true}）——它的成败与文案决定回执内容，
 *       所以同样不能直发（否则玩家会看到两遍：自己的输出 + 聚合回执）；</li>
 *   <li>服务端执行 3/4 级指令——结果文案回传给发起玩家。</li>
 * </ul>
 *
 * <p>行数与总长有上限：回执是给玩家看的一句话，不做控制台级全量转储；
 * {@code shouldInformAdmins=false} 保证别的组的指令不会打扰本组管理员的控制台。</p>
 *
 * <p>线程模型：{@link CopyOnWriteArrayList} 写（指令执行线程）读（回包线程）安全，无锁。</p>
 */
public final class CommandFeedback implements CommandSource {

    /** 回传的最大行数。 */
    private static final int MAX_LINES = 8;

    /** 回传文本的总长上限（字符）。 */
    private static final int MAX_CHARS = 512;

    private final List<String> lines = new CopyOnWriteArrayList<>();

    @Override
    public void sendSystemMessage(Component component) {
        if (lines.size() < MAX_LINES) {
            lines.add(component.getString());
        }
    }

    @Override
    public boolean acceptsSuccess() {
        return true;
    }

    @Override
    public boolean acceptsFailure() {
        return true;
    }

    @Override
    public boolean shouldInformAdmins() {
        return false; // 这是别的组/服务端的指令回执，不打扰本地控制台
    }

    /** 已收集文本（换行连接，截断到上限）；无输出返回空串。 */
    public String joined() {
        String joined = String.join("\n", lines);
        return joined.length() > MAX_CHARS ? joined.substring(0, MAX_CHARS) : joined;
    }
}
