package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mojang.authlib.GameProfile;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.server.MinecraftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 权限下发（服务端，规范"服务端可以执行原主客户端不能执行的 tp 指令"的权限基础）。
 *
 * <h2>要解决的问题</h2>
 * 组世界的权威演算是主客户端的集成服务端，而它发布时不开放作弊
 * （{@code GroupHost} 的 {@code publishServer(…, false, …)}，有意决策），
 * 集成服又从不读取 {@code ops.json} —— 于是玩家的本地权限恒为 0：
 * F3+F4 被客户端闸门静默拦下、指令补全树按权限过滤后连 {@code /gamemode} 都看不到。
 * <p>
 * 本类把<b>物理服务端 ops 名单上的真实等级</b>按"组 → 组成员"下发，主客户端在自己的
 * 集成服务端里按名单精确落权（{@link imsng.player_to_player.group.LocalPermissions}）。
 * 与"跨组广播执行的权限钳到 2"是两件不同的事：这里给的是<b>本地权限位</b>
 * （决定这台客户端自己能看到/能做什么），转发执行的权限依旧单独钳制。
 *
 * <h2>何时下发</h2>
 * <ul>
 *   <li>事件驱动：HELLO（重连）、角色指派、合并提交、分离 —— 组或成员一变化就推；</li>
 *   <li>周期兜底：每 {@value #SWEEP_INTERVAL_SECONDS} 秒在服务器主线程重算一次，
 *       <b>只在签名变化时</b>才发 —— 覆盖控制台 {@code /op}、直接编辑 ops.json 等
 *       绕过模组链路的变化，无需每条指令一次查询。</li>
 * </ul>
 *
 * <h2>线程模型</h2>
 * ops 名单（{@code ServerOpList}）内部是非并发 Map，读写都必须在服务器主线程；
 * 因此所有计算都在 {@code server.execute(...)} 里做。网络发送
 * （{@link ControlConnection#send}）任意线程可调。整个下发是"缓存"语义：
 * 丢了、迟了都不影响权威（权威永远在物理服务端，转发执行时按名单重新核对）。
 */
public final class PermissionRelay {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/permission");

    /** 周期兜底间隔（秒）：ops 名单变化极低频，5 秒足够"服务端 /op 之后立刻能用"。 */
    private static final int SWEEP_INTERVAL_SECONDS = 5;

    /** 名单数组构造（PERMISSION_RELAY 帧与 HELLO_ACK 附带的名单共用）。 */
    private static JsonArray buildMemberArray(List<MemberPermission> list) {
        JsonArray array = new JsonArray();
        for (MemberPermission member : list) {
            JsonObject row = new JsonObject();
            row.addProperty("uuid", member.playerId().toString());
            row.addProperty("name", member.playerName());
            row.addProperty("level", member.level());
            array.add(row);
        }
        return array;
    }

    /** 直接按组成员构造名单数组（HELLO_ACK 用；在服务器主线程调用）。 */
    private JsonArray buildMembers(UUID groupId) {
        List<MemberPermission> list = new ArrayList<>();
        for (UUID member : groups.membersOf(groupId)) {
            String name = ConnectionIdentity.nameOf(member);
            int level = server.getProfilePermissions(new GameProfile(member,
                    name != null ? name : "?"));
            list.add(new MemberPermission(member, name != null ? name : "?", level));
            levelSnapshot.put(member, level);
        }
        return buildMemberArray(list);
    }

    /** 一名成员的权限档（下发给主客户端的名单行）。 */
    public record MemberPermission(UUID playerId, String playerName, int level) {
    }

    private final MinecraftServer server;
    private final GroupTable groups;

    /** groupId → 上次下发的签名（"uuid:name:level,…"），用于跳过未变化的重复帧。 */
    private final Map<UUID, String> lastPushed = new ConcurrentHashMap<>();

    /**
     * clientId → 真实等级<b>快照</b>：只在服务器主线程写，Netty/io 线程读。
     * <p>
     * 为什么要有快照：{@code ServerOpList} 内部是普通 HashMap，服务器主线程的
     * {@code /op}、{@code /deop} 会改它 —— Netty 线程直接读就是数据竞争（可能读到
     * 丢失更新，把合法指令判成权限不足）。指令上送的权限裁定读这里。
     */
    private final Map<UUID, Integer> levelSnapshot = new ConcurrentHashMap<>();

    /** 周期兜底任务句柄。 */
    private volatile ScheduledFuture<?> sweepTask;

    public PermissionRelay(MinecraftServer server, GroupTable groups) {
        this.server = server;
        this.groups = groups;
    }

    /** 启动周期兜底（幂等；服务停止时 {@link #shutdown()}）。 */
    public void start() {
        if (sweepTask != null) {
            return;
        }
        sweepTask = ThreadPools.scheduler().scheduleWithFixedDelay(
                this::sweep, SWEEP_INTERVAL_SECONDS, SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /** 停止周期兜底并清空去重签名（服务停止时调用）。 */
    public void shutdown() {
        ScheduledFuture<?> task = sweepTask;
        sweepTask = null;
        if (task != null) {
            task.cancel(false);
        }
        lastPushed.clear();
        levelSnapshot.clear();
    }

    /** 该客户端的真实权限等级快照；未收录返回 null（调用方按 0 处理）。 */
    public Integer levelOf(UUID clientId) {
        return clientId == null ? null : levelSnapshot.get(clientId);
    }

    /**
     * 把该客户端所在组的权限名单附到 HELLO_ACK 上（加性字段）。
     * <p>
     * 为什么要走 ACK：重连时服务端会立刻补推一次名单，但客户端此刻还没注册
     * PERMISSION_RELAY 处理器（注册在 HELLO_ACK 之后），帧会被"无处理器"丢弃，
     * 而去重签名又让后续推送不再重发 —— 本地权限位就会停在旧值（常见为 0）。
     * ACK 是应答路径（客户端在等它的 future），必然送达；周期兜底随后仍会重发一次
     * 作为幂等重试（客户端重复应用同一等级是空操作）。
     */
    public void appendToAck(JsonObject ack, UUID clientId) {
        UUID groupId = clientId != null ? groups.groupOf(clientId) : null;
        if (groupId == null) {
            return; // 还没入组：角色指派时会推
        }
        ack.addProperty("permissionsGroupId", groupId.toString());
        ack.add("permissions", buildMembers(groupId));
    }

    /** 调度一次下发（任意线程；实际计算在服务器主线程）。 */
    public void push(UUID groupId) {
        if (groupId == null) {
            return;
        }
        try {
            server.execute(() -> pushNow(groupId));
        } catch (RuntimeException e) {
            LOGGER.debug("权限下发调度失败（服务端正在停止？）: {}", e.toString());
        }
    }

    /**
     * 按客户端身份补推其所在组的名单（HELLO 重连时调用）。
     * <p>
     * 重连的客户端进程可能已经换了世界会话，本地名单缓存为空 —— 只靠"变化才发"的
     * 去重会把它漏掉，所以这里<b>强制重发</b>（清掉签名再推）。
     */
    public void pushForClient(UUID clientId) {
        UUID groupId = clientId != null ? groups.groupOf(clientId) : null;
        if (groupId == null) {
            return; // 还没进组：角色指派时会推
        }
        lastPushed.remove(groupId);
        push(groupId);
    }

    /** 调度全部在线组的一次下发（角色指派/合并提交后调用，覆盖成员变化）。 */
    public void pushAll() {
        try {
            server.execute(this::sweepNow);
        } catch (RuntimeException e) {
            LOGGER.debug("权限全量下发调度失败（服务端正在停止？）: {}", e.toString());
        }
    }

    /** 周期兜底：在服务器主线程逐组重算，仅变化时下发。 */
    private void sweep() {
        try {
            server.execute(this::sweepNow);
        } catch (RuntimeException e) {
            // 服务端停止中：下一次（若还活着）再试即可
        }
    }

    private void sweepNow() {
        Set<UUID> liveMembers = new java.util.HashSet<>();
        for (UUID groupId : groups.primariesSnapshot().keySet()) {
            liveMembers.addAll(groups.membersOf(groupId));
            pushNow(groupId);
        }
        // 已解散的组清掉签名、离线的成员清掉等级快照，防 map 无限增长
        lastPushed.keySet().removeIf(groupId -> !groups.exists(groupId));
        levelSnapshot.keySet().retainAll(liveMembers);
    }

    /**
     * 计算并下发一个组的权限名单（必须在服务器主线程；幂等，无变化不发）。
     * 下发给该组<b>主客户端</b>的控制连接 —— 只有它持有集成服务端，落权在它那里做。
     */
    private void pushNow(UUID groupId) {
        Set<UUID> members = groups.membersOf(groupId);
        if (members.isEmpty()) {
            return;
        }
        List<MemberPermission> list = new ArrayList<>(members.size());
        StringBuilder signature = new StringBuilder();
        for (UUID member : members) {
            String name = ConnectionIdentity.nameOf(member);
            // 客户端约定 clientId == 玩家档案 UUID，故这里等价于按玩家档案查 ops 名单
            int level = server.getProfilePermissions(new GameProfile(member,
                    name != null ? name : "?"));
            list.add(new MemberPermission(member, name != null ? name : "?", level));
            signature.append(member).append(':').append(name).append(':').append(level)
                    .append(','); // 名字进签名：改名也算变化（落权第二判据是名字）
            levelSnapshot.put(member, level);
        }
        String sig = signature.toString();
        if (sig.equals(lastPushed.get(groupId))) {
            return; // 无变化：不发（周期兜底因此完全静默）
        }

        UUID primary = groups.primaryOf(groupId);
        ControlConnection conn = primary != null ? HelloHandler.connectionOf(primary) : null;
        if (conn == null || !conn.isOpen()) {
            return; // 主客户端不在线：它重新 HELLO 时会再推
        }

        JsonObject out = new JsonObject();
        out.addProperty("groupId", groupId.toString());
        out.add("members", buildMemberArray(list));
        conn.send(ControlMessage.of(MessageType.PERMISSION_RELAY, out));
        lastPushed.put(groupId, sig);
        LOGGER.info("权限已下发: 组 {} → {} 名成员（{}）", groupId, list.size(),
                list.stream().map(m -> m.playerName() + "=" + m.level()).toList());
    }
}
