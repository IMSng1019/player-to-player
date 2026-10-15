package imsng.player_to_player.registry;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import imsng.player_to_player.util.ThreadPools;
import imsng.player_to_player.server.ConnectionIdentity;
import imsng.player_to_player.server.ExitTransactionHandlers;
import imsng.player_to_player.server.GroupTable;
import imsng.player_to_player.server.HelloHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 区块注册表子系统的控制协议消息处理器（服务端侧，DESIGN.md 第 5 节；
 * 协议语义见 {@code netproto/MessageType.java} 20-29 段注释）。
 * <ul>
 *   <li>{@code CHUNK_CLAIM_REQUEST}（json: dimension, x, z, groupId）→
 *       {@code CHUNK_CLAIM_RESPONSE}（granted / 拒绝原因：blockingChunk + blockingGroup；
 *       granted 时附 hasServerData 指示走服务端数据还是种子生成）；</li>
 *   <li>{@code CHUNK_RELEASE}（json: dimension, x, z, groupId, releaseId；binary 可携带最终
 *       区块数据）→ {@code CHUNK_RELEASE_ACK}。规范"玩家卸载区块……将最后的区块
 *       数据由主客户端发给服务端"：binary 非空时先经 {@link ChunkWriteback} 写回
 *       MCA 并刷盘，<b>再</b>释放占用 —— 顺序保证释放一经确认，其他组随后的申请
 *       立刻能看到 hasServerData=true；</li>
 *   <li>{@code CHUNK_PROBE_REQUEST}（json: dimension, x, z, groupId）→
 *       {@code CHUNK_PROBE_RESPONSE}（blocked / blockingChunk / blockingGroup）：
 *       与申请同一套"目标+四邻"检查但<b>不登记占用</b>（Phase 4 传送门预检、
 *       末影珍珠交接判定用，纯内存查表）；</li>
 *   <li>{@code PLAYER_POS_UPDATE}（json: uuid, dimension, x, y, z）：刷新玩家表，无应答。</li>
 * </ul>
 * <p>
 * 线程模型：claim 是纯内存操作 + 授予路径的磁盘探测，转 io() 池后应答；
 * release 因可能携带写回数据（解压 + MCA 写 + 刷盘），一律转 io() 池。
 */
public final class RegistryHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/registry");

    private RegistryHandlers() {
    }

    /**
     * 把区块注册表相关处理器挂到控制服务器的分发表上。
     *
     * @param reg       消息路由注册表（ControlServer）
     * @param registry  区块注册表
     * @param players   玩家表
     * @param writeback 最终区块数据写回（Phase 2；null = 无写回能力，只释放占用）
     */
    public static void register(HandlerRegistry reg, ChunkRegistry registry, PlayerTable players,
                                ChunkWriteback writeback,
                                ExitTransactionHandlers exits, GroupTable groups) {
        ChunkReleaseExecutionTracker releaseTracker =
                new ChunkReleaseExecutionTracker();
        reg.on(MessageType.CHUNK_CLAIM_REQUEST,
                (conn, msg) -> handleClaim(conn, msg, registry, exits));
        reg.on(MessageType.CHUNK_RELEASE,
                (conn, msg) -> handleRelease(
                        conn, msg, registry, writeback, exits, releaseTracker));
        reg.on(MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM,
                (conn, msg) -> handleReleaseConfirmation(
                        conn, msg, registry, exits, releaseTracker));
        reg.on(MessageType.CHUNK_PROBE_REQUEST, (conn, msg) -> handleProbe(conn, msg, registry));
        reg.on(MessageType.PLAYER_POS_UPDATE,
                (conn, msg) -> handlePosUpdate(conn, msg, players, groups));
    }

    // ------------------------------------------------------------ 区块申请

    private static void handleClaim(ControlConnection conn, ControlMessage msg,
                                    ChunkRegistry registry,
                                    ExitTransactionHandlers exits) {
        ChunkKey key = parseKey(msg.json());
        UUID groupId = parseUuid(JsonUtil.getString(msg.json(), "groupId", ""));
        if (key == null || groupId == null || !groupId.equals(conn.peerId())) {
            conn.send(error(msg, "invalid_request", "dimension/x/z/groupId 缺失或非法"));
            return;
        }
        // tryClaim 授予路径含 region 文件探测（磁盘 IO），整体转 io() 池执行后应答
        ThreadPools.io().execute(() -> {
            java.util.concurrent.atomic.AtomicReference<ChunkRegistry.ClaimResult> resultRef =
                    new java.util.concurrent.atomic.AtomicReference<>();
            boolean current = exits != null && exits.writeRealtimeIfAllowed(conn, () -> {
                resultRef.set(registry.tryClaim(key, groupId));
                return true;
            });
            if (!current || resultRef.get() == null) {
                conn.send(error(msg, "stale_session", "连接已被新会话替换或正在退出"));
                return;
            }
            ChunkRegistry.ClaimResult result = resultRef.get();
            JsonObject out = new JsonObject();
            out.addProperty("granted", result.granted());
            if (result.granted()) {
                out.addProperty("hasServerData", result.hasServerData());
            } else {
                // 拒绝原因：占用组 + 阻塞区块（客户端据此对该组发起预连接）
                out.addProperty("blockingChunk", result.blockingChunk().asString());
                out.addProperty("blockingGroup", result.blockingGroup().toString());
            }
            conn.send(msg.reply(MessageType.CHUNK_CLAIM_RESPONSE, out, null));
            LOGGER.debug("区块申请: {} 组={} granted={}", key.asString(), groupId, result.granted());
        });
    }

    // ------------------------------------------------------------ 区块释放

    private static void handleRelease(ControlConnection conn, ControlMessage msg,
                                      ChunkRegistry registry, ChunkWriteback writeback,
                                      ExitTransactionHandlers exits,
                                      ChunkReleaseExecutionTracker releaseTracker) {
        ChunkKey key = parseKey(msg.json());
        UUID groupId = parseUuid(JsonUtil.getString(msg.json(), "groupId", ""));
        UUID releaseId = parseUuid(JsonUtil.getString(
                msg.json(), "releaseId", ""));
        if (key == null || groupId == null || releaseId == null
                || !groupId.equals(conn.peerId())) {
            conn.send(error(msg, "invalid_request",
                    "dimension/x/z/groupId/releaseId 缺失或非法"));
            return;
        }
        // 必须在进入 cached IO 队列前登记：排队中的旧重试同样会影响 receipt 的安全回收。
        if (!releaseTracker.begin(releaseId, groupId)) {
            conn.send(error(msg, "release_id_conflict",
                    "releaseId 已被另一组的在途释放请求占用"));
            return;
        }
        byte[] finalData = msg.binary();
        // 写回（解压 + MCA 写 + 刷盘）是重活，整体转 io 池；顺序必须是先写回后释放
        Runnable releaseTask = () -> {
            try {
                processRelease(conn, msg, registry, writeback, exits,
                        key, groupId, releaseId, finalData);
            } finally {
                // 成功、拒绝、异常都必须排空计数；若 confirmation 已到达，最后一个任务
                // 在这里安全回收 receipt，晚到旧请求便不可能再误伤后来重新 claim 的区块。
                releaseTracker.finish(releaseId, groupId, registry);
            }
        };
        try {
            ThreadPools.io().execute(releaseTask);
        } catch (RuntimeException schedulingFailure) {
            // 线程池关闭/拒绝执行时任务体不会进入 finally，必须在提交方补偿计数。
            releaseTracker.finish(releaseId, groupId, registry);
            LOGGER.warn("区块释放任务无法进入 IO 队列: {} 组={} releaseId={}",
                    key.asString(), groupId, releaseId, schedulingFailure);
            conn.send(error(msg, "release_scheduling_failed",
                    schedulingFailure.toString()));
        }
    }

    /** 在 IO 线程执行一次释放；在途计数的进入和离开由调用方统一包住。 */
    private static void processRelease(
            ControlConnection conn, ControlMessage msg,
            ChunkRegistry registry, ChunkWriteback writeback,
            ExitTransactionHandlers exits, ChunkKey key,
            UUID groupId, UUID releaseId, byte[] finalData) {
        int payloadBytes = finalData != null ? finalData.length : 0;
        // 大区块 payload 的 SHA-256 同样属于重活，不能在 Netty 事件循环计算。
        String payloadSha256 = Sha256.hex(
                finalData != null ? finalData : ControlMessage.NO_BINARY);
        java.util.concurrent.atomic.AtomicReference<ChunkWriteback.ReleaseResult>
                resultRef = new java.util.concurrent.atomic.AtomicReference<>();
        boolean current;
        try {
            current = exits != null && exits.writeRealtimeIfAllowed(conn, () -> {
                boolean hasFinalData = payloadBytes > 0;
                ChunkWriteback.ReleaseTransaction transaction =
                        new ChunkWriteback.ReleaseTransaction() {
                            @Override
                            public ChunkWriteback.ReleaseResult replay() {
                                ChunkReleaseReceipt receipt =
                                        registry.findReleaseReceipt(
                                                releaseId, key, groupId,
                                                payloadBytes, payloadSha256);
                                return receipt != null
                                        ? new ChunkWriteback.ReleaseResult(
                                        receipt.finalDataWritten(), true)
                                        : null;
                            }

                            @Override
                            public ChunkWriteback.ReleaseResult commit(
                                    boolean finalDataWritten) {
                                if (hasFinalData != finalDataWritten) {
                                    return new ChunkWriteback.ReleaseResult(
                                            finalDataWritten, false);
                                }
                                ChunkReleaseReceipt completed =
                                        registry.completeRelease(
                                                new ChunkReleaseReceipt(
                                                        releaseId, key, groupId,
                                                        payloadBytes,
                                                        payloadSha256,
                                                        finalDataWritten));
                                return completed != null
                                        ? new ChunkWriteback.ReleaseResult(
                                        completed.finalDataWritten(), true)
                                        : new ChunkWriteback.ReleaseResult(
                                        finalDataWritten, false);
                            }
                        };
                ChunkWriteback.ReleaseResult result;
                if (writeback == null) {
                    result = transaction.replay();
                    if (result == null) {
                        if (hasFinalData) {
                            LOGGER.warn("收到最终区块数据但服务端没有写回能力: {} 组={}",
                                    key.asString(), groupId);
                            return false;
                        }
                        result = transaction.commit(false);
                    }
                } else {
                    // 写回实现持有与实时上行相同的区块锁，并在锁内先查询回执。
                    // 首次操作按“最终写回 -> WAL 原子回执+删除 ownership”完成；
                    // ACK 丢失后的重放直接返回回执，不再写 NBT。
                    result = writeback.finalizeAndRelease(
                            key, groupId, finalData, transaction);
                }
                resultRef.set(result);
                if (hasFinalData && (result == null
                        || !result.finalDataWritten())) {
                    LOGGER.warn("最终区块数据写回失败，保留占用并等待重试: {} 组={}",
                            key.asString(), groupId);
                }
                return result != null && result.released();
            });
        } catch (IllegalArgumentException conflict) {
            LOGGER.warn("拒绝冲突的区块释放重试: {} 组={} releaseId={}",
                    key.asString(), groupId, releaseId, conflict);
            conn.send(error(msg, "release_id_conflict", conflict.getMessage()));
            return;
        } catch (RuntimeException failure) {
            LOGGER.warn("区块释放处理异常: {} 组={} releaseId={}",
                    key.asString(), groupId, releaseId, failure);
            conn.send(error(msg, "release_failed", failure.toString()));
            return;
        }
        if (!current) {
            conn.send(error(msg, "release_rejected",
                    "连接已失效、退出已冻结或最终数据未落盘"));
            return;
        }
        ChunkWriteback.ReleaseResult result = resultRef.get();
        if (result == null || !result.released()) {
            LOGGER.warn("区块释放未生效（未占用或占用者不符）: {} 组={}",
                    key.asString(), groupId);
        }
        JsonObject out = new JsonObject();
        out.addProperty("released", result != null && result.released());
        out.addProperty("finalDataWritten",
                result != null && result.finalDataWritten());
        conn.send(msg.reply(MessageType.CHUNK_RELEASE_ACK, out, null));
    }

    /**
     * 正 ACK 已到达客户端后的回执回收。删除动作自身具备幂等性：即使本应答丢失，
     * 客户端重发时服务端在“回执已不存在”状态仍返回 accepted=true。
     */
    private static void handleReleaseConfirmation(
            ControlConnection conn, ControlMessage msg,
            ChunkRegistry registry, ExitTransactionHandlers exits,
            ChunkReleaseExecutionTracker releaseTracker) {
        UUID releaseId = parseUuid(JsonUtil.getString(
                msg.json(), "releaseId", ""));
        UUID groupId = parseUuid(JsonUtil.getString(
                msg.json(), "groupId", ""));
        if (releaseId == null || groupId == null
                || !groupId.equals(conn.peerId())) {
            conn.send(error(msg, "invalid_request",
                    "releaseId/groupId 缺失、非法或与连接身份不符"));
            return;
        }
        ThreadPools.io().execute(() -> {
            java.util.concurrent.atomic.AtomicBoolean accepted =
                    new java.util.concurrent.atomic.AtomicBoolean();
            boolean current;
            try {
                current = exits != null && exits.writeRealtimeIfAllowed(
                        conn, () -> {
                            accepted.set(releaseTracker.confirm(
                                    releaseId, groupId, registry));
                            return accepted.get();
                        });
            } catch (RuntimeException failure) {
                LOGGER.warn("释放回执确认落盘失败: group={} releaseId={}",
                        groupId, releaseId, failure);
                conn.send(error(msg, "receipt_confirm_failed",
                        failure.toString()));
                return;
            }
            JsonObject out = new JsonObject();
            out.addProperty("accepted", current && accepted.get());
            conn.send(msg.reply(
                    MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM, out, null));
        });
    }

    // ------------------------------------------------------------ 区块探测

    /** 只读探测（Phase 4）：纯内存查表，Netty 线程直接应答，无磁盘 IO。 */
    private static void handleProbe(ControlConnection conn, ControlMessage msg, ChunkRegistry registry) {
        ChunkKey key = parseKey(msg.json());
        UUID groupId = parseUuid(JsonUtil.getString(msg.json(), "groupId", ""));
        if (key == null || groupId == null) {
            conn.send(error(msg, "invalid_request", "dimension/x/z/groupId 缺失或非法"));
            return;
        }
        java.util.concurrent.atomic.AtomicReference<ChunkRegistry.ClaimResult> resultRef =
                new java.util.concurrent.atomic.AtomicReference<>();
        if (!HelloHandler.withCurrentSession(conn, () -> {
            resultRef.set(registry.probe(key, groupId));
            return true;
        })) {
            conn.send(error(msg, "stale_session", "连接已被新会话替换"));
            return;
        }
        ChunkRegistry.ClaimResult result = resultRef.get();
        JsonObject out = new JsonObject();
        out.addProperty("blocked", !result.granted());
        if (!result.granted()) {
            out.addProperty("blockingChunk", result.blockingChunk().asString());
            out.addProperty("blockingGroup", result.blockingGroup().toString());
        }
        conn.send(msg.reply(MessageType.CHUNK_PROBE_RESPONSE, out, null));
    }

    // ------------------------------------------------------------ 玩家位置

    private static void handlePosUpdate(ControlConnection conn, ControlMessage msg,
                                        PlayerTable players, GroupTable groups) {
        UUID playerId = parseUuid(JsonUtil.getString(msg.json(), "uuid", ""));
        if (playerId == null) {
            return; // 无应答消息：非法数据静默丢弃并记日志即可
        }
        String playerName = JsonUtil.getString(msg.json(), "name", "");
        // 身份自洽校验（复查 G-06）：只接受"本连接所在组成员"的坐标。玩家表落库后
        // 这些坐标会直接成为跨组 /tp 的落点，不校验就等于把落点交给任意已握手连接。
        if (!ConnectionIdentity.isOwnGroupMember(groups, conn.peerId(), playerId, playerName)) {
            LOGGER.warn("拒绝越权位置上报: 连接 {} 声称玩家 {}（{}）",
                    conn.peerId(), playerId, playerName);
            return;
        }
        String dimension = JsonUtil.getString(msg.json(), "dimension", "minecraft:overworld");
        HelloHandler.withCurrentSession(conn, () -> {
            // 未登记的行忽略（HELLO 声明时建行）；已登记则整行刷新位置
            players.declare(playerId, playerName.isEmpty() ? null : playerName);
            players.updatePosition(playerId, dimension,
                    getDouble(msg.json(), "x"), getDouble(msg.json(), "y"),
                    getDouble(msg.json(), "z"));
            return true;
        });
    }

    // ------------------------------------------------------------ 工具

    /** 从 JSON 解析 ChunkKey（dimension/x/z）；缺失/非法返回 null。 */
    private static ChunkKey parseKey(JsonObject json) {
        String dimension = JsonUtil.getString(json, "dimension", "");
        if (dimension.isEmpty() || !json.has("x") || !json.has("z")) {
            return null;
        }
        try {
            return new ChunkKey(dimension, json.get("x").getAsInt(), json.get("z").getAsInt());
        } catch (Exception e) {
            return null; // x/z 不是数字（入站数据不可信）
        }
    }

    /** 防御性 UUID 解析：null / 格式非法返回 null 而非抛异常。 */
    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static double getDouble(JsonObject obj, String key) {
        var el = obj.get(key);
        return el != null && el.isJsonPrimitive() ? el.getAsDouble() : 0.0;
    }

    /** 构造保留 _rid 的 ERROR 应答（json: code, message）。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
