package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.group.ExitPayloadProtocol;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.ChunkWriteback;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.SharedConstants;
import net.minecraft.core.registries.Registries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.storage.ChunkStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.Optional;

/**
 * 区块数据面处理器（服务端，Phase 2；DESIGN.md 路线图"区块数据上行与 MCA 单区块写回"）。
 * <p>
 * 规范出处：服务端"需要实时计算区块的更改更新本地区块文件（……服务端应该具有在
 * mca 文件中修改区块的能力）"、"若该区块在服务端有数据……则由服务端发送给主客户端加载"。
 * <ul>
 *   <li>{@link MessageType#CHUNK_DATA_REQUEST}（json: dimension, x, z）→
 *       {@link MessageType#CHUNK_DATA}（json: exists；binary: 压缩区块 NBT）——
 *       仅占用组可请求（申请授予后才有数据可拿，防任意区块窥探）；</li>
 *   <li>{@link MessageType#CHUNK_DATA_UPLOAD}（json: dimension, x, z；binary:
 *       压缩区块 NBT）：主客户端集成服务端存盘时的实时上行，写回 MCA；</li>
 *   <li>{@link ChunkWriteback}（CHUNK_RELEASE 携带的最终数据）：写回 + 阻塞刷盘，
 *       保证释放后其他组的 hasServerData 探测立刻可见。</li>
 * </ul>
 *
 * <h2>MCA 单区块写回的实现选择</h2>
 * 不自实现 MCA 格式（规范提示"可参考 MCA Selector"），而是直接走服务端自身的
 * {@link ChunkStorage#write}（{@code level.getChunkSource().chunkMap}）——
 * 它经由 IOWorker 在 region 文件内做单区块定位写入，正是 MCA Selector 同款能力，
 * 且与服务端自己的 region 文件句柄共用一套存储栈，天然避免双写句柄冲突；
 * IOWorker 邮箱线程安全，可从任意线程提交。
 * <p>
 * 配套的 {@code mixin.ChunkMapMixin} 已取消物理服务端<b>自身</b>的区块存盘
 * （suspendWorldTick 门控）：服务端内存中登录期加载的陈旧区块永远不会写盘，
 * MCA 的唯一写入来源就是本类 —— 上行数据不会被陈旧内存态覆盖。
 *
 * <h2>校验（入站不可信）</h2>
 * 占用组核对（注册表）、NBT 解压上限、xPos/zPos 与请求一致、DataVersion 不高于
 * 本服版本（防未来版本数据写入旧存档），任一不符即拒绝。
 * <p>
 * 线程模型：handler 在 Netty 事件循环上，解压/校验/磁盘 IO 一律转 {@link ThreadPools#io()}。
 */
public final class ChunkDataHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/chunk-data");

    /** 单区块 NBT 解压后大小上限（防解压炸弹；正常区块远小于此）。 */
    private static final long MAX_CHUNK_NBT_BYTES = 64L * 1024 * 1024;

    /** 压缩后大小上限：不超过协议帧长减去帧头/JSON 余量。 */
    private static final int MAX_COMPRESSED_BYTES = Protocol.MAX_FRAME_BYTES - 64 * 1024;

    private final MinecraftServer server;
    private final ChunkRegistry registry;
    private final ChunkWriteCoordinator writes = new ChunkWriteCoordinator();
    /** 批次 WAL append 与 ownership release 的线性化锁。 */
    private final Object durableBatchLock = new Object();
    /** 运行期批次的耐久索引；为空时保留无 WAL 的单区块写回行为。 */
    private final DurableChunkJournal durableJournal;
    private volatile ExitTransactionHandlers exits;

    private ChunkDataHandlers(MinecraftServer server, ChunkRegistry registry,
                              DurableChunkJournal durableJournal) {
        this.server = server;
        this.registry = registry;
        this.durableJournal = durableJournal;
    }

    /**
     * 注册区块数据面处理器。
     *
     * @return 供 CHUNK_RELEASE 处理链使用的最终数据写回接口
     */
    public static ChunkDataHandlers register(HandlerRegistry reg, MinecraftServer server,
                                             ChunkRegistry registry) {
        return register(reg, server, registry, null);
    }

    /** 注册带 WAL 的 v5 批次处理器。 */
    public static ChunkDataHandlers register(HandlerRegistry reg, MinecraftServer server,
                                             ChunkRegistry registry,
                                             DurableChunkJournal durableJournal) {
        ChunkDataHandlers handlers = new ChunkDataHandlers(server, registry, durableJournal);
        reg.on(MessageType.CHUNK_DATA_REQUEST, handlers::handleDataRequest);
        reg.on(MessageType.CHUNK_DATA_UPLOAD, handlers::handleUpload);
        reg.on(MessageType.CHUNK_DATA_BATCH, handlers::handleBatch);
        return handlers;
    }

    void setExitTransactions(ExitTransactionHandlers exits) {
        this.exits = exits;
    }

    /** 服务端重启后把 WAL 索引中的领先快照重新排入 MCA 物化队列。 */
    void materializeRecovered() {
        if (durableJournal == null) {
            return;
        }
        List<ChunkDelta> recovered = new java.util.ArrayList<>();
        durableJournal.forEachLatest(entry -> recovered.add(new ChunkDelta(
                entry.key(), entry.version(), entry.payload(), entry.sha256())));
        scheduleBatchMaterialization(recovered);
    }

    /** 退出 COMMIT 已写入 WAL 后的非阻塞 MCA 物化入口。 */
    void materializeDurable(ChunkKey key, byte[] gzipNbt) {
        scheduleMaterialization(key, gzipNbt);
    }

    /** 退出复合提交只向 IO 池排一个任务，任务内顺序物化整批区块。 */
    void materializeDurableBatch(List<ChunkDelta> deltas) {
        scheduleBatchMaterialization(deltas);
    }

    // ------------------------------------------------------------ 数据下发

    /** CHUNK_DATA_REQUEST → CHUNK_DATA（占用组专属；读经 IOWorker，压缩在 io 池）。 */
    private void handleDataRequest(ControlConnection conn, ControlMessage msg) {
        ChunkKey key = parseKey(msg.json());
        UUID peer = conn.peerId();
        if (key == null || peer == null) {
            conn.send(error(msg, "invalid_request", "dimension/x/z 缺失或非法"));
            return;
        }
        // 只有占用组可请求区块数据：申请授予（CHUNK_CLAIM_RESPONSE granted）在先
        UUID owner = registry.ownerOf(key);
        if (owner == null || !owner.equals(peer)) {
            conn.send(error(msg, "not_owner", "该区块未被你的组占用: " + key.asString()));
            return;
        }
        // WAL 是 MCA 的领先视图。优先返回它可见的数据，避免后台物化尚未完成时
        // 新接管客户端拿到旧的 region 内容。
        if (durableJournal != null) {
            Optional<DurableChunkJournal.Entry> durable = durableJournal.latest(key);
            if (durable.isPresent()) {
                JsonObject out = new JsonObject();
                out.addProperty("dimension", key.dimension());
                out.addProperty("x", key.x());
                out.addProperty("z", key.z());
                out.addProperty("exists", true);
                conn.send(msg.reply(MessageType.CHUNK_DATA, out,
                        durable.get().payload()));
                return;
            }
        }
        ServerLevel level = resolveLevel(key.dimension());
        if (level == null) {
            conn.send(error(msg, "unknown_dimension", "维度不存在: " + key.dimension()));
            return;
        }
        // ChunkStorage.read 经 IOWorker 异步完成；压缩转 io 池，不占 IOWorker 线程
        level.getChunkSource().chunkMap.read(new ChunkPos(key.x(), key.z()))
                .whenCompleteAsync((opt, err) -> {
                    try {
                        if (err != null) {
                            LOGGER.warn("区块读取失败: {}", key.asString(), err);
                            conn.send(error(msg, "read_failed", String.valueOf(err)));
                            return;
                        }
                        JsonObject out = new JsonObject();
                        out.addProperty("dimension", key.dimension());
                        out.addProperty("x", key.x());
                        out.addProperty("z", key.z());
                        if (opt.isEmpty()) {
                            // region 文件存在但恰缺该区块（RegionFileProbe 轻量判定的
                            // 已知误报面）：告知不存在，客户端回退种子生成
                            out.addProperty("exists", false);
                            conn.send(msg.reply(MessageType.CHUNK_DATA, out, null));
                            return;
                        }
                        ByteArrayOutputStream bos = new ByteArrayOutputStream(64 * 1024);
                        NbtIo.writeCompressed(opt.get(), bos);
                        out.addProperty("exists", true);
                        conn.send(msg.reply(MessageType.CHUNK_DATA, out, bos.toByteArray()));
                        LOGGER.debug("区块数据下发: {} → {} ({} 字节压缩)",
                                key.asString(), peer, bos.size());
                    } catch (Exception e) {
                        LOGGER.warn("区块数据下发失败: {}", key.asString(), e);
                        conn.send(error(msg, "serialize_failed", e.toString()));
                    }
                }, ThreadPools.io());
    }

    // ------------------------------------------------------------ 数据上行

    /** CHUNK_DATA_UPLOAD：实时上行；完成校验并提交 IOWorker 后返回 written 确认。 */
    private void handleUpload(ControlConnection conn, ControlMessage msg) {
        ChunkKey key = parseKey(msg.json());
        UUID peer = conn.peerId();
        UUID uploadId = parseUploadId(msg.json());
        if (key == null || peer == null || uploadId == null) {
            conn.send(error(msg, "invalid_request", "dimension/x/z 缺失或非法"));
            return;
        }
        byte[] gzipNbt = msg.binary();
        // 解压/校验/写盘全部转 io 池
        ThreadPools.io().execute(() -> {
            ExitTransactionHandlers currentExits = exits;
            boolean written = currentExits != null
                    && currentExits.writeRealtimeIfAllowed(conn,
                    () -> durableJournal != null
                            ? appendLegacySingle(peer, key, gzipNbt, uploadId)
                            : validateAndWrite(key, peer, gzipNbt, true, false));
            if (!written) {
                conn.send(error(msg, "upload_rejected", "区块上行被拒: " + key.asString()));
                return;
            }
            JsonObject out = new JsonObject();
            out.addProperty("written", true);
            conn.send(msg.reply(MessageType.CHUNK_DATA_UPLOAD, out, null));
        });
    }

    /** v5 单区块快速路径同样进入 WAL，避免每次实时上行都 flush MCA。 */
    private boolean appendLegacySingle(UUID peer, ChunkKey key, byte[] gzipNbt,
                                       UUID uploadId) {
        synchronized (durableBatchLock) {
            try {
                if (registry.isExitFrozen(peer)) {
                    return false;
                }
                return appendDurableSingle(
                        peer, key, gzipNbt, MAX_COMPRESSED_BYTES, uploadId);
            } catch (Exception e) {
                LOGGER.warn("单区块实时上行写入 WAL 失败: {}", key.asString(), e);
                return false;
            }
        }
    }

    /** 调用方必须持有 durableBatchLock；ownership 校验与 WAL append 不可被释放穿插。 */
    private boolean appendDurableSingle(UUID peer, ChunkKey key, byte[] gzipNbt,
                                        int maxCompressedBytes)
            throws IOException {
        // 释放和旧退出恢复没有网络请求 ID；每次被授权执行都必须是新的顺序事件，
        // 不能按内容寻址，否则 A→B→A 会把第三次 A 错判为第一次 A 的重放。
        return appendDurableSingle(peer, key, gzipNbt, maxCompressedBytes,
                UUID.randomUUID());
    }

    private boolean appendDurableSingle(UUID peer, ChunkKey key, byte[] gzipNbt,
                                        int maxCompressedBytes, UUID eventId)
            throws IOException {
        UUID owner = registry.ownerOf(key);
        if (eventId == null || owner == null || !owner.equals(peer)
                || decodeValidatedChunk(key, gzipNbt, maxCompressedBytes) == null) {
            return false;
        }
        ChunkDeltaBatch batch = new ChunkDeltaBatch(eventId, 0, 0,
                List.of(ChunkDelta.of(key, 0, gzipNbt)));
        durableJournal.append(peer, batch);
        scheduleMaterialization(key, gzipNbt);
        return true;
    }

    /**
     * v5 客户端提供稳定 uploadId；缺失字段的兼容请求按一次独立接收事件处理。
     * 非法 UUID 必须拒绝，避免攻击者把不同载荷强行绑定到不可预测身份。
     */
    private static UUID parseUploadId(JsonObject json) {
        String value = JsonUtil.getString(json, "uploadId", "");
        if (value.isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * CHUNK_DATA_BATCH：完整校验后一次追加 WAL。MCA 写回只进入后台队列，不阻塞 ACK；
     * 因而 RTT 与区块数量无关，退出时只需等待最后一个批次的 durable ACK。
     */
    private void handleBatch(ControlConnection conn, ControlMessage msg) {
        ThreadPools.io().execute(() -> {
            JsonObject out = new JsonObject();
            boolean accepted = false;
            try {
                UUID peer = conn.peerId();
                ChunkDeltaBatch batch = ChunkDeltaBatch.decode(msg.binary());
                if (peer == null || batch.deltas().isEmpty()
                        || msg.binary().length >= Protocol.MAX_FRAME_BYTES) {
                    throw new IllegalArgumentException("批次身份或帧长度非法");
                }
                String jsonBatchId = JsonUtil.getString(msg.json(), "batchId", "");
                int jsonSequence = JsonUtil.getInt(msg.json(), "sequence", -1);
                String jsonSha256 = JsonUtil.getString(msg.json(), "sha256", "");
                if (!batch.batchId().toString().equals(jsonBatchId)
                        || batch.sequence() != jsonSequence
                        || !imsng.player_to_player.util.Sha256.hex(msg.binary())
                        .equals(jsonSha256)) {
                    throw new IllegalArgumentException("批次 JSON 与二进制身份不一致");
                }
                ExitTransactionHandlers currentExits = exits;
                java.util.concurrent.atomic.AtomicReference<DurableChunkJournal.AppendResult>
                        resultRef = new java.util.concurrent.atomic.AtomicReference<>();
                java.util.function.BooleanSupplier operation = () -> {
                    resultRef.set(appendValidatedBatch(peer, batch));
                    return true;
                };
                accepted = currentExits == null
                        ? operation.getAsBoolean()
                        : currentExits.writeRealtimeIfAllowed(conn, operation);
                if (!accepted) {
                    throw new IllegalStateException("批次上行被 ownership/退出保护拒绝");
                }
                DurableChunkJournal.AppendResult result = resultRef.get();
                out.addProperty("accepted", true);
                out.addProperty("written", true);
                out.addProperty("durableEpoch",
                        result != null ? result.durableEpoch() : 0L);
                out.addProperty("duplicate", result != null && result.duplicate());
                conn.send(msg.reply(MessageType.CHUNK_DATA_BATCH, out, null));
            } catch (Exception e) {
                LOGGER.warn("区块增量批次处理失败", e);
                out.addProperty("accepted", false);
                out.addProperty("written", false);
                conn.send(msg.reply(MessageType.CHUNK_DATA_BATCH, out, null));
            }
        });
    }

    private DurableChunkJournal.AppendResult appendValidatedBatch(UUID peer,
                                                                    ChunkDeltaBatch batch) {
        synchronized (durableBatchLock) {
            for (ChunkDelta delta : batch.deltas()) {
                ChunkKey key = delta.key();
                UUID owner = registry.ownerOf(key);
                if (owner == null || !owner.equals(peer)
                        || registry.isExitFrozen(peer)) {
                    throw new IllegalStateException("批次区块 ownership 校验失败");
                }
                try {
                    if (decodeValidatedChunk(key, delta.payload(), MAX_COMPRESSED_BYTES) == null) {
                        throw new IllegalStateException("批次区块 NBT 校验失败");
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("批次区块 NBT 解码失败", e);
                }
            }
            if (durableJournal == null) {
                // 没有 WAL 的兼容测试/旧部署：仍保证一个请求内完成全部写回。
                for (ChunkDelta delta : batch.deltas()) {
                    if (!validateAndWrite(delta.key(), peer, delta.payload(), false, false)) {
                        throw new IllegalStateException("兼容批次区块写回失败");
                    }
                }
                return null;
            }
            DurableChunkJournal.AppendResult result = durableJournal.append(peer, batch);
            scheduleBatchMaterialization(batch.deltas());
            return result;
        }
    }

    /** 后台物化单区块，失败可由启动恢复扫描再次补做，不影响已返回的 durable ACK。 */
    private void scheduleMaterialization(ChunkKey key, byte[] gzipNbt) {
        scheduleBatchMaterialization(List.of(ChunkDelta.of(key, 0, gzipNbt)));
    }

    /** 每个网络/WAL 批次只提交一个后台任务，避免无界 cached pool 瞬间扩张。 */
    private void scheduleBatchMaterialization(List<ChunkDelta> deltas) {
        if (deltas == null || deltas.isEmpty()) {
            return;
        }
        List<ChunkDelta> immutable = List.copyOf(deltas);
        ThreadPools.io().execute(() -> {
            for (ChunkDelta delta : immutable) {
                materializeOne(delta.key(), delta.payload());
            }
        });
    }

    private void materializeOne(ChunkKey key, byte[] gzipNbt) {
        writes.withLock(key, () -> {
            try {
                if (durableJournal != null) {
                    String expected = imsng.player_to_player.util.Sha256.hex(gzipNbt);
                    boolean stillLatest = durableJournal.latest(key)
                            .map(entry -> expected.equals(entry.sha256()))
                            .orElse(false);
                    if (!stillLatest) {
                        return true; // 已有更新 epoch，旧物化任务直接丢弃
                    }
                }
                CompoundTag tag = decodeValidatedChunk(key, gzipNbt, MAX_COMPRESSED_BYTES);
                if (tag == null) {
                    return false;
                }
                ServerLevel level = resolveLevel(key.dimension());
                if (level == null) {
                    return false;
                }
                level.getChunkSource().chunkMap.write(new ChunkPos(key.x(), key.z()), tag);
                return true;
            } catch (Exception e) {
                LOGGER.warn("区块耐久批次异步物化失败，将由后续恢复重试: {}",
                        key.asString(), e);
                return false;
            }
        });
    }

    /**
     * CHUNK_RELEASE 携带的最终数据写回（{@link ChunkWriteback} 实现）：
     * v5 先把最终快照同步 force 到世界 WAL，再与 ownership 回执线性化提交；MCA 只作
     * 后台物化视图，不进入释放 ACK 的关键路径。无 WAL 的兼容路径仍同步刷 IOWorker。
     * 调用方（RegistryHandlers）已在 io 池上，WAL 与兼容磁盘等待都不会阻塞 Netty。
     */
    ChunkWriteback.ReleaseResult finalizeAndRelease(
            ChunkKey key, UUID uploader, byte[] gzipNbt,
            ChunkWriteback.ReleaseTransaction transaction) {
        boolean hasFinalData = gzipNbt != null && gzipNbt.length > 0;
        if (durableJournal == null) {
            return writes.finalizeAndRelease(
                    key,
                    hasFinalData,
                    hasFinalData
                            ? () -> validateAndWriteLocked(
                            key, uploader, gzipNbt, true, false,
                            MAX_COMPRESSED_BYTES)
                            : null,
                    transaction);
        }

        // 锁序固定为“区块锁 -> 批次 WAL 锁 -> registry claimLock”。运行期批次只取
        // 批次锁，因此最终 WAL 记录与 ownership 删除之间不再存在可插入窗口。
        return writes.withLock(key, () -> {
            synchronized (durableBatchLock) {
                return writes.finalizeAndReleaseWithinChunkLock(
                        hasFinalData,
                        hasFinalData
                                ? () -> appendReleaseFinal(
                                key, uploader, gzipNbt)
                                : null,
                        transaction);
            }
        });
    }

    /** 调用方同时持有区块锁与 durableBatchLock。 */
    private boolean appendReleaseFinal(ChunkKey key, UUID uploader,
                                       byte[] gzipNbt) {
        try {
            return appendDurableSingle(
                    uploader, key, gzipNbt, MAX_COMPRESSED_BYTES);
        } catch (IOException e) {
            LOGGER.warn("区块释放最终数据写入 WAL 失败: {}",
                    key.asString(), e);
            return false;
        }
    }

    /** RegistryHandlers 使用的最终释放写回接口。 */
    ChunkWriteback releaseWriteback() {
        return this::finalizeAndRelease;
    }

    /**
     * 退出事务区块写入。v5 新事务只会传实际 written 数据；preserveExisting 分支
     * 仅供升级前已经 committed 的 data/marker 暂存恢复。两种情况都要求事务组仍持有区块。
     */
    boolean writeExit(UUID groupId, ChunkKey key, byte[] gzipNbt,
                      boolean preserveExisting) {
        return writes.withLock(key, () -> {
            UUID owner = registry.ownerOf(key);
            if (owner == null || !owner.equals(groupId)) {
                LOGGER.warn("拒绝退出区块写入（所有权不符）: {} 事务组={} 占用组={}",
                        key.asString(), groupId, owner);
                return false;
            }
            if (preserveExisting) {
                return existingChunkIsReadable(key);
            }
            if (durableJournal != null) {
                synchronized (durableBatchLock) {
                    try {
                        return appendDurableSingle(groupId, key, gzipNbt,
                                ExitPayloadProtocol.MAX_ASSEMBLED_BYTES);
                    } catch (IOException e) {
                        LOGGER.warn("旧退出事务区块写入 WAL 失败: {}", key.asString(), e);
                        return false;
                    }
                }
            }
            return validateAndWriteLocked(key, groupId, gzipNbt, false, true,
                    ExitPayloadProtocol.MAX_ASSEMBLED_BYTES);
        });
    }

    /** pending 暂存前只做所有权与 NBT 校验，不修改正式 region；preserve 仅兼容旧恢复。 */
    boolean validateExit(UUID groupId, ChunkKey key, byte[] gzipNbt,
                         boolean preserveExisting) {
        return writes.withLock(key, () -> {
            UUID owner = registry.ownerOf(key);
            if (owner == null || !owner.equals(groupId)) {
                return false;
            }
            if (preserveExisting) {
                return existingChunkIsReadable(key);
            }
            try {
                return decodeValidatedChunk(key, gzipNbt,
                        ExitPayloadProtocol.MAX_ASSEMBLED_BYTES) != null;
            } catch (Exception e) {
                LOGGER.warn("退出区块暂存校验失败: {}", key.asString(), e);
                return false;
            }
        });
    }

    /** 刷新全部维度的 I/O worker；退出提交确认前调用。 */
    void flushAll() {
        for (ServerLevel level : server.getAllLevels()) {
            level.getChunkSource().chunkMap.flushWorker();
        }
    }

    /**
     * 校验并写回单区块（io 线程）。
     *
     * @param flush 是否阻塞至 IOWorker 落盘（释放路径 true，实时上行 false）
     */
    private boolean validateAndWrite(ChunkKey key, UUID uploader, byte[] gzipNbt,
                                     boolean flush, boolean allowExitFrozen) {
        return writes.withLock(key, () -> validateAndWriteLocked(
                    key, uploader, gzipNbt, flush, allowExitFrozen,
                    MAX_COMPRESSED_BYTES));
    }

    private boolean validateAndWriteLocked(ChunkKey key, UUID uploader, byte[] gzipNbt,
                                           boolean flush, boolean allowExitFrozen,
                                           int maxCompressedBytes) {
        try {
            // ---- 权威校验：上传方必须是注册表登记的占用组 ----
            // （Phase 1/2 约定 groupId == 主客户端 clientId，故直接与 peerId 比对）
            UUID owner = registry.ownerOf(key);
            if (owner == null || !owner.equals(uploader)) {
                LOGGER.warn("拒绝区块上行（非占用组）: {} 上传方={} 占用组={}",
                        key.asString(), uploader, owner);
                return false;
            }
            if (!allowExitFrozen && registry.isExitFrozen(uploader)) {
                LOGGER.info("拒绝退出冻结后的迟到区块上行: {} 组={}",
                        key.asString(), uploader);
                return false;
            }
            CompoundTag tag = decodeValidatedChunk(
                    key, gzipNbt, maxCompressedBytes);
            if (tag == null) {
                return false;
            }
            ServerLevel level = resolveLevel(key.dimension());
            if (level == null) {
                LOGGER.warn("拒绝区块上行（维度不存在）: {}", key.dimension());
                return false;
            }
            // ---- 写回：服务端自身存储栈的单区块写入（IOWorker 线程安全）----
            ChunkStorage storage = level.getChunkSource().chunkMap;
            storage.write(new ChunkPos(key.x(), key.z()), tag);
            if (flush) {
                storage.flushWorker(); // 阻塞至落盘（io 线程上允许）
            }
            LOGGER.debug("区块写回完成: {} ({} 字节压缩, flush={})",
                    key.asString(), gzipNbt.length, flush);
            return true;
        } catch (IOException e) {
            LOGGER.warn("区块上行数据解析失败: {}", key.asString(), e);
            return false;
        } catch (Exception e) {
            LOGGER.error("区块写回异常: {}", key.asString(), e);
            return false;
        }
    }

    private CompoundTag decodeValidatedChunk(ChunkKey key, byte[] gzipNbt,
                                              int maxCompressedBytes)
            throws IOException {
        if (gzipNbt == null || gzipNbt.length == 0
                || gzipNbt.length > maxCompressedBytes) {
            LOGGER.warn("拒绝区块上行（数据缺失或超限）: {} ({} 字节)",
                    key.asString(), gzipNbt == null ? 0 : gzipNbt.length);
            return null;
        }
        CompoundTag tag = NbtIo.readCompressed(new ByteArrayInputStream(gzipNbt),
                NbtAccounter.create(MAX_CHUNK_NBT_BYTES));
        int xPos = tag.getInt("xPos");
        int zPos = tag.getInt("zPos");
        if (xPos != key.x() || zPos != key.z()) {
            LOGGER.warn("拒绝区块上行（坐标不符）: 声称 {} 实际 xPos={} zPos={}",
                    key.asString(), xPos, zPos);
            return null;
        }
        int dataVersion = ChunkStorage.getVersion(tag);
        int current = SharedConstants.getCurrentVersion().getDataVersion().getVersion();
        if (dataVersion > current) {
            LOGGER.warn("拒绝区块上行（DataVersion {} 高于本服 {}）: {}",
                    dataVersion, current, key.asString());
            return null;
        }
        return tag;
    }

    /** legacy preserve marker 只有在 IOWorker 能实际读到该区块时才能完成恢复。 */
    private boolean existingChunkIsReadable(ChunkKey key) {
        try {
            ServerLevel level = resolveLevel(key.dimension());
            if (level == null) {
                return false;
            }
            level.getChunkSource().chunkMap.flushWorker();
            boolean exists = level.getChunkSource().chunkMap
                    .read(new ChunkPos(key.x(), key.z())).join().isPresent();
            if (!exists) {
                LOGGER.warn("退出事务要求保留服务端区块，但实际不存在: {}",
                        key.asString());
            }
            return exists;
        } catch (Exception e) {
            LOGGER.warn("验证退出保留区块失败: {}", key.asString(), e);
            return false;
        }
    }

    // ------------------------------------------------------------ 工具

    /** 按维度标识解析 ServerLevel；未知维度返回 null。 */
    private ServerLevel resolveLevel(String dimension) {
        ResourceLocation location = ResourceLocation.tryParse(dimension);
        if (location == null) {
            return null;
        }
        return server.getLevel(ResourceKey.create(Registries.DIMENSION, location));
    }

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

    /** 构造保留 _rid 的 ERROR 应答（json: code, message）。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
