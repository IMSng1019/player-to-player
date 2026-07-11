package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
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
import java.util.UUID;

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

    private ChunkDataHandlers(MinecraftServer server, ChunkRegistry registry) {
        this.server = server;
        this.registry = registry;
    }

    /**
     * 注册区块数据面处理器。
     *
     * @return 供 CHUNK_RELEASE 处理链使用的最终数据写回接口
     */
    public static ChunkWriteback register(HandlerRegistry reg, MinecraftServer server,
                                          ChunkRegistry registry) {
        ChunkDataHandlers handlers = new ChunkDataHandlers(server, registry);
        reg.on(MessageType.CHUNK_DATA_REQUEST, handlers::handleDataRequest);
        reg.on(MessageType.CHUNK_DATA_UPLOAD, handlers::handleUpload);
        return handlers::writeFinalAndFlush;
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

    /** CHUNK_DATA_UPLOAD：实时上行（fire-and-forget，失败仅记日志 + 回 ERROR 供诊断）。 */
    private void handleUpload(ControlConnection conn, ControlMessage msg) {
        ChunkKey key = parseKey(msg.json());
        UUID peer = conn.peerId();
        if (key == null || peer == null) {
            conn.send(error(msg, "invalid_request", "dimension/x/z 缺失或非法"));
            return;
        }
        byte[] gzipNbt = msg.binary();
        // 解压/校验/写盘全部转 io 池
        ThreadPools.io().execute(() -> {
            if (!validateAndWrite(key, peer, gzipNbt, false)) {
                conn.send(error(msg, "upload_rejected", "区块上行被拒: " + key.asString()));
            }
        });
    }

    /**
     * CHUNK_RELEASE 携带的最终数据写回（{@link ChunkWriteback} 实现）：
     * 写回后<b>阻塞刷 IOWorker</b>——释放一经确认，其他组的申请随时可能到来，
     * hasServerData 的 region 文件探测必须能看到刚写入的数据。
     * 调用方（RegistryHandlers）已在 io 池上，阻塞安全。
     */
    private boolean writeFinalAndFlush(ChunkKey key, UUID uploader, byte[] gzipNbt) {
        return validateAndWrite(key, uploader, gzipNbt, true);
    }

    /**
     * 校验并写回单区块（io 线程）。
     *
     * @param flush 是否阻塞至 IOWorker 落盘（释放路径 true，实时上行 false）
     */
    private boolean validateAndWrite(ChunkKey key, UUID uploader, byte[] gzipNbt, boolean flush) {
        try {
            // ---- 权威校验：上传方必须是注册表登记的占用组 ----
            // （Phase 1/2 约定 groupId == 主客户端 clientId，故直接与 peerId 比对）
            UUID owner = registry.ownerOf(key);
            if (owner == null || !owner.equals(uploader)) {
                LOGGER.warn("拒绝区块上行（非占用组）: {} 上传方={} 占用组={}",
                        key.asString(), uploader, owner);
                return false;
            }
            if (gzipNbt == null || gzipNbt.length == 0 || gzipNbt.length > MAX_COMPRESSED_BYTES) {
                LOGGER.warn("拒绝区块上行（数据缺失或超限）: {} ({} 字节)",
                        key.asString(), gzipNbt == null ? 0 : gzipNbt.length);
                return false;
            }
            // ---- 解析（带解压上限，防解压炸弹）----
            CompoundTag tag = NbtIo.readCompressed(new ByteArrayInputStream(gzipNbt),
                    NbtAccounter.create(MAX_CHUNK_NBT_BYTES));
            // ---- 一致性校验：NBT 里的坐标必须与声称的键一致（防错位覆盖）----
            int xPos = tag.getInt("xPos");
            int zPos = tag.getInt("zPos");
            if (xPos != key.x() || zPos != key.z()) {
                LOGGER.warn("拒绝区块上行（坐标不符）: 声称 {} 实际 xPos={} zPos={}",
                        key.asString(), xPos, zPos);
                return false;
            }
            // ---- 版本校验：不接受高于本服的 DataVersion（未来版本数据会毁存档）----
            int dataVersion = ChunkStorage.getVersion(tag);
            int current = SharedConstants.getCurrentVersion().getDataVersion().getVersion();
            if (dataVersion > current) {
                LOGGER.warn("拒绝区块上行（DataVersion {} 高于本服 {}）: {}",
                        dataVersion, current, key.asString());
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
