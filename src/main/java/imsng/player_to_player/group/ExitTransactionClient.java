package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 客户端退出事务重放器。任何连接、请求、应答或服务端重启错误都只触发下一轮重试，
 * 方法在服务端明确确认前绝不返回。
 */
public final class ExitTransactionClient {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/exit-client");

    /** 小批次沿用单区块消息形态以减少封装开销；大退出一律走 v5 批次。 */
    private static final int BATCH_MIN_CHUNKS = 3;
    /** 单批记录预算，留出批次头、JSON 与控制帧余量。 */
    private static final int BATCH_RECORD_BYTES = 4 * 1024 * 1024;

    public interface ConnectionFactory {
        ControlConnection connect() throws Exception;

        void invalidate(ControlConnection connection);
    }

    @FunctionalInterface
    public interface RetryWaiter {
        void await(int attempt);
    }

    public record BeginOutcome(ExitManifest manifest, boolean committed) {
    }

    private final ConnectionFactory connections;
    private final RetryWaiter waiter;

    public ExitTransactionClient(ConnectionFactory connections, RetryWaiter waiter) {
        this.connections = connections;
        this.waiter = waiter;
    }

    /** 无限重试 EXIT_BEGIN，并返回服务端冻结后的权威清单。 */
    public BeginOutcome beginUntilAccepted(ExitManifest requested) {
        int attempt = 0;
        while (true) {
            attempt++;
            ControlConnection connection = null;
            try {
                connection = connections.connect();
                return beginOnce(connection, requested);
            } catch (Exception e) {
                LOGGER.warn("EXIT_BEGIN 第 {} 次尝试失败，将持续重试: {}",
                        attempt, e.toString());
                if (connection != null) {
                    connections.invalidate(connection);
                }
                waiter.await(attempt);
            }
        }
    }

    /**
     * 无限重放完整事务。每次连接都从 EXIT_BEGIN 开始，故服务端重启清空运行期
     * received 集合后也能恢复；begin 已返回 committed 时立即成功。
     */
    public void commitUntilAccepted(ExitBundle bundle) {
        int attempt = 0;
        while (true) {
            attempt++;
            ControlConnection connection = null;
            try {
                connection = connections.connect();
                BeginOutcome begin = beginOnce(connection, bundle.manifest());
                if (begin.committed()) {
                    return;
                }
                if (!begin.manifest().equals(bundle.manifest())) {
                    throw new IOException("服务端退出清单与本地最终数据包不一致");
                }
                for (UUID playerId : bundle.playerIds().stream().sorted().toList()) {
                    JsonObject json = idJson(bundle.manifest().shutdownId());
                    json.addProperty("playerUuid", playerId.toString());
                    sendPayload(connection, MessageType.EXIT_PLAYER_DATA, json,
                            bundle.playerData(playerId));
                }
                Comparator<ChunkKey> byKey = Comparator.comparing(ChunkKey::asString);
                var writtenChunks = bundle.writtenChunks().stream().sorted(byKey).toList();
                if (writtenChunks.size() >= BATCH_MIN_CHUNKS) {
                    sendChunkBatches(connection, bundle, writtenChunks);
                } else {
                    for (ChunkKey key : writtenChunks) {
                        sendChunk(connection, bundle, key, bundle.chunkData(key));
                    }
                }
                JsonObject commitJson = idJson(bundle.manifest().shutdownId());
                JsonArray writtenChunkKeys = new JsonArray(writtenChunks.size());
                for (ChunkKey key : writtenChunks) {
                    writtenChunkKeys.add(key.asString());
                }
                commitJson.add("writtenChunks", writtenChunkKeys);
                ControlMessage commit = request(connection, ControlMessage.of(
                        MessageType.EXIT_COMMIT, commitJson));
                requireBoolean(commit, MessageType.EXIT_COMMIT, "committed");
                return;
            } catch (Exception e) {
                LOGGER.warn("退出最终提交第 {} 次尝试失败，将从头重放: {}",
                        attempt, e.toString());
                if (connection != null) {
                    connections.invalidate(connection);
                }
                waiter.await(attempt);
            }
        }
    }

    private BeginOutcome beginOnce(ControlConnection connection,
                                   ExitManifest requested) throws Exception {
        ControlMessage response = request(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, requested.toJson()));
        if (response.type() != MessageType.EXIT_BEGIN
                || !JsonUtil.getBoolean(response.json(), "accepted", false)
                || !response.json().has("manifest")
                || !response.json().get("manifest").isJsonObject()) {
            throw new IOException("物理服务端未接受退出保护意图");
        }
        ExitManifest canonical = ExitManifest.fromJson(
                response.json().getAsJsonObject("manifest"));
        return new BeginOutcome(canonical,
                JsonUtil.getBoolean(response.json(), "committed", false));
    }

    private void sendChunk(ControlConnection connection, ExitBundle bundle,
                           ChunkKey key, byte[] data) throws Exception {
        JsonObject json = idJson(bundle.manifest().shutdownId());
        json.addProperty("dimension", key.dimension());
        json.addProperty("x", key.x());
        json.addProperty("z", key.z());
        sendPayload(connection, MessageType.EXIT_CHUNK_DATA, json, data);
    }

    /**
     * 将退出尾部区块合并为少量批次。每个批次只有一次 request/ACK，服务端在 WAL
     * 暂存完成后应答；最终耐久性由 EXIT_COMMIT 的复合 WAL 提供。断线时外层会从
     * EXIT_BEGIN 重连并按 batchId 幂等重放。
     */
    private void sendChunkBatches(ControlConnection connection, ExitBundle bundle,
                                  List<ChunkKey> writtenChunks) throws Exception {
        List<ChunkDelta> deltas = new ArrayList<>(writtenChunks.size());
        for (ChunkKey key : writtenChunks) {
            byte[] payload = bundle.chunkData(key);
            if (payload == null || payload.length == 0) {
                throw new IOException("退出区块载荷缺失: " + key.asString());
            }
            // 退出清单中的每个坐标只保留最终快照；服务端用批次 epoch 保证顺序，
            // version=0 仅用于批内重复检测，不依赖客户端重启后的计数器。
            deltas.add(ChunkDelta.of(key, 0, payload));
        }
        List<ChunkDeltaBatch> batches = ChunkDeltaBatch.split(
                bundle.manifest().shutdownId(), 0, deltas, BATCH_RECORD_BYTES);
        long started = System.nanoTime();
        long bytes = 0;
        for (ChunkDeltaBatch batch : batches) {
            byte[] encoded = batch.encode();
            if (encoded.length >= Protocol.MAX_FRAME_BYTES) {
                throw new IOException("退出区块批次超过控制帧上限: " + encoded.length);
            }
            JsonObject json = idJson(bundle.manifest().shutdownId());
            json.addProperty("batchId", batch.batchId().toString());
            json.addProperty("sequence", batch.sequence());
            json.addProperty("epoch", batch.epoch());
            json.addProperty("deltaCount", batch.deltas().size());
            json.addProperty("sha256", Sha256.hex(encoded));
            ControlMessage response = request(connection, ControlMessage.of(
                    MessageType.EXIT_CHUNK_BATCH, json, encoded));
            if (response.type() != MessageType.EXIT_CHUNK_BATCH
                    || !JsonUtil.getBoolean(response.json(), "accepted", false)
                    || !JsonUtil.getBoolean(response.json(), "written", false)) {
                throw new IOException("退出区块批次应答失败 sequence=" + batch.sequence());
            }
            bytes += encoded.length;
        }
        LOGGER.info("退出区块尾部批次提交完成: batches={}, bytes={}, upload_ms={}",
                batches.size(), bytes,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
    }

    private void sendPayload(ControlConnection connection, MessageType type,
                             JsonObject baseJson, byte[] payload) throws Exception {
        int count = ExitPayloadProtocol.fragmentCount(payload.length);
        String sha256 = Sha256.hex(payload);
        for (int index = 0; index < count; index++) {
            JsonObject json = baseJson.deepCopy();
            json.addProperty("fragmentIndex", index);
            json.addProperty("fragmentCount", count);
            json.addProperty("totalBytes", payload.length);
            json.addProperty("sha256", sha256);
            ControlMessage response = request(connection, ControlMessage.of(
                    type, json, ExitPayloadProtocol.fragment(payload, index)));
            boolean written = JsonUtil.getBoolean(response.json(), "written", false);
            boolean accepted = JsonUtil.getBoolean(
                    response.json(), "accepted", written);
            if (response.type() != type || !accepted
                    || (index == count - 1 && !written)) {
                throw new IOException("退出事务分片应答失败: " + type
                        + " index=" + index);
            }
        }
    }

    private static ControlMessage request(ControlConnection connection,
                                          ControlMessage message) throws Exception {
        return connection.request(message).get(
                Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    private static void requireBoolean(ControlMessage response, MessageType type,
                                       String field) throws IOException {
        if (response.type() != type
                || !JsonUtil.getBoolean(response.json(), field, false)) {
            throw new IOException("退出事务应答失败: " + type + "/" + field);
        }
    }

    private static JsonObject idJson(UUID shutdownId) {
        JsonObject json = new JsonObject();
        json.addProperty("shutdownId", shutdownId.toString());
        return json;
    }
}
