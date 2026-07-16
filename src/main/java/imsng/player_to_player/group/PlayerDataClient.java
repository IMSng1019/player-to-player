package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.nbt.CompoundTag;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** 客户端通过既有控制协议取得并验证物理服务器权威玩家状态。 */
public final class PlayerDataClient {

    private PlayerDataClient() {
    }

    /**
     * 请求指定玩家的权威 NBT。调用方必须位于 IO 线程；任何失败都抛出异常，
     * 由世界切换层取消角色申请，不允许静默回退本地旧状态。
     */
    public static CompoundTag requestAuthoritative(ControlConnection conn, UUID playerId)
            throws IOException, InterruptedException {
        if (conn == null || playerId == null) {
            throw new IOException("玩家数据请求参数不完整");
        }

        JsonObject requestJson = new JsonObject();
        requestJson.addProperty("playerUuid", playerId.toString());
        ControlMessage response;
        try {
            response = conn.request(ControlMessage.of(
                            MessageType.PLAYER_DATA_REQUEST, requestJson))
                    .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException e) {
            throw new IOException("物理服务器玩家数据请求失败", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }

        if (response.type() != MessageType.PLAYER_DATA) {
            throw new IOException("物理服务器拒绝玩家数据请求: " + response.type());
        }
        if (!JsonUtil.getBoolean(response.json(), "exists", false)) {
            throw new IOException("物理服务器没有该玩家的权威数据");
        }
        if (response.binary().length == 0) {
            throw new IOException("物理服务器返回的玩家数据为空");
        }
        return PlayerStateNbt.decodeValidated(response.binary());
    }
}
