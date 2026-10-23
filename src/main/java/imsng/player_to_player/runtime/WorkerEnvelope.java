package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;

/** 保留业务帧与二进制附件；远端 _rid 留在嵌套对象中，绝不与本机 IPC 序号混用。 */
public final class WorkerEnvelope {
    private WorkerEnvelope() { }
    public static ControlMessage wrap(String op, ControlMessage message) {
        JsonObject json = new JsonObject();
        json.addProperty("op", op);
        json.addProperty("messageType", message.type().id());
        json.add("message", message.json().deepCopy());
        return ControlMessage.of(MessageType.WORKER_CONTROL, json, message.binary());
    }
    public static ControlMessage unwrap(ControlMessage message) {
        MessageType type = MessageType.fromId(message.json().get("messageType").getAsInt());
        if (type == null || type == MessageType.WORKER_CONTROL || type == MessageType.WORKER_EVENT)
            throw new IllegalArgumentException("非法的子进程业务帧类型");
        return ControlMessage.of(type, message.json().getAsJsonObject("message").deepCopy(), message.binary());
    }
}
