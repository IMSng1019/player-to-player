package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WorkerEnvelopeTest {
    @Test void keepsNestedRequestIdSeparateFromIpcCorrelationAndPreservesBinary() throws Exception {
        Class<?> type = assertDoesNotThrow(() -> Class.forName(
                "imsng.player_to_player.runtime.WorkerEnvelope"), "必须隔离父子IPC与远端请求序号");
        JsonObject json = new JsonObject();
        json.addProperty("_rid", 73);
        json.addProperty("dimension", "minecraft:overworld");
        ControlMessage source = ControlMessage.of(MessageType.CHUNK_DATA, json, new byte[]{1,2,3});
        ControlMessage envelope = (ControlMessage) type.getMethod("wrap", String.class, ControlMessage.class)
                .invoke(null, "remote_push", source);
        envelope.json().addProperty("_rid", 1001);
        ControlMessage decoded = (ControlMessage) type.getMethod("unwrap", ControlMessage.class)
                .invoke(null, envelope);
        assertEquals(73, decoded.rid());
        assertEquals(MessageType.CHUNK_DATA, decoded.type());
        assertArrayEquals(source.binary(), decoded.binary());
        assertEquals(73, source.rid());
    }
}
