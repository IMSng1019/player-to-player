package imsng.player_to_player.env.restart;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** 仅在父子进程匿名管道上传递的有界二进制消息；没有临时参数文件或 shell 转义。 */
final class RestartWire {
    static final int READY = 0x50325001;
    static final int COMMIT = 0x50325002;
    static final int COMMITTED = 0x50325003;
    private static final int MAX_BYTES = 4 * 1024 * 1024;

    record Request(Path dataRoot, UUID taskId, List<String> command) {
        Request { command = List.copyOf(command); }
        @Override public String toString() { return "RestartRequest[arguments redacted]"; }
    }
    private RestartWire() { }

    static void write(DataOutputStream out, Request request) throws IOException {
        out.writeInt(READY);
        List<String> values = new ArrayList<>();
        values.add(request.dataRoot().toString());
        values.add(request.taskId().toString());
        values.addAll(request.command());
        if (values.size() < 3 || values.size() > 8192) throw new IOException("启动参数数量无效");
        out.writeInt(values.size());
        int total = 0;
        for (String value : values) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            total += bytes.length;
            if (total > MAX_BYTES) throw new IOException("启动参数过大");
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        out.flush();
    }

    static Request read(DataInputStream in) throws IOException {
        if (in.readInt() != READY) throw new IOException("重启管道版本不匹配");
        int count = in.readInt();
        if (count < 3 || count > 8192) throw new IOException("启动参数数量无效");
        List<String> values = new ArrayList<>();
        int remaining = MAX_BYTES;
        for (int i = 0; i < count; i++) {
            int length = in.readInt();
            if (length < 0 || length > remaining) throw new IOException("启动参数长度无效");
            remaining -= length;
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            values.add(new String(bytes, StandardCharsets.UTF_8));
        }
        return new Request(Path.of(values.get(0)), UUID.fromString(values.get(1)), values.subList(2, values.size()));
    }
}
