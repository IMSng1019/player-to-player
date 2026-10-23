package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.ExitBundle;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** 子进程停止后的退出尾部先保存到本机独占文件，再通知父进程；子 JVM 退出不会丢失尾部。 */
final class WorkerExitBundleStore {
    private static final int MAX_PAYLOAD = 64 * 1024 * 1024;
    private WorkerExitBundleStore() { }
    static void write(Path file, ExitBundle bundle) throws IOException {
        Files.createDirectories(file.getParent());
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(temp)))) {
            out.writeInt(1);
            byte[] manifest = bundle.manifest().toJson().toString().getBytes(StandardCharsets.UTF_8);
            bytes(out, manifest);
            out.writeInt(bundle.playerIds().size());
            for (UUID player : bundle.playerIds()) { out.writeUTF(player.toString()); bytes(out, bundle.playerData(player)); }
            out.writeInt(bundle.writtenChunks().size());
            for (ChunkKey chunk : bundle.writtenChunks()) { out.writeUTF(chunk.asString()); bytes(out, bundle.chunkData(chunk)); }
        }
        try (var channel = java.nio.channels.FileChannel.open(temp, StandardOpenOption.WRITE)) { channel.force(true); }
        Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
    static ExitBundle read(Path file) throws IOException {
        try (var in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
            if (in.readInt() != 1) throw new IOException("未知退出尾部格式");
            ExitManifest manifest = ExitManifest.fromJson(JsonUtil.parseObject(new String(bytes(in), StandardCharsets.UTF_8)));
            Map<UUID, byte[]> players = new HashMap<>();
            int count = boundedCount(in.readInt());
            for (int i = 0; i < count; i++) players.put(UUID.fromString(in.readUTF()), bytes(in));
            Map<ChunkKey, byte[]> chunks = new HashMap<>();
            count = boundedCount(in.readInt());
            for (int i = 0; i < count; i++) chunks.put(ChunkKey.parse(in.readUTF()), bytes(in));
            if (in.read() != -1) throw new IOException("退出尾部含多余数据");
            return new ExitBundle(manifest, players, chunks);
        }
    }
    private static int boundedCount(int count) throws IOException {
        if (count < 0 || count > 1_000_000) throw new IOException("退出尾部记录数非法"); return count;
    }
    private static void bytes(DataOutputStream out, byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
    private static byte[] bytes(DataInputStream in) throws IOException {
        int size = in.readInt(); if (size < 0 || size > MAX_PAYLOAD) throw new IOException("退出尾部长度非法");
        byte[] bytes = in.readNBytes(size); if (bytes.length != size) throw new EOFException(); return bytes;
    }
}
