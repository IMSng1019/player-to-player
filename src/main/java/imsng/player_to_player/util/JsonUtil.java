package imsng.player_to_player.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JSON 工具：统一持有 GSON 实例（MC 自带 GSON，不引入新依赖），
 * 并提供带原子写的文件读写（配置/注册表落盘时避免半写状态损坏文件）。
 */
public final class JsonUtil {

    /** 通用 GSON：紧凑输出，用于网络消息。 */
    public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    /** 美化 GSON：带缩进，用于落盘的配置/清单文件（方便人工查看编辑）。 */
    public static final Gson PRETTY = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

    private JsonUtil() {
    }

    /** 解析字符串为 JsonObject（非对象或语法错误抛 IllegalArgumentException）。 */
    public static JsonObject parseObject(String json) {
        JsonElement element = JsonParser.parseString(json);
        if (!element.isJsonObject()) {
            throw new IllegalArgumentException("期望 JSON 对象，实际: " + json);
        }
        return element.getAsJsonObject();
    }

    /** 从文件读取并反序列化为指定类型；文件不存在返回 null。 */
    public static <T> T readFile(Path path, Class<T> type) throws IOException {
        if (!Files.isRegularFile(path)) {
            return null;
        }
        String content = Files.readString(path, StandardCharsets.UTF_8);
        return GSON.fromJson(content, type);
    }

    /**
     * 原子写文件：先写临时文件再 move 替换，避免进程中途被杀导致配置损坏。
     * Windows 上 ATOMIC_MOVE 可能不受支持，降级为 REPLACE_EXISTING。
     */
    public static void writeFileAtomic(Path path, Object value) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, PRETTY.toJson(value), StandardCharsets.UTF_8);
        try {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // 部分文件系统不支持原子移动，降级为普通替换
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** 安全读取字符串字段，缺失返回默认值。 */
    public static String getString(JsonObject obj, String key, String fallback) {
        JsonElement el = obj.get(key);
        return el != null && el.isJsonPrimitive() ? el.getAsString() : fallback;
    }

    /** 安全读取整数字段，缺失或类型不符返回默认值。 */
    public static int getInt(JsonObject obj, String key, int fallback) {
        JsonElement el = obj.get(key);
        if (el == null || !el.isJsonPrimitive()) {
            return fallback;
        }
        try {
            return el.getAsInt();
        } catch (NumberFormatException | UnsupportedOperationException e) {
            // 入站 JSON 不可信：isJsonPrimitive 通过但值不是数字（如 "abc"/true）时
            // getAsInt 会抛异常，统一按缺失处理返回 fallback，防恶意帧打崩处理链
            return fallback;
        }
    }

    /** 安全读取长整数字段，缺失或类型不符返回默认值。 */
    public static long getLong(JsonObject obj, String key, long fallback) {
        JsonElement el = obj.get(key);
        if (el == null || !el.isJsonPrimitive()) {
            return fallback;
        }
        try {
            return el.getAsLong();
        } catch (NumberFormatException | UnsupportedOperationException e) {
            // 同 getInt：入站 JSON 不可信，非数字原始值按缺失处理
            return fallback;
        }
    }

    /** 安全读取布尔字段，缺失或类型不符返回默认值。 */
    public static boolean getBoolean(JsonObject obj, String key, boolean fallback) {
        JsonElement el = obj.get(key);
        if (el == null || !el.isJsonPrimitive()) {
            return fallback;
        }
        try {
            return el.getAsBoolean();
        } catch (NumberFormatException | UnsupportedOperationException e) {
            // 同 getInt：入站 JSON 不可信，异常按缺失处理
            return fallback;
        }
    }
}
