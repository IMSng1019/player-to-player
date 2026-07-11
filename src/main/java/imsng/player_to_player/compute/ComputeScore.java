package imsng.player_to_player.compute;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;

/**
 * 一台客户端机器的算力评分（算力表中的一行）。
 * <p>
 * 规范：算力取决于 CPU 的单核能力（Geekbench 非官方 API 查询，失败降级本地跑分）；
 * 成为主客户端还需满足剩余内存 ≥ 服务端配置阈值（默认 0.5 GB）。
 *
 * @param cpuModel        CPU 型号字符串（如 "AMD Ryzen 7 5800X"）
 * @param singleCoreScore 单核算力分（Geekbench 6 量级）
 * @param source          评分来源："geekbench"（API 查询）或 "local"（本地微基准换算）
 * @param freeMemoryBytes 测量时的系统可用物理内存（字节）
 * @param totalMemoryBytes 系统总物理内存（字节）
 */
public record ComputeScore(
        String cpuModel,
        long singleCoreScore,
        String source,
        long freeMemoryBytes,
        long totalMemoryBytes) {

    public static final String SOURCE_GEEKBENCH = "geekbench";
    public static final String SOURCE_LOCAL = "local";

    /** 是否满足主客户端的内存门槛。 */
    public boolean meetsMemoryRequirement(long minFreeMemoryBytes) {
        return freeMemoryBytes >= minFreeMemoryBytes;
    }

    /** 序列化为 JSON（COMPUTE_REPORT 消息体）。 */
    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("cpuModel", cpuModel);
        obj.addProperty("singleCoreScore", singleCoreScore);
        obj.addProperty("source", source);
        obj.addProperty("freeMemoryBytes", freeMemoryBytes);
        obj.addProperty("totalMemoryBytes", totalMemoryBytes);
        return obj;
    }

    /** 从 JSON 反序列化（字段缺失取安全默认值，容忍旧版本客户端）。 */
    public static ComputeScore fromJson(JsonObject obj) {
        return new ComputeScore(
                JsonUtil.getString(obj, "cpuModel", "unknown"),
                JsonUtil.getLong(obj, "singleCoreScore", 0L),
                JsonUtil.getString(obj, "source", SOURCE_LOCAL),
                JsonUtil.getLong(obj, "freeMemoryBytes", 0L),
                JsonUtil.getLong(obj, "totalMemoryBytes", 0L));
    }
}
