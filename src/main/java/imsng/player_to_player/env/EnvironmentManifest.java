package imsng.player_to_player.env;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 环境文件清单：相对路径（正斜杠分隔）→ SHA-256 + 文件大小。
 * <p>
 * 规范出处（player_to_player-prompt.txt）："服务端向客户端检查玩家环境的哈希值
 * （哈希值使用SHA256算法）校验本地环境是否与服务端的环境的哈希值相同
 * （服务端的哈希值在服务端启动时计算）如果不同则服务端向客户端发送新的环境文件更新环境"。
 * 设计出处：DESIGN.md 第 4 节 —— 清单规范化序列后再取 SHA-256 作为全局环境哈希，
 * 客户端先比对全局哈希，不同再逐文件 diff 只下载差异。
 * <p>
 * 本类不可变：构造时把条目拷入 {@link TreeMap}（按路径字典序），因此
 * {@link #globalHash()} 的拼接顺序确定，同一批文件必然得到同一个全局哈希 ——
 * 服务端启动时计算一次即可反复使用。
 */
public final class EnvironmentManifest {

    /**
     * 清单条目：单个环境文件的 SHA-256（小写十六进制）与字节大小。
     * 大小随清单下发，客户端同步前即可统计需下载的总字节数（进度日志用）。
     */
    public record Entry(String sha256, long size) {
    }

    /** 路径 → 条目，TreeMap 拷贝后包一层不可变视图（见类 Javadoc 的确定性要求）。 */
    private final Map<String, Entry> files;

    /** 全局环境哈希：构造时即计算（清单不可变，算一次缓存终身有效）。 */
    private final String globalHash;

    /**
     * @param files 相对路径（正斜杠分隔）→ 条目；本类会拷贝一份并按路径排序，
     *              调用方之后对原 map 的修改不影响本清单
     */
    public EnvironmentManifest(Map<String, Entry> files) {
        TreeMap<String, Entry> sorted = new TreeMap<>(files);
        this.files = Collections.unmodifiableMap(sorted);
        this.globalHash = computeGlobalHash(sorted);
    }

    /** 不可变有序（路径字典序）的 路径 → 条目 映射。 */
    public Map<String, Entry> files() {
        return files;
    }

    /**
     * 全局环境哈希：对每条 {@code 路径 NUL sha256 NUL size 换行} 按路径序
     * 拼接后整体取 SHA-256。NUL（{@code \0}）作字段分隔、换行作条目分隔，
     * 保证不同字段组合不可能拼出相同序列（路径/哈希中不会出现 NUL），
     * 且顺序确定 → 哈希确定（服务端启动时算一次即可）。
     */
    public String globalHash() {
        return globalHash;
    }

    private static String computeGlobalHash(TreeMap<String, Entry> sorted) {
        StringBuilder sb = new StringBuilder(sorted.size() * 112);
        for (Map.Entry<String, Entry> e : sorted.entrySet()) {
            sb.append(e.getKey()).append('\0')
                    .append(e.getValue().sha256()).append('\0')
                    .append(e.getValue().size()).append('\n');
        }
        return Sha256.hex(sb.toString());
    }

    /**
     * 序列化为 JSON（ENV_MANIFEST 消息的 manifest 字段）。
     * <pre>
     * { "files": { "&lt;相对路径&gt;": { "sha256": "…", "size": 123 }, … } }
     * </pre>
     * 全局哈希不随 JSON 传输 —— {@link #fromJson} 按同一算法重算，
     * 天然防止"清单与哈希不一致"的畸形数据。
     */
    public JsonObject toJson() {
        JsonObject filesObj = new JsonObject();
        for (Map.Entry<String, Entry> e : files.entrySet()) {
            JsonObject entryObj = new JsonObject();
            entryObj.addProperty("sha256", e.getValue().sha256());
            entryObj.addProperty("size", e.getValue().size());
            filesObj.add(e.getKey(), entryObj);
        }
        JsonObject root = new JsonObject();
        root.add("files", filesObj);
        return root;
    }

    /**
     * 从 JSON 反序列化（网络入站数据按不可信处理：缺失/畸形条目静默跳过，
     * 不抛异常 —— 单条坏数据不应打断整次同步，真正的差异由 sha256 校验兜底）。
     */
    public static EnvironmentManifest fromJson(JsonObject json) {
        Map<String, Entry> result = new TreeMap<>();
        JsonElement filesEl = json != null ? json.get("files") : null;
        if (filesEl != null && filesEl.isJsonObject()) {
            for (Map.Entry<String, JsonElement> e : filesEl.getAsJsonObject().entrySet()) {
                if (!e.getValue().isJsonObject()) {
                    continue; // 条目不是对象：跳过（容忍畸形数据）
                }
                JsonObject entryObj = e.getValue().getAsJsonObject();
                String sha256 = JsonUtil.getString(entryObj, "sha256", "");
                long size = JsonUtil.getLong(entryObj, "size", -1L);
                if (e.getKey().isEmpty() || sha256.isEmpty() || size < 0) {
                    continue; // 关键字段缺失/非法：跳过
                }
                result.put(e.getKey(), new Entry(sha256, size));
            }
        }
        return new EnvironmentManifest(result);
    }

    /**
     * 与本地清单求差：返回"本清单（服务端）有、而本地缺失或哈希不同"的相对路径列表
     * （即客户端需要下载的文件），按路径字典序。
     * <p>
     * 注意方向性：只算服务端 → 本地的下载集；本地多出的文件不在返回值里
     * （规范未要求删除客户端多余文件，多余文件不影响哈希一致性判断之外的运行）。
     *
     * @param localOrNull 本地扫描出的清单；null 视为本地为空（全量下载）
     */
    public List<String> diffAgainst(EnvironmentManifest localOrNull) {
        List<String> toDownload = new ArrayList<>();
        Map<String, Entry> local = localOrNull != null ? localOrNull.files() : Map.of();
        for (Map.Entry<String, Entry> e : files.entrySet()) {
            Entry localEntry = local.get(e.getKey());
            if (localEntry == null || !localEntry.sha256().equals(e.getValue().sha256())) {
                toDownload.add(e.getKey());
            }
        }
        return toDownload;
    }

    @Override
    public String toString() {
        return "EnvironmentManifest{" + files.size() + " files, globalHash=" + globalHash + "}";
    }
}
