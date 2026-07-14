package imsng.player_to_player.env;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * 环境同步 —— 服务端侧消息处理器（Phase 1 仅挂接在服务端；规范允许中转服务端也
 * 承担环境分发 —— "同时可以给主客户端和副客户端分发模组文件以及配置文件（同步环境文件）"，
 * 该能力划入 Phase 2，届时复用本类挂到中转端的控制端口上，见 ProxyServerService）。
 * <p>
 * 规范出处：服务端"负责给主客户端和副客户端分发模组文件以及配置文件使其环境相同"。
 * 处理三类请求：
 * <ul>
 *   <li>{@link MessageType#ENV_MANIFEST_REQUEST}：按请求方目标端过滤 mods/ 下
 *       不适用前缀的条目后回 {@link MessageType#ENV_MANIFEST}；</li>
 *   <li>{@link MessageType#ENV_FILE_REQUEST}：分块下发文件内容
 *       （块大小 {@link GlobalConfig#envFileChunkBytes}）；</li>
 *   <li>{@link MessageType#MOD_LIST_REQUEST}：mods/ 目录清单 + 前缀解析结果。</li>
 * </ul>
 * <b>一致性与安全</b>：清单响应携带 {@code snapshotId}，后续每个文件请求必须指定
 * 同一 ID；文件内容只从该不可变快照的内容寻址 Blob 读取。路径仍以快照清单为白名单，
 * 请求方无法借相对路径访问源目录中的任意文件。
 * <p>
 * <b>线程</b>：handle 运行在 Netty 事件循环，磁盘读一律转 {@link ThreadPools#io()}。
 */
public final class EnvSyncServerHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env");

    /** mods 目录在环境清单中的相对路径前缀（前缀过滤只作用于该目录下的条目）。 */
    private static final String MODS_PREFIX = "mods/";

    private EnvSyncServerHandlers() {
    }

    /**
     * 注册全部环境同步处理器。
     *
     * @param reg        消息路由注册表（ControlServer）
     * @param snapshots 不可变环境快照管理器
     * @param config    总配置（分块大小等）
     */
    public static void register(HandlerRegistry reg, EnvironmentSnapshotManager snapshots,
                                GlobalConfig config) {
        reg.on(MessageType.ENV_MANIFEST_REQUEST, (conn, msg) ->
                handleManifestRequest(conn, msg, snapshots));
        reg.on(MessageType.ENV_FILE_REQUEST, (conn, msg) ->
                handleFileRequest(conn, msg, snapshots, config));
        reg.on(MessageType.MOD_LIST_REQUEST, (conn, msg) ->
                handleModListRequest(conn, msg, snapshots));
    }

    // ------------------------------------------------------------ ENV_MANIFEST

    /**
     * ENV_MANIFEST_REQUEST（json: target）→ ENV_MANIFEST。
     * <p>
     * 应答字段：
     * <ul>
     *   <li>{@code globalHash} —— 未过滤的原始全局哈希（服务端启动时算出的那一个，
     *       与 HELLO_ACK 中的 envHash 同源，客户端可先粗比对）；</li>
     *   <li>{@code filteredHash} —— 按 target 过滤 mods/ 后清单视图的全局哈希
     *       （客户端同步完成后本地环境应与它一致）；</li>
     *   <li>{@code manifest} —— 过滤后的清单 JSON。</li>
     * </ul>
     */
    private static void handleManifestRequest(ControlConnection conn, ControlMessage msg,
                                              EnvironmentSnapshotManager snapshots) {
        EnvironmentSnapshot snapshot = snapshots.current();
        if (snapshot == null) {
            conn.send(error(msg, "env_not_ready", "环境清单尚未就绪（扫描中或扫描失败）"));
            return;
        }
        EnvironmentManifest full = snapshot.manifest();
        String targetName = JsonUtil.getString(msg.json(), "target", "");
        ModPrefixResolver.Target target = parseTarget(targetName);
        // 过滤要重建 TreeMap 并重算全局哈希，大清单下非零成本，转 io() 池执行
        ThreadPools.io().execute(() -> {
            try {
                EnvironmentManifest filtered = filterForTarget(full, target);
                JsonObject out = new JsonObject();
                out.addProperty("snapshotId", snapshot.snapshotId());
                out.addProperty("globalHash", full.globalHash());
                out.addProperty("filteredHash", filtered.globalHash());
                out.add("manifest", filtered.toJson());
                conn.send(msg.reply(MessageType.ENV_MANIFEST, out, null));
            } catch (Exception e) {
                LOGGER.warn("处理 ENV_MANIFEST_REQUEST 失败", e);
                conn.send(error(msg, "manifest_failed", e.toString()));
            }
        });
    }

    /**
     * 生成按目标端过滤后的清单视图：mods/ 下前缀不适用于 target 的条目剔除，
     * 其余（配置、数据包等非 mod 文件）原样保留。target 缺失/未知时不过滤
     * （入站数据不可信 —— 容忍未知枚举，宁可多发不可漏发）。
     */
    private static EnvironmentManifest filterForTarget(EnvironmentManifest full,
                                                       ModPrefixResolver.Target target) {
        if (target == null) {
            return full;
        }
        Map<String, EnvironmentManifest.Entry> kept = new TreeMap<>();
        for (Map.Entry<String, EnvironmentManifest.Entry> e : full.files().entrySet()) {
            String path = e.getKey();
            if (path.startsWith(MODS_PREFIX)) {
                // 前缀只解析纯文件名（mods/ 下可能还有子目录，取最后一段）
                String fileName = path.substring(path.lastIndexOf('/') + 1);
                if (!ModPrefixResolver.appliesTo(fileName, target)) {
                    continue;
                }
            }
            kept.put(path, e.getValue());
        }
        return new EnvironmentManifest(kept);
    }

    // ------------------------------------------------------------ ENV_FILE

    /**
     * ENV_FILE_REQUEST（json: path, offset）→ ENV_FILE_DATA
     * （json: path, offset, total, last；binary: 文件内容块）。
     * <p>
     * 按 offset 读一块 {@link GlobalConfig#envFileChunkBytes}，客户端循环请求直到
     * last=true。无状态设计（每请求带 offset）：服务端不用维护每连接的传输会话，
     * 客户端断线重连后可从任意 offset 续传。
     */
    private static void handleFileRequest(ControlConnection conn, ControlMessage msg,
                                          EnvironmentSnapshotManager snapshots,
                                          GlobalConfig config) {
        String snapshotId = JsonUtil.getString(msg.json(), "snapshotId", "");
        String path = JsonUtil.getString(msg.json(), "path", "");
        long offset = JsonUtil.getLong(msg.json(), "offset", -1L);

        if (snapshotId.isBlank()) {
            conn.send(error(msg, "invalid_snapshot", "缺少 snapshotId"));
            return;
        }
        EnvironmentSnapshot snapshot = snapshots.find(snapshotId);
        if (snapshot == null) {
            conn.send(error(msg, "snapshot_not_found", "环境快照不存在或已过期: " + snapshotId));
            return;
        }
        EnvironmentManifest.Entry entry;
        try {
            entry = snapshots.entry(snapshot, path);
        } catch (IOException e) {
            conn.send(error(msg, "not_in_manifest", "路径不在环境清单中: " + path));
            return;
        }
        if (offset < 0 || offset > entry.size()) {
            conn.send(error(msg, "invalid_offset", "offset 非法: " + offset));
            return;
        }

        // 磁盘读转 io() 池，绝不占用 Netty 事件循环
        ThreadPools.io().execute(() -> {
            try {
                int chunkBytes = Math.max(1, config.envFileChunkBytes);
                byte[] chunk = snapshots.readChunk(snapshot, path, offset, chunkBytes);
                boolean last = offset + chunk.length >= entry.size();

                JsonObject out = new JsonObject();
                out.addProperty("snapshotId", snapshot.snapshotId());
                out.addProperty("path", path);
                out.addProperty("offset", offset);
                out.addProperty("total", entry.size());
                out.addProperty("last", last);
                conn.send(msg.reply(MessageType.ENV_FILE_DATA, out, chunk));
            } catch (IOException e) {
                LOGGER.warn("环境快照文件读取失败: {}@{}", snapshotId, path, e);
                snapshots.requestRefresh();
                conn.send(error(msg, "read_failed", e.toString()));
            }
        });
    }

    // ------------------------------------------------------------ MOD_LIST

    /**
     * MOD_LIST_REQUEST（json: target）→ MOD_LIST
     * （json: mods:[{file,sha256,size,targets:[..]}]）。
     * <p>
     * 列出清单中 mods/ 目录下适用于 target 的全部 mod 文件及其前缀解析结果；
     * target 缺失/未知时列出全部（与清单过滤同样的容忍策略）。
     */
    private static void handleModListRequest(ControlConnection conn, ControlMessage msg,
                                             EnvironmentSnapshotManager snapshots) {
        EnvironmentSnapshot snapshot = snapshots.current();
        if (snapshot == null) {
            conn.send(error(msg, "env_not_ready", "环境清单尚未就绪（扫描中或扫描失败）"));
            return;
        }
        EnvironmentManifest current = snapshot.manifest();
        String targetName = JsonUtil.getString(msg.json(), "target", "");
        ModPrefixResolver.Target target = parseTarget(targetName);
        ThreadPools.io().execute(() -> {
            try {
                JsonArray mods = new JsonArray();
                for (Map.Entry<String, EnvironmentManifest.Entry> e
                        : current.files().entrySet()) {
                    String path = e.getKey();
                    if (!path.startsWith(MODS_PREFIX)) {
                        continue;
                    }
                    String fileName = path.substring(path.lastIndexOf('/') + 1);
                    if (target != null && !ModPrefixResolver.appliesTo(fileName, target)) {
                        continue;
                    }
                    JsonObject mod = new JsonObject();
                    mod.addProperty("file", fileName);
                    mod.addProperty("sha256", e.getValue().sha256());
                    mod.addProperty("size", e.getValue().size());
                    JsonArray targets = new JsonArray();
                    for (ModPrefixResolver.Target t : ModPrefixResolver.targetsOf(fileName)) {
                        targets.add(t.name());
                    }
                    mod.add("targets", targets);
                    mods.add(mod);
                }
                JsonObject out = new JsonObject();
                out.add("mods", mods);
                conn.send(msg.reply(MessageType.MOD_LIST, out, null));
            } catch (Exception e) {
                LOGGER.warn("处理 MOD_LIST_REQUEST 失败", e);
                conn.send(error(msg, "mod_list_failed", e.toString()));
            }
        });
    }

    // ------------------------------------------------------------ 工具

    /** 解析目标端枚举名；未知/缺失返回 null（入站不可信数据，容忍未知枚举）。 */
    private static ModPrefixResolver.Target parseTarget(String name) {
        if (name == null || name.isBlank()) {
            return null;
        }
        try {
            return ModPrefixResolver.Target.valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            LOGGER.debug("未知目标端枚举，按不过滤处理: {}", name);
            return null;
        }
    }

    /** 构造保留 _rid 的 ERROR 应答（json: code, message），使请求方 future 能收到失败。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
