package imsng.player_to_player.world;

import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * 服务端侧的世界身份标记（落盘在 {@code player-to-player/world-identity.json}）。
 * <p>
 * 服务端的运行期状态目录（{@code chunk-journal/}、{@code registry/}、
 * {@code exit-transactions/}、{@code exit-payloads/}）<b>不随世界名分割</b>，
 * 换世界后旧 WAL 仍会被 {@code RegionFileProbe.hasServerData} 当作"服务端已有数据"，
 * 于是旧世界的区块会被下发给新世界。启动时用本标记识别"世界已被替换"，
 * 把上述状态整体隔离（见 {@link imsng.player_to_player.util.Quarantine}）。
 * <p>
 * 字段刻意保持简单（GSON 直接反序列化）：{@code formatVersion} 与 {@code identityHash}
 * 是判定依据，其余仅用于诊断。
 */
public final class WorldIdentityMarker {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/world-identity");

    /** 身份口径版本（取自 {@link WorldIdentity#FORMAT_VERSION}）。 */
    public String formatVersion = "";

    /** 标记写入时的世界身份哈希；空串 = 当时无法确定身份。 */
    public String identityHash = "";

    /** 当时的世界名（诊断用；世界名可变，不作为判定依据）。 */
    public String worldName = "";

    /** 标记写入时间（毫秒）。 */
    public long recordedAtEpochMillis = 0L;

    /** 读取标记；文件不存在或损坏返回 {@code null}（按"无标记"处理）。 */
    public static WorldIdentityMarker read(Path path) {
        try {
            return JsonUtil.readFile(path, WorldIdentityMarker.class);
        } catch (Exception e) {
            LOGGER.warn("世界身份标记读取失败（按缺失处理）: {}", path, e);
            return null;
        }
    }

    /** 原子落盘。 */
    public void save(Path path) throws IOException {
        JsonUtil.writeFileAtomic(path, this);
    }
}
