package imsng.player_to_player.registry;

import imsng.player_to_player.config.GlobalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 区块注册表的 MySQL 后端（Phase 4；规范"区块列表<b>亦或者一个 MySQL 数据库</b>"）。
 * <p>
 * <b>依赖策略</b>（DESIGN.md 第 10 节"不引入第三方依赖"）：本类只编码到 JDK 自带的
 * {@code java.sql}，<b>不捆绑</b> MySQL 驱动 —— JDBC 4 的 {@link DriverManager} 会经
 * ServiceLoader 自动发现类路径上的驱动。管理员启用本后端时需自行把
 * {@code mysql-connector-j} 加入服务端 JVM 类路径（例如
 * {@code java -cp fabric-server-launch.jar:mysql-connector-j.jar net.fabricmc.loader.impl.launch.knot.KnotServer}）。
 * 驱动缺失/连接失败时 {@link #connect} 抛异常，{@code P2PServerService} 捕获后
 * 回退 {@link JsonRegistryStore}，服务端功能不受影响。
 * <p>
 * <b>持久化模型</b>：与 JSON 后端一致的"周期全量快照"（30s 脏检查 + 停机终写）——
 * 事务内 {@code DELETE 全表 + 批量 INSERT}。注册表规模 = 全服在线加载区块数
 * （数千量级），全量重写在 io 线程上毫秒级完成；逐占用增量写留待需要时优化。
 * <p>
 * 线程模型：每次 load/save 独立开连接（30s 一次的低频操作，省去连接保活/重连
 * 状态机；连接失败只记日志，内存权威状态不受影响，下个周期自动重试）。
 */
public final class MysqlRegistryStore implements RegistryStore {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/registry-mysql");

    /** 占用表：主键 (dimension, x, z) 保证同区块单占用与幂等覆盖。 */
    private static final String DDL = """
            CREATE TABLE IF NOT EXISTS p2p_chunk_claims (
              dimension  VARCHAR(128) NOT NULL,
              x          INT NOT NULL,
              z          INT NOT NULL,
              group_id   CHAR(36) NOT NULL,
              claimed_at BIGINT NOT NULL,
              PRIMARY KEY (dimension, x, z)
            )""";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private MysqlRegistryStore(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * 建立后端：探活连接 + 建表。任何失败（驱动缺失/地址错误/权限不足）都在
     * 这里抛出，让调用方在<b>启动期</b>就决定回退 JSON，而不是运行到一半才发现。
     *
     * @throws SQLException 驱动缺失（"No suitable driver"）或连接/建表失败
     */
    public static MysqlRegistryStore connect(GlobalConfig.MysqlConfig config) throws SQLException {
        String url = config.jdbcUrl == null ? "" : config.jdbcUrl.trim();
        if (url.isEmpty()) {
            throw new SQLException("mysql.jdbcUrl 未配置");
        }
        MysqlRegistryStore store = new MysqlRegistryStore(url, config.username, config.password);
        try (Connection conn = store.open(); Statement st = conn.createStatement()) {
            st.execute(DDL);
        }
        return store;
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    @Override
    public Map<ChunkKey, UUID> load() {
        Map<ChunkKey, UUID> result = new HashMap<>();
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT dimension, x, z, group_id FROM p2p_chunk_claims");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                try {
                    result.put(new ChunkKey(rs.getString(1), rs.getInt(2), rs.getInt(3)),
                            UUID.fromString(rs.getString(4)));
                } catch (Exception e) {
                    LOGGER.warn("跳过 MySQL 注册表坏记录: {}", e.toString());
                }
            }
        } catch (SQLException e) {
            LOGGER.error("MySQL 注册表读取失败（按空表启动，占用状态可能丢失）", e);
        }
        return result;
    }

    @Override
    public void save(Map<ChunkKey, UUID> snapshot) {
        try (Connection conn = open()) {
            conn.setAutoCommit(false); // 清空 + 重写必须原子，中途崩溃不能留半份快照
            try (Statement clear = conn.createStatement()) {
                clear.executeUpdate("DELETE FROM p2p_chunk_claims");
            }
            try (PreparedStatement insert = conn.prepareStatement(
                    "INSERT INTO p2p_chunk_claims (dimension, x, z, group_id, claimed_at)"
                            + " VALUES (?, ?, ?, ?, ?)")) {
                long now = System.currentTimeMillis();
                for (Map.Entry<ChunkKey, UUID> e : snapshot.entrySet()) {
                    insert.setString(1, e.getKey().dimension());
                    insert.setInt(2, e.getKey().x());
                    insert.setInt(3, e.getKey().z());
                    insert.setString(4, e.getValue().toString());
                    insert.setLong(5, now);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            // 落盘失败不致命：内存表仍是权威，下个周期重试；崩溃最多丢一个周期
            LOGGER.error("MySQL 注册表落盘失败（下个周期重试）", e);
        }
    }

    @Override
    public String describe() {
        return "MySQL (" + jdbcUrl + ")";
    }
}
