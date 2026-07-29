package imsng.player_to_player.server;

import java.util.Map;
import java.util.UUID;

/** 退出事务元数据持久化接口，便于用内存实现测试幂等状态机。 */
interface ExitTransactionPersistence {

    Map<UUID, ExitTransactionRecord> load();

    void save(ExitTransactionRecord record);
}

