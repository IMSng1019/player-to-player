package imsng.player_to_player.server;

import imsng.player_to_player.group.PlayerStateNbt;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/** 服务端玩家 NBT 的统一校验与原子写入实现。 */
public final class PlayerDataFileStore {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/player-data");

    /** 固定条带锁避免每玩家锁对象永久增长；同一 UUID 始终落到同一条带。 */
    private static final Object[] WRITE_LOCKS = new Object[256];

    static {
        java.util.Arrays.setAll(WRITE_LOCKS, ignored -> new Object());
    }

    private PlayerDataFileStore() {
    }

    /**
     * 验证 gzip 玩家 NBT 后写入 {@code <playerId>.dat}。任何失败都保持现有文件不变。
     */
    public static boolean write(Path dataDir, UUID playerId, byte[] gzipNbt) {
        return writeIfAllowed(dataDir, playerId, gzipNbt, () -> true);
    }

    /**
     * 按玩家串行执行“退出屏障检查 + 原子耐久替换”。检查与写入在同一锁内，
     * 保证退出最终写入之后排队的旧实时任务会重新看到屏障并被拒绝。
     */
    public static boolean writeIfAllowed(Path dataDir, UUID playerId, byte[] gzipNbt,
                                         BooleanSupplier allowed) {
        if (dataDir == null || playerId == null || gzipNbt == null || gzipNbt.length == 0) {
            return false;
        }
        Object lock = WRITE_LOCKS[(playerId.hashCode() & Integer.MAX_VALUE)
                % WRITE_LOCKS.length];
        synchronized (lock) {
            if (allowed == null || !allowed.getAsBoolean()) {
                return false;
            }
            return writeLocked(dataDir, playerId, gzipNbt);
        }
    }

    private static boolean writeLocked(Path dataDir, UUID playerId, byte[] gzipNbt) {
        Path tmp = dataDir.resolve(playerId + ".dat.p2p-tmp");
        Path target = dataDir.resolve(playerId + ".dat");
        try {
            CompoundTag validated = PlayerStateNbt.decodeValidated(gzipNbt);
            Files.createDirectories(dataDir);
            Files.deleteIfExists(tmp);
            NbtIo.writeCompressed(validated, tmp);
            force(tmp);
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            force(target);
            return true;
        } catch (Exception e) {
            LOGGER.warn("玩家数据校验或写回失败: {}", playerId, e);
            return false;
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (Exception e) {
                LOGGER.debug("玩家数据临时文件清理失败: {}", tmp, e);
            }
        }
    }

    private static void force(Path file) throws java.io.IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }
}
