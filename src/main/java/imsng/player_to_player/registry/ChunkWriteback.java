package imsng.player_to_player.registry;

import java.util.UUID;

/**
 * 区块最终数据写回接口（Phase 2）。
 * <p>
 * {@link RegistryHandlers} 处理 CHUNK_RELEASE 时，若帧携带最终区块数据
 * （规范"玩家卸载区块……将最后的区块数据由主客户端发给服务端"），先经本接口
 * 写回服务端存档再释放占用。实现在 {@code server.ChunkDataHandlers}
 * （需要 MinecraftServer，注册表包不反向依赖 server 包，故以接口解耦）。
 */
@FunctionalInterface
public interface ChunkWriteback {

    record ReleaseResult(boolean finalDataWritten, boolean released) {
    }

    /**
     * 在同一区块写锁内参与释放的幂等事务。
     * {@link #replay()} 在任何 NBT 写入前查询既有耐久回执；只有首次操作才调用
     * {@link #commit(boolean)} 建立回执并删除 ownership。
     */
    interface ReleaseTransaction {

        /** @return 已完成操作的原始结果；首次执行返回 null */
        ReleaseResult replay();

        /** 最终数据处理完成后原子提交释放；失败返回 released=false。 */
        ReleaseResult commit(boolean finalDataWritten);
    }

    /**
     * 在该区块与实时上行共用的锁内，校验并耐久写回最终数据（WAL force 或兼容
     * IOWorker flush），然后调用 release 删除 ownership。实现必须保证回调返回前
     * 不释放线性化锁，防迟到实时上行插入“最终耐久写入”和“所有权删除”之间。
     *
     * @param key      区块键
     * @param uploader 上传方 clientId（权威校验：必须等于注册表登记的占用组）
     * @param gzipNbt  压缩的区块 NBT；null/空表示干净区块只释放 ownership
     * @param transaction 同区块锁内先查回执、首次完成时再提交的释放事务
     * @return 最终数据与 ownership 各自是否已确认
     */
    ReleaseResult finalizeAndRelease(ChunkKey key, UUID uploader, byte[] gzipNbt,
                                     ReleaseTransaction transaction);
}
