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

    /**
     * 校验并把最终区块数据写回存档，<b>并阻塞至落盘</b>（保证释放后其他组
     * 的 hasServerData 探测能看到该数据）。
     *
     * @param key      区块键
     * @param uploader 上传方 clientId（权威校验：必须等于注册表登记的占用组）
     * @param gzipNbt  压缩的区块 NBT（NbtIo.writeCompressed 产物）
     * @return 是否成功写回（失败只记日志，不阻断释放流程）
     */
    boolean writeFinalAndFlush(ChunkKey key, UUID uploader, byte[] gzipNbt);
}
