package imsng.player_to_player.metadata;

/** 元数据只是迁移复合屏障之一，不能替代实体/区块尾部和预连接确认。 */
public final class MetadataTransferBarrier {
    private MetadataTransferBarrier() { }

    public static boolean accepts(long requiredVersion, long plannedTerm, long appliedVersion,
                                  long appliedTerm, long currentTerm) {
        return plannedTerm > 0 && plannedTerm == currentTerm && appliedTerm == currentTerm
                && appliedVersion >= requiredVersion;
    }
}
