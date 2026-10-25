package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;

import java.util.Set;
import java.util.UUID;

/**
 * 模组 level.dat 扩展字段的显式适配合同。未注册字段只保存和传输，绝不隐式授权任意组写入。
 * 所有方法都在所属服务器线程执行；实现不得捕获另一个世界的对象，也不得把磁盘旧值当作内存状态。
 */
public interface MetadataAdapter {
    enum Ownership {
        /** 必须在打开存档前应用；运行期变更应关闭计算实例并重新启动。 */
        STARTUP,
        /** 只能由协调端排序并提交，包括自然计时及全局命令的结果。 */
        COORDINATOR,
        /** 只有持有对应模拟资源所有权及当前任期的主客户端可以提出变更。 */
        SIMULATION_GROUP,
        /** 仅随快照保留的记录字段，不因自动保存产生实时补丁。 */
        RECORD,
        /** 每组自己的模拟时钟，不被全局昼夜修正覆盖。 */
        LOCAL_CLOCK,
        /** 独立玩家状态通道负责的字段。 */
        PLAYER,
        /** 无适配器的字段仅作无损保存；不宣称支持热应用。 */
        PRESERVED
    }

    /** 此适配器独占负责的 Data 顶层键；不得覆盖原版键或其他已注册适配器。 */
    Set<String> fields();

    Ownership ownership();

    /**
     * 模拟字段提交授权。适配器必须根据权威元数据中自己的资源归属声明校验当前组。
     * 默认拒绝；声明 SIMULATION_GROUP 本身不等于给所有主客户端授予覆盖权限。
     */
    default boolean canWrite(UUID groupId, CompoundTag authoritativeData) { return false; }

    /** 从真实运行对象采集；返回值只可含 fields() 声明的键。 */
    CompoundTag capture(MinecraftServer server);

    /** 打开存档前处理必要的派生数据；默认采用字段本身的 NBT。 */
    default void applyStartup(CompoundTag localData, CompoundTag authoritativeData) { }

    /** 完整权威镜像应用；缺少此前存在的声明字段意味着删除，应撤销旧运行对象。 */
    void applyRuntime(MinecraftServer server, CompoundTag authoritativeData);
}
