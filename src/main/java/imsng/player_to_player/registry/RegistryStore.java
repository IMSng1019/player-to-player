package imsng.player_to_player.registry;

import java.util.Map;
import java.util.UUID;

/**
 * 区块注册表持久化后端（Phase 4；规范"每一个被加载区块会在服务端有一个区块列表
 * <b>亦或者一个 MySQL 数据库</b>"）。
 * <p>
 * {@link ChunkRegistry} 的内存表是权威状态，后端只负责"重启不丢占用"的持久化：
 * <ul>
 *   <li>{@link JsonRegistryStore}：本地 JSON 文件（默认，零依赖）；</li>
 *   <li>{@link MysqlRegistryStore}：MySQL（{@code GlobalConfig.mysql.enabled} 开启时，
 *       初始化失败自动回退 JSON）。</li>
 * </ul>
 * 线程模型：load 在服务端启动线程调用一次；save 在 scheduler/io 线程调用
 * （ChunkRegistry 已保证与 claim/release 互斥地取快照，后端只见不可变快照）。
 */
public interface RegistryStore {

    /**
     * 恢复上次运行的占用状态（服务端启动时调用一次）。
     * 单条坏数据应跳过不中断（宁可丢一条记录也不能让服务端起不来）。
     *
     * @return 区块 → 占用组；无持久化数据时返回空表
     */
    Map<ChunkKey, UUID> load();

    /**
     * 全量落盘当前占用快照（定期 + 关闭时调用）。
     * 实现必须整体覆盖旧状态 —— 已释放的占用如果残留，重启会"复活"。
     */
    void save(Map<ChunkKey, UUID> snapshot);

    /** 后端的人类可读描述（启动日志用）。 */
    String describe();

    /** 释放后端资源（服务停止时调用；幂等）。 */
    default void close() {
    }
}
