# 区块预卸载屏障与增量退出回传设计

## 背景与根因

2026-07-31 的实测中，视距 12 的单玩家退出清单分别包含 2443 和 2363 个区块，
两次退出都只能处理约 28 个区块/秒，耗时 85～87 秒。日志中的区块数是组的完整
所有权清单，不等于玩家屏幕内的完整区块数：当前加载门控位于
`ChunkMap.readChunk`，世界生成依赖的 ProtoChunk 也会取得所有权，而现有释放只依赖
Fabric 的完整区块卸载事件，外围生成区块可能长期留在清单中。

退出数据路径还把“所有权集合”“脏数据集合”和“未确认集合”混为一体：

- `freezeForExit()` 把全部 claimed 区块放入事务清单；
- 停服扫描无条件序列化全部仍加载的 LevelChunk；
- 实时上行成功后只移除发送队列，没有移除 `latestSnapshots`；
- 没有最终 NBT 的区块仍逐个发送 `preserveExisting`，服务端逐项读盘、写 marker、
  `force(true)` 并应答。

结果是已经实时确认的旧快照再次完整上传，未修改区块也产生一次网络往返和同步文件操作。

## 已批准语义

区块卸载必须采用严格的两阶段顺序：

1. 原版准备卸载时，区块进入 `PRE_UNLOAD`。
2. 上传与释放未获得物理服务端确认前，本地不允许卸载。
3. 服务端已有权威数据且本地没有修改、没有未确认版本时，只提交无数据释放。
4. 新生成区块、本地脏区块或仍有未确认版本时，提交最终压缩 NBT。
5. 服务端必须先校验并刷盘最终数据，再耐久释放所有权，最后返回 ACK。
6. 失败、超时、断线或负 ACK 均保持区块加载并无限重试。
7. 只有收到 `released=true`，且携带数据时同时收到 `finalDataWritten=true`，才允许原版继续卸载。

正常退出仍完整冻结并释放组的所有权清单，但只回传最后一次服务端确认之后产生的脏数据。

## 状态模型

每个本地已占用区块维护以下互斥/组合状态：

- `SERVER_DATA_KNOWN`：本次 claim 时服务端已有数据，或本次会话中已有一次区块写入得到 ACK。
- `DIRTY_UNACKED`：存在比服务端最近确认版本更新的本地快照。
- `PRE_UNLOAD`：原版已请求卸载，正在执行最终写回与所有权释放。
- `RELEASED`：服务端已确认释放；本地原版卸载可以继续。

`ChunkStorage.write` 仍是普通脏数据捕获咽喉。同一区块只保存最新不可变 NBT 快照。发送某版本
成功时，只有该版本仍是当前最新版本才能清除 `DIRTY_UNACKED`；旧请求的迟到 ACK 不得清除
后来产生的新版本。实时 ACK 后不再为了退出保留已确认快照。

claim 返回 `hasServerData=false` 时，即使玩家没有手工修改，第一次卸载也必须建立服务端数据，
因为客户端生成的区块已成为本组权威状态。任意实时写入 ACK 后可转为
`SERVER_DATA_KNOWN=true`。

## 预卸载屏障

在 `ChunkMap.scheduleUnload(long, ChunkHolder)` 的 HEAD 注入，但不取消或重写原版卸载算法。
Mixin 取得 `ChunkHolder.getChunkToSave()` 的当前 future，并通过 accessor 调用
`ChunkHolder.addSaveDependency(...)`，把 P2P 预卸载 future 添加为原版存盘依赖。

预卸载 future 的流程为：

1. 等待原版当前 `chunkToSave` 完成，取得最终 `ChunkAccess`。这同时覆盖 LevelChunk 和
   世界生成外围 ProtoChunk。
2. 回到集成服务端主线程检查 `isUnsaved`、服务端数据基线和未确认状态。
3. 需要数据时在主线程执行 `ChunkSerializer.write`，后台 gzip；干净且服务端已有数据时
   不生成 NBT。
4. 调用带稳定 `releaseId` 的可重试 `CHUNK_RELEASE`。客户端在正 ACK 前继续保留 claimed
   状态；服务端以单条 WAL 原子记录 ownership 删除与成功回执，ACK 丢失/重启后的同 ID
   直接重放原结果，不重复写最终数据。
5. 成功后删除本地 claim、基线及未确认快照，完成 save dependency。
6. 后台可靠发送 `CHUNK_RELEASE_RECEIPT_CONFIRM` 回收服务端回执，不给卸载增加第二个 RTT。
7. 原版 `scheduleUnload` 的后续回调才会执行本地 save、实体卸载和内存移除。

同一 ChunkKey 的重复卸载调度复用一个 future，不能产生并行释放事务。原版最终本地 save
仍可执行，但此时 claimed 已移除，`ChunkUploadService` 会丢弃迟到的本地落盘旁挂，避免二次上行。

## 全局退出与预卸载协调

全局退出开始时先进入 `exitPreparing`：拒绝新 claim，但允许已经开始的预卸载继续重试。
退出编排异步等待全部预卸载 future 成功完成，然后才执行 `freezeForExit()` 固定剩余所有权。
这样不会把同一区块同时交给普通 `CHUNK_RELEASE` 和退出事务。

冻结后不再安装新的预卸载依赖；停服由退出事务统一释放剩余清单。`ChunkUploadService` 停止向
已冻结的物理服发送普通实时请求，但继续接收停服 save 产生的最新脏快照。`SERVER_STOPPING`
不再扫描并序列化所有加载区块，最终 bundle 只读取未确认快照。

## 增量退出事务

`ExitManifest.chunks` 继续表示完整所有权清单，保证断线保护和原子整组释放不变。
`ExitBundle.writtenChunks` 只包含最终需要覆盖物理服的区块；其余清单项隐式表示“物理服现有
数据已经是权威版本”，不再逐项发送 preserve marker。

客户端流程：

1. 重放 `EXIT_BEGIN`；
2. 上传全部最终玩家数据；
3. 只上传 `writtenChunks`；
4. `EXIT_COMMIT` 携带排序后的 `writtenChunks` 清单；
5. 服务端确认本轮已收到全部玩家及声明的脏区块后，持久化 committed 意图；
6. 只应用声明的脏区块，未声明区块不读盘、不写 marker；
7. flush 正式存储并一次性耐久释放完整 ownership manifest。

事务持久化记录 committed 时的 `writtenChunks`，使服务端在 committed 后、正式应用前崩溃，
重启仍能准确区分必须应用的数据和隐式干净区块。旧事务文件没有该字段时继续按旧
data/marker 规则恢复，不能破坏已经进入 committed 的历史事务。

## 并发与失败处理

- 普通实时上传与预卸载释放可能短暂交叠；最终 `CHUNK_RELEASE` 总携带当前最终快照，服务端
  在同一会话/所有权锁下先写后释放。释放成功后迟到实时上传因所有权消失而被拒绝，不能覆盖。
- 释放失败不从 claimed 集合删除，也不完成原版 save dependency。
- 成功 ACK 丢失时相同 releaseId 必须在服务端重启前后都返回原正 ACK；命中回执不得再次
  写最终 NBT。收到正 ACK 后的回执确认在后台重试，不阻塞本地卸载。
- 退出准备等待预卸载清空；网络长期不可用时退出和区块卸载都保持等待，不用超时换取数据丢失。
- 旧版本实时 ACK 只确认旧版本，不能把同键新快照标为干净。
- Mixin 仅在 `GroupRuntime.isManagedLevel` 为真时生效，普通单人世界和专用服务端保持原版行为。
- 预卸载屏障必须在所有 ChunkAccess 上生效，以回收不触发 Fabric LevelChunk 卸载事件的生成依赖占用。

## 协议兼容性

`EXIT_COMMIT` 新增 `writtenChunks[]`，且干净清单项不再发送 `EXIT_CHUNK_DATA preserveExisting`。
该语义与旧服务端不兼容，因此 `Protocol.VERSION` 从 3 提升到 4。消息编号不复用、不改号。
服务端保留旧 committed 事务文件的恢复逻辑，但新旧在线节点不能混连。

## 测试策略

- 实时上行 ACK 清除当前快照；旧 ACK 不清除新版本；失败保留最新版本。
- claim 时无服务端数据的区块即使 `isUnsaved=false` 也必须在首次释放时携带数据。
- 干净且服务端已有数据的区块释放不携带 NBT。
- `CHUNK_RELEASE` 失败时 claimed 与屏障保持；成功 ACK 后才移除并允许卸载。
- `CHUNK_RELEASE` 正 ACK 丢失后，重试与跨重启重试均命中耐久回执且最终 NBT 只写一次；
  回执确认丢失时后台重试，服务端重复确认幂等。
- 重复预卸载调度复用同一 future；LevelChunk 和 ProtoChunk 都走屏障。
- 退出准备等待在途预卸载；冻结后不创建新屏障。
- ExitBundle 允许 ownership manifest 大于 written 集合。
- 新客户端不再为干净区块发送退出消息；commit 只等待声明的 written 集合。
- committed 的 written 集合耐久恢复；旧事务 marker 格式仍能恢复。
- 定向测试、全部测试和完整 Fabric remap build 均通过；实机验证断网重试、移动卸载、世界生成外围
  claim 回收以及退出耗时。
