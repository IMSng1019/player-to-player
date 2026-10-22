# P2P 切换加载优化实施计划

目标：在 Fabric 1.20.4 / Java 17 下缩短可提前完成的切换准备和空等时间，保留原版世界加载、登录、保存生命周期、权威玩家状态、世界身份隔离及单写者约束。

执行授权：2026-10-22 用户已从只读规划切换为执行，采用兼容性优先方案。使用 superpowers:executing-plans 的逐项验证原则；本轮无需再次询问是否开始。

## 设计选择

采用“提前准备 + 原版切换”。不采用客户端世界对象/网络连接强行换绑；不把 NBT 缓存称为运行中的影子服务端，也不把网络 ACK 当成持久化 ACK。全流程 300ms 不作承诺，冻结、断开至登录、区块可用分别计量。

## 实施顺序与验收

1. 阶段计时：新增可测试的单调时钟追踪器，接入 WorldSession、WorldSwitcher、SecondaryJoiner、LocalWorldLauncher、MergeClient、PresyncSender、GroupHost。关联同一流程的 ID，覆盖准备、建连、冻结、断开、登录、可操作等阶段；取消/失败要结束追踪。日志不输出地址、玩家 NBT 或凭据。保留退出事务已有计时。
2. 切换前建连：拆出 SecondaryJoiner 的 prepare/activate/close，缓存有界、可取消、一次消费的隧道资源。合并计划先通知需要迁移的成员准备到新主的连接，全部需要迁移的在线成员准备成功才冻结；B 组已有正常连接保留。新流程须有会话用途和事务 ID，防同一 peer 的预同步与游戏隧道误认领。失败保留旧连接；提交后才走原版断开/登录。
3. 事件唤醒：ChunkClaimClient 在 MERGE_COMMIT/权威归属变化时唤醒被拒申请，保持服务端重新授权；同键最多一个在途申请，旧定时器/旧响应不能复活已结束请求。保留默认 10 秒重试作通知丢失的兜底。所有释放通知须在耐久完成后发出。
4. 双角色环境复用：同一同步批次以 SHA-256 + size 复用已验证内容，两角色仍各自独立物化文件；禁止硬链接可写配置。保留角色清单过滤、snapshotId 绑定、路径检查及下载失败重试。测试共同内容只下载一次、错误缓存回退、角色写入隔离、快照过期。
5. 完整迁移屏障：核实 chunk、entities、POI、scheduled ticks、玩家、世界 SavedData/模组状态的捕获和加载入口。预同步按世界身份/事务 ID/序列隔离，最终缺失/损坏/超限不得 ACK 成功。冻结前检查准备状态；冻结后确认最终状态耐久及原主停止写入，再允许归属提交。提交前失败撤销准备，提交后故障按事务状态恢复，不能盲目解冻原主。
6. 分离预同步：候选形成即开始预传，10 秒为最短稳定时间而非强制提交期限；候选成员、区块范围、传送门票据变化撤销/更新准备。仅在步骤 5 完整状态屏障满足时转移所有权；新主走原版启动后才通知其余成员连接。无法证明状态完整时保留原组，不启用未经验证的全热备。
7. 主动退出交接：在既有退出屏障前选接班人、准备环境和数据；成功交接后退出者只清理自身，不解散新组。交接失败保留原组或回到既有耐久退出事务，不能提前释放占用；单人组继续原 EXIT_BEGIN/EXIT_COMMIT 流程。普通退出、关闭窗口、重复点击、连接丢失均应覆盖。

## 主要文件

- 客户端：`client/session/WorldSession.java`、`ClientExitCoordinator.java`、`client/group/{WorldSwitcher,SecondaryJoiner,MergeClient,LocalWorldLauncher,GroupHost}.java`。
- 公共端：`group/{ChunkClaimClient,PresyncSender,PresyncReceiver,PresyncStore,PresyncProtocol,SplitMonitor,GroupRuntime}.java`、`p2p/{TcpTunnel,P2PSessions}.java`、`env/EnvSyncClient.java`。
- 服务端：`server/{MergeCoordinator,GroupTable,HelloHandler,ChunkDataHandlers,ExitTransactionHandlers}.java`、`registry/ChunkRegistry.java`。
- 协议：`netproto/{MessageType,Protocol}.java`。不复用已有消息编号；破坏语义的变更升级协议版本并拒绝旧端混连，不通过注释宣称兼容。

## 验证

- 有状态/并发/持久化行为先添加失败回归，再实现，再运行聚焦测试。
- 每阶段记录实际验证命令与结果，最终 `./gradlew.bat test`、`./gradlew.bat build`、`git diff --check`。
- 故障矩阵：直连/中转/禁用中转，超时/断流/重复与乱序事件，准备完成前离服、提交前后崩溃、磁盘写入失败、世界身份更换、多玩家合并与分离、跨维度票据、箱子/红石/实体/村民 POI/模组 SavedData、主退出及单人退出。
- 真实多机 smoke test 未运行时必须明确标为未验证，单元测试与 build 不替代真实运行证据。

## 本轮状态

已实施步骤 1–4，并完成步骤 5 中的坏记录拒绝、接收取消互斥、ACK 超时和控制面提交屏障。步骤 5 的完整状态迁移、步骤 6 的真实分离预同步及步骤 7 的主动退出交接尚未实施。本轮没有引入影子实例，也没有把现有分离/退出逻辑改称为完整交接。

### 实际变更

- `WorkflowTrace`、`SwitchTelemetry` 记录单调时钟阶段时长。`playable` 指原版加载界面退出且脚下区块到达，不代表整个视距已经完成渲染。`GroupRuntime` 另记录从冻结到解除的总时间，包含提交及旧成员断开等待。
- `PreparedTunnel` 准备期间不连接 MC LAN，直到本机原版客户端真正连入才激活。A 的同一可靠通道依次承载预同步和游戏连接，避免旧 NAT 端点模型下第二次打洞失败；成员准备失败保持旧连接。取消发送可靠 FIN，并在有界关闭期重传丢失的关闭帧。
- `MergeReadiness` 等待全体迁移成员及 B 接收器准备，再允许 A 开始同步；提交须同时收到 A 冻结完成和 B 尾部确认。源成员集合变化时原子拒绝提交。已提交事务排空期间阻止重叠迁移，原主在其余旧成员主动断开后再关服；提交结果未知时不盲目恢复旧写者。
- `ChunkClaimClient` 以操作身份、代次及在途标记去重事件/定时重试。`OwnershipChangeBroadcaster` 每 100ms 合并归属变化，通知发生在注册表 WAL 成功之后；默认 10 秒定时兜底不变。
- `EnvSyncClient.syncPair` 以内容摘要与大小复用本批次角色文件；独立复制并重新校验，下载临时文件唯一，所有目标路径均检查符号链接。没有跨世界静态缓存或配置硬链接；正常本地 diff 不增加一轮全内容哈希。

### 协议迁移

`Protocol.VERSION` 从 5 升到 6，服务端、中转端和全部客户端必须一起升级。新增 `MERGE_START(107)`、`CHUNK_OWNERSHIP_CHANGED(109)`、`MERGE_DRAINED(110)`；撮合请求同类型应答新增 `sessionId`，按它领取具体传输；预同步通道新增准备握手，并在尾部之后转换为隧道准备协议。现有编号不重用。本轮未新增 Mixin 声明或修改 fabric.mod.json。

### 完整迁移的实际阻塞与后续任务

已对本机 Fabric Loom 缓存的 1.20.4 Mojang 映射 JAR 使用 `javap -p` 核实：`EntityStorage` 拥有独立 `IOWorker` 和 `loadEntities/storeEntities/flush`；`PoiManager` 继承 `SectionStorage`，后者有自己的存储、脏集合及 `flush(ChunkPos)`；`DimensionDataStorage` 另有 SavedData 缓存和 `save()`。现有 `ChunkStorageMixin`、`ChunkSerializer.write`、区块 WAL/预同步协议没有把这些持久域纳入同一最终状态清单。世界级 SavedData 也不能按区块简单切分或覆盖；任意模组内存状态不能由通用区块 NBT 推断。

因此下一阶段须依次实施并验证：

1. 为 entities、POI、玩家及世界/模组持久域定义带世界身份、事务 ID、序列和摘要的迁移清单，以及明确的状态提供者能力；缺失能力不可声称完整热迁移。
2. 接入各持久域的原版保存/加载入口和物理服复合耐久提交，覆盖实体删除与 UUID、乘客关系、POI 占用和异步刷盘完成；不只检查区块文件存在。
3. 建立原主停止接受新 claim、排空在途释放/上传、最终状态捕获及写者代次屏障。当前世界 tick 冻结不等于任意模组和游戏包处理均被冻结，不能据此保证全部状态静止。
4. 为组表、注册表迁移及提交结果建立可重放事务记录；崩溃/断连后查询确定结果，禁止根据本地超时同时恢复两个写者。
5. 完成上述协议后，把分离候选的 10 秒稳定窗口用于真实快照/增量预传；候选变化取消，准备不完就保留原组。主动退出复用同一事务，以现有耐久退出作为无接班人或失败兜底。

### 已执行验证

- 基线 `./gradlew.bat test` 通过；新增回归经过失败→修复→聚焦重跑。
- `./gradlew.bat test build` 通过；406 项测试，0 失败、0 错误、0 跳过。
- 覆盖丢失首个 FIN、远端名额回收、单次激活、游戏字节透明转发、旧定时器/调度异常、在途唤醒、退出迟到响应、WAL 失败不通知、准备后成员变化、坏 NBT/暂存超限、环境复用/源变更/快照过期/链接隔离。
- 两轮独立静态复查发现的问题已修复并补充相关回归；`git diff --check` 通过。
- 未运行真实多机 Minecraft smoke test；尚无冻结时间、黑屏时间或大整合包性能改善的实测结论。构建产物位于 `build/libs/player_to_player-1.0.2.jar`。
