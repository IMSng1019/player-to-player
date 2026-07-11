## review-spec 规范符合性

### [major] src/client/java/imsng/player_to_player/client/session/WorldSession.java

问题: 环境校验不是校验本地环境的真实哈希，而是比对上次同步成功后缓存的 lastEnvironmentHash（WorldSession.java:159）。规范要求"校验本地环境是否与服务端的环境的哈希值相同"——若玩家在两次加入之间修改、损坏或删除了 primary/secondary environment 目录中的文件，缓存哈希仍与服务端 envHash 相等，同步被跳过，客户端将带着不一致环境进入游戏，破坏"确认环境相同后"才继续的核心前置条件。（本地真实扫描+diff 逻辑在 EnvSyncClient 中已存在，只是被缓存短路了）

建议: 去掉以缓存哈希短路的判断（或仅将其作为快速通过的提示），每次加入世界都对两套环境目录执行 EnvSyncClient 的"拉清单→本地扫描→diff"流程；diff 为空时自然零下载，成本只有一次本地扫描，语义才与规范一致。也可改为记录并比对各 target 的 filteredHash 与本地实际扫描哈希。

### [minor] src/client/java/imsng/player_to_player/client/session/WorldSession.java

问题: 服务端环境扫描未完成时 HELLO_ACK 返回 envReady=false，HelloHandler 注释承诺"客户端应稍后重试环境比对"（HelloHandler.java:120），但 WorldSession.java:159-163 在 envReady=false 时仅打日志跳过，session 期间永不重试——服务端启动初期加入的玩家整局都不会同步环境，只有退出重进才会触发，与规范"玩家加入世界时校验环境"的工作流不符。

建议: envReady=false 时在 io 线程上做带上限的轮询重试（例如每 5s 重发一次 HELLO 或新增 ENV_STATUS 查询，直至 envReady=true 后执行同步），并用 generation 防止玩家离开后残留任务。

### [minor] src/main/java/imsng/player_to_player/proxy/ProxyServerService.java

问题: 中转服务端未挂接环境分发：EnvSyncServerHandlers 类注释声明"服务端与中转服务端共用，二者都承担环境分发"（DESIGN.md 第 4 节、规范第 38 行"中转服务端……同步环境文件"），且 Phase 1 范围含"环境文件扫描与同步"与"中转端服务骨架"，但 ProxyServerService.start() 只启动 RelayCore，环境分发被标记为 Phase 2 TODO（ProxyServerService.java:41），与设计文档口径自相矛盾。

建议: 要么在 ProxyServerService 中扫描本地环境目录并注册 EnvSyncServerHandlers（骨架已可复用），要么修正 DESIGN.md §4/EnvSyncServerHandlers Javadoc，把"中转端环境分发"明确划入 Phase 2，消除范围声明与实现的矛盾。

### [minor] src/main/java/imsng/player_to_player/config/GlobalConfig.java

问题: 规范第 38 行"中转服务端需要设定自己的服务端"未落地：Phase 1 范围含"角色/配置"，GlobalConfig 只有服务端→中转的 relayServerAddress，proxy_server 模式没有任何指向其上级服务端的配置项（地址/端口），中转端无从知道自己隶属哪个服务端。

建议: 在 GlobalConfig 增加 proxy_server 模式使用的 parentServerAddress（或 serverAddress）字段，即便 Phase 1 只做配置占位与加载校验，也应满足"角色/配置"范围承诺。

### [minor] src/main/java/imsng/player_to_player/env/EnvironmentScanner.java

问题: 内置排除表硬编码 "world/" 前缀（EnvironmentScanner.java:42-50）：若服务端 server.properties 的 level-name 不是默认的 "world"（如 myworld），则 myworld/region、myworld/poi、myworld/entities、myworld/DIM-1、myworld/DIM1 全部不会被排除，动辄数 GB 的区块数据会被算入环境清单分发给所有客户端，且每次存档写盘都会使环境哈希失配触发全量重传——虽然规范原文写的是 ./world/*，但其意图显然是排除存档维度数据目录。

建议: P2PServerService.start() 已持有 server.getWorldPath(LevelResource.ROOT)，将实际存档目录名（worldRoot 相对 serverRoot 的相对路径）动态拼入排除表传给 EnvironmentScanner.scan()，替代硬编码的 "world/" 前缀（保留 "world/" 作为兜底亦可）。

## review-tech 技术正确性

### [critical] src/main/java/imsng/player_to_player/mixin/ServerLevelMixin.java

问题: 在 HEAD 取消 ServerLevel.tick(BooleanSupplier) 的前提论证错误：1.20.4 mojmap 中该方法内部包含 this.getChunkSource().tick(hasTimeLeft, true)（区块广播/加载/卸载、DistanceManager ticket 更新）和 this.entityManager.tick()（实体分区加载）。整体取消后服务端不再向玩家发送任何区块包，玩家登录后永久卡在 'Loading terrain' 界面，也不会有区块加载/卸载——与 Javadoc 中 'ServerLevel.tick 内部只包含纯世界演算，整体取消是安全的' 的说法直接矛盾，suspendWorldTick=true（默认值）下服务器实际不可用。

建议: 不要整体取消 tick。保留服务面调用：改为注入/Redirect 具体的演算子调用（tickTime、advanceWeatherCycle、tickChunk/tickCustomSpawners、方块与流体计划 tick、实体 tick 循环），或在 HEAD 取消后仍手动调用 getChunkSource().tick(...) 与 entityManager.tick()（需 accessor）。至少要在 DESIGN 文档与代码注释中更正该结论并补充联机验证。

### [major] src/main/java/imsng/player_to_player/netproto/ControlCodec.java

问题: Decoder 的未知消息类型分支用 ctx.writeAndFlush(ControlMessage) 应答 ERROR，但出站事件从 decoder 的 ctx 只向 head 传播，会跳过位于其后（更靠 tail）的 Encoder（管线顺序 frame→decoder→encoder→idle→connection）。未编码的 ControlMessage 到达 HeadContext 被 filterOutboundMessage 以 unsupported message type 拒绝，promise 静默失败——ERROR 帧永远发不出去，对端携带 _rid 的 request() 只能等满 15 秒超时，与注释声称的'让对端请求快速失败'相反。

建议: 改用 ctx.channel().writeAndFlush(...)（从 tail 走完整出站管线），或把 Encoder 加在 Decoder 之前（更靠 head 的位置）。建议给该写操作挂 listener 记录失败日志，避免同类问题再次静默。

### [major] src/main/java/imsng/player_to_player/server/P2PServerService.java

问题: onDisconnect 清理（第 128-138 行）按 peerId 无条件执行 players.remove/computes.remove/registry.releaseAll。客户端快速重连时 HelloHandler 会关闭旧连接（HelloHandler.java:101-105），旧连接的断连回调随后触发，误删新连接刚上报的算力表条目并 releaseAll 释放该组（groupId==主客户端 clientId）占用的全部区块——新会话状态被旧连接的收尾清空。HelloHandler.unregister 内部用了 remove(key,value) 条件移除，但这三项清理没有同样的守卫。

建议: 清理前判断该连接是否仍是当前连接：if (HelloHandler.connectionOf(peerId) != conn) 时仅做 unregister，跳过 players/computes/registry 的清理；或把整套清理逻辑收进 HelloHandler.unregister 的条件移除成功分支。

### [major] src/main/java/imsng/player_to_player/p2p/P2PSessions.java

问题: 静态会话表 SESSIONS 没有整体关闭入口，WorldSession.onLeave 只关闭 ControlClient。打洞成功的 P2PChannel 持有的 UDP socket、阻塞在 receive 上的 io 线程、以及每 1 秒调度一次的 keepalive 任务（P2PChannel.start 的 scheduleWithFixedDelay）在离开世界后全部残留，继续向旧对端发送加密 keepalive；多次进出世界会话资源持续累积泄漏。

建议: 给 P2PSessions 增加 closeAll()：遍历 SESSIONS 逐个 close 并清空表；在 WorldSession.onLeave 中调用。P2PChannel.close 已幂等，keepalive 任务与接收循环会随之退出。

### [major] src/client/java/imsng/player_to_player/client/session/WorldSession.java

问题: openSession 存在 TOCTOU 竞态（第 125-129 行）：io 线程检查 myGeneration != generation 通过后、写入 controlClient = cc 之前，主线程的 onLeave 可能执行（generation++ 并关闭当时为 null 的 controlClient），随后 io 线程把 cc 写入 controlClient——这条连接（含其专属 NioEventLoopGroup）永远不会被关闭。catch 分支的 closeIfCurrent(myGeneration) 在代数已变时也不关 cc，同样漏掉这条连接。

建议: 用一把锁把 {代数校验 + controlClient 赋值} 与 onLeave 的 {generation++ + 取出并关闭} 原子化；或赋值后二次校验代数，发现过期立即 cc.close()。catch 分支对本方法创建的 cc 应无条件 close（cc 是局部创建的，过期时关闭它不会误伤新会话）。

### [major] src/main/java/imsng/player_to_player/env/EnvironmentScanner.java

问题: scan 对每个文件提交一个哈希任务到 ThreadPools.io()（Executors.newCachedThreadPool，无界）：cached 池在无空闲线程时总是新建线程，目录遍历远快于磁盘哈希，大型服务端目录（几千个文件的整合包）会瞬间创建数千个线程，导致线程句柄/内存耗尽风险，且并发磁盘随机读反而拖慢总吞吐。

建议: 哈希并行度限定为固定值（如 min(4, CPU 核数)）：用固定大小池或信号量限流，也可以直接串行（Sha256.hexOfFile 已是流式读，顺序读盘吞吐通常更好）。

### [minor] src/main/java/imsng/player_to_player/netproto/ControlClient.java

问题: connect() 依赖 connect future 完成即认为连接可用，但 Netty 的 AbstractNioChannel.fulfillConnectPromise 先 trySuccess 再 fireChannelActive，NettyControlConnection.channel 字段在 channelActive 里才赋值——调用线程从 sync 唤醒后立刻 request()（如 WorldSession 的 HELLO）时 channel 可能仍为 null，send() 按'连接已关闭'静默丢帧，HELLO 只能等 15 秒超时。

建议: 把 channel 赋值提前到 handlerAdded(ctx)（handler 加入管线时 ctx.channel() 已可用），或在 connect() 成功后显式等待 channelActive（例如让 NettyControlConnection 暴露一个 ready future）。

### [minor] src/main/java/imsng/player_to_player/util/JsonUtil.java

问题: getInt/getLong/getBoolean 只校验 isJsonPrimitive，对类型错误的原始值（如 {"version":"abc"}）getAsInt/getAsLong 会抛 NumberFormatException：经处理器路径的异常被 channelRead0 吞掉但不回 ERROR，请求方傻等 15 秒；更糟的是 ControlMessage.rid()（getAsLong）在 channelRead0 的处理器 try 块之外被调用（NettyControlConnection.java:148），恶意帧携带非数字 _rid 会直接走 exceptionCaught 断开连接。

建议: 在各 getter 内 catch NumberFormatException/UnsupportedOperationException 返回 fallback；hasRid() 同时校验 _rid 是否为数字原始值（isJsonPrimitive && getAsJsonPrimitive().isNumber()）。

### [minor] src/main/java/imsng/player_to_player/registry/RegistryHandlers.java

问题: CHUNK_CLAIM_REQUEST/CHUNK_RELEASE/PLAYER_POS_UPDATE 及 env 同步处理器均不校验 connection.peerId()（是否完成 HELLO），未握手的连接可以用任意伪造 groupId 申请/释放区块、下载全部环境文件；P2PBrokerHandlers 做了 peerId 校验，其余处理器没有对齐。

建议: 在这些处理器入口统一加 connection.peerId() == null 即回 ERROR 的守卫（或在 NettyControlConnection 分发层对除 HELLO/PING/PONG 外的类型做握手门控）。

### [minor] src/main/java/imsng/player_to_player/env/EnvSyncServerHandlers.java

问题: handleFileRequest 中 manifest.get() 可能为 null（服务端扫描未完成或扫描失败时恒为 null），current.files() 的 NPE 发生在 Netty 事件循环上且在 io 任务的 try 之外——异常被 channelRead0 记日志吞掉，不回 ERROR，客户端该次 ENV_FILE_REQUEST 傻等 15 秒超时（恶意客户端可无视 envReady=false 直接触发）。

建议: 方法开头判空：current == null 时回 error(msg, "env_not_ready", ...) 并 return。

### [minor] src/main/java/imsng/player_to_player/config/GlobalConfig.java

问题: envFileChunkBytes 未与 Protocol.MAX_FRAME_BYTES 联动校验（注释仅写'须小于协议最大帧长'）：配置成 ≥16MB-帧头 时，ENV_FILE_DATA 出站帧超过对端 LengthFieldBasedFrameDecoder 上限，对端抛 TooLongFrameException 断连，环境同步反复失败且原因难排查。同理 nonEnvironmentPaths 默认为空时环境清单会把 server.properties（含 rcon.password）、ops.json、白名单等敏感文件分发给所有客户端。

建议: 加载配置时 clamp：envFileChunkBytes = Math.min(envFileChunkBytes, MAX_FRAME_BYTES - 64*1024) 并对非法值告警；EnvironmentScanner 的内置排除项中追加 server.properties 等含凭据的文件，或在文档中显著提示管理员配置 nonEnvironmentPaths。

### [minor] src/main/java/imsng/player_to_player/netproto/ControlServer.java

问题: stop() 的 Javadoc 声称'关闭全部连接'，实际只同步关闭 listen channel；已接受的子连接要等 workerGroup.shutdownGracefully 的静默期（默认约 2 秒）由 NioEventLoop.closeAll 关闭。P2PServerService.stop 在 control.stop() 返回后立即做 registry.shutdown() 最终落盘，静默期内事件循环仍可能处理排队的 CHUNK_CLAIM/CHUNK_RELEASE，这些变更发生在最终 persist 之后，重启即丢失。

建议: stop() 中跟踪并显式关闭全部子连接（如用 ChannelGroup 在 initChannel 时收集，stop 时 group.close().sync()），或至少 awaitTermination worker group 后再返回，保证最终 persist 覆盖所有变更。

