# 2026-10-31 版本化 Fabric 启动包识别修复

## 故障证据

提供的 `test_log/26.10.31_2/cilent/latest.log` 在 18:17:57 正常退出，随后测试实例的重启任务记录为 `CLIENT_STARTED`、`failure=NONE`。独立客户端的 `game/logs/latest.log` 第 117 行显示，18:18:20 后台计算服务启动失败，异常为“同步环境缺少 fabric-server-launch.jar”。旧的 10 月 22 日崩溃报告与此次故障无关；提供的服务端日志在 18:10 已结束，不能当作本次连接的同时段服务端记录。

实际下载环境和安装目录均有 `fabric-server-mc.1.20.4-loader.0.19.5-launcher.1.1.2.jar`，入口为 `net.fabricmc.installer.ServerLauncher`，并具有完整的 `.fabric/server` 缓存及相对依赖。问题来自 `RuntimeLaunchCommand` 硬编码文件名，并非下载遗漏。

## 修复与兼容范围

正式启动命令仅检查已安装环境根目录的普通 JAR，通过清单 `Main-Class` 识别官方安装器入口和 `net.fabricmc.loader.impl.launch.server.FabricServerLauncher`。标准名、官方版本化名及重命名均受支持。无候选或存在多个候选时明确报错；不执行环境脚本，不遍历可能包含多个旧 Loader 的缓存目录，也不选择普通原版服务端或无关诊断工具。

没有改动协议版本、消息类型、Mixin 声明或模组元数据。部署时应更新中心服发布的环境模组和原启动器客户端模组；仅更新本地客户端而保留中心服旧 JAR，会在环境同步时重新安装旧实现。

## 验证结果

- 先补充回归测试，旧实现 7 项中有 5 项失败；修改后全部通过。覆盖标准名、官方版本化名、中文重命名、无关/损坏 JAR、缺失清单、旧缓存和多候选。
- 使用测试实例中的真实官方启动包、Loader 0.19.5 缓存、Minecraft 1.20.4 依赖和 Fabric API，复制到临时目录，并装入本次构建的 remap JAR；原实例保持只读，没有使用真实世界或账号。
- Java 17 正式入口冒烟共 14 项，全部通过：7 项启动命令回归、5 项安装测试、2 项真实计算进程测试。实际启动场景检查服务端专用模组与配置、元数据确认、未授权直连拒绝、授权玩家身份、无玩家时零出生点区块申请，以及退出载荷交付和进程释放；另一场景检查启动取消。
- 完整 `.\gradlew.bat test build` 成功，共 671 项测试，668 项通过，3 项需要显式开启的进程测试在普通构建中跳过。`git diff --check` 通过。
- 构建产物：`build/libs/player_to_player-1.0.2.jar`。独立代码审查无阻断问题。

正式入口冒烟可复用以下命令，路径参数应指向本机只读的已下载主角色环境，不包含在仓库配置中：

```powershell
.\gradlew.bat test '-Pp2pWorkerProductionEnvironment=<已下载的主角色环境目录>' --tests '*WorkerProcessSmokeTest' --tests '*RuntimeLaunchCommandTest' --tests '*RuntimeInstallationTest'
```

该模式固定使用 Java 17 和本次 remap JAR，不走 Loom 开发入口；测试复制器目前用于官方安装器及 `.fabric/server` 缓存布局。传统根目录启动包若依赖额外根 `server.jar` 或外部 properties，需相应扩展测试复制规则。本次没有操作远程服务端，也未重跑图形客户端及公网认证的完整回服流程，正式计算进程冒烟不代表该全链路已验收。
