# 2026-10-30 客户端重复重启与个人设置丢失排查

## 现场证据

本次只读检查 `test_log/26.10.30` 的两份日志，以及日志指向的本机测试目录中的隔离实例、启动清单和重启任务状态。下文省略服务器地址、账号标识和完整本机路径；未修改测试实例，也未读取或记录认证启动命令。

- 服务端日志第 343 行：22:43:48 发布包含 68 个文件的环境快照；第 369 行：22:48:54 发布相同快照。期间没有服务端环境版本反复变化的证据。
- 服务端日志第 354、358、361、364 行：22:44:45、22:45:34、22:46:55、22:48:09 的登录均在环境预检阶段被拒绝，未进入正常玩家会话。
- 客户端日志第 100–102 行：22:48:10 主副环境均为下载 0、复用 68，22:48:11 执行正常 `Stopping!`。此次退出由环境重启流程触发，并非游戏崩溃。
- 本机测试目录有 4 份使用同一运行版本的隔离安装记录，每份包含 54 个启动文件。逐文件重新计算 SHA-256，四份均无缺失、无内容差异。其余 14 个文件属于世界的玩家数据、统计、进度和动态数据，本来就不安装到渲染客户端。
- 3 份重启任务均为 `CLIENT_STARTED`、`failure=NONE`；对应子进程日志正常初始化 Minecraft。前两次隔离进程再次连接后又下载 0、复用 68，仍然要求新的安装。
- 本机正式模组 JAR 的 `fabric.mod.json` 包含 `ClientRuntimeStartup` 的 `preLaunch` 入口，启动证据捕获并非漏注册。

## 根因与修复边界

`FabricRuntimeMods.capture` 只过滤元数据类型不是 `fabric` 的容器。但本次使用的 Fabric Loader 0.19.3 自身也是 `type=fabric` 的顶层模组容器：对现场 Loader JAR 的字节码检查确认 `V1ModMetadata.getType()` 返回 `fabric`，`ClasspathModCandidateFinder` 在正式环境中将 `LOADER_CODE_SOURCE` 加入候选。它不会被内嵌模组规则排除。

因此实际加载表中有 `fabricloader`，而服务端的 `mods/` 清单只包含 Fabric API 和本模组。原来的 `ClientEnvironmentCompatibility.check` 把 Loader 误报为额外通用模组，`EnvironmentPreflightClient` 随即要求重新安装；新进程仍然有同一个 Loader，条件永远不能满足。仅增加重启次数、重复下载或清空缓存均不能解决该问题。

修复只在“额外已加载模组”检查中排除 Fabric 运行平台的保留 ID：`fabricloader`、`minecraft`、`java`。真实额外通用模组仍拒绝；如果环境清单显式要求这些 ID，对应版本和 JAR 哈希仍严格校验，不能借平台名称绕过内容检查。此规则同时用于渲染客户端与独立计算服务端。

个人设置丢失来自 `ClientRuntimeProfile.install` 每次创建新的独立 `game` 目录，而旧流程只安装服务端文件与客户端专用模组 JAR，没有继承本地 `options.txt`。因此新进程按首次启动处理，语言、首次引导、音量和按键等恢复默认。个人设置应保存在每个服务器/世界对应的本地目录，并在新实例启动前恢复，不参与服务端环境哈希及启动一致性检查。

本次实现将 `options.txt`、`optionsof.txt`、`optionsshaders.txt` 保存在 `player-to-player/<服务器+世界>/client-settings/`。首次安装从当前游戏继承完整选项文件；以后优先读取该服务器已有选项。同服务器重装前以及正常退出时，把当前实例的最新选项保存回来。启动器原目录不会被覆盖，模组的 `config/` 仍按服务器环境管理。强杀或崩溃不保证把最后一次修改归档到稳定设置目录，运行实例中的原选项文件仍保留。

复用当前进程时还会检查运行目录所属世界，而不只比较环境哈希。这样两台服务器使用相同模组和配置时，切服也会装入目标服务器自己的个人选项，不会把 B 服设置误存回 A 服。后台文件复制保留现有的连接取消、代次核验及重启提交屏障。

升级时应统一替换中心服、中转服和原启动器客户端的模组 JAR。没有修改协议版本、消息类型、Mixin 声明或模组元数据；环境规则新增个人选项排除，旧服务端如果把个人选项纳入清单，新客户端会拒绝该清单，不能将本次修复理解为支持任意旧版本混连。

22:48:47 最后一次登录另有 `Authentication servers are down`（服务端日志第 366–368 行）。这是独立的认证服务失败；本次环境匹配修复不能证明当时认证服务恢复正常。服务端启动时大量旧 `.fabric/processedMods` 快照被跳过的警告也不是本次持续重装的直接原因：有效快照已经发布，下载与复用均成功。

## 回归验证

- `ClientEnvironmentCompatibilityTest.launchPlatformDoesNotRequireInstallingTheLoaderAsAServerMod`：修复前执行失败，证明正确业务模组加正常 Loader 仍被误判；覆盖客户端、计算服务端以及真实额外通用模组仍拒绝。
- `ClientEnvironmentCompatibilityTest.explicitlyDistributedPlatformIdsStillRequireTheActualJarBytes`：覆盖环境清单显式声明平台 ID 时，错误内容仍被拒绝。

- 新增设置测试覆盖语言、音量、键位、首次引导、完整选项字节保留、正常退出后再次启动、同服重装、不同服务器隔离、相同环境不同服务器的目录归属、原目录不被覆盖、缺少本机选项、文件大小限制和符号链接边界。
- 环境路径排除、个人选项不影响运行版本、旧安装清单不校验个人选项均有回归测试；相关旧行为的失败已在实现修复前确认。
- 最终 `.\gradlew.bat test build` 成功：666 项测试，663 项通过，3 项需显式开启的进程测试在普通构建中跳过。`git diff --check` 通过。产物为 `build/libs/player_to_player-1.0.2.jar`。
- Java 17 定向冒烟：60 项全部通过，无跳过。命令如下，重启助手从正式 remap JAR 加载；同时启动真实 Fabric 独立计算服务端验证 server-only 模组、配置和退出链路。

```powershell
.\gradlew.bat test -Pp2pRestartSmoke -Pp2pWorkerSmoke -Pp2pSmokeJavaVersion=17 --tests '*Restart*Test' --tests '*ClientLaunchArgumentsTest' --tests '*ClientRuntimeStartupTest' --tests '*ClientEnvironmentCompatibilityTest' --tests '*ClientLocalSettingsTest' --tests '*EnvironmentRuntimeIdentityTest' --tests '*EnvironmentPathPolicyTest' --tests '*WorkerProcessSmokeTest' --tests '*WorkerRpc*Test'
```

独立审查发现的“同环境切服复用错误目录”已修复并复核，无其余阻断提交的问题。本次没有用真实账号重新执行图形客户端的“语言/音量检查 → 自动重启 → 公网回服”全链路；进程冒烟与文件测试不能替代该实机验收，也不验证公网认证服务的可用性。
