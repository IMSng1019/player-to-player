# Complete Environment Runtime Implementation Plan

> **For Codex:** 使用 superpowers:executing-plans / subagent-driven-development 按依赖执行，用户已授权当前会话实施，无需再次询问执行方式。

**Goal:** 使环境同步在登录前完成，保证文件与运行版本一致，并将玩家侧世界计算接入隔离 Fabric 服务端进程及版本化更新流程。

**Architecture:** 保留现有快照和控制协议，将下载发布、客户端运行校验、玩家侧服务端进程与更新事务拆开。所有组计算仍遵守现有区块申请和耐久退出屏障。

**Tech Stack:** Java 17, Minecraft 1.20.4, Fabric Loader 0.19.3, Netty, JUnit 5, Gradle wrapper.

---

### Task 1: 固定版本、完整文件校验与事务发布

Files: `env/EnvSyncClient.java`, `env/EnvSyncServerHandlers.java`, `env/EnvironmentSnapshotManager.java`, 新环境发布辅助类及 `src/test/java/.../env/*Test.java`。

1. 写失败测试：服务端变化不能让双角色混版；删除旧文件；下载失败不能污染旧版本；清单哈希错误必须失败。
2. 运行 `.\gradlew.bat test --tests 'imsng.player_to_player.env.*'` 确认新增测试失败原因。
3. 加入指定版本清单、暂存安装、整体复验与失败回滚；复用已有内容并保留路径边界检查。
4. 运行上述测试并检查中转源更新门与缓存版本。

### Task 2: 登录前准备与实际运行环境校验

Files: `client/boot/ClientBootstrap.java`, 新连接前置 Mixin/服务，`client/session/WorldSession.java`, 环境运行清单类，服务端环境探测处理器。

1. 定义纯 Java 的前置状态机并写测试：普通服务器、P2P 准备失败、取消、重复连接、模组不匹配。
2. 实现只读预检握手，不占用玩家的正式 HELLO 会话和区块归属。
3. 在正式登录前同步并核对实际加载的模组；明确报告需重启，不通过修改已加载类冒充生效。
4. 编译客户端并运行相关测试。

### Task 3: 玩家侧独立服务端运行

Files: 新 `runtime/` 公共运行组件，`core/P2PBootstrap.java`, `group/GroupRuntime.java`, `client/group/LocalWorldLauncher.java`, `GroupHost.java`, `WorldSession.java` 和合并/退出适配。

1. 写进程生命周期和父子桥接契约测试：令牌、就绪、退出、断连与请求对应。
2. 从同步环境物化独立工作目录和启动参数，保持主副模组物理环境约束。
3. 子进程接入已有 GroupRuntime；父进程桥接控制连接与入站指令，P2P 隧道连接子进程游戏端口。
4. 接入合并、分离、退出屏障及崩溃清理；避免同时启动两个权威 tick 实例。
5. 运行进程测试与可用的真实启动冒烟。

### Task 4: 在线环境更新、应用状态与反向变更

Files: `env/` 更新服务，服务端注册入口，客户端会话生命周期，相关测试和配置。

1. 写版本冲突、禁止路径、旧会话、删除与幂等重试测试。
2. 服务器发布通知与客户端后台准备新版本，记录待重启而不覆盖运行目录。
3. 实现按基础版本比较的受限上传事务和服务端发布；明确区分环境文件与存档数据。
4. 更新版本协商、协议迁移与使用文档。

### Task 5: 集成验证与审查

1. `.\gradlew.bat test`、`.\gradlew.bat build`。
2. 审查所有兼容性敏感变更（协议号、消息类型、Mixin、Fabric 元数据）。
3. 记录真实冒烟执行情况、资源限制和剩余限制；保持最终工作区可审阅。

## 执行记录（2026-10-24）

- Tasks 1–4 的代码和自动化验证已完成；中断恢复、取消提交、实际模组/启动配置、
  父子身份与退出、运行版本准入、配置文件并发冲突和世界资源映射均已接入调用链。
- Task 5：最终 `test build` 成功（557 通过，2 个显式进程冒烟默认跳过）。
  显式进程冒烟另跑 2 项通过，RPC/环境证明 5 项通过，`git diff --check` 通过。
- 增加组运行版本准入保护：新版本角色等待全部旧版本组退出，合并计划/提交拒绝混版。
- 已处理审查发现的停服配置收尾、世界资源遗漏、RPC 断连、子进程退出屏障、
  权限名单早晚顺序、跨世界回传、迟到停止回调以及原版游戏设置遗漏。
- 实际双图形客户端合并/分离、图形重启操作尚未进行，保留为实际部署验收项。
  未承诺任意模组完整内存热迁移。详见 `docs/ENVIRONMENT_RUNTIME.md`。
- 源码、测试与文档整理为本地 Git 提交；构建 JAR 保留在生成目录，未部署到真实服务器。
