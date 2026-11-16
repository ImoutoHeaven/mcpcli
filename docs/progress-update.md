# mcpcli 0.1.0 工作进度

更新时间：2026-11-16 18:14（Asia/Shanghai）

## 目标与发布门禁

审查提交 `285b0f19c85ef6deba34991baa131be6365a76e4` 和全部仓库，修复新增及既有问题，完成 Alpine、Debian、FreeBSD、macOS、Windows x64 与 ARM64 的干净重构、原生测试、E2E CI 和发版 CI。

发布门禁：冻结最终 checkpoint → 主代理全树审查 SHIP → 独立只读 reviewer 全树审查 SHIP → 该提交 CI 全绿 → 主分支/0.1.0 标签与发版 → 验证产物 → 清理 WIP 分支。任何新代码改动都会使旧的最终审查失效，需要重新核验。

## 当前状态

- 已验证目标主机、仓库和 GitHub 登录；初始工作区干净，`main` 比 `origin/main` 领先一个待审查提交。
- 基线：`285b0f19c85ef6deba34991baa131be6365a76e4`，父提交 `315a2467381a917c1ca7687450b8d585a03c634e`。
- 远程：`https://github.com/ImoutoHeaven/mcpcli.git`。初始远程仅有 `main`，尚无远程标签；目前仅推送了 WIP 检查点用于 CI。`main` 尚未更新，本地旧 `v0.1.0` 标签将于最终门禁通过后替换为正式发版提交。
- 已建立隔离工作树和分工：主代理 `wip/release-0.1.0`；平台重构 `wip/platform`；CI/E2E `wip/ci`；只读 reviewer 使用 detached 工作树。
- Step 1 基线审查完成（双方 NO-SHIP）；Step 2 已修复并提交确认问题；Step 3 原生平台层、回归与 CI 已集成，正在跑真实平台 CI 和冻结前复核。

## 并行职责

| 角色 | 责任 | 工作树 |
|---|---|---|
| 主代理 | HTTP/stdio 核心、配置、集成、checkpoint 审查、CI 跟踪、发布门禁 | `/root/mcpcli` |
| 平台子代理 | 平台原语、IPC/运行时/daemon、进程生命周期、安全文件读取 | `/root/mcpcli-worktrees/platform-20261116` |
| CI 子代理 | 原生平台矩阵、测试移植、E2E、产物与发版工作流 | `/root/mcpcli-worktrees/ci-20261116` |
| 只读 reviewer | 独立提交审查、全仓审查、最终冻结 SHA 的 SHIP/NO-SHIP | `/root/mcpcli-worktrees/reviewer-20261116` |

## 检查记录

基线双方审查均为 NO-SHIP：R1（排队超时后执行）、R2（GET SSE MIME 判断）、R3（成功结果在清理中断时丢失）。详见 [审查记录](audit-review.md)。

主代理基线 fmt/clippy/38 单元 + 18 协议通过；reviewer 独立 38+18 和 CLI 20/20 通过，并额外复现上述三个缺陷。

核心 GET 修复与 Windows stdio Job 启动已集成；早期 41 单元 + 18 协议验证已被后续整合回归覆盖。

## 当前集成与 CI

- C2 冻结提交：`cf62d0ff93ee3117a00fb4e1c48c336d42f4e964`，tree `d9f25083d23caba46dc0490eecb37f81d2f533ef`。
- 主代理和 reviewer 均独立通过 fmt、Clippy、45 单元 + 19 协议测试、release CLI E2E 23/23；reviewer 的技能目录竞态及尾缀链接外部复现已确认 fail closed。
- C2 CI：[`43845012976`](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43845012976)；发版预演：[`43845013015`](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43845013015)。仍在采集 Windows 两架构及原生 ACL 证据。
- 首轮 CI 已验证 Ubuntu、Alpine 两架构、Debian 两架构、FreeBSD 和真实 fastexec+Playwright 套件；Windows 测试环境问题已修复后进入 C2。macOS 揭示测试 HTTPServer 的反向 DNS 查询会阻塞启动，已改为纯回环绑定。
- C2 发版预演 FreeBSD 归档/解包 E2E 已通过；Linux 归档在 Debian 上暴露 Python 3.11 解包参数差异，正在修复。
- reviewer 新发现 R8：oneshot inspect 不存在的工具时，元数据错误路径可能跳过 DELETE。主代理正在统一结果与错误的清理所有权，CI 子代理补充正常、超时及中断清理回归。
- R8 已修复；reviewer 独立 8 项定向复验通过，主代理扩展后 CLI 25/25 通过。
- R9：Windows 祖先权限遗漏可设置 junction 的写权限，已增加拒绝规则和原生证明测试；主代理/reviewer 源码复核认可，等待 Windows 两架构实际执行。
- macOS DNS 夹具、Debian 归档提取、FreeBSD mdconfig 清理修复均已整合；归档安全自测 3/3 通过。
- 当前双方仍 **NO-SHIP**，因为 C3 候选尚未完成冻结后的全树审查与所有平台 CI。接下来冻结 C3，重跑完整 CI 与七包发版预演。

## 发布状态

未进入 Step 4；尚未推送主分支、标签或发布 Release，未清理既有工作树。
