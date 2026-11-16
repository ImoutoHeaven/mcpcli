# mcpcli 0.1.0 工作进度

更新时间：2026-11-16 17:44（Asia/Shanghai）

## 目标与发布门禁

审查提交 `285b0f19c85ef6deba34991baa131be6365a76e4` 和全部仓库，修复新增及既有问题，完成 Alpine、Debian、FreeBSD、macOS、Windows x64 与 ARM64 的干净重构、原生测试、E2E CI 和发版 CI。

发布门禁：冻结最终 checkpoint → 主代理全树审查 SHIP → 独立只读 reviewer 全树审查 SHIP → 该提交 CI 全绿 → 主分支/0.1.0 标签与发版 → 验证产物 → 清理 WIP 分支。任何新代码改动都会使旧的最终审查失效，需要重新核验。

## 当前状态

- 已验证目标主机、仓库和 GitHub 登录；初始工作区干净，`main` 比 `origin/main` 领先一个待审查提交。
- 基线：`285b0f19c85ef6deba34991baa131be6365a76e4`，父提交 `315a2467381a917c1ca7687450b8d585a03c634e`。
- 远程：`https://github.com/ImoutoHeaven/mcpcli.git`。当前远程仅有 `main`，尚无远程标签；本地已有旧的 `v0.1.0` 标签，最终会先核实指向和发布状态再处理。
- 已建立隔离工作树和分工：主代理 `wip/release-0.1.0`；平台重构 `wip/platform`；CI/E2E `wip/ci`；只读 reviewer 使用 detached 工作树。
- 当前执行 Step 1：提交与全仓独立审查；Step 3 的平台和 CI 设计并行展开。

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

核心 GET 修复与 Windows stdio Job 启动已实现；当前 Linux fmt/clippy/41 单元 + 18 协议通过。平台、CI/E2E 改动尚未集成，不能视为最终验证。

## 发布状态

未进入 Step 4；尚未推送主分支、标签或发布 Release，未清理既有工作树。
