# mcpcli 0.1.0 工作进度

更新时间：2026-11-16 18:27（Asia/Shanghai）

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

- C3：`9626d98ba14e004bb3d98b95876df4ec5316234c`，tree `72180159f5af0f4419942874fff24db928bc0772`。主代理/reviewer 均独立通过 fmt、Clippy、45 单元 + 19 协议 + 25 CLI E2E，归档安全自测 3/3。
- R8 已修复；reviewer 额外 8 项 oneshot 缺失工具、DELETE 失败/超时/中断复验通过。
- R9 Windows runtime 祖先 junction 权限拒绝已实现；进一步审查发现技能路径存在相邻 R10，现已改为基于固定目录句柄逐组件打开，原生回归待新 CI。
- C2 Windows x64/ARM64 各 47 单元 + 19 协议实际通过，DACL/default profile/Job 测试均有证据；CLI 在首个 persistent 调用挂住，定位为 R11 标准句柄继承导致捕获输出无法结束。main 的启动句柄隔离修复已提交，Windows 两架构聚焦编译/Clippy 通过。
- macOS 的原生 ACL 测试通过；HTTP 测试服务器的反向 DNS 阻塞已修。FreeBSD 全套测试与必跑 NFSv4 回归通过，夹具卸载后 mdconfig 尾随空格问题已修。Debian 归档提取已改为支持 Python 3.11 的显式安全提取。
- 旧 C2 [CI](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43845012976) 和 [发版预演](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43845013015) 已取消，完成日志均已收集；没有把取消或部分成功当作门禁通过。
- C3 因 R10/R11 保持 **NO-SHIP**，未推送。Windows 测试的有界输出捕获和任务状态诊断已整合，保留真实管道 EOF 验证；现在冻结 C4，重跑全部原生 CI 与七包发版预演。

## 发布状态

未进入 Step 4；尚未推送主分支、标签或发布 Release，未清理既有工作树。
