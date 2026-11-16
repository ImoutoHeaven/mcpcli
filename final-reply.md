# mcpcli 0.1.0 执行记录：尚未发版

## 本轮接续状态（2026-11-16 19:15，Asia/Shanghai）

执行通道已经恢复：仓库读取、git、gh 和格式检查均可执行。当前仍为 **NO-SHIP**，正式 main/tag/release 和 WIP 清理尚未开始。

已核对本地 HEAD `40a64fcdcfc2e75937d266744a64f944ae0f5c0c` 与交接一致；HTTP 客户端和 Windows 目录创建修复仍在工作区。下一步将这些增量冻结为 C5，启动独立工作树的只读全仓审查、平台复核、CI/七包预演和完整本地回归。

当前远程 main 仍为 `315a2467381a917c1ca7687450b8d585a03c634e`，远程尚无正式标签。用户已有的推送和发布授权继续有效；保留同一冻结版本的双 SHIP、全树审查和 CI 全绿门禁。

以下保留上一轮交接记录，历史阻断状态以本节为准。

---

记录时间：2026-11-16 19:02（Asia/Shanghai）

**当前结论为 NO-SHIP。此文件不是发布成功回执。**

## 完成程度

| 用户步骤 | 当前状态 |
|---|---|
| Step 1：审查 285b0f1 和全仓 | 已完成；主代理与独立只读 reviewer 确认原有绿色测试仍漏掉必须修复的问题 |
| Step 2：修复并提交 | 主要修复已提交；最后 HTTP 与 Windows 目录创建修复仍未提交 |
| Step 3：跨平台、原生/E2E/发版 CI | 实现已集成并实际运行多轮；最终冻结版本尚未取得全绿 |
| Step 4：main、0.1.0、清理 WIP | **未进入**；没有推送主分支、正式标签或 Release，没有清理 WIP |

用户要求的“双 SHIP、最后冻结全树审查、CI 全绿”门禁尚未满足，不能发布。

## 当前版本

- 仓库：<https://github.com/ImoutoHeaven/mcpcli>。
- 当前本地集成分支：`wip/release-0.1.0`。
- 本地 HEAD：`40a64fcdcfc2e75937d266744a64f944ae0f5c0c`，tree：`effabe8e71bcd9a05749f447c11fc35f56095127`。
- 最新远程冻结检查点 C4a：`b55d053c8611b34148255a19e27587d09fb22931`，tree：`7f7bd7b2264b4a81417956fff59b7726e95f192f`，位于 `wip/release-checkpoint`。
- 本次工作没有更新 main；初始 origin/main 为 `315a2467381a917c1ca7687450b8d585a03c634e`。
- 旧本地 v0.1.0 标签未替换，本次未创建远程标签或 Release。

## 已完成的主要工作

主代理、平台子代理、CI 子代理和独立只读 reviewer 使用隔离工作树并行工作。已审查初始提交、整个仓库和后续冻结检查点；详细问题、来源、级别及修复记录见 [docs/audit-review.md](docs/audit-review.md)。

确认并处理的问题包括：排队请求越过原始超时仍派发；GET SSE 的 Content-Type/状态/大小边界；oneshot 已知结果和错误清理路径；技能根目录及逐组件路径身份；macOS/FreeBSD ACL；Windows 线程归属、目录权限及 daemon 继承捕获输出句柄；发布包集合污染及 Windows CRT 依赖。

平台层及测试已覆盖 Alpine、Debian、FreeBSD、macOS Intel/ARM64、Windows x64/ARM64。两个 GitHub workflow 已包括原生测试、CLI E2E、真实 MCP 服务 E2E、FreeBSD 必跑 NFSv4 fixture，以及七种包的解包 smoke、集合/架构/依赖校验、SHA256SUMS 和有门禁的 draft release。

## 最新独立验证

新只读 reviewer 完整审查提交 `40a64fc` 的所有受跟踪生产文件、测试、脚本、workflow、文档、配置和锁文件，并独立取得：

| 项目 | 结果 |
|---|---|
| fmt / Clippy | 通过，Clippy 零警告 |
| Rust 单元 | 45/45 |
| Rust 协议 | 19/19 |
| release CLI E2E | 25/25，0 skip |
| 归档自测 | 3/3 |
| PE 门禁自测 | 6/6 |

未确认新的 must-fix；但最新未提交修复没有完整回归，最终原生 CI 未通过，因此 reviewer 仍为 **NO-SHIP**，主代理同样 **NO-SHIP**。

## 已结束的 C4a CI

- [原生 CI 43849272616](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43849272616)：**failure**。
- [发布预演 43849272672](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43849272672)：**failure**。

主要剩余事项及修复进展：

| 项目 | 现状 |
|---|---|
| Windows 运行目录创建 | 原生两架构推翻旧负向测试的权限假设。工作区已改为从固定父句柄创建、再核对公共路径身份；聚焦 Windows 双架构编译/Clippy 通过，待提交及原生回归 |
| Windows 包的 VCRUNTIME140.dll 依赖 | 静态 CRT 提交 `4b92702` 和 PE 门禁 `8026c65` 已完成；必须重建并验证实际新包 |
| Debian 无系统 CA 时纯 HTTP 客户端构建失败 | 工作区增加仅 http 构建失败时的空根证书回退；无 CA HTTP 成功且自签 HTTPS 仍拒绝的定向测试通过，待完整 26 项 CLI 及打包回归 |
| macOS Intel 握手测试冷启动竞态 | `40a64fc` 已修复，先观察握手阻塞再验证超时；Linux 独立协议回归通过，待 macOS 重跑 |

HTTP 修复没有关闭证书或主机名验证。实际锁定的 reqwest 为 **0.13.5**，独立 reviewer 已核对匹配版本源码。

C4a 已有多项真实成功证据：Windows 协议与 CLI/归档烟测、FreeBSD 全套及额外 NFSv4 用例、Linux 原生容器与真实服务 E2E、macOS ARM 原生和两个 macOS 包均通过。但旧检查点总体失败，这些结果不能替代最终同一冻结提交的全绿。

## 未提交内容

产品修改位于：

- `src/mcp.rs`。
- `src/platform/windows.rs`、`src/platform/tests.rs`。
- `tests/cli_e2e.py`、`tests/fixtures/protocol_server.py`。
- `tests/fixtures/untrusted-test-cert.pem`、`tests/fixtures/untrusted-test-key.pem`，仅用于测试的自签证书材料。

本文件及 `docs/progress-update.md`、`docs/audit-review.md` 的本次状态更新也尚未提交。不得把当前 HEAD 的验证结果表述为覆盖全部这些增量。

## 为什么尚未完成

主代理的后续远端执行调用，包括只读 `git status` 和 `true`，均被工具中止，返回 **`aborted before dispatch`**，没有更详细原因。此前两个旧子代理另收到自动 cybersecurity risk 检查中止。

相关诊断路线已停止，未创建/推送诊断分支；没有通过其他执行器或子代理绕过被中止的执行。可用的文件接口用于读取和保存本次交接记录。执行通道恢复前，无法完成当前差异的提交、全套回归、新冻结检查点 CI 和最终发布。

这不是等待用户再次授权。用户此前已经明确授权推送、CI、发版和 WIP 清理；当前阻断来自执行工具，且规定的发布质量门禁尚未满足。

## 恢复后的准确接续点

1. 复核并提交当前产品修复及状态文档，记录新的冻结 SHA/tree。
2. 完整本地回归及 26 项 CLI E2E，通过后推送 WIP 检查点。
3. 跟踪该提交的全部原生 CI、七包预演，特别核验 Windows 双架构目录回归/静态 CRT 实际产物、Debian 无 CA、macOS 握手和 FreeBSD fixture。
4. 主代理与独立只读 reviewer 对同一最终冻结全树各自给出 SHIP。
5. 才能推送 main、正式 v0.1.0，完成正式发布 CI 和七包/校验和核验、发布 Release，最后清理 WIP 分支及工作树。

详细进度和证据位置见 [docs/progress-update.md](docs/progress-update.md)。
