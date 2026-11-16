# mcpcli 0.1.0 工作进度

## 本轮接续状态（2026-11-16 19:15，Asia/Shanghai）

执行通道已经恢复：仓库读取、git、gh 和格式检查均可执行。当前仍为 **NO-SHIP**，正式 main/tag/release 和 WIP 清理尚未开始。

已核对本地 HEAD `40a64fcdcfc2e75937d266744a64f944ae0f5c0c` 与交接一致；HTTP 客户端和 Windows 目录创建修复仍在工作区。下一步将这些增量冻结为 C5，启动独立工作树的只读全仓审查、平台复核、CI/七包预演和完整本地回归。

当前远程 main 仍为 `315a2467381a917c1ca7687450b8d585a03c634e`，远程尚无正式标签。用户已有的推送和发布授权继续有效；保留同一冻结版本的双 SHIP、全树审查和 CI 全绿门禁。

以下保留上一轮交接记录，历史阻断状态以本节为准。

---

更新时间：2026-11-16 19:02（Asia/Shanghai）

## 当前结论

**NO-SHIP。尚未完成发布门禁，未进入 Step 4。**

主代理和独立只读 reviewer 均未给予最终 SHIP。已完成基线 commit 和全仓审查、大部分问题修复及跨平台/CI 实现；最新两项产品修复仍在工作区，尚未提交并完成原生 CI。主分支未推送，正式标签和 Release 未发布，WIP 分支及工作树未清理。

## 目标与不可省略的门禁

目标：审查 `285b0f19c85ef6deba34991baa131be6365a76e4` 及全仓；修复 introduced/preexisting 问题；干净支持 Alpine、Debian、FreeBSD、macOS、Windows x64/ARM64；建立原生、E2E 和七包发版 CI。

最终顺序：提交并冻结完整树 → 主代理与独立只读 reviewer 审查同一 SHA/tree 并同时 SHIP → 该版本全部 CI/七包预演通过 → 推送 main 和正式 v0.1.0 → 正式发版工作流通过、核验产物并发布 → 清理 WIP。新代码改动会使旧冻结审查失效。用户已授权这些操作，不需要重新索取授权。

## 精确版本

| 项目 | 值 |
|---|---|
| 仓库 | https://github.com/ImoutoHeaven/mcpcli |
| 初始待审查提交 | `285b0f19c85ef6deba34991baa131be6365a76e4` |
| 初始 origin/main | `315a2467381a917c1ca7687450b8d585a03c634e` |
| 当前本地分支 | `wip/release-0.1.0` |
| 当前本地 HEAD | `40a64fcdcfc2e75937d266744a64f944ae0f5c0c` |
| 当前本地 HEAD 的 tree | `effabe8e71bcd9a05749f447c11fc35f56095127` |
| 最新已推送的冻结检查点 C4a | `b55d053c8611b34148255a19e27587d09fb22931` |
| C4a tree | `7f7bd7b2264b4a81417956fff59b7726e95f192f` |
| C4a 远程分支 | `wip/release-checkpoint` |

上述本地 HEAD 不包含最后的 HTTP 与 Windows 目录创建修复，也不包含本次状态文档更新。旧本地 v0.1.0 标签未替换，不能将其视为本次发布。

## 独立工作树与职责

| 角色 | 工作树 | 状态 |
|---|---|---|
| 主代理 | `/root/mcpcli` | 集成、核心修复、冻结审查、最终门禁 |
| 平台子代理 | `/root/mcpcli-worktrees/platform-20261116` | 平台提交已集成；后续诊断被自动风险检查中止 |
| CI 子代理 | `/root/mcpcli-worktrees/ci-20261116` | CI/E2E/归档提交已集成；已交接 |
| 原只读 reviewer | `/root/mcpcli-worktrees/reviewer-20261116` | C4a NO-SHIP；后续调用被自动风险检查中止 |
| 新独立只读 reviewer | `/root/mcpcli-worktrees/release-review-20261116` | 完整审查 HEAD 40a64fc；维持 NO-SHIP |

reviewer 未修改生产受跟踪文件、提交、推送或发版。子代理没有替主代理重试被中止的执行命令。

## 已完成的审查与修复

审查记录见 [audit-review.md](audit-review.md)。确认并处理了绝对请求截止时间、GET SSE MIME/状态/事件大小限制、oneshot 已知结果和错误路径清理、技能根目录及逐组件固定句柄、macOS/FreeBSD ACL、Windows 线程归属与标准句柄继承、发布归档集合等问题。

新增平台层覆盖 Unix descriptor 和 Windows named pipe/SID/DACL/Job；移除 Linux-only 假设。配置、公开文档、平台测试和跨平台错误路径一并重构。用户要求保留的 .gitignore 规则未变。

两个 workflow 已接入原生平台测试、便携 CLI E2E、真实 MCP 服务 E2E、FreeBSD 必跑 NFSv4 ACL、七种发布包解包 smoke、严格产物集合与 SHA256SUMS。发布前必须全部成功，失败不会创建 release bundle/draft。

## C4a 实际 CI 结果

- [原生 CI 43849272616](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43849272616)：**failure**。
- [发布预演 43849272672](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43849272672)：**failure**。
- 两个 run 均已自然结束。bundle 和 draft release 正确跳过；没有将部分成功当作门禁通过。

| 发现 | C4a 证据 | 当前处理 |
|---|---|---|
| Windows 祖先目录权限假设不足 | x64/ARM64 的同一负向测试失败 | 当前工作区改为固定父目录句柄、相对句柄创建并核对发布路径身份；未提交、未原生验收 |
| Windows 包依赖外置 VC runtime | 两个 EXE 均导入 VCRUNTIME140.dll | 静态 CRT 已提交 `4b92702`；PE 架构及普通/延迟导入门禁已提交 `8026c65`；等待新产物 |
| 无 CA 的 Debian 无法创建纯 HTTP client | Linux 两架构包在 Debian 的第五项 smoke 失败 | 当前工作区只对 http 目标增加空信任集合构建回退；新增 HTTP 成功/TLS 仍拒绝用例已定向通过；未提交 |
| macOS Intel 握手夹具冷启动竞态 | 旧 150ms 测试在 PID 出现前读取 | 已提交 `40a64fc`，先观察握手阻塞再验证超时清理；等待 macOS 重跑 |

已取得的 C4a 原生证据：Windows 各 48 项单测通过、1 项失败，19 项协议通过，CLI 各 23 通过和 2 项 Unix 专属 skip；两个 Windows 包各五项 smoke 通过。FreeBSD 46 单元、19 协议、25 CLI、额外 NFSv4 fixture 及发布包全部通过。Ubuntu、Alpine/Debian 原生容器及真实服务 E2E 通过；macOS ARM 原生及两个 macOS 发布包通过。以上均是旧检查点证据，不能替代最后版本重验。

## 当前未提交修复和已做验证

未提交产品文件：`src/mcp.rs`、`src/platform/windows.rs`、`src/platform/tests.rs`、`tests/cli_e2e.py`、`tests/fixtures/protocol_server.py`，以及两个仅供 TLS 测试的 PEM fixture。

HTTP 修复保留默认客户端构建；仅 http 目标在构建失败时使用显式空根证书集合，不关闭 TLS 证书或主机名验证。实际 Cargo.lock 锁定 reqwest **0.13.5**，新 reviewer 已按匹配源码复核。新定向 E2E 1/1 通过：无 CA 的纯 HTTP 调用成功，自签 HTTPS 仍拒绝。此用例使完整 CLI 集合增至 26 项，尚未跑完新全套。

Windows 修复让运行目录创建基于已验证父句柄，完成后重开公共路径核对文件身份；不再依赖原测试中已经被原生反例推翻的权限假设。Windows x64/ARM64 聚焦平台编译及 Clippy 均通过，尚未完成原生执行。

最新独立 reviewer 在完整提交 `40a64fc` 上通过 fmt、Clippy、45/45 单元、19/19 协议、25/25 CLI（0 skip）、归档自测 3/3、PE 自测 6/6，并读完全部受跟踪文件。没有确认新的 must-fix，但它只对未提交增量做了源码预审，不能给予最终 SHIP。

## 执行阻断

主代理后续远端 exec 调用（包括只读 git status 和 true）均返回 `aborted before dispatch`，未给出更具体原因。另两个旧子代理收到自动 cybersecurity risk 检查中止。涉及的诊断路线已停止，没有创建或推送诊断分支，没有借其他执行器或子代理代跑被中止的命令。

文件读取仍可用。当前通过文件接口保存交接记录；这些文档修改也尚未提交。无法继续执行提交、完整回归、新 CI 推送、发版和清理。

## 恢复执行后的接续点

1. 检查当前差异，完成 HTTP/Windows 修复及文档提交，记录新的冻结 SHA/tree。
2. 完整本地回归含 26 项 CLI，随后推送 WIP 检查点并跟踪原生 CI 和七包预演。
3. 必须看到 Windows 两架构运行目录回归、静态 CRT 实际 PE 产物检查、Debian 无 CA 解包 smoke、macOS 握手，以及 FreeBSD 必跑 fixture 全绿。
4. 主代理和独立只读 reviewer 对同一最终冻结全树同时给出 SHIP。
5. 才能推送 main、替换旧本地标签并推送正式 v0.1.0；跟踪正式发布 CI，验证七包和校验和后发布，最后清理 WIP。

## 证据位置

- 主代理：`/root/mcpcli-validation/`。
- 原 reviewer：`/root/mcpcli-reviewer-artifacts/`。
- CI 和产物：`/root/mc-ci-tmp/`。
- 新 reviewer 回归日志：`/tmp/fastexec-0/ns/efcdf4fd170e1e33/logs/3hf0ty48n5ds0w2j39p1ysbm8m-xm.log`。
