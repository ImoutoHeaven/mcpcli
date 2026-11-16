# mcpcli 0.1.0 工作进度

更新时间：2026-11-16 11:45 UTC。

## 当前状态

**NO-SHIP，等待 C6 最终全平台验收。** R14 已修复并集成；C6 在 C5 基础上只包含该修复、对应回归和三份状态记录。main、正式 v0.1.0、Release 和 WIP 清理等待同一提交双 SHIP 及全绿门禁。

| 项目 | 当前值 |
|---|---|
| 已通过 CI 的检查点 C5 | `196e0baeec7ceaa3dac75e1690579064b0917fde` |
| Tree | `386421b8e8cc83d9a1476f2792466ed912453c40` |
| 集成分支 | `wip/release-0.1.0` |
| 远程检查点分支 | `wip/release-checkpoint` |
| 原生 CI | [43856257333](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43856257333)，success |
| 七包预演 | [43856257349](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43856257349)，success |

C5 已提交 HTTP 客户端、Windows 目录创建及对应回归。R14 集成提交为 `c74b61ec7576027b856783e286192c21f28ed902`；包含本记录的下一提交为 C6，使用新的 CI 运行验收，C5 结果只作为已完成证据。执行通道当前可用。

## 工作安排

- 主代理在 `/root/mcpcli` 负责集成、全树复核、进度记录和发布门禁。
- 实现代理 `gemini-4-hydrogen:xhigh` 在 `/root/mcpcli-worktrees/pi-impl-c5` 跟踪 CI、核对产物和处理必要修复。
- 独立只读 reviewer `gpt-6.1-sol:max` 在 `/root/mcpcli-worktrees/pi-review-c5` 审查完整受跟踪树。

原生 CI 已覆盖 Ubuntu、Alpine/Debian 双架构、FreeBSD、macOS 双架构、Windows 双架构，以及真实 fastexec/Playwright stdio 和 HTTP E2E。七包预演另验证打包、解包运行、架构、运行库依赖及 SHA256SUMS。

## 发布门禁

1. 冻结包含全部必要改动的最终提交。
2. 主代理和独立 reviewer 对同一完整树各自 SHIP。
3. 同一版本的原生 CI 和七包预演全部成功。
4. 推送 main 和正式 v0.1.0，跟踪正式发布 CI。
5. 核验正式七包和校验和，发布 Release，再清理 WIP 分支及工作树。

主代理和独立 reviewer 均完成 C5 全树阅读，并各自通过隔离 Alpine 回归：fmt、Clippy、45 单元、19 协议、26 CLI（0 skip）、3 归档和 6 Windows 依赖门禁自测。两人均确认 R14：rmcp 的 DELETE 适配器通过 error_for_status 接受 3xx，不能据此确认会话删除。实现代理已完成精确状态检查及重定向回归：新增断言在修复前失败，修复后 fmt/Clippy、45 单元、19 协议、26 CLI（0 skip）、3 归档和 6 Windows 依赖自测通过。主代理已复核补丁与调用方。C6 仍须重新取得全平台原生、真实服务 E2E、七包预演和双方最终结论。问题依据见 [audit-review.md](audit-review.md)。
