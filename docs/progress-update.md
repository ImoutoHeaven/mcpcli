# mcpcli 0.1.0 工作进度

更新时间：2026-11-16 12:03 UTC。

## 当前状态

**NO-SHIP，等待 C7 最终门禁。** C7 在 C6 上只增加进程观察辅助函数的 ESRCH 修复、4 项回归及状态记录，生产代码与 C6 完全一致。main、正式 v0.1.0、Release 和 WIP 清理等待同一冻结提交的双方 SHIP、原生 CI 与七包预演全绿。

| 检查点 | 提交 | 验收结果 |
|---|---|---|
| C5 | `196e0baeec7ceaa3dac75e1690579064b0917fde` | [原生 43856257333](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43856257333)、[七包 43856257349](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43856257349) 成功；独立审查发现 R14 |
| C6 | `3abf3dc7ef0505919878fcc6b7633891128c5ea8` | [七包 43860523488](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43860523488) 成功；[原生 43860523244](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43860523244) 因 R15 测试竞态失败 |
| R15 集成 | `3e75a3fa3fcc03eb7ca4a077650186881e2e0b75` | 辅助函数及 4 项回归已提交；含本记录的下一提交冻结为 C7 |

R14 集成提交 `c74b61ec7576027b856783e286192c21f28ed902` 修正 HTTP DELETE 对 3xx 的成功误判。R15 将读取刚退出 Linux 进程的 ESRCH 与 ENOENT 一同视为已退出；权限、I/O 和解析错误仍失败。两项回归均有修复前失败、修复后通过的证据。

## 审查与验证

- 主代理和独立只读 reviewer 已完成 C5 全树阅读，逐项核对 C6 增量，并各自通过隔离 Alpine 的 fmt、Clippy、45 单元、19 协议、26 CLI（0 skip）、3 归档和 6 Windows 依赖门禁自测。
- reviewer 已确认 R14 最小修复；主代理另使用 C6 实际 musl 发布包在 Debian 12 容器通过解包 smoke 5/5、完整 CLI 26/26、归档 3/3、Windows 依赖门禁 6/6。
- 实现代理在 R15 修复后通过 fmt、Clippy、完整 runner（45 单元、19 协议、4 Python、26 CLI，0 skip）、归档 3/3、Windows 依赖门禁 6/6；主代理另在只读源容器独立通过 4 项 Python 回归。
- C7 必须独立取得全部原生 CI、真实 fastexec/Playwright E2E、七包预演和完整树最终审查结论；已有绿色结果不替代这一门禁。

## 工作安排

主代理在 `/root/mcpcli` 集成并管理门禁。实现代理 `gemini-4-hydrogen:xhigh` 在 `/root/mcpcli-worktrees/pi-impl-c5` 工作；只读 reviewer `gpt-6.1-sol:max` 在 `/root/mcpcli-worktrees/pi-review-c5` 独立审查。集成分支为 `wip/release-0.1.0`，远程检查点分支为 `wip/release-checkpoint`。

## 发布顺序

冻结完整树并取得双方 SHIP及全部绿色门禁后，推送 main 和 v0.1.0。随后跟踪正式发布 CI，核验七包及 SHA256SUMS，发布 draft Release，最后清理 WIP。清理前保存旧引用和需保留的验证记录，不丢弃未归档改动。

问题依据见 [audit-review.md](audit-review.md)，验收摘要见 [../final-reply.md](../final-reply.md)。
