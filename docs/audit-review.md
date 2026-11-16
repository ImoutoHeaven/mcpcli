# 0.1.0 提交与全仓审查

## 基线

- Commit：`285b0f19c85ef6deba34991baa131be6365a76e4`
- Tree：`b306a4908bb16b2e6cd00f9b362b0536762787b8`
- 父提交：`315a2467381a917c1ca7687450b8d585a03c634e`
- 负载假设：同用户多个 CLI 并发访问一个配置/会话下的 daemon；MCP 服务由用户配置，HTTP 对端、技能内容和命令输入不可假定总是正确。
- 独立 reviewer 已阅读全部 tracked 生产代码、测试、构建与文档以及相关 rmcp 3.9.0 源码。其角色只读，未修改生产代码。
- 基线双方结论：主代理 **NO-SHIP**；只读 reviewer **NO-SHIP**。

## 必须修复

| ID | 严重性 | 来源 | 可复现问题 | 修复与验收要求 |
|---|---|---|---|---|
| R1 | High | 285b0f1 的前置连接门控引入 | 占满 32 个已连接但未提交帧的 socket 后，`--timeout 1 call s increment` 在约 4.852 秒后成功并产生副作用。`daemon::handle` 在 accept 后重新计算预算，`runtime::exchange` 额外增加 5 秒。 | 请求携带同主机单调时钟的绝对截止时间；client 和 daemon 都遵守原截止时间；排队耗尽预算时零派发。 |
| R2 | Medium | 既有问题；该提交仅修复 POST | GET SSE 合法 `TEXT/EVENT-STREAM; charset=UTF-8` 超时，非法 `text/event-stream-INVALID` 被接受。SDK GET 默认代码采用大小写敏感前缀；3xx SSE 响应也可能被解析。 | GET 使用精确、大小写无关的 MIME essence 检查；要求成功状态；保留单事件内存上限、401/403 与 URL 脱敏。增加真正经 GET 返回工具结果的测试。 |
| R3 | Medium | 既有问题 | `--oneshot` 工具返回成功后，DELETE 清理阻塞期间 SIGINT 导致 stdout 为空、exit 130、`outcome_unknown:true`，丢失已知结果。 | 将已知结果保留至输出；中断仅终止清理等待并单独报告，已知调用结果的 uncertainty 为 false。 |

## 已独立核验的历史修复

`reset --force` 与 restart 清理恢复、0711 祖先目录穿透、CRLF/空文本输出、公开文档自洽、共享保留头列表、残留状态临时文件替换、配置结构错误脱敏诊断均已实际实现。并发超时与 HTTP MIME 的覆盖仍不完整，分别见 R1、R2。

## 基线独立验证

- 主代理：`cargo fmt --all -- --check`、`cargo clippy --locked --all-targets -- -D warnings`、`cargo test --locked --all-targets` 通过；38 单元 + 18 协议。
- reviewer：独立 `cargo test --locked --all-targets` 通过同样 56 项；`tests/cli_e2e.py` 20/20。
- reviewer 额外复现 R1/R2/R3；这些缺陷未被已有绿色测试覆盖。
- reviewer 证据位于工作主机 `/root/mcpcli-reviewer-artifacts/`；最终验收将以仓库内测试和 GitHub CI 为持续可复现证据。

## 跨平台审查范围

移除 Linux-only 编译门禁及 `/proc/self/exe`、`/proc/self/fd`、固定 socket 路径长度、Linux-only `prctl`/`O_PATH` 等假设；原生 Windows named pipe/SID/DACL、文件锁、Job 生命周期；Unix descriptor 相对安全遍历；所有承诺平台的原生测试、CLI E2E 和打包后 smoke。

## 最终冻结审查

尚未开始；当前任何局部绿色结果均不构成最终 SHIP 或发布授权门禁通过。
