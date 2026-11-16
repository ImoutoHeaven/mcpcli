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

## 重构与补充审查

| ID | 严重性 | 来源 | 问题与修复 |
|---|---|---|---|
| R4 | Medium | 新增 Windows 实现的中间检查点 | 线程快照中的 TID 在 OpenThread 前可能被复用。现在打开线程后用 GetProcessIdOfThread 验证实际归属，再恢复线程；原生测试拒绝错误归属。 |
| R5 | Medium | 既有技能目录访问 | symlink_metadata 后、canonicalize 前替换根目录可使外部内容被当作挂载技能读取。reviewer 的 realpath 注入实测返回 `outside root secret`。现在先固定原始目录句柄，规范化后重开并核对 inode/file ID；同时修复 `link/` 和 `link/.` 绕过 final-component NOFOLLOW 的变体，测试覆盖 Unix 与 Windows 分隔符。 |
| R6 | Medium | 新增 macOS/FreeBSD 实现的中间检查点 | 只检查祖先模式位会漏掉额外 ACL 修改授权。现在检查 native ACL 的 mutation grant，允许无害 deny/read 权限；macOS 原生测试和 FreeBSD 专用 NFSv4 文件系统测试必须执行。 |
| R7 | Medium | 新增 Release CI 的中间检查点 | `release-*` 同时匹配旧日志和聚合包，失败重跑可污染严格七包集合。改为 `release-archive-*`，证据与聚合包分离，并明确替换本次 run 的旧同名产物。 |
| R8 | Medium | R3 修复后的 C2 检查点 | oneshot inspect 在工具不存在时，经 `?` 提前返回会丢弃连接；reviewer 5 次独立复现中 4 次缺少 DELETE，基线与合法 inspect 对照均正常。现在将元数据转换错误连同连接交给统一输出/清理路径；保留 unknown_tool 和退出码 2，清理失败仅追加诊断。 |

此外完成 Windows 命令路径解析、drive-relative 路径拒绝、Windows 环境变量名大小写碰撞拒绝、原生 endpoint 身份稳定化，以及过期请求连接/写帧前短路。这些属于新增平台必须补齐的行为。

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

C2：`cf62d0ff93ee3117a00fb4e1c48c336d42f4e964`，tree `d9f25083d23caba46dc0490eecb37f81d2f533ef`。主代理和 reviewer 各自独立通过 fmt、Clippy、45 单元 + 19 协议 + 23 CLI E2E；reviewer 独立确认技能 root 竞态和尾缀 symlink 两个外部复现均已被拒绝。其后发现 R8，因此双方仍为 **NO-SHIP**。

真实平台 CI 另发现并修复 Windows SYSTEM 测试临时目录/DACL 句柄/CRLF 夹具、macOS HTTP 测试服务器反向 DNS 启动阻塞，以及 Debian Python 3.11 解包 API 差异。这些测试和打包修复均须纳入下一冻结提交重新验收。

最终冻结 SHA 和双方 SHIP 将在原生 CI 与完整回归完成后记录；当前任何局部绿色结果均不构成发布门禁通过。
