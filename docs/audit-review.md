# 0.1.0 提交与全仓审查

## 当前验收状态

C5 `196e0baeec7ceaa3dac75e1690579064b0917fde`（tree `386421b8e8cc83d9a1476f2792466ed912453c40`）已纳入 R9、R12、R13 的最终修复及测试。以下各检查点记录属于历史证据，其中“工作区未提交”“执行阻断”不代表当前状态。

主代理和独立只读 reviewer 均已复核完整受跟踪树，并各自通过隔离 Alpine 的 fmt、Clippy、45 单元、19 协议、26 CLI（0 skip）、3 归档及 6 Windows 依赖检查自测。原生 CI [43856257333](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43856257333) 与七包预演 [43856257349](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43856257349) 均成功。依赖交叉核对确认 R14，双方维持 **NO-SHIP**，等待修复及新冻结提交全部门禁。

### R14：DELETE 重定向被误记为成功清理

级别 Medium；来源为既有 HTTP 清理适配。`BoundedHttp::delete_session` 委托 rmcp 3.9.0 的 reqwest 实现，后者调用 `Response::error_for_status`；reqwest 0.13.5 仅将 4xx/5xx 转为错误。因此关闭重定向跟随时，DELETE 307 仍返回成功，reset/stop 可错误确认清理。修复要求：自行检查 DELETE 响应，仅接受 2xx 和协议允许的 404/405，保留两秒期限、错误脱敏和清理记录；回归验证 307 报告清理失败、保留已知调用结果且不跟随重定向。修复已集成为 `c74b61ec7576027b856783e286192c21f28ed902`；实现代理在修复前确认两条回归失败，修复后 fmt/Clippy、45 单元、19 协议、26 CLI（0 skip）、3 归档及 6 Windows 依赖自测通过。主代理已复核补丁及调用方；最终结论取决于包含状态记录的 C6 全树复核和全平台 CI。

### R15：Linux 测试进程观察遗漏 ESRCH

C6 `3abf3dc7ef0505919878fcc6b7633891128c5ea8` 的直接原生 CI 在 Alpine amd64 读取刚退出进程的 `/proc/PID/stat` 时收到 ProcessLookupError；辅助函数只捕获 FileNotFoundError，导致正确退出被报为测试失败。同一提交的七包预演全部通过，不能代替修复这个已确认的测试竞态。

测试修复已集成为 `3e75a3fa3fcc03eb7ca4a077650186881e2e0b75`：仅 ENOENT/ESRCH 视为已退出，其他观察错误继续失败。4 项 Python 回归覆盖真实当前进程、消失状态、权限/I/O 错误传播和 zombie/live 解析，并接入原生 runner。修复前 ESRCH 用例失败，修复后完整本地 runner、只读 Alpine 与主代理 Debian 容器的辅助回归均通过。生产代码无变化。

当前 C7 等待同一冻结提交的双方最终 SHIP、原生 CI 和七包预演。当前版本和后续验收步骤见 [progress-update.md](progress-update.md)。

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
| R9 | Medium | 新增 Windows 实现的 C2 检查点 | 祖先 DACL 检查和固定目录句柄不足以保证按公共路径创建的目标身份。初始修复仅补充 mutation grant 拒绝；C4a 两个 Windows 原生 runner 随后推翻了原测试对另一权限分支的安全假设。当前工作区改为基于已验证父句柄相对创建，并重开公共路径核对身份；未提交，尚待双架构原生验收。 |
| R10 | Medium | 新增 Windows 技能遍历实现 | 技能路径在每层检查后仍按完整路径打开下一层，允许已固定的空目录被原地转换为 junction 后访问外部资源。改为 NtCreateFile + RootDirectory 按句柄解析单个分量；枚举继续基于 GetFileInformationByHandleEx。确定性回归覆盖文件、嵌套目录及已打开目录的转换。 |
| R11 | High | 新增 Windows daemon 启动实现 | Rust 1.92 默认继承全部可继承句柄；重定向新子进程 stdio 不会排除 CLI 原始捕获管道，daemon 会延长它们的生命期。两架构 E2E 均在首次 persistent 调用等待输出 EOF 时挂住。main 在任何线程/子进程启动前清除原始三个标准句柄的继承标志；显式配置的子进程 stdio 仍由标准库复制并传入。 |

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

真实平台 CI 另发现并修复 Windows SYSTEM 测试临时目录/DACL 句柄/CRLF 夹具、macOS HTTP 测试服务器反向 DNS 启动阻塞，以及 Debian Python 3.11 解包 API 差异。这些测试和打包修复已纳入 C3 候选，须在新冻结提交重新验收。FreeBSD 的必跑 NFSv4 回归本身通过，另修正 mdconfig 尾随空格导致的夹具清理失败。

R8 独立复验：reviewer 重建 `4ec42aa9c74fc2acdf60c836bce6fd4f990ecec8`，5 次正常缺失工具查询、HTTP 500 清理、DELETE 超时、清理中断共 8 项均保留原错误且恰好发送一次 DELETE；主代理新增后 CLI 25/25 通过。

R9 历史源码依据：[Microsoft MS-FSA §2.1.5.10.37](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/4aeefef8-92c3-4abc-af7a-a610caf8a165)。C4a 原生执行说明仅凭原先的权限推断不足以通过验收；后续修复改为固定父句柄创建和路径身份校验。相关 API 依据：[NtCreateFile](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntcreatefile)。必须以新冻结版本的双架构原生结果为门禁。

C3：`9626d98ba14e004bb3d98b95876df4ec5316234c`，tree `72180159f5af0f4419942874fff24db928bc0772`。主代理和 reviewer 均独立通过 fmt、Clippy、45 单元 + 19 协议 + 25 CLI E2E，归档安全自测 3/3；因随后确认 R10/R11，C3 未推送，双方 **NO-SHIP**。

C2 Windows 的可复查证据确认 x64、ARM64 各 47 单元 + 19 协议全过，包含 DACL、默认用户目录、Job breakaway、线程所有权；CLI 卡在第一个持久 daemon 场景，不能当作平台验收通过。旧预演及直接 CI 已取消以释放 runner，完成的证据已归档。

R11 源码依据：[Rust 1.92 Windows Command 实现](https://github.com/rust-lang/rust/blob/1.92.0/library/std/src/sys/pal/windows/process.rs)。测试 harness 同时修复 Windows subprocess.run 在超时 kill 后无界等待 EOF 的诊断缺口；继续通过管道捕获输出验证产品行为。

最终冻结 SHA 和双方 SHIP 将在原生 CI 与完整回归完成后记录；当前任何局部绿色结果均不构成发布门禁通过。

## C4a 原生与产物审查补充

更新时间：2026-11-16 19:02（Asia/Shanghai）。

C4a 为 `b55d053c8611b34148255a19e27587d09fb22931`，tree `7f7bd7b2264b4a81417956fff59b7726e95f192f`。主代理与只读 reviewer 均为 **NO-SHIP**。

- [原生 CI 43849272616](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43849272616)：failure。
- [发布预演 43849272672](https://github.com/ImoutoHeaven/mcpcli/actions/runs/43849272672)：failure。
- run 均已结束，bundle 和 draft release 正确未获放行。

| ID | 严重性 | 来源 | 发现及当前修复状态 |
|---|---|---|---|
| R12 | Medium | 新增 Windows 发布包 | 两个 EXE 导入 VCRUNTIME140.dll，但归档未提供运行库；runner 自带运行库掩盖交付缺陷。已提交 MSVC 静态 CRT 配置 `4b92702` 和 PE 架构、普通/延迟导入门禁 `8026c65`。6 项 PE 自测通过，旧 mcpcli 两包被正确拒绝，仍待新的实际产物验证。 |
| R13 | Medium | 既有 HTTP 客户端构建路径 | 在无系统 CA 的 Debian 精简环境，即使目标为 http，默认 TLS verifier 构建也失败。当前工作区仅在默认构建失败且目标为 http 时用显式空根证书集合重建，保留严格 TLS 验证。定向 E2E 验证纯 HTTP 成功、自签 HTTPS 仍拒绝；未提交且尚未全套回归。 |

R9 新修复已通过 Windows x64/ARM64 聚焦平台编译和 Clippy；当前仅源码预审通过，不能等同于原生执行。R10 固定句柄技能遍历回归在 C4a 两个 Windows runner 均通过。R11 修复后两个架构的真实 PIPE CLI 调用能结束，持久 daemon 继续工作，CLI 各 23 通过加 2 个 Unix 专属 skip；协议各 19 通过，归档 smoke 各 5/5。这些成功没有消除同一 run 的其他失败。

FreeBSD 的 46 单元、19 协议、25 CLI、额外必跑 NFSv4 fixture 及清理、发布包均通过。Linux 原生容器和真实服务 E2E 通过。macOS Intel 的旧 150ms 握手 fixture 有冷启动竞态；`40a64fc` 已改为先观察实际握手阻塞再断言超时与子进程退出，待原生重跑。

## 最新独立全树审查与未完成门禁

新只读 reviewer 在隔离工作树完整审查：

- SHA：`40a64fcdcfc2e75937d266744a64f944ae0f5c0c`。
- Tree：`effabe8e71bcd9a05749f447c11fc35f56095127`。
- 覆盖全部受跟踪 Rust、测试、脚本、workflow、锁文件、配置、文档和示例。
- 独立 fmt/Clippy、45 单元、19 协议、25 CLI（0 skip）、3 归档自测、6 PE 自测全部通过。
- 没有确认新的 must-fix 或 should-fix；但 HTTP/Windows 未提交增量仅完成源码预审，最终原生及七包验证未完成，明确 **NO-SHIP**。

实际 Cargo.lock 的 reqwest 是 **0.13.5**，声明中的 0.13.3 并非精确锁定。新 reviewer 已核对 0.13.5 的 tls_certs_only(empty) 源码：显式空根证书集合保持证书及主机名验证，不是跳过验证。

主代理执行通道随后连只读 git status 都返回 `aborted before dispatch`，未给出更详细原因。此前两个旧子代理的诊断调用被自动 cybersecurity risk 检查中止；对应诊断路线已停止，未创建或推送诊断分支，未通过其他代理代跑被中止命令。

当前 HTTP/Windows 修复及本次文档更新尚未提交。main、正式 v0.1.0、Release 和 WIP 清理均未推进。恢复后必须冻结完整树、同 SHA 的全平台 CI/七包全绿、双方全树 SHIP，才可进入 Step 4。
