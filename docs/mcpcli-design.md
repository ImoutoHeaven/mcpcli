# mcpcli 0.1.0 design

## 1. Product contract

mcpcli is a standalone Linux command-line client for configured MCP tools and
local Agent Skills. A short-lived CLI process can discover capabilities, inspect
one tool, call it, or read one skill resource. A local daemon retains connections
and application state between CLI processes.

Version 0.1.0 supports stdio and Streamable HTTP, including the modern
2026-07-28 protocol and legacy lifecycle negotiation through rmcp 3.9.0. The
preferred modern version is 2026-07-28; the requested legacy version is
2025-11-25. Compatibility with older revisions follows the SDK's negotiation.
Only tools are exposed. Resources, prompts, sampling, roots, interactive
elicitation/MRTR, asynchronous Tasks, OAuth login, the old two-endpoint
HTTP+SSE transport, and an outbound MCP-server mode are outside this release.
The client does not advertise unsupported capabilities.

The supported platform is Linux with /proc and Unix domain sockets. The tested
release target is x86_64-unknown-linux-gnu. This is a native executable; it does
not need Python or Node to run. A configured MCP server may need its own runtime.
This client is a trusted-command runner, not a security sandbox.

## 2. Why a daemon

The modern MCP specification is no longer intrinsically a connection-scoped
initialize/session protocol. Application state still matters: a stdio process
owns its browser, tasks, and in-memory data, and legacy servers can require
initialized sessions. HTTP servers may also keep application state independent
of protocol sessions.

One daemon owns both transports. There is no separate HTTP session-id file mode:
an ID alone does not retain notifications, request routing, an HTTP stream,
protocol negotiation, or application semantics.

The daemon starts with zero downstream processes. A server is opened on its first
tools, inspect, or call request. Connections stay alive until reset, daemon stop,
a signal, or process failure. There is no automatic idle eviction, because
eviction can destroy state that a later CLI command expects. --oneshot provides
an explicit connection that is closed after a single command.

Session identity is the canonical configuration-file path plus --session NAME.
Different names have separate daemons and stdio processes. This controls client
identity; it cannot isolate shared remote application data or a browser profile
that two independently configured servers intentionally share.

## 3. Command and disclosure model

| Command | Work performed | Starts an MCP connection |
| --- | --- | --- |
| catalog | Configured server descriptions and skill metadata | No |
| search QUERY | Offline substring search of those names/descriptions | No |
| tools SERVER | Fresh names and bounded description summaries | Yes, if needed |
| inspect SERVER TOOL | Full tool metadata and input/output schemas | Yes, if needed |
| call SERVER TOOL | One tool invocation | Yes, if needed |
| skills list | Skill metadata and validation warnings | No |
| skills show NAME | Complete SKILL.md plus its base directory | No |
| skills files NAME | Relative resource paths, without file contents | No |
| skills read NAME PATH | One bounded UTF-8 resource | No |
| reset [--force] SERVER | Close that connection and discard its client state | No new connection |
| daemon start/status/stop/restart/run | Manage the selected daemon | No downstream connection |

There is no startup scan of all MCP servers. Tool schemas are fetched only for
the selected server. Every tools and inspect command fetches the current list;
every call also performs fresh discovery before dispatch. This enables modern
HTTP x-mcp-header mapping and prevents invoking a tool that the SDK filtered
out or the server withdrew. Pagination, discovery, and dispatch share one
operation budget.

There is no persistent schema cache. The SDK response cache is disabled. The SDK
still retains internal per-tool metadata for HTTP header mapping; fresh
discovery and current-list membership checks govern dispatch. The persistent
connection can handle legacy or unsolicited list-change notifications. Modern
subscription-based notifications are not subscribed to in 0.1.0. Reading the
current list does not depend on a notification or a previously cached flag.

A full JSON Schema is not a general command-line flag grammar. Synthesizing CLI
flags from arbitrary schemas introduces name-normalization collisions (such as
`fooBar` versus `foo_bar`), ambiguous union or array coercion, nested-object
escaping hazards, and brittle behavior when unknown properties appear. The CLI
therefore uses one explicit JSON-object input, supplied by exactly one of
--args JSON, --args-file PATH, or --stdin. An omitted input is {}.

## 4. Configuration and environment

Configuration is JSON. The default path is ./mcpcli.json; --config and
MCPCLI_CONFIG select another file. The CLI does not search ancestor projects or
implicitly execute commands from another application's configuration.

The version field defaults to 1. servers is a map of aliases to tagged transport
definitions. skills is a list of explicit directory paths. mcpServers and
skillDirs are accepted aliases for those two top-level fields; other unknown
configuration fields are rejected.

| Transport | Fields |
| --- | --- |
| stdio | transport: stdio; command; optional args, env, cwd, description |
| HTTP | transport: http; url; optional headers, description |

Paths resolve from the configuration file's directory. A command containing a
slash is resolved there; an ordinary command name uses the daemon's PATH.
The default server cwd is the configuration directory. Arguments are passed
directly to the executable, without shell evaluation.

Only complete env/header values of the form ${NAME} are expanded. Missing or
non-UTF-8 variables are errors. There is no expansion in command arguments and
no recursive interpolation. HTTP URLs cannot contain userinfo or fragments.
Transport-owned headers (including Host, Content-Length, Content-Type, Accept,
Connection, Transfer-Encoding, Upgrade, TE, Trailer, Last-Event-Id, and Mcp-*
headers) cannot be overridden. Headers are checked case-insensitively at both
configuration load and connection setup; values are validated without echoing
credentials. Configuration shape errors report field paths and value kinds
without printing rejected scalar values.

The daemon inherits its launch environment and applies each server's explicit
overrides. This environment is fixed until restart. The effective configuration,
including explicitly expanded env/header values and resolved paths, has a
SHA-256 fingerprint. Requests with a different fingerprint are rejected rather
than silently using stale configuration. Changes to implicit inherited
environment require an explicit restart.

Server and session aliases contain 1-64 ASCII letters, digits, underscores, or
hyphens. Configuration is limited to 1 MiB, 64 servers, and 64 skill roots.
Local configuration remains executable trust: selecting a stdio definition
authorizes that executable to run as the current user.

## 5. Runtime identity and local IPC

Runtime storage is selected in this order:

1. MCPCLI_RUNTIME_DIR, when set.
2. XDG_RUNTIME_DIR/mcpcli.
3. /tmp/mcpcli-UID.

The runtime leaf and namespace directories must be real, user-owned directories
with mode 0700. Unsafe ancestors, symlinks at protected paths, unexpected file
types, wrong owners, and permissive leaf modes are rejected. Existing unsafe
directories are not chmod'ed into compliance. Runtime lock files must be
single-link, user-owned regular files with mode 0600, opened with NOFOLLOW,
CLOEXEC, and NONBLOCK.

A hash of the canonical config path and session name creates the namespace.
Every request carries the IPC version, namespace, executable SHA-256, and
configuration digest. The daemon rejects incompatible callers. Its executable
identity is computed from /proc/self/exe, so replacing a binary on disk does
not change the identity of a running process. A changed binary requires
daemon restart.

startup.lock serializes starters. daemon.lock is held for the daemon lifetime.
Locks are never deleted: replacing a lock inode could create two owners.
The daemon removes stale sockets only while holding the lifetime lock and only
when the entry is an owned socket. Unix peer credentials must match the current
effective UID. Socket paths must fit Linux's 107-byte pathname limit.

IPC frames have a four-byte little-endian length followed by UTF-8 JSON.
The receiver checks the declared length before allocating the body; truncated
frames, invalid frames, and over-limit messages are rejected. A connection
carries exactly one request and one response.

Two sockets separate admission:

- sock: at most 32 active data connections and eight active server operations.
- control.sock: at most eight active control connections, accepting only status/stop.

Admission permits are acquired before accepting sockets so bursts beyond 32
connections wait in the kernel listen queue within the caller's deadline rather
than being dropped prematurely. Five-second read/write deadlines bound
unfinished framing and slow output. The control listener remains available when
all data connections are occupied. Requests for one server serialize on its
mutex. The server mutex is acquired before a global work permit, so a queue for
one slow server does not consume all eight active-operation slots. Different
servers can proceed concurrently.

## 6. Failure, cancellation, and shutdown

A request deadline covers waiting for its server/work slot, connection setup,
fresh paginated discovery, and invocation. Connection setup is additionally
limited to 30 seconds. The command's timeout range is 1-3600 seconds; IPC
transfer and cleanup have their own bounded grace periods.

No layer automatically retries tools/call. reqwest retries and redirects are
disabled. Legacy HTTP session-expiry reinitialization is disabled, because it
can replay a failed POST. A failed discovery or an absent tool is reported as
not dispatched. A typed dispatch flag records when the invocation enters the
SDK. Errors after this boundary conservatively report outcome_unknown.
Cancellation is a request to stop work, not a rollback of side effects.

Closing a CLI socket cancels its pending request; it does not destroy the
daemon's server connection. Subsequent commands can continue using that
connection. If the server connection has died, it is not silently replaced:
calls report session_lost and require reset SERVER.

A reset marks the server unavailable before awaiting cleanup and retains the
connection while cleanup is pending or fails. A canceled reset therefore
cannot leave an apparently fresh connection slot that silently restarts a
stateful server. By default, only confirmed closure clears the slot. When the
local client worker has confirmed termination and only remote HTTP session
deletion (DELETE) failed or timed out, `reset --force SERVER` allows discarding
the slot with a cleanup diagnostic so other servers in the same daemon session
remain undisturbed.

Shutdown closes admission, cancels active requests, drains handlers, and closes
all connections. The state record is atomically replaced via a single owned
`state.json.tmp` file and synced. clean_stop is set only when the required
cleanup results succeed. Cleanup errors are recorded and surfaced by daemon
stop. `daemon restart` stops the running daemon and, once its exit is confirmed,
starts a replacement instance even if remote cleanup reported warnings on
stderr. An unexpected death is not interpreted as proof that a particular
supervisor killed it; it reports lost state and requires an explicit
start/restart. Foreground daemon run is available for ordinary process
supervision.

The SDK can discard close errors or detach a private task if its own timeout
helper is used. mcpcli instead retains the owned close future inside the
connection, joins it, and separately records transport cleanup failures.
The eight-second close deadline does not discard that future; a later close
can continue waiting. HTTP DELETE has a two-second deadline. HTTP 404 and the
protocol-permitted 405 allow local closure; they do not prove that remote
application state was erased. Other cleanup failures remain visible.

A stdio child has its own process group. Shutdown closes stdin, waits briefly,
then uses TERM/KILL as needed and reaps the child. WNOWAIT observations retain
the unreaped leader until all process-group signals are complete, preventing
a reused numeric PGID from being signaled. A full stdin pipe cannot block cleanup
indefinitely. The child also receives PDEATHSIG for unexpected parent death.
This is cleanup for trusted process groups, not containment of descendants
that deliberately escape through setsid or another supervisor.

The CLI handles SIGINT and SIGTERM. Its runtime is shut down before returning
ExitCode, allowing transport destructors to clean up same-group descendants.
A three-second runtime grace bounds an interrupted blocking stdin reader.
SIGKILL cannot run userspace cleanup; only the direct-child death signal is
guaranteed in that case.

Downstream stderr is discarded to avoid unbounded logs and accidental
credential capture. Diagnose a server by running it directly under appropriate
supervision. The private startup log contains mcpcli daemon diagnostics.

## 7. Input, results, and exit statuses

Argument JSON rejects duplicate keys, trailing data, non-object roots, excessive
depth, and over-limit input. Integer literals must fit i64 or u64. Other number
tokens are compared as exact decimal values before and after serialization;
inputs that would lose decimal precision are rejected. float_roundtrip keeps
accepted numbers stable across repeated IPC/SDK decoding. Numeric text inside
strings and unusual object keys remain ordinary data. This input guarantee does
not add arbitrary-precision numeric support to the SDK's downstream response
parser.

Default stdout is one JSON value. For calls, it is the SDK's complete standard
CallToolResult, including structuredContent, images, audio, resource content,
annotations, and isError when present. The JSON-RPC transport envelope is not
included. Unknown non-standard top-level fields are subject to SDK decoding;
the CLI does not promise arbitrary vendor-field preservation.

--text is an explicit presentation mode. It emits text blocks, reports omitted
non-text blocks on stderr, and reports structured content availability when
text has already been emitted. If there is only structured content, it prints
that JSON. CRLF line endings are normalized to LF while standalone carriage
returns and other terminal control characters are escaped; empty text results
produce no blank line.

A successful tool result followed by an --oneshot cleanup failure remains on
stdout; the cleanup diagnostic goes to stderr and the exit status is nonzero.
This prevents a lost result from encouraging an unsafe replay.

| Exit | Meaning |
| --- | --- |
| 0 | Successful command |
| 1 | Local runtime/output or cleanup failure |
| 2 | Usage, config, input, or local capability lookup error |
| 3 | MCP CallToolResult reports isError |
| 4 | MCP protocol/transport failure or daemon communication failure |
| 5 | Deadline exceeded |
| 6 | Incompatible daemon or lost session state requiring explicit recovery |
| 130 | SIGINT interruption |
| 143 | SIGTERM interruption |

Operational failures use a JSON error object on stderr with code, kind,
message, and outcome_unknown when applicable. Clap's argument/help diagnostics
use its conventional text format. Skill validation warnings and explicit text
mode notices also go to stderr. Exit status alone is not a retry policy.

## 8. Agent Skills

Skills are mounted local directories, not an installation or execution service.
A root can be one skill directory or a directory whose immediate children are
skills. Only SKILL.md identifies a skill. A recursive scan of arbitrary Markdown
would incorrectly classify references and notes.

Real YAML parsing supports standard frontmatter, multiline text, CRLF, and BOM.
Name, description, directory-name agreement, and standard optional field types
are checked. Unknown extension metadata is retained in the original manifest.
Duplicate names from different sources are errors. Repeated sources are
deduplicated with a warning. Invalid individual manifests are warned about and
skipped; missing configured roots are errors.

Discovery reads metadata. show returns the whole manifest and its canonical
base directory. files lists resource paths. read returns one UTF-8 text file.
No command runs scripts, interprets allowed-tools as permission, follows remote
references, or modifies installed skills.

Resource access walks intermediate directory components with openat using
O_PATH | O_NOFOLLOW | O_DIRECTORY (allowing traversal beneath 0711 execute-only
ancestor directories for unprivileged users) and opens only the target directory
or regular file with O_RDONLY | O_NOFOLLOW | O_NONBLOCK. Absolute paths,
traversal, symlinks, special files, excessive directory depth, and
binary/non-UTF-8 resources are rejected. Directory enumeration uses
/proc/self/fd. Discovery records a canonical root path; each read reopens and
validates its directory components. A symlink replacement is rejected. Ordinary
file changes remain visible on the next read.

| Limit | Value |
| --- | --- |
| CLI arguments / configuration | 1 MiB each |
| Argument container nesting | 64 |
| IPC request / response | 4 MiB / 32 MiB |
| MCP stdio message | SDK 16 MiB |
| HTTP POST response | 16 MiB total, including streamed SSE bytes |
| Background HTTP SSE | 16 MiB per event |
| Tool catalog | 256 pages, 10,000 tools, 16 MiB |
| Skill manifest/resource | 1 MiB |
| YAML frontmatter | 64 KiB, with node/depth/alias budgets |
| Skills / scanned entries | 1,024 / 4,096 |
| Resource directory depth | 16 |

## 9. Verification

Run `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`,
and `cargo test --locked --all-targets` before publishing. The explicit Python
acceptance runners in tests cover CLI lifecycle and real MCP services. Supply
local dependency paths using their command-line options. Tests do not establish
platform support beyond the environments they actually ran on.

## 10. References

- [MCP 2026-07-28 transports](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports)
- [MCP legacy HTTP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
- [Official Rust SDK](https://github.com/modelcontextprotocol/rust-sdk)
- [Agent Skills specification](https://agentskills.io/specification)
- [Microsoft Playwright MCP](https://github.com/microsoft/playwright-mcp)
- [fastexec daemon design reference](https://github.com/ImoutoHeaven/fastexec-mcp/tree/a9b2985fd21143dc68abd0c04d795deeed91a8e4)

The fastexec reference informed private runtime directories, separate startup and
lifetime locks, bounded framing, identity checks, and process ownership. mcpcli
has its own shorter-lived CLI clients and retains server sessions independently
of individual IPC connections.
