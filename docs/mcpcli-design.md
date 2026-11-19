# mcpcli design

## 1. Product contract

mcpcli is a native command-line client for configured MCP tools, a global
AGENTS.md document, and local Agent Skills. Each CLI process reads global
instructions, discovers tools, inspects metadata, invokes a tool, or reads a
skill resource. A local daemon keeps connections and application state alive
between commands.

mcpcli supports stdio and Streamable HTTP, including the modern
2026-07-28 protocol and legacy lifecycle negotiation through rmcp 3.9.0. The
preferred modern version is 2026-07-28; the requested legacy version is
2025-11-25. Compatibility with older revisions follows the SDK's negotiation.
Only tools are exposed. Resources, prompts, sampling, roots, interactive
elicitation/MRTR, asynchronous Tasks, OAuth login, the old two-endpoint
HTTP+SSE transport, and an outbound MCP-server mode are outside this release.
The client does not advertise unsupported capabilities.

Supported native systems are Alpine and Debian Linux (x86-64 and ARM64),
FreeBSD (x86-64), macOS (Intel and Apple silicon), and Windows (x86-64 and ARM64).
Linux release archives use musl and are tested on both distributions. Unix uses
local domain sockets; Windows uses named pipes. This is a native executable; it
does not need Python or Node to run. A configured MCP server may need its own
runtime. This client is a trusted-command runner, not a security sandbox.

## 2. Connection ownership

The daemon owns both transports, including notifications, request routing, HTTP
streams, and protocol negotiation. A stdio server keeps its browser, tasks, and
in-memory data for the connection lifetime. Legacy servers can require an
initialized session; HTTP application state can exist independently of protocol
sessions. The modern MCP protocol also supports a lifecycle without connection-scoped
initialization. An HTTP session ID alone cannot retain the live client connection.

The daemon opens a server on its first server, tools, inspect, or call request. Connections
stay alive until reset, daemon stop, a signal, or process failure. Idle connections
remain open to preserve state for later commands. --oneshot opens a connection
that closes after one command.

Session identity is the canonical configuration-file path plus --session NAME.
Different names have separate daemons and stdio processes. This controls client
identity; it cannot isolate shared remote application data or a browser profile
that two independently configured servers intentionally share.

## 3. Command and disclosure model

| Command | Work performed | Starts an MCP connection |
| --- | --- | --- |
| catalog | Full global AGENTS.md (when configured), server descriptions, and skill metadata | No |
| agents | Full configured global AGENTS.md document | No |
| search QUERY | Offline substring search across global AGENTS.md, server descriptions, and skill metadata | No |
| server SERVER | Server instructions, identity, capabilities, and lifecycle metadata | Yes, if needed |
| tools SERVER | Fresh, complete paginated tool definitions | Yes, if needed |
| inspect SERVER TOOL | Full tool metadata and input/output schemas | Yes, if needed |
| call SERVER TOOL | One tool invocation | Yes, if needed |
| skills list | Skill metadata and validation warnings | No |
| skills show NAME | Complete SKILL.md plus its base directory | No |
| skills files NAME | Relative resource paths, without file contents | No |
| skills read NAME PATH | One bounded UTF-8 resource | No |
| reset [--force] SERVER | Close that connection and discard its client state | No new connection |
| daemon start/status/stop/restart/run | Manage the selected daemon | No downstream connection |

Tool schemas are fetched only for the selected server. Every tools, inspect, and
call command fetches the current list. Fresh discovery supplies modern HTTP
x-mcp-header mappings and excludes tools filtered by the SDK or withdrawn by the
server. Pagination, discovery, and dispatch share one operation budget.

There is no persistent schema cache. The SDK response cache is disabled. The SDK
still retains internal per-tool metadata for HTTP header mapping; fresh
discovery and current-list membership checks govern dispatch. The persistent
connection can handle legacy or unsolicited list-change notifications. Modern
subscription-based notifications are not subscribed to. Reading the current
list does not depend on a notification or a previously cached flag.

Tool arguments use one JSON object, supplied by exactly one of --args JSON,
--args-file PATH, or --stdin. An omitted input is {}. Fields retain their schema
names, including distinct names such as `fooBar` and `foo_bar`. Union types,
arrays, nested objects, and unknown properties stay in JSON instead of being
mapped to generated flags.

## 4. Configuration and environment

Configuration is JSON. The default path is ./mcpcli.json; --config and
MCPCLI_CONFIG select another file. The CLI reads only that file, without searching
ancestor projects or importing another application's configuration.

The version field defaults to 1. Optional agents references one global AGENTS.md
file (or null when unset). servers is a map of aliases to tagged transport
definitions. skills is a list of explicit directory paths. mcpServers and
skillDirs are accepted aliases for servers and skills; other unknown
configuration fields are rejected. Global AGENTS.md scope belongs strictly to
the selected mcpcli configuration; mcpcli never walks working directories or
ancestor repositories for project-scoped AGENTS.md files.

| Transport | Fields |
| --- | --- |
| stdio | transport: stdio; command; optional args, env, cwd, description |
| HTTP | transport: http; url; optional headers, description |

Paths resolve from the configuration file's directory. An explicit executable
path uses native directory separators; an ordinary command name uses the
daemon's PATH. Windows drive-relative paths are rejected, and environment
overrides cannot contain two names that Windows treats as the same key.
The default server cwd is the configuration directory. Arguments are passed
directly through the native process API. Windows npm launchers require an
explicit `npx.cmd` command; batch files follow Windows command-processor rules.

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

`MCPCLI_RUNTIME_DIR` selects an explicit runtime directory on every platform.
Otherwise Unix uses `XDG_RUNTIME_DIR/mcpcli-UID` or `/tmp/mcpcli-UID`; Windows
uses `LOCALAPPDATA/mcpcli`. System directory aliases are canonicalized before
creating a namespace.

On Unix, runtime leaf and namespace directories must be real, user-owned
directories with mode 0700. Unsafe ancestors, symlinks at protected paths,
unexpected file types, wrong owners, and permissive leaf modes are rejected.
macOS and FreeBSD ACLs are checked in addition to mode bits. Existing unsafe
directories are not chmod'ed into compliance. Runtime lock files must be
single-link, user-owned regular files with mode 0600, opened with NOFOLLOW,
CLOEXEC, and NONBLOCK.

Windows creates protected DACLs granting access to the current user. Validation
also trusts SYSTEM, Administrators and the Windows servicing account when
checking existing ancestors. Existing files/directories must have the expected
ownership and access grants. Reparse points, alternate data streams and hard-linked private
files are rejected. Ancestor grants that permit directory replacement or in-place
junction conversion are refused. Directory handles prevent replacement while protected
file operations are in progress.

A hash of the canonical config path and session name creates the namespace.
Every request carries IPC version 2, namespace, executable SHA-256,
configuration digest and an absolute monotonic deadline. The daemon rejects
incompatible callers; there is no IPC migration path. Linux hashes its mapped
executable through `/proc/self/exe`; other platforms use the native executable
path. A changed binary requires `daemon restart`.

startup.lock serializes starters. daemon.lock is held for the daemon lifetime.
Locks use native file locking and are never deleted: replacing a lock file
could create two owners. The daemon removes stale Unix sockets only while
holding the lifetime lock and only when the entry is an owned socket. Unix peer
credentials must match the current effective UID; socket paths must fit that
platform's native pathname limit. Windows named pipes reject remote clients,
use protected per-user DACLs, and verify peer process token SIDs. Both endpoint
names include the namespace identity.

IPC frames have a four-byte little-endian length followed by UTF-8 JSON.
The receiver checks the declared length before allocating the body; truncated
frames, invalid frames, and over-limit messages are rejected. A connection
carries exactly one request and one response.

Separate data and control endpoints isolate admission:

- Data: at most 32 active connections and eight active server operations.
- Control: at most eight active connections, accepting only status/stop.

Admission permits are acquired before accepting sockets so bursts beyond 32
connections wait in native endpoint admission within the caller's deadline.
A cross-process monotonic timestamp keeps backlog time inside the original
budget; the daemon checks expiry before mutations or downstream connection
creation, and the client refuses late writes. Five-second framing caps bound
unfinished input and slow output, without extending the client's request deadline. The control listener remains available when
all data connections are occupied. Requests for one server serialize on its
mutex. The server mutex is acquired before a global work permit, so a queue for
one slow server does not consume all eight active-operation slots. Different
servers can proceed concurrently.

## 6. Failure, cancellation, and shutdown

A request deadline covers waiting for its server/work slot, connection setup,
fresh paginated discovery, and invocation. Connection setup is additionally
limited to 30 seconds. The command's timeout range is 1-3600 seconds. IPC transfer does not renew the
request budget. Connection cleanup has a separate bounded grace period.

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

mcpcli retains and joins the close future, recording transport cleanup failures
separately from the SDK task result. If the eight-second close deadline expires,
a later close can continue waiting on the same future. HTTP DELETE has a
two-second deadline and accepts only 2xx, 404, or the protocol-permitted 405.
Other statuses, including redirects, leave remote cleanup unconfirmed. A 404 or
405 allows local closure without proving that remote application state was erased.

On Unix, a stdio child has its own process group. Shutdown closes stdin, waits briefly,
then uses TERM/KILL as needed and reaps the child. WNOWAIT observations retain
the unreaped leader until all process-group signals are complete, preventing
a reused numeric PGID from being signaled. A full stdin pipe cannot block cleanup
indefinitely. Linux and FreeBSD also request a direct-child death signal for
unexpected parent death. macOS relies on pipe closure and orderly cleanup.
This is cleanup for trusted process groups, not containment of descendants
that deliberately escape through setsid or another supervisor.

At startup on Windows, the CLI clears inheritance on its incoming standard handles,
so detached daemons cannot keep a caller's captured output pipes open. Explicit child
stdio is still duplicated and inherited for that child.

Windows creates stdio children suspended, assigns them to a kill-on-close Job,
verifies opened thread ownership, and then resumes them. Cleanup terminates the
Job and waits for both the child and the Job's active process count. A daemon
launch must detach from any enclosing Job; a supervisor that forbids breakaway
must host `daemon run` explicitly.

The CLI handles SIGINT/SIGTERM on Unix and Ctrl-C/Ctrl-Break on Windows. Its
runtime is shut down before returning ExitCode, allowing transport destructors
to clean up owned children. A three-second runtime grace bounds an interrupted
blocking stdin reader. Forced process termination cannot run userspace cleanup;
Windows Job closure and Linux/FreeBSD direct-child death signals provide the
platform's additional protection.

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

Default stdout is one JSON value. A live server description is
`{"source":"initialize"|"server/discover","protocolVersion":...,"result":{...}}`.
The result is from the connection's lifecycle exchange; the sibling
`protocolVersion` records what the client negotiated. Reusing the connection
does not redo that exchange. Upstream instructions are not the offline global
AGENTS.md and do not grant authority.

`tools SERVER` returns `{"pages":[...]}`, retaining each list response, including
its cursor and metadata. `inspect SERVER TOOL` returns the selected complete
definition; `call` returns the complete tool result, including structured and
non-text content. JSON preserves response members the SDK does not model, but
does not promise whitespace or object key order. Responses must fit the
transport and IPC bounds, use unique keys, and contain numbers representable
without changing their decimal value. Matching upstream JSON-RPC errors are
reported on stderr as `rpc_error` alongside the CLI diagnostic, not as
successful results. Locally synthesized diagnostics do not claim upstream provenance.

--text is an explicit presentation mode. It emits text blocks, reports omitted
non-text blocks on stderr, and reports structured content availability when
text has already been emitted. If there is only structured content, it prints
that JSON. CRLF line endings are normalized to LF while standalone carriage
returns and other terminal control characters are escaped; empty text results
produce no blank line.

A successful tool result followed by an --oneshot cleanup failure remains on
stdout; the cleanup diagnostic goes to stderr and the exit status is nonzero.
If the result already reports a tool error, its exit status remains 3.
An interruption after the tool result arrived also preserves that result and
reports interrupted cleanup with a known outcome. This prevents a lost result
from encouraging an unsafe replay.

| Exit | Meaning |
| --- | --- |
| 0 | Successful command |
| 1 | Local runtime/output or cleanup failure |
| 2 | Usage, config, input, or local capability lookup error |
| 3 | MCP CallToolResult reports isError |
| 4 | MCP protocol/transport failure or daemon communication failure |
| 5 | Deadline exceeded |
| 6 | Incompatible daemon or lost session state requiring explicit recovery |
| 130 | SIGINT / Ctrl-C interruption |
| 143 | SIGTERM / Ctrl-Break interruption |

Operational failures use a JSON error object on stderr with code, kind,
message, and outcome_unknown when applicable. Clap's argument/help diagnostics
use its conventional text format; top-level `--help` also appends the full
configured global AGENTS.md when the selected configuration and file are valid. Skill validation warnings and explicit text
mode notices also go to stderr. Exit status alone is not a retry policy.

## 8. Global AGENTS.md and Agent Skills

When `agents` is set, `catalog`, `agents`, matching `search` results, and
top-level `--help` return the complete UTF-8 contents of that single global
file rather than a truncated summary. Unlike skills, global instructions apply
across every `mcpcli` invocation and carry no YAML frontmatter requirement.
The configured parent directory is pinned and canonicalized, and the leaf file
is opened without following symlinks, reparse points, special files, or Windows
alternate data streams. Ordinary edits to the file are visible on the next read.

A skill root is a local directory containing SKILL.md, or a directory whose
immediate children contain skills. References and other Markdown files remain
resources within their skill. The CLI reads mounted skills without installing
or executing them.

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

Unix resource access walks intermediate directory components with openat using
O_PATH (Linux) or O_SEARCH (FreeBSD/macOS), plus O_NOFOLLOW and O_DIRECTORY.
This permits traversal beneath execute-only ancestors. Final reads use O_RDONLY,
O_NOFOLLOW and O_NONBLOCK; directory enumeration uses the open descriptor through
fdopendir/readdir. Windows resolves each component with NtCreateFile relative to a
pinned parent directory handle and refuses reparse points. It enumerates the live
directory handle with GetFileInformationByHandleEx; an in-place junction conversion
cannot redirect the next open or enumeration through the old pathname. Absolute paths,
traversal, symlinks/reparse points, alternate data streams, special files, excessive directory depth, and binary/non-UTF-8 resources
are rejected. Discovery pins the configured directory before canonicalizing it
and verifies that the resolved directory has the same native identity. Trailing
separators and `/.` cannot hide a symlink leaf. Existing ancestor aliases may
resolve during mounting; each later read reopens and validates the canonical
directory components. A symlink replacement is rejected. Ordinary
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
| Global AGENTS.md | 1 MiB |
| Skill manifest/resource | 1 MiB |
| YAML frontmatter | 64 KiB, with node/depth/alias budgets |
| Skills / scanned entries | 1,024 / 4,096 |
| Resource directory depth | 16 |

## 9. Verification

`ci.yml` runs formatting, Clippy with warnings denied, Rust unit/protocol tests,
Python helper regressions, and the release CLI E2E suite on Ubuntu, Alpine,
Debian, FreeBSD, macOS and Windows.
Linux containers cover x86-64 and ARM64, macOS covers Intel and Apple silicon,
and Windows covers x86-64 and ARM64. The root permission fixture is mandatory
in container and FreeBSD jobs. OS-specific tests are recorded explicitly; Windows
security has native DACL/reparse/ownership tests. Process inspection errors fail
cleanup tests instead of being interpreted as an exited process.

The real-service job pins fastexec and Playwright MCP dependencies and runs
Chromium against a local test page through stdio and Streamable HTTP. All jobs
must pass the aggregate CI gate.

`release.yml` also runs on the WIP release-checkpoint branch for a full preflight.
A pushed `vX.Y.Z` tag must match Cargo.toml and point to a commit on main. It
reruns CI, builds seven platform archives, extracts and tests each executable,
checks the complete asset set, and emits SHA256SUMS. Linux archives are exercised
on Alpine and Debian. Only then does it create a draft GitHub Release. Publishing
the draft follows verification of the frozen commit and release assets.

## 10. References

- [MCP 2026-07-28 transports](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports)
- [MCP legacy HTTP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
- [Official Rust SDK](https://github.com/modelcontextprotocol/rust-sdk)
- [Agent Skills specification](https://agentskills.io/specification)
- [Microsoft Playwright MCP](https://github.com/microsoft/playwright-mcp)
- [fastexec daemon design reference](https://github.com/ImoutoHeaven/fastexec-mcp/tree/a9b2985fd21143dc68abd0c04d795deeed91a8e4)

The fastexec reference covers private runtime directories, separate startup and
lifetime locks, bounded framing, identity checks, and process ownership. mcpcli
retains server sessions independently of its short-lived CLI connections.
