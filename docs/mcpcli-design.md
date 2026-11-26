# mcpcli design

## 1. Product contract

`mcpcli` is a native command-line client for configured MCP tools, MCP
`2026-07-28` Events (`io.modelcontextprotocol/events`, [SEP-3415](https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx)),
OAuth 2.1 HTTP server authentication, a global `AGENTS.md` file, and local Agent
Skills. Each invocation reads global instructions, manages OAuth credentials,
lists or inspects tools, calls a tool, lists, polls, or streams events, or reads
a skill resource, while a local daemon keeps connections and server state alive
across commands.

The client speaks `stdio` and single-endpoint Streamable HTTP through
`rmcp 3.9.0`. It prefers protocol version `2026-07-28`, falls back to
`2025-11-25` lifecycle negotiation, and accepts older Streamable HTTP revisions
(`2025-06-18` and `2025-03-26`) negotiated by the SDK. Over HTTP, `mcpcli`
supports single-endpoint Streamable HTTP only and does not implement the
deprecated `2024-11-05` two-endpoint HTTP+SSE (`/sse`) transport. Only tools
and the `io.modelcontextprotocol/events` extension capability
(`capabilities.extensions["io.modelcontextprotocol/events"]`) are exposed.
MCP server resource endpoints (`resources/*`), prompts (`prompts/*`), sampling,
roots, interactive elicitation (MRTR), asynchronous Tasks, deprecated
`2024-11-05` two-endpoint `/sse`, and outbound MCP server hosting are out of
scope.

The dependency is the unmodified crates.io `rmcp =3.9.0` package, pinned by
`Cargo.lock`. The repository has no `rmcp` vendor copy or Cargo patch. Local
byte owners cover objects held by this application and release them at the
handoff to the SDK. Guarantees that require changes to SDK model fields, queues,
or caches are outside this implementation (see WONT FIX below).

Supported targets are Alpine and Debian Linux (x86-64 and ARM64, static musl),
FreeBSD (x86-64), macOS (Intel and Apple silicon), and Windows (x86-64 and
ARM64). Local IPC uses Unix domain sockets or Windows named pipes. `mcpcli`
requires no external language runtime, though a configured MCP server may
require Python, Node, or another interpreter. Configured stdio commands and
event callback commands run with the current user's permissions.

## 2. Connection ownership

The daemon owns stdio children, HTTP streams, protocol negotiation,
notifications, and request routing. A stdio server retains its child processes
and in-memory state for the lifetime of that connection. Session-based servers
use an initialized session (`initialize`), while `2026-07-28` servers can answer
`server/discover` without connection-scoped initialization. Remote HTTP
services can also hold application state outside MCP session IDs, and an HTTP
session ID by itself cannot preserve a live client worker across processes.

Connections open lazily on the first `server`, `tools`, `inspect`, `call`, or
`events` command for that server and stay open until `reset`, `daemon stop`, a
termination signal, or transport failure. `--oneshot` opens a private
connection for one command and closes it before exit.

Session identity combines the canonical configuration path with `--session NAME`
or `MCPCLI_SESSION` (default `default`). Distinct session names run separate
daemons and stdio processes. Client sessions isolate local connections; two
servers pointed at the same remote service or shared browser profile still share
that upstream state.

## 3. Command and disclosure model

| Command | Work performed | Opens MCP connection |
| --- | --- | --- |
| `catalog` | Global `AGENTS.md` (if configured), server descriptions, and skill metadata | No |
| `agents` | Full configured global `AGENTS.md` file | No |
| `search QUERY` | Offline case-insensitive substring match across `AGENTS.md`, servers, and skills | No |
| `auth login SERVER` | OAuth 2.1 PKCE (`S256`) discovery, registration, and token storage for an HTTP server | No |
| `auth status [SERVER]` | Stored OAuth credential state, scopes, and expiry without exposing raw tokens | No |
| `auth logout SERVER` | Remove stored OAuth credentials for an HTTP server | No |
| `server SERVER` | Lifecycle metadata, negotiated version, capabilities, and upstream instructions | If needed |
| `tools SERVER` | Fresh, complete paginated tool definitions | If needed |
| `inspect SERVER TOOL` | Complete definition and schemas for one tool | If needed |
| `call [--inline-media] SERVER TOOL` | One tool call, saving binary media locally by default | If needed |
| `events list SERVER` | Fresh paginated event definitions from `events/list` | If needed |
| `events poll SERVER EVENT` | Single `events/poll` batch fetch with optional cursor and filter arguments | If needed |
| `events stream SERVER EVENT` | Foreground push or poll stream emitting SEP-3415 NDJSON or invoking a callback | If needed |
| `skills` (`skill`) `list` | Mounted skill metadata and validation warnings | No |
| `skills` (`skill`) `show NAME` | Full `SKILL.md` manifest and canonical skill directory | No |
| `skills` (`skill`) `files NAME` | Relative resource file paths under that skill | No |
| `skills` (`skill`) `read NAME PATH` | Full UTF-8 contents of one skill resource | No |
| `reset [--force] SERVER` | Close one server connection and clear its local slot | No |
| `daemon start|status|stop|restart|run` | Manage the daemon for the selected session | No |

Tool schemas are fetched only for the requested server. Every `tools`,
`inspect`, and `call` command fetches the current tool pages before acting,
populating `2026-07-28` HTTP `x-mcp-header` (`Mcp-Param-*`) mappings and
dropping tools withdrawn by the server or rejected by the SDK. Discovery,
pagination, and tool dispatch share one operation deadline.

`mcpcli` stores no persistent tool schema cache and disables the SDK response
cache. Live connections accept `notifications/tools/list_changed` messages
without subscribing to `2026-07-28` tool notification streams, and fresh listing
never relies on notification delivery.

Tool and event filter arguments are passed as one JSON object through
`--args JSON`, `--args-file PATH`, or `--stdin` (`call` defaults to `{}`).
Property names, including distinct spellings such as `fooBar` and `foo_bar`,
nested objects, arrays, and union shapes pass straight through in JSON.

## 4. Configuration and environment

Configuration defaults to `./mcpcli.json` and can be overridden with `-c PATH`,
`--config PATH`, or `MCPCLI_CONFIG`. `mcpcli` reads only the selected file and
never searches parent directories or imports settings from other tools.

`version` defaults to `1`. Optional `agents` names one global `AGENTS.md` file
(or `null`). `servers` maps aliases to transport definitions, and `skills`
lists local skill directories (`mcpServers` and `skillDirs` are accepted field
aliases; unknown fields fail validation).

| Transport | Fields |
| --- | --- |
| `stdio` | `transport: "stdio"`, `command`, optional `args`, `env`, `cwd`, `description` |
| `http` | `transport: "http"`, `url`, optional `headers`, `description` |

Relative paths resolve from the configuration file's directory, which is also
the default `cwd` for stdio servers. Plain command names resolve via the
daemon's `PATH`. Explicit executable paths on Windows accept `/` or `\`, while
drive-relative paths such as `C:server.exe` are rejected and `env` overrides
cannot contain keys that collide case-insensitively on Windows. Arguments go
directly to the OS process API; npm wrappers on Windows require `.cmd` (such as
`npx.cmd`).

`env` expands whole-value references such as `"${NAME}"`. HTTP `headers` also
expand inline templates such as `"Bearer ${MCP_TOKEN}"`.
Missing or non-UTF-8 referenced variables fail configuration loading, and
`args` values are never interpolated. HTTP URLs cannot include credentials or
fragments. Protocol-owned headers (`Host`, `Content-Length`, `Content-Type`,
`Accept`, `Connection`, `Transfer-Encoding`, `Upgrade`, `TE`, `Trailer`,
`Last-Event-Id`, `Mcp-*`, and `Mcp_*`) are blocked case-insensitively at load
time and request build time. Validation errors report field paths and JSON
types while omitting secret values.

The daemon snapshots its launch environment on startup and computes a SHA-256
digest over the resolved configuration and expanded values. OAuth 2.1 tokens
managed by `mcpcli auth` live in a separate private `credentials.json` file
outside that digest so token acquisition, rotation, and logout never invalidate
a running daemon. Callers whose configuration digest differs are rejected until
`daemon restart`. Server and session names allow 1 to 64 ASCII alphanumerics,
`_`, or `-`. Configuration files are capped at 1 MiB, 64 servers, and 64 skill
roots.

## 5. Runtime identity and local IPC

`MCPCLI_RUNTIME_DIR` overrides the runtime root on all platforms. By default,
Unix uses `$XDG_RUNTIME_DIR/mcpcli-<uid>` or `/tmp/mcpcli-<uid>`, and Windows
uses `%LOCALAPPDATA%\mcpcli`. Existing parent directory aliases are
canonicalized before creating namespace subdirectories.

Unix runtime and namespace directories must be real directories owned by the
effective UID with mode `0700` and no permissive POSIX or NFSv4 ACL entries on
macOS and FreeBSD. Unsafe directories are rejected in place without `chmod`.
Lock files are user-owned, single-link regular files (`0600`) opened with
`O_NOFOLLOW`, `O_CLOEXEC`, and `O_NONBLOCK`.

Windows creates directories and files with owner-only DACLs, while trusting
`SYSTEM`, `Administrators`, and `TrustedInstaller` on existing ancestors.
Validation rejects unexpected owners, permissive ACEs, reparse points,
alternate data streams, hard-linked private files, and ancestor permissions
that allow directory replacement or junction grafting. Pinned directory handles
keep verified paths stable during file operations.

Each namespace hashes the canonical configuration path and session name. Every
IPC request carries protocol version `4` (`ipc::PROTOCOL = 4`), namespace,
executable SHA-256 (`/proc/self/exe` on Linux, native executable path
elsewhere), configuration digest, timeout budget, monotonic deadline, and
optional explicit deadline. Version, binary, or configuration mismatches fail
with exit status `6` until `daemon restart`.

`startup.lock` serializes concurrent starters and `daemon.lock` stays locked
for the daemon's lifetime. Lock files are never unlinked, and stale Unix
sockets are unlinked only while holding `daemon.lock` after verifying socket
ownership. Unix peers are checked against the effective UID; Windows named
pipes block remote clients and check the peer token SID.

All IPC frames use a 4-byte little-endian byte length followed by UTF-8 JSON.
Outbound frames serialize in a single bounded pass backed by leases from the
1 GiB process active byte budget, and receivers reject empty, truncated, or
over-limit lengths before allocating body buffers. Standard operations
(`status`, `stop`, `server`, `tools`, `call`, `reset`, `events_list`, and
`events_poll`) exchange one `Request` and one `Response`.

Streaming operations (`events_stream`) read one `Request` on the data endpoint,
acquire one of `MAX_EVENT_STREAMS = 32` concurrent stream permits, release the
short-lived accept connection permit so active event streams cannot starve
regular RPC traffic, and write a sequence of length-prefixed `StreamFrame`
records (`protocol: 4`, `namespace`, `build`, `epoch`, `kind`). Each `kind`
carries `frame: "notification"` (`method` and optional `params`),
`frame: "error"` (`error: Failure`), or `frame: "end"`. Streaming IPC frames
omit the 1-hour total request cap and apply the 60-second per-frame idle
writeback clock (`TRANSFER_IDLE_TIMEOUT`), canceling the upstream subscription
as soon as the CLI closes the socket, the server enters `session_lost`, `reset`
runs, or the daemon stops.

Separate listeners prevent data traffic from starving management commands:

- Data endpoint accepts up to 32 concurrent setup/RPC connections, 8 active
  request/response server operations, and 32 concurrent `events_stream`
  subscriptions.
- Control endpoint accepts up to 8 concurrent connections and rejects any
  operation other than `status` and `stop` with `control_only`.

Connection permits are acquired before `accept` so excess callers wait in the
OS listen queue under their original monotonic deadline. Per-server mutexes
are acquired before the 8-slot global operation semaphore so queued calls to
one slow server leave other servers free to run. Request reads have a 5-second
framing timeout. After sending a request/response RPC, the CLI waits for the
validated 4-byte response header under its remaining operation budget, then
switches response body reads to a 60-second idle progress clock (capped by any
remaining explicit `--timeout` deadline) aligned with the daemon's 60-second
idle writeback clock.

## 6. Transports, failure handling, and shutdown

`--timeout` has two command-specific contracts. Regular request/response
commands interpret `--timeout` in seconds (`1` to `3600`, default `60`) covering
slot queueing, connection setup (capped at `30` seconds), discovery or listing,
and request execution. `events stream` interprets `--timeout` in milliseconds as
the total foreground run budget (unbounded when omitted, while connection and
subscription handshakes remain capped at `30` seconds). Local IPC body transfer
uses its 60-second idle progress clock once a valid frame header arrives, and
connection teardown has a separate bounded grace window.

`tools/call` is never retried. `reqwest` retries, HTTP redirects, and automatic
HTTP session reinitialization are disabled so a POST cannot be replayed after
partial execution. If discovery fails or the tool is absent, the error reports
`outcome_unknown: false`. Once dispatch crosses into the SDK, a dispatch flag
is set and subsequent transport or timeout failures report
`outcome_unknown: true`. Cancellation asks the server to stop work; side
effects that already ran remain in effect.

The application's stdio and HTTP adapters enforce a 512 MiB message or event
limit and use a shared 1 GiB non-blocking byte budget for their buffers and
decoded values. `list_tool_pages` enforces a 16 MiB catalog limit. SDK-owned
allocations after handoff have the limits described under WONT FIX:

- `ChildTransport` reads stdio lines directly via `BufReader::fill_buf` and
  `consume`, releases the completed line buffer, and refuses writes once
  terminated.
- `BoundedHttp` enforces 512 MiB on `application/json` bodies, and custom
  `BoundedSseStream` parses POST SSE and background GET SSE events up to
  512 MiB per event (handling split UTF-8 BOMs, comments, `\r`/`\n`/`\r\n`, and
  ignored NUL `id` fields). Per [WHATWG event stream interpretation](https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation),
  fields without a colon have empty values, repeated `event`/`id`/valid `retry`
  fields overwrite earlier values, unknown fields are ignored, and invalid or
  overflowing `retry` values are ignored. Completed line buffers are released
  or moved into their event; their capacity remains charged while retained.
- SSE `id` values have a separate 8 KiB UTF-8 byte limit before SDK handoff.
  An oversized ID terminates the connection and pending call immediately;
  GET streams do not reconnect and POST calls are not replayed. Event/data
  limits remain 512 MiB. The SDK keeps its own reconnect-ID strings as bounded
  metadata outside application leases.
- Invalid UTF-8 detected by the local SSE parser is a terminal parse error.
  Its cause reaches the waiting operation even after a prior event ID;
  ordinary network errors and EOF keep the SDK's existing reconnect policy.
- `Ingress` owns each raw buffer and reserves the lossless `Value` plus JSON
  scratch before parsing. Scratch is one reusable `Vec` whose allowance is
  twice the longest escaped string or numeric token whose mantissa exceeds
  `u64` (at least 8 bytes), retained until the JSON deserializer finishes.
  Numeric losslessness checks compare borrowed canonical decimal views without
  digit copies. Raw bytes are released before conversion from a borrowed
  `&Value` into the official SDK model. That synchronous conversion reserves
  one payload-sized final model and two payload-sized temporary copies for
  upstream string-ID and subscription-metadata conversions. Temporary credit
  is returned after conversion; the local model remains in `Leased` until
  the transport returns it to the SDK. Its later SDK lifetime is not covered.
- Matched tool calls and RPC errors give the SDK small stubs while the lossless
  result or error keeps its own lease. SSE uses the official raw `Sse` API:
  the local parser queues owned events, encodes validated stubs before handoff,
  and keeps each event charged while it remains in that queue. The SDK parses
  the returned event again. Lossless initialize/discover output, tool pages,
  names, and locally retained pagination cursors have application owners;
  these owners make no promise about SDK peer-info or schema-cache copies.
- Application-created call parameters reserve their construction and local
  wait, ending when the application's future ends. MCP stdio and HTTP bodies
  use the bounded capacity-counting JSON writer. Stdio keeps the body until
  writing finishes; HTTP uses `Bytes::from_owner`, so request-body clones
  retained by the transport keep that encoded buffer charged. CLI arguments
  use leased readers before parsing, reserve the decoded value and scratch,
  and move their owned object into the operation. The source owner follows
  CLI/daemon IPC writeback and oneshot dispatch without another argument-map
  clone. The original `--args` `String` separately reserves its actual capacity
  at CLI entry and retains that charge through output.
- The application's client service keeps the official SDK dispatch,
  cancellation, and progress behavior. Its noninteractive defaults drop each
  request or notification before its context. Unsupported server methods
  receive a fixed local error without echoing arbitrary method strings.
- Buffer leases count allocated capacity. Growth reserves the replacement
  before copying and returns the old charge after freeing its allocation.
  IPC decoding holds separate leases for the frame and decoded value; the
  value lease follows the result through CLI output. RPC errors share their
  exact `error` value and lease across diagnostic conversions and clones.
  The SDK receives a small error stub (long diagnostics use a 1 KiB UTF-8
  prefix and an ellipsis); `rpc_error` preserves the original fields and full
  message. Text and diagnostic output write borrowed spans and stream JSON,
  avoiding full-size rendering copies. Server, tool-list, and inspect text
  also stream borrowed fields; inspect moves its selected raw tool from the
  owned pages, normalizing and truncating its short display name without
  copying the full name.
- Oversize frames and byte-budget exhaustion store a connection-scoped
  termination error, suppress HTTP SSE GET reconnects, and wake pending
  operations immediately.

### WONT FIX: SDK-owned allocation lifetimes

Exact accounting after an object crosses the official `rmcp` API would require
upstream ownership hooks. This includes SSE reparsing and queued messages,
unpolled handler tasks, request IDs retained for cancellation and response
sending, peer-info metadata, tool schemas and cache keys, pagination copies,
and call-parameter clones retained after cancellation. The SDK's subscription-ID
getter and disabled-resource-cache URI preparation also keep upstream behavior.
Application guards do not imply coverage of those allocations, and a green
application test does not establish that coverage. These items are WONT FIX in
this repository under the unmodified-upstream requirement; they require an
upstream SDK change before an exact ownership guarantee can be made.

Disconnecting a CLI client cancels its in-flight operation while keeping a
healthy server connection open for future commands. When a server connection
dies, including when an upstream session-bearing Streamable HTTP server returns
HTTP `404 Not Found` for an expired or evicted `Mcp-Session-Id`, `BoundedHttp`
records connection termination via `termination.terminate`, marks the
connection closed, cancels active event streams through `mark_lost()`
(`streams_cancel.cancel()`), and transitions the server slot into
`session_lost` (exit status `6`). Subsequent commands for that server report
`session_lost` until `reset SERVER` clears the slot. `reset` marks the slot
unavailable before awaiting cleanup and keeps the slot blocked if cleanup is
interrupted or fails. If the local client worker has already exited and only
the remote HTTP `DELETE` failed or timed out (2-second deadline; `2xx`, `404`,
and `405` count as clean), `reset --force SERVER` clears that slot with a
stderr warning while leaving other servers untouched.

Daemon shutdown closes listeners, cancels active calls and event streams, joins
handlers, closes every server connection (up to 8 seconds per close future,
retained across retries if interrupted), and atomically writes `state.json` via
`state.json.tmp` with `clean_stop: true` only when all local and required
remote cleanups succeed. `daemon restart` waits for the old daemon to exit
before starting a new one. After an unexpected daemon exit, commands report
lost state and require an explicit `start` or `restart`.

On Unix, each stdio child runs in its own process group. Teardown closes
`stdin`, waits briefly, sends `SIGTERM` and `SIGKILL` to the group while
keeping the leader unreaped (`WNOWAIT`) until signaling finishes so the numeric
PGID cannot be recycled, and then reaps the child. Linux and FreeBSD also arm a
parent-death signal on direct children. Process-group cleanup terminates normal
child trees; descendants that detach into a new session outlive it.

On Windows, startup clears handle inheritance on standard streams so detached
daemons never hold a caller's pipes open. Stdio children spawn suspended, join
a kill-on-close Job object after thread ownership checks, and then resume.
Daemon backgrounding detaches from any parent Job; supervisors that forbid Job
breakaway should run `daemon run` directly. Child `stderr` is redirected to
null so noisy servers cannot fill disks or leak secrets into daemon logs.

## 7. MCP Events

`mcpcli events` implements client operations for the MCP `2026-07-28` Events
extension (`capabilities.extensions["io.modelcontextprotocol/events"]`,
[SEP-3415](https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx)).
Servers that do not advertise `io.modelcontextprotocol/events` fail capability
checks locally before dispatching `events/*` requests.

- `events list SERVER` paginates `events/list` up to `MAX_EVENT_PAGES = 100`
  within the active byte budget and returns `{"events":[...]}`.
- `events poll SERVER EVENT` sends one `events/poll` request with optional
  filter `arguments`, `--cursor`, `--max-age-ms`, and `--max-events`, returning
  the lossless upstream result object.
- `events stream SERVER EVENT` runs in the foreground under `--mode auto`
  (tries `events/stream` push first, falling back to `events/poll` when push is
  unsupported), `--mode push`, or `--mode poll`, with optional `--cursor` or
  `--cursor-file PATH`, `--max-age-ms`, `--heartbeat-ms` (default `30000`),
  `--once`, and `--timeout` (millisecond run budget).

On Streamable HTTP, `events/poll` and `events/stream` attach `Mcp-Name` for the
target event and map scalar filter `arguments` (`string`, `integer`, or
`boolean`) along `inputSchema` `properties` paths that declare a valid
`"x-mcp-header": "<Suffix>"` onto `Mcp-Param-<Suffix>`. Both `Mcp-Name` and
`Mcp-Param-*` pass values through verbatim when every character is printable
ASCII (`0x20..=0x7E`), neither the first nor last byte is `' '` or `'\t'`, and
the value does not begin with `=?base64?`; all other values encode as
`=?base64?<standard-base64>?=` using standard `=`-padded Base64. During HTTP
`events/list`, event definitions with invalid `x-mcp-header` schemas are
omitted with a `stderr` warning. If HTTP `events/poll` or `events/stream`
receives `-32020` (`HeaderMismatch`) while header annotations were uncached or
stale, the client refreshes `events/list` once and retries with updated headers.
On `stdio` transport, HTTP header annotations have no wire meaning: `events/list`
ignores `x-mcp-header` and retains every valid event definition.

`EventRouter` pre-registers subscription slots before writing `events/stream` on
the wire so early `notifications/events/active` frames arriving ahead of the
RPC response are buffered in order. Each notification is capped at
`MAX_EVENT_NOTIFICATION_BYTES = 4 MiB` with per-subscription queue depth
`EVENT_CHANNEL_CAPACITY = 128`. Receiving `notifications/events/terminated` or
closing the stream starts best-effort cancellation of the underlying SDK
`RequestHandle`, bounded by `CANCEL_TIMEOUT = 2s`. Explicit close and dropping the
stream release local routing and ingress tracking before waiting for cancellation;
a slow cancellation acknowledgment does not change an already delivered event or
committed cursor. The daemon bounds concurrent streams at
`MAX_EVENT_STREAMS = 32`; foreground handshakes cap at
`ACTIVE_HANDSHAKE_TIMEOUT = 30s`, poll intervals enforce
`MIN_POLL_INTERVAL_MS = 1000`, and transient stream reconnects apply exponential
backoff from `INITIAL_BACKOFF = 1s` to `MAX_BACKOFF = 60s`.

`events stream` emits pure SEP-3415 `EventOccurrence` objects (`eventId`,
`name`, `timestamp`, `data`, and optional `cursor`) as NDJSON lines on `stdout`
or callback `stdin` from `events/poll` batches and `notifications/events/event`
push notifications. Batch or stream truncation indicators (`truncated: true` on
`events/poll` responses or `notifications/events/active` notifications) write
warnings to `stderr` and never inject synthetic `truncated` fields into
`EventOccurrence` payloads.

`--cursor-file PATH` persists opaque UTF-8 cursors up to
`MAX_CURSOR_BYTES = 64 KiB` via `CursorStore` without stripping whitespace or
line endings. Updates write to a private sibling temporary file
(`.{name}.tmp-{pid}-{seq}-{rand}`, mode `0600` on Unix or owner-only DACL on
Windows), call `sync_all`, atomically replace the target via
`replace_file_with_retry`, and sync the parent directory (unlinking the
temporary file on failure). Cursors advance only after `stdout` flush or
callback acknowledgment succeeds, preserving at-least-once delivery.

When `-- <CMD>...` is supplied, `CallbackExecutor` runs the callback with
per-event deadline `--exec-timeout` in milliseconds (default
`DEFAULT_EXEC_TIMEOUT = 30s`), teardown grace `GRACEFUL_SHUTDOWN_TIMEOUT = 2s`,
and a `MAX_STDERR_BYTES = 64 KiB` rolling `stderr` capture buffer:

- `--exec-mode oneshot` (default) spawns `<CMD>...` per event inside its own
  Unix process group or Windows kill-on-close Job object, writes one NDJSON
  `EventOccurrence` line to `stdin`, closes `stdin`, and requires exit status
  `0` before committing the cursor. Timeouts or I/O errors terminate and reap
  the full process tree.
- `--exec-mode stream` keeps a persistent worker process across events, writing
  one NDJSON line to `stdin` concurrently with reading one bounded
  (`MAX_ACK_LINE_BYTES = 64 KiB`) JSON acknowledgment line from `stdout`.
  `{"ok":true}` commits the event cursor; `{"ok":false,"error":"..."}`,
  invalid JSON, premature `stdout` EOF, or timeout shuts down and reaps the
  worker tree before surfacing the failure.

## 8. OAuth 2.1 authentication

`mcpcli auth` provides a non-GUI, Unix-style OAuth 2.1 client workflow for
Streamable HTTP MCP servers:

- `mcpcli auth login SERVER [--scope SCOPE]... [--client-id ID] [--no-browser]`
- `mcpcli auth status [SERVER]`
- `mcpcli auth logout SERVER`

`auth login` discovers Protected Resource Metadata ([RFC 9728](https://datatracker.ietf.org/doc/html/rfc9728))
from the server's `WWW-Authenticate` `resource_metadata` parameter or
well-known endpoint, verifies canonical resource binding, and fetches
Authorization Server Metadata ([RFC 8414](https://datatracker.ietf.org/doc/html/rfc8414)).
`auth login` binds an ephemeral `127.0.0.1:0` loopback HTTP callback listener
(`http://127.0.0.1:<port>/callback`) before client resolution: when
`--client-id` is omitted, every login performs Dynamic Client Registration
([RFC 7591](https://datatracker.ietf.org/doc/html/rfc7591)) as a public native CLI client
(`token_endpoint_auth_method = "none"`, `grant_types = ["authorization_code", "refresh_token"]`,
`response_types = ["code"]`) bound to that invocation's loopback `redirect_uri`
rather than reusing a prior client registration tied to an older port.
Authorization requests always use Authorization Code with PKCE `S256`
([RFC 7636](https://datatracker.ietf.org/doc/html/rfc7636)), a cryptographic `state` parameter, and
the `resource` URI from the verified metadata ([RFC 8707](https://datatracker.ietf.org/doc/html/rfc8707)).
Credential keys preserve non-root path slashes and query components: `/mcp`,
`/mcp/`, and `/mcp?tenant=a` remain distinct resources. Protected Resource
Metadata discovery inserts the well-known path while preserving the resource query.

For non-loopback MCP servers, all OAuth metadata, registration, authorization,
token, and revocation endpoints must use `https://` and must not address or
resolve in DNS to loopback, private, link-local, or cloud metadata ranges.
Literal `http://` loopback OAuth endpoints are allowed only when the configured
MCP server URL itself targets loopback (`127.0.0.0/8`, `::1`,
`::ffff:127.0.0.0/104`, or `localhost`).

`auth login` prints the authorization URL to `stderr`, opens the default
browser unless `--no-browser` is passed, and exchanges the authorization code
after constant-time `state` (and optional `iss`) verification. With
`--no-browser`, `auth login` skips browser launch while accepting either the
loopback callback or a pasted redirect URL or authorization code on `stdin` for
SSH and headless sessions. The login deadline covers discovery, response bodies,
the callback, token exchange, and waiting to persist the credential.

Acquired tokens, refresh tokens, expiration timestamps, granted scopes, and
client metadata are stored in a private `credentials.json` file (`0600` on
Unix, owner-only DACL on Windows, updated under a lock via atomic
temporary-file rename). Credential path resolution checks `MCPCLI_CREDENTIALS`
first, then `<MCPCLI_RUNTIME_DIR>/credentials.json` when `MCPCLI_RUNTIME_DIR`
is set; otherwise Unix resolves `$XDG_STATE_HOME/mcpcli/credentials.json` (when
`XDG_STATE_HOME` is an absolute path), `$HOME/.local/state/mcpcli/credentials.json`
(when `HOME` is an absolute path), or `<runtime>/credentials.json`, and Windows
resolves `%LOCALAPPDATA%\mcpcli\state\credentials.json`. Writing to an unusable
selected state path fails without choosing another location. Explicit
`Authorization` headers configured in `mcpcli.json` take precedence over stored
OAuth credentials. Because `credentials.json` is separate from `mcpcli.json`
and excluded from the daemon configuration `digest`, logging in, rotating
tokens, or logging out takes effect immediately on live daemons without
restarting other server sessions.

When a stored access token is within 30 seconds of expiry (`expires_at <= now + 30s`)
or an upstream HTTP server returns any `401 Unauthorized` while a
`refresh_token` is held, `BoundedHttp` runs `grant_type=refresh_token` once
under a per-resource file lock shared by daemon and oneshot processes, atomically
persists rotated credentials, and retries the request once. Login commits and logout
use the same lock, so a completed refresh cannot overwrite a later login or restore
a logged-out credential. The complete refresh exchange, including its response body,
has a 15-second deadline. If the token endpoint rejects refresh with
`invalid_grant`, `invalid_client`, or `unauthorized_client`, `mcpcli` clears
the stored `refresh_token` on disk and withholds confirmed-expired access
tokens from upstream requests.

The logout deadline covers lock acquisition and local credential deletion.
After deletion, remote revocation is best effort and uses the remaining budget.

When an HTTP server returns `401 Unauthorized` or `403 Forbidden` that cannot
be satisfied automatically, `BoundedHttp` parses RFC 6750 and RFC 9728
`WWW-Authenticate: Bearer` parameters (`error`, `error_description`, `scope`,
`resource_metadata`) and returns structured diagnostics (`kind: "auth_required"`
on `401` and `kind: "insufficient_scope"` on `403`) naming any required scopes
and `mcpcli auth login <SERVER>` without echoing bearer secrets.

## 9. Input validation, media cache, and exit statuses

Tool and event argument JSON must be a single object within 1 MiB and 64
nesting levels, with unique keys at every level. Integer tokens must fit `i64`
or `u64`, and other numbers must preserve their exact decimal value when
round-tripped through `serde_json` (`float_roundtrip`).

Default stdout is JSON (or NDJSON for `events stream`). `server SERVER` outputs
`{"source":"initialize"|"server/discover","protocolVersion":...,"result":{...}}`
from the initial handshake without re-running negotiation on later calls.
Upstream server instructions remain distinct from the local `AGENTS.md` file.
Server descriptions, instructions, and tool or event metadata are untrusted
data and never authorize tool execution or data disclosure.

`tools SERVER` outputs `{"pages":[...]}` with raw cursors and metadata intact,
`inspect SERVER TOOL` outputs the matching raw tool object, and `call` projects
directly from the ingress-verified raw `serde_json::Value` so unknown fields
and explicit `null` members survive intact. Upstream JSON-RPC errors go to
stderr in the diagnostic's `rpc_error` field, which is omitted on locally
generated errors.

By default, `call` removes Base64 `data` and `blob` strings in place from
`image`, `audio`, and binary `resource` blocks in `result.content` (preserving
text resources that carry a string `text` field alongside `"blob": null`),
decodes them into a private media directory using random hex stems and a fixed
21-MIME extension allowlist (`.mp4`, `.webm`, `.mov`, `.3gp`, `.avi`, `.m4a`,
`.wav`, `.mp3`, `.ogg`, `.flac`, `.aac`, `.aiff`, `.pdf`, `.png`, `.jpg`,
`.gif`, `.webp`, `.bmp`, `.tiff`, `.heic`, `.heif`, or `.bin` for other MIME
types, ignoring upstream URIs), and returns `{result, savedMedia}`. All other
upstream fields, including a remote `result.savedMedia`, remain untouched. The
local top-level `savedMedia` array (empty when nothing was saved) contains
entries (`contentIndex`, `kind`, `savedPath`, `mimeType`, `bytes`).
`--inline-media` skips extraction and wrapping and prints the verified MCP
result with Base64 strings and all remote fields untouched. Local records
reserve their path, MIME, kind, and field-name payload before construction and
retain that charge with the result. Path construction and the rollback list
reserve their overlapping allocations separately. MIME matching borrows its
source string.

Media directory resolution uses `<MCPCLI_RUNTIME_DIR>/media` when
`MCPCLI_RUNTIME_DIR` is set; otherwise Unix tries `$XDG_CACHE_HOME/mcpcli/media`,
`$HOME/.cache/mcpcli/media`, and `<runtime>/media`, and Windows uses
`%LOCALAPPDATA%\mcpcli\media` (`0700` or owner-only DACL, independent of Unix
socket path length limits). A canonical cache directory that cannot be
represented as UTF-8 fails with a local cache error before any media file is
published; paths in JSON are never lossy. Both the daemon and `--oneshot`
acquire `<media_dir>/.lock` to serialize garbage collection, the 2 GiB cache
cap, `.tmp-<pid>-<seq>-<rand>` writes (`flush` without `sync_all`), atomic
renames, and `.inflight-<id>.lock` batch leases. Garbage collection skips
active in-flight batches and files within 60 seconds of publication. Completing
local IPC or `--oneshot` stdout writeback records its completion timestamp in
the batch's existing lock file, without acquiring the directory lock. The file
records `writing` before output starts, then a completed timestamp or an
aborted write. GC imports completed timestamps before applying the 60-second
retention window. Ordinary decode or capacity errors roll back that call's
files; explicitly aborted writes do not refresh retention, and crashes before
writeback starts remain recoverable on the next GC pass.

After GC acquires an inactive batch's OS lock, a remaining `writing` or
unreadable record gets one conservative 60-second protection window. GC
persists that new timestamp before retiring the batch, so later collections can
recover its space without blocking other calls. An aborted write keeps its
original publication time. Because cache files survive reboots while the
monotonic clock restarts, GC normalizes future publication and completion
timestamps to the current time once; that preserves a full protection window
without retaining files indefinitely. Oneshot reports completion errors to
stderr and exits nonzero. If daemon completion fails after the IPC response has
been flushed, the daemon reports that local commit failure on its stderr
without emitting a second response frame. Only a missing index starts an empty
cache index. Other index read errors stop GC, and both index reads and atomic
writes use the same 4 MiB limit.

`--text` renders `text` blocks, inline text `resource` blocks (`type: "resource"`
carrying `resource.text`), `resource_link` blocks (`type: "resource_link"` with
URI and optional metadata), and `Saved <kind> [<mimeType>] (<bytes> B): <path>`
lines for saved binary media. Inline binary blocks retained via
`--inline-media` or unrecognized non-text blocks emit a summary notice on
stderr, and `structuredContent` JSON prints to stdout when no content blocks
were emitted (otherwise noting its presence on stderr). Text output normalizes
`CRLF` to `LF`, escapes bare `CR` and control characters, and emits nothing for
empty text blocks. If `--oneshot` cleanup fails or is interrupted after a tool
result arrives, the result is still written to stdout before reporting the
cleanup failure on stderr.

| Exit | Meaning |
| --- | --- |
| 0 | Command succeeded |
| 1 | Local runtime, media cache, output, or cleanup failure |
| 2 | CLI usage, configuration, argument validation, or unknown server/tool/skill |
| 3 | MCP `CallToolResult` returned `isError: true` |
| 4 | MCP protocol/transport/authentication failure or daemon communication failure |
| 5 | Operation or transfer deadline exceeded |
| 6 | Daemon protocol/binary/config mismatch or lost session requiring recovery |
| 130 | Interrupted by `SIGINT` or `Ctrl-C` |
| 143 | Interrupted by `SIGTERM` or `Ctrl-Break` |

Operational errors emit `{"error":{"code":...,"kind":...,"message":...}}` on
stderr (with `outcome_unknown: true` when dispatch already occurred). Top-level
`--help` appends the configured global `AGENTS.md` when valid. Check
`outcome_unknown` alongside the exit status before retrying a failed command.

## 10. Global AGENTS.md and Agent Skills

When `agents` is configured, `catalog`, `agents`, matching `search` queries,
and top-level `--help` return the full UTF-8 contents of that file without
requiring YAML frontmatter. `mcpcli` never walks working directories or parent
repositories for project-scoped `AGENTS.md` files. Reads pin and canonicalize
the parent directory and open the leaf file without following symlinks, reparse
points, special files, or Windows alternate data streams, while reflecting
normal file edits on the next invocation.

Each `skills` root mounts either a single skill directory containing `SKILL.md`
or a parent directory whose immediate subdirectories contain `SKILL.md`. YAML
frontmatter is parsed with `serde-saphyr` (supporting multiline scalars, BOM,
and `CRLF`) and checked for directory-name agreement and standard field types
while preserving extension fields. Duplicate skill names across distinct roots
fail discovery; repeated paths to the same directory deduplicate with a
warning, invalid individual manifests are skipped with a warning, and missing
configured roots fail. No command executes skill scripts, follows external
links, or treats `allowed-tools` as authorization.

Skill file reads traverse intermediate directories handle-by-handle: Unix uses
`openat` with `O_PATH` (Linux) or `O_SEARCH` (macOS and FreeBSD) plus
`O_NOFOLLOW | O_DIRECTORY` so traversal works beneath `0711` ancestors, reading
leaves with `O_RDONLY | O_NOFOLLOW | O_NONBLOCK` and `fdopendir`; Windows uses
relative `NtCreateFile` opens from a pinned root handle with reparse points
blocked and `GetFileInformationByHandleEx` directory enumeration. Symlinks,
reparse points, traversal segments, alternate data streams, special files, deep
trees, and non-UTF-8 or binary files are rejected.

| Limit | Value |
| --- | --- |
| CLI arguments / configuration | 1 MiB each |
| Argument container nesting | 64 |
| Process active byte budget | 1 GiB |
| IPC protocol / frame limits | Version 4; 4 MiB request / 512 MiB response or stream frame |
| IPC request read / frame transfer idle | 5 s / 60 s |
| Concurrent daemon event streams | 32 (`MAX_EVENT_STREAMS`) |
| Event notification / queue depth | 4 MiB / 128 notifications per subscription |
| Event catalog pages | 100 (`MAX_EVENT_PAGES`) |
| Cursor file / stream ack / callback stderr | 64 KiB each |
| MCP stdio message | 512 MiB per line |
| HTTP JSON response | 512 MiB |
| HTTP POST / GET SSE | 512 MiB per event |
| SSE event ID | 8 KiB of UTF-8 bytes |
| Media cache | 2 GiB total, 60 s retention after writeback |
| Tool catalog | 256 pages, 10,000 tools, 16 MiB |
| Global `AGENTS.md` | 1 MiB |
| Skill manifest / resource | 1 MiB |
| YAML frontmatter | 64 KiB, with node, depth, and alias budgets |
| Skills / scanned entries | 1,024 / 4,096 |
| Resource directory depth | 16 |

## 11. Verification

`ci.yml` checks formatting, Clippy (`-D warnings`), Rust unit, protocol, and
multimodal tests, Python unit tests, and the release CLI E2E suite across
Ubuntu, Alpine 3.23, Debian 12 (x86-64 and ARM64), FreeBSD 15.1, macOS 15
(Intel and Apple silicon), Windows Server 2022 (x86-64), and Windows 11
(ARM64). Container and FreeBSD lanes enforce the root `0711` traversal test,
Windows lanes run native DACL, reparse-point, and ownership checks and launch
daemon tests via Task Scheduler outside the runner's Job object, and cleanup
tests fail if process inspection errors occur. A separate job runs real
`fastexec`, Playwright MCP, and Chromium over stdio and Streamable HTTP.

`release.yml` validates preflight builds on `wip/release-checkpoint` and tagged
releases (`vX.Y.Z` matching `Cargo.toml` on `main`), builds all seven release
archives (including `LICENSE` and `NOTICE`), extracts and smoke-tests each
binary (including Alpine and Debian for Linux musl builds), writes
`SHA256SUMS`, and creates a draft GitHub Release.

## 12. References

- [MCP 2026-07-28 transports](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports)
- [MCP 2025-11-25 HTTP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
- [MCP Events extension (SEP-3415)](https://github.com/modelcontextprotocol/ext-events/blob/main/specification/beta/events/events.mdx)
- [MCP authorization specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization)
- [RFC 9728 OAuth 2.0 Protected Resource Metadata](https://datatracker.ietf.org/doc/html/rfc9728)
- [RFC 8414 OAuth 2.0 Authorization Server Metadata](https://datatracker.ietf.org/doc/html/rfc8414)
- [RFC 7591 OAuth 2.0 Dynamic Client Registration](https://datatracker.ietf.org/doc/html/rfc7591)
- [RFC 7636 Proof Key for Code Exchange (PKCE)](https://datatracker.ietf.org/doc/html/rfc7636)
- [RFC 8707 Resource Indicators for OAuth 2.0](https://datatracker.ietf.org/doc/html/rfc8707)
- [Official Rust MCP SDK](https://github.com/modelcontextprotocol/rust-sdk)
- [Agent Skills specification](https://agentskills.io/specification)
- [Microsoft Playwright MCP](https://github.com/microsoft/playwright-mcp)
- [fastexec daemon reference](https://github.com/ImoutoHeaven/fastexec-mcp/tree/a9b2985fd21143dc68abd0c04d795deeed91a8e4)
