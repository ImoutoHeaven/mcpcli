# mcpcli

A native CLI for MCP tools, MCP Events, MCP Tasks, OAuth 2.1 HTTP servers, a global `AGENTS.md`
file, and local or remote [Agent Skills](https://agentskills.io). A local daemon keeps stdio and
Streamable HTTP connections alive across commands.

## Install

Download an archive and `SHA256SUMS` from [releases](https://github.com/ImoutoHeaven/mcpcli/releases),
verify the checksum, and put `mcpcli` or `mcpcli.exe` on `PATH`.

| System | Architecture | Archive suffix |
| --- | --- | --- |
| Alpine / Debian Linux | x86-64 | `linux-amd64.tar.gz` |
| Alpine / Debian Linux | ARM64 | `linux-arm64.tar.gz` |
| FreeBSD | x86-64 | `freebsd-amd64.tar.gz` |
| macOS | Intel / Apple silicon | `macos-amd64.tar.gz` / `macos-arm64.tar.gz` |
| Windows | x86-64 / ARM64 | `windows-amd64.zip` / `windows-arm64.zip` |

Linux archives use static musl. Python or Node is needed only when a configured MCP server uses it.

## Configure

Create `mcpcli.json`, or select another file with `-c PATH`, `--config PATH`, or `MCPCLI_CONFIG`:

~~~json
{
  "version": 1,
  "agents": "./AGENTS.md",
  "servers": {
    "filesystem": {
      "transport": "stdio",
      "command": "npx",
      "args": ["--yes", "@modelcontextprotocol/server-filesystem", "."],
      "timeout": 120,
      "description": "Local workspace file operations"
    },
    "remote": {
      "transport": "http",
      "url": "https://example.com/mcp",
      "headers": { "Authorization": "Bearer ${MCP_TOKEN}" },
      "enabled": true,
      "description": "Remote Streamable HTTP MCP service"
    }
  },
  "skills": []
}
~~~

Paths resolve from the configuration directory; `"~"` and leading `"~/"` (plus `"~\\"` on Windows)
expand from `HOME` or `USERPROFILE` in `agents`, `skills[]`, `stdio.command`, and `stdio.cwd`.
`env` expands whole-value references such as `"${VAR}"`. HTTP `headers` and `oauth` (`client_id`,
`client_secret`) also expand inline templates such as `"Bearer ${MCP_TOKEN}"`.
Both use the launch environment; `args` pass directly to the OS process API without interpolation.
On Windows, use `npx.cmd` for npm launchers. Optional per-server `enabled: false` disables a server
without requiring its local `cwd`, `HOME`, or `${VAR}` secrets on the current host, and optional
`timeout` (`1` to `3600` seconds) sets that server's default `--timeout`. HTTP servers also accept
an optional `oauth` object (`client_id`, `client_secret`, `client_name`, `callback_port`,
`callback_path`, `scopes`, `auth_server_metadata_url`, `account`). Optional `agents` points to one
global `AGENTS.md` file, and each `skills` entry mounts a skill directory or a parent directory of
skills.

## Authenticate

For OAuth 2.1 Streamable HTTP servers without a static `Authorization` header, sign in with
PKCE (`S256`):

~~~sh
mcpcli auth login remote
mcpcli auth login remote --callback-port 8765 --callback-path /callback --client-name my-cli
mcpcli auth login remote --scope mcp:tools --scope mcp:events --client-id my-cli --no-browser
mcpcli auth status
mcpcli auth status remote
mcpcli auth logout remote
~~~

`auth login` discovers Protected Resource Metadata and Authorization Server Metadata (or fetches
`oauth.auth_server_metadata_url` directly when configured), binds `127.0.0.1:<port><path>`
(ephemeral port `0` and `/callback` by default), registers a public client when no `client_id` is
configured or passed, and completes Authorization Code + PKCE (`S256`) over the loopback callback
(or reads a pasted redirect URL or authorization code on `stdin` with `--no-browser`). Set
`oauth.account` to isolate credentials for multiple accounts against the same server URL.

Credentials live in a private file (`0600` on Unix, owner-only DACL on Windows) resolved in order:
`MCPCLI_CREDENTIALS`, `<MCPCLI_RUNTIME_DIR>/credentials.json`, `$XDG_STATE_HOME/mcpcli/credentials.json`
or `$HOME/.local/state/mcpcli/credentials.json` or `<runtime>/credentials.json` on Unix, and
`%LOCALAPPDATA%\mcpcli\state\credentials.json` on Windows. Stored tokens stay outside `mcpcli.json`
and rotate under a per-resource file lock shared by daemon and oneshot processes near expiry or
upon any `401` response when a `refresh_token` is present, without restarting the daemon.

## Use

~~~sh
mcpcli config validate
mcpcli catalog
mcpcli agents
mcpcli search file
mcpcli server filesystem
mcpcli tools filesystem
mcpcli tools filesystem --filter read --summary
mcpcli inspect filesystem read_file
mcpcli inspect filesystem read_file --schema read_file.schema.json --schema-mode compatible
mcpcli call filesystem read_file --args '{"path":"Cargo.toml"}'
mcpcli call filesystem read_file path:=Cargo.toml
mcpcli call filesystem read_file -- -flag:=1
mcpcli call filesystem read_file --schema read_file.schema.json path:=Cargo.toml
mcpcli call filesystem long_task --max-timeout 600 --args '{}'
mcpcli call filesystem long_task --detach --args '{}'
mcpcli tasks get filesystem task_123 --wait --max-timeout 600
mcpcli tasks update filesystem task_123 --args '{"confirm":true}'
mcpcli tasks cancel filesystem task_123
mcpcli call --inline-media filesystem read_file --args '{"path":"logo.png"}'
mcpcli skills builtin
mcpcli skills list
mcpcli skill show example-skill
mcpcli skills files example-skill
mcpcli skills read example-skill references/guide.md
mcpcli skills list --server remote
mcpcli skills show --server remote example-skill
mcpcli skills files --server remote --dir references example-skill
mcpcli skills read --server remote --inline-media example-skill references/guide.md
mcpcli events list remote
mcpcli events poll remote github.pr
mcpcli events stream remote github.pr --mode auto --cursor-file ./github.cursor
mcpcli events stream remote github.pr --cursor-file ./github.cursor --exec-mode stream -- my-handler
mcpcli daemon status
mcpcli reset filesystem
mcpcli daemon restart
mcpcli daemon stop
~~~

`call` and `events poll` / `events stream` accept `--args JSON`, `--args-file PATH`, or
`--stdin` (`call` defaults to `{}`). `call` also accepts trailing `KEY:=VALUE` positional
arguments (mutually exclusive with `--args`, `--args-file`, and `--stdin`). Each item splits on
the first `:=`, parses valid lossless JSON literals into typed JSON values, and falls back to a
string otherwise. Pass `--` before keys that start with `-` (for example, `-- -flag:=1`).

`inspect` and `call` accept `--schema <FILE>` and `--schema-mode exact|compatible` (default
`compatible`). The snapshot file can be a full tool object containing `inputSchema` (and optional
`outputSchema`) or a raw input schema object. `compatible` checks backward-compatible subschema
evolution (such as new optional input parameters or widening `integer` to `number`) and fails
closed on non-core keywords; `exact` requires normalized deep equality. A mismatch exits with
code `2` (`kind: "schema_mismatch"`) before sending `tools/call`.

Output is JSON on stdout, or readable text with `--text`. By default, `call` decodes binary
`image`, `audio`, and blob `resource` blocks into a private local cache and returns
`{result, savedMedia}` with local paths in `savedMedia`; pass `--inline-media` to keep Base64
payloads inline.

### Skills

`skill` is an alias for `skills`. `mcpcli skills builtin` runs offline without `mcpcli.json` and
outputs the embedded agent skill guide (`--text` writes raw `SKILL.md`).

Without `--server`, `skills list|show|files|read` reads local directories from `mcpcli.json`.
With `--server <SERVER>`, `skills` queries remote Skills over MCP (`SEP-2640`) while keeping
remote skills isolated from local skill roots. `skills files --server <SERVER>` accepts
`--dir <REL_DIR>` for directory reads, and `skills read --server <SERVER>` accepts
`--inline-media`. Static remote manifests are verified for clean relative paths under the skill
root, exact byte length, `sha256:` digests, and YAML frontmatter equality against the manifest
before any content is written to disk or stdout. Binary remote resources save through `MediaStore`
by default.

### MCP Tasks

For servers advertising `io.modelcontextprotocol/tasks` (`SEP-2663`), `mcpcli call` polls
`resultType: "task"` responses until completion by default. Pass `--detach` to return the initial
`CreateTaskResult` handle immediately without polling.

`tasks` (alias `task`) manages existing tasks:

- `tasks get <SERVER> <TASK_ID> [--wait] [--inline-media] [--max-timeout <SECS>]` fetches a task
  snapshot or waits for a terminal state, extracting completed binary media into `savedMedia`
  unless `--inline-media` is set.
- `tasks update <SERVER> <TASK_ID> (--args <JSON> | --args-file <PATH> | --stdin)` submits
  `inputResponses` for an `input_required` task.
- `tasks cancel <SERVER> <TASK_ID>` requests task cancellation.

Once `call` receives a `CreateTaskResult`, any non-`completed` failure (including `failed`,
`cancelled`, `input_required`, timeout, `SIGINT`/`SIGTERM`, or `session_lost`) sets
`outcome_unknown: true` and attaches `data.taskId` so callers do not blindly replay side effects.
Read-only `tasks get` keeps `outcome_unknown: false` on all failure exits.

### Events and daemon

`events stream <SERVER> <EVENT> [--mode auto|push|poll] [--cursor-file <PATH>] [--exec-mode oneshot|stream] [-- <CMD>...]`
writes unmodified SEP-3415 `EventOccurrence` objects as NDJSON on `stdout`, or delivers each
event over `stdin` to `<CMD>...` before advancing `--cursor-file`. Batch or stream truncation
diagnostics go to `stderr`. `--timeout` sets the request deadline in seconds (`1` to `3600`,
defaulting to the server's `timeout` or `60`) for regular commands, acts as the progress-resettable
inactivity timeout for `call` (with optional `--max-timeout` in `1` to `86400` seconds; unbounded
when omitted), and sets the run deadline budget in milliseconds for `events stream`.

The daemon starts on demand. `--session NAME` or `MCPCLI_SESSION` isolates client sessions,
`--oneshot` opens a single-command connection, and `daemon run` runs in the foreground. Use
`daemon restart` after configuration changes or binary updates that keep the same IPC version.

When upgrading from a previous IPC protocol version (for example, IPC 5 in v0.7 to IPC 6), run
`mcpcli daemon stop` with the older binary before replacing it, selecting each active
configuration and session. After installing the new binary, `daemon start` or the next command
starts a new daemon using IPC version 6. If the binary was already replaced, use a retained or
re-downloaded copy of the previous binary to stop the old daemon.

See [docs/mcpcli-design.md](docs/mcpcli-design.md) for protocol behavior, security boundaries,
limits, and exit codes.

## Build and test

Building requires Rust 1.92+ and a C toolchain:

~~~sh
cargo build --release --locked
cargo install --path . --locked
~~~

Tests also require Python 3.11+ (`python` on Windows):

~~~sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
python3 scripts/ci/run-tests.py
~~~

[CI runs](https://github.com/ImoutoHeaven/mcpcli/actions/workflows/ci.yml) ·
[Release runs](https://github.com/ImoutoHeaven/mcpcli/actions/workflows/release.yml) ·
[Release guide](RELEASING.md)

## License

[Apache License, Version 2.0](LICENSE) and [third-party notices](NOTICE).
