# mcpcli

A native CLI for MCP tools, MCP Events, OAuth 2.1 HTTP servers, a global `AGENTS.md` file,
and local [Agent Skills](https://agentskills.io). A local daemon keeps stdio and Streamable HTTP
connections alive across commands.

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
      "description": "Local workspace file operations"
    },
    "remote": {
      "transport": "http",
      "url": "https://example.com/mcp",
      "headers": { "Authorization": "Bearer ${MCP_TOKEN}" },
      "description": "Remote Streamable HTTP MCP service"
    }
  },
  "skills": []
}
~~~

Paths resolve from the configuration directory. `env` expands whole-value references such as
`"${VAR}"`. HTTP `headers` also expand inline templates such as `"Bearer ${MCP_TOKEN}"`.
Both use the launch environment; `args` pass directly to the OS process API without interpolation.
On Windows, use `npx.cmd` for npm launchers. Optional `agents` points to one global `AGENTS.md`
file, and each `skills` entry mounts a skill directory or a parent directory of skills.

## Authenticate

For OAuth 2.1 Streamable HTTP servers without a static `Authorization` header, sign in with
PKCE (`S256`):

~~~sh
mcpcli auth login remote
mcpcli auth login remote --scope mcp:tools --scope mcp:events --client-id my-cli --no-browser
mcpcli auth status
mcpcli auth status remote
mcpcli auth logout remote
~~~

`auth login` discovers Protected Resource Metadata and Authorization Server Metadata, registers
a public client for that login's ephemeral `127.0.0.1:0` loopback callback `redirect_uri` when
`--client-id` is omitted, and completes Authorization Code + PKCE (`S256`) over the loopback
callback (or reads a pasted redirect URL or authorization code on `stdin` with `--no-browser`).

Credentials live in a private file (`0600` on Unix, owner-only DACL on Windows) resolved in order:
`MCPCLI_CREDENTIALS`, `<MCPCLI_RUNTIME_DIR>/credentials.json`, `$XDG_STATE_HOME/mcpcli/credentials.json`
or `$HOME/.local/state/mcpcli/credentials.json` or `<runtime>/credentials.json` on Unix, and
`%LOCALAPPDATA%\mcpcli\state\credentials.json` on Windows. Stored tokens stay outside `mcpcli.json`
and rotate under a per-resource file lock shared by daemon and oneshot processes near expiry or
upon any `401` response when a `refresh_token` is present, without restarting the daemon.

## Use

~~~sh
mcpcli catalog
mcpcli agents
mcpcli search file
mcpcli server filesystem
mcpcli tools filesystem
mcpcli inspect filesystem read_file
mcpcli call filesystem read_file --args '{"path":"Cargo.toml"}'
mcpcli call --inline-media filesystem read_file --args '{"path":"logo.png"}'
mcpcli skills list
mcpcli skill show example-skill
mcpcli skills files example-skill
mcpcli skills read example-skill references/guide.md
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
`--stdin` (`call` defaults to `{}`). `skill` is an alias for `skills`. Output is JSON on stdout,
or readable text with `--text`. By default, `call` decodes binary `image`, `audio`, and blob
`resource` blocks into a private local cache and returns `{result, savedMedia}` with local paths
in `savedMedia`; pass `--inline-media` to keep Base64 payloads inline.

`events stream <SERVER> <EVENT> [--mode auto|push|poll] [--cursor-file <PATH>] [--exec-mode oneshot|stream] [-- <CMD>...]`
writes unmodified SEP-3415 `EventOccurrence` objects as NDJSON on `stdout`, or delivers each
event over `stdin` to `<CMD>...` before advancing `--cursor-file`. Batch or stream truncation
diagnostics go to `stderr`. `--timeout` sets the request deadline in seconds (`1` to `3600`,
default `60`) for regular commands, and sets the run deadline budget in milliseconds for
`events stream`.

The daemon starts on demand. `--session NAME` or `MCPCLI_SESSION` isolates client sessions,
`--oneshot` opens a single-command connection, and `daemon run` runs in the foreground. Restart
the daemon after changing `mcpcli.json` or the `mcpcli` binary.

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
