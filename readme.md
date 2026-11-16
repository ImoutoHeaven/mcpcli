# mcpcli

A standalone Rust CLI for MCP tools and local [Agent Skills](https://agentskills.io).
Version 0.1.0 supports Alpine, Debian, FreeBSD, macOS, and Windows, with stdio
and Streamable HTTP transports.

## Install

Download an archive and `SHA256SUMS` from the [releases](https://github.com/ImoutoHeaven/mcpcli/releases).
Extract it, verify its checksum, and place `mcpcli` (or `mcpcli.exe`) on your PATH.
The executable includes its client runtime; Python and Node are only needed when
a configured MCP server requires them.

| System | Architecture | Archive suffix |
| --- | --- | --- |
| Alpine / Debian Linux | x86-64 | `linux-amd64.tar.gz` |
| Alpine / Debian Linux | ARM64 | `linux-arm64.tar.gz` |
| FreeBSD | x86-64 | `freebsd-amd64.tar.gz` |
| macOS | Intel / Apple silicon | `macos-amd64.tar.gz` / `macos-arm64.tar.gz` |
| Windows | x86-64 / ARM64 | `windows-amd64.zip` / `windows-arm64.zip` |

Linux archives use musl and run on both tested distributions. CI runs native
builds and tests on Alpine 3.23, Debian 12, FreeBSD 15.1, macOS 15, Windows Server
2022 (x86-64), and Windows 11 (ARM64).

## Build

~~~sh
cargo build --release --locked
cargo install --path . --locked
~~~

Rust 1.92+ and a native C toolchain are required to build. MCP servers keep their
own runtime dependencies.

## Configure

Create `mcpcli.json` in your working directory, or pass `--config PATH`:

~~~json
{
  "version": 1,
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
      "headers": { "Authorization": "${MCP_AUTHORIZATION}" },
      "description": "Remote Streamable HTTP MCP service"
    }
  },
  "skills": []
}
~~~

Each entry in `skills` mounts a local skill directory (containing `SKILL.md`) or
a parent directory whose immediate subdirectories are skills (for example,
`"skills": ["./skills"]` for `./skills/example-skill/SKILL.md`).

Set `MCP_AUTHORIZATION` to the full header value (for example, `Bearer TOKEN`),
or remove the `remote` entry when not needed. Environment references must occupy
the entire string value (`"${VAR}"`). Relative paths resolve from the
configuration file's directory. On Windows, use `npx.cmd` for npm-based servers;
explicit executable paths can use either directory separator. Drive-relative
paths such as `C:server.exe` are rejected. Configured stdio commands run with
your user permissions.

## Use

~~~sh
mcpcli catalog
mcpcli search file
mcpcli tools filesystem
mcpcli inspect filesystem read_file
mcpcli call filesystem read_file --args '{"path":"Cargo.toml"}'
mcpcli skills list
mcpcli skills show example-skill
mcpcli skills files example-skill
mcpcli skills read example-skill references/guide.md
~~~

Calls accept one of `--args JSON`, `--args-file PATH`, or `--stdin` (omitted
input defaults to `{}`). JSON is the default output; `--text` selects readable
text output. Arguments must be a JSON object with unique keys and numbers that
can be forwarded without decimal-value changes.

The daemon starts lazily and preserves connections across CLI invocations. Use
`--session NAME` to select an isolated client session, or `--oneshot` for a
single fresh connection. Calls are never automatically replayed. Session
isolation does not override a remote server's shared application state.

~~~sh
mcpcli daemon status
mcpcli reset filesystem
mcpcli reset --force remote
mcpcli daemon stop
mcpcli daemon restart
mcpcli daemon run
~~~

Changing the configuration or binary requires `daemon restart`. An unexpected
daemon exit requires an explicit `daemon start` or `daemon restart`. If a
remote HTTP server fails to confirm `DELETE` during session reset after the
local client has stopped, `reset --force SERVER` discards the local session
slot without disrupting other servers in the same daemon session. `daemon run`
supports foreground process supervision.

See [the design](docs/mcpcli-design.md) for limits, exit codes, transport
semantics, and the supported scope.

## Verify

~~~sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
python3 scripts/ci/run-tests.py
~~~

Tests require Python 3.11+ in addition to Rust (`python` on Windows). The runner
executes Rust unit/protocol tests and the release CLI E2E suite. Native Windows
CI runs persistent-daemon tests outside the runner's process Job. The real-service
CI lane additionally tests fastexec, Playwright MCP and Chromium over stdio and
HTTP. Release CI extracts each archive and exercises its actual executable
before creating a draft release.

[CI runs](https://github.com/ImoutoHeaven/mcpcli/actions/workflows/ci.yml) ·
[Release runs](https://github.com/ImoutoHeaven/mcpcli/actions/workflows/release.yml)

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
