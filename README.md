# mcpcli

A Rust CLI for MCP tools and local [Agent Skills](https://agentskills.io).
A local daemon keeps stdio and Streamable HTTP connections alive between commands.

## Install

Download an archive and `SHA256SUMS` from [releases](https://github.com/ImoutoHeaven/mcpcli/releases).
Verify the archive's checksum, extract it, and put `mcpcli` or `mcpcli.exe` on PATH.
Python and Node are needed only if a configured MCP server requires them.

| System | Architecture | Archive suffix |
| --- | --- | --- |
| Alpine / Debian Linux | x86-64 | `linux-amd64.tar.gz` |
| Alpine / Debian Linux | ARM64 | `linux-arm64.tar.gz` |
| FreeBSD | x86-64 | `freebsd-amd64.tar.gz` |
| macOS | Intel / Apple silicon | `macos-amd64.tar.gz` / `macos-arm64.tar.gz` |
| Windows | x86-64 / ARM64 | `windows-amd64.zip` / `windows-arm64.zip` |

Linux packages use musl. CI tests Alpine 3.23, Debian 12, FreeBSD 15.1, macOS 15,
Windows Server 2022 (x86-64), and Windows 11 (ARM64).

## Configure

Create `mcpcli.json`, or select a file with `--config PATH` or `MCPCLI_CONFIG`:

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

Set `MCP_AUTHORIZATION` to the full header value, such as `Bearer TOKEN`, or remove
`remote`. Environment references must occupy the entire value (`"${VAR}"`).
Paths resolve from the config directory. On Windows, use `npx.cmd`; explicit
executable paths accept either separator, but drive-relative paths such as
`C:server.exe` are rejected. Configured commands run with your user permissions.

Each `skills` entry mounts a directory containing `SKILL.md`, or a parent whose
immediate subdirectories contain skills. For example, `"skills": ["./skills"]`
loads `./skills/example-skill/SKILL.md`.

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

Calls accept `--args JSON`, `--args-file PATH`, or `--stdin`; omitted input is `{}`.
Input must be a JSON object with unique keys and numbers that retain their decimal
value when forwarded. Output is JSON by default; `--text` prints readable text.

The daemon starts on demand. `--session NAME` isolates client connections;
`--oneshot` opens a fresh connection for one command. A remote server can still
share application state across sessions. Tool calls are never automatically replayed.

~~~sh
mcpcli daemon status
mcpcli reset filesystem
mcpcli reset --force remote
mcpcli daemon stop
mcpcli daemon restart
mcpcli daemon run
~~~

Restart after changing the config or binary. An unexpected daemon exit requires
an explicit start or restart. `reset --force SERVER` discards a locally closed
session whose remote HTTP deletion failed, without resetting other servers.
Use `daemon run` for foreground supervision.

See [the design](docs/mcpcli-design.md) for limits, exit codes, and protocol scope.

## Build and test

Building requires Rust 1.92+ and a native C toolchain:

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

The runner executes Rust unit/protocol tests, Python checks, and CLI E2E tests.
Windows CI runs daemon tests outside the runner's process Job. Separate CI jobs
test real fastexec, Playwright MCP and Chromium over stdio/HTTP, and run each
extracted release package before creating a draft release.

[CI runs](https://github.com/ImoutoHeaven/mcpcli/actions/workflows/ci.yml) ·
[Release runs](https://github.com/ImoutoHeaven/mcpcli/actions/workflows/release.yml)

## License

[Apache License, Version 2.0](LICENSE).
