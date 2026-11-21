# mcpcli

A native CLI for MCP tools, a global `AGENTS.md` file, and local [Agent Skills](https://agentskills.io).
A local daemon keeps stdio and Streamable HTTP connections alive across commands.

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

Linux archives use musl. Python or Node is needed only when a configured MCP server uses it.

## Configure

Create `mcpcli.json`, or select another file with `--config PATH` or `MCPCLI_CONFIG`:

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
      "headers": { "Authorization": "${MCP_AUTHORIZATION}" },
      "description": "Remote Streamable HTTP MCP service"
    }
  },
  "skills": []
}
~~~

Paths resolve from the configuration directory, and full `"${VAR}"` values expand
from the launch environment. On Windows, use `npx.cmd` for npm launchers. Optional
`agents` points to one global `AGENTS.md` file, and each `skills` entry mounts a
skill directory or a parent directory of skills.

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
mcpcli skills show example-skill
mcpcli skills files example-skill
mcpcli skills read example-skill references/guide.md
mcpcli daemon status
mcpcli reset filesystem
mcpcli daemon restart
mcpcli daemon stop
~~~

`call` accepts `--args JSON`, `--args-file PATH`, or `--stdin`, defaulting to `{}`.
Output is JSON on stdout, or readable text with `--text`. By default, `call` decodes
binary `image`, `audio`, and blob `resource` blocks into a private local cache and
returns `{result, savedMedia}` with local paths in `savedMedia`; pass
`--inline-media` to print the original result with Base64 payloads inline.

The daemon starts on demand. `--session NAME` isolates client sessions, `--oneshot`
opens a single-command connection, and `daemon run` runs in the foreground. Restart
the daemon after changing the configuration or binary.

See [docs/mcpcli-design.md](docs/mcpcli-design.md) for protocol behavior, security
boundaries, limits, and exit codes.

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
[Release runs](https://github.com/ImoutoHeaven/mcpcli/actions/workflows/release.yml)

## License

[Apache License, Version 2.0](LICENSE).
