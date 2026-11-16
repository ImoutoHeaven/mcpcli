# mcpcli

A standalone Rust CLI for MCP tools and local [Agent Skills](https://agentskills.io).
Version 0.1.0 supports Linux, stdio, and Streamable HTTP.

## Build

~~~sh
cargo build --release --locked
cargo install --path . --locked
~~~

Rust 1.92+ is required to build. MCP servers keep their own runtime dependencies.

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
configuration file's directory. Configured stdio commands run with your user
permissions.

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
semantics, and the supported scope. Tests require Python 3 in addition to Rust.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
