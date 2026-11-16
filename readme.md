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

Create mcpcli.json in your working directory, or pass --config PATH:

~~~json
{
  "version": 1,
  "servers": {
    "local": {
      "transport": "stdio",
      "command": "fastexec",
      "env": { "FASTEXEC_DAEMON": "off" }
    },
    "remote": {
      "transport": "http",
      "url": "https://example.com/mcp",
      "headers": { "Authorization": "${MCP_AUTHORIZATION}" }
    }
  },
  "skills": []
}
~~~

Each entry in skills mounts a local skill directory or a directory of skills.
Add your skill directories to the skills list.
Set MCP_AUTHORIZATION to the full header value (for example, Bearer TOKEN), or
remove the remote example. Environment references must occupy the entire value.
Paths resolve from the configuration file. Configured commands run with your user
permissions.

## Use

~~~sh
mcpcli catalog
mcpcli search browser
mcpcli tools local
mcpcli inspect local read
mcpcli call local read --args '{"path":"/etc/hosts","limit":5}'
mcpcli skills list
mcpcli skills show browser-check
mcpcli skills read browser-check references/guide.md
~~~

Calls accept one of --args JSON, --args-file PATH, or --stdin. JSON is the default
output; --text selects readable output. Arguments must be an object, with unique
keys and numbers that can be forwarded without decimal-value changes.

The daemon starts lazily and preserves connections between commands. Use
--session NAME to select a separate client session, or --oneshot for one fresh
connection. Calls are never automatically replayed. Session isolation does not
override a remote server's shared application state.

~~~sh
mcpcli daemon status
mcpcli reset local
mcpcli daemon stop
mcpcli daemon restart
mcpcli daemon run
~~~

Changing the configuration or binary requires daemon restart. An unexpected
daemon death requires an explicit daemon start or restart. daemon run supports
foreground supervision.

See [the design](docs/mcpcli-design.md) for limits, exit codes, transport semantics,
and the supported scope. Tests require Python 3 in addition to Rust.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
