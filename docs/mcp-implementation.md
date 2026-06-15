# pgx MCP Server Implementation Plan

## Overview
Add MCP (Model Context Protocol) server support to pgx, allowing LLMs (Claude, etc.) to interact with PostgreSQL through pgx as an MCP server.

## Implementation Steps

### 1. Add `rmcp` dependency and `mcp` feature to `Cargo.toml`
- Add `rmcp` crate (official Rust MCP SDK) with server, stdio transport, and macros
- Feature-gate under `mcp` feature

### 2. Create MCP server module (`src/commands/mcp/mod.rs`)
- `McpArgs` CLI args: `--transport stdio|sse`, `--host`, `--port`
- `PgxMcpHandler` with DB connection URL
- Tool definitions using `#[tool_router(server_handler)]`:
  - `query(sql)` — execute SQL, return formatted results
  - `list_tables(schema?)` — introspect tables
  - `describe_table(table, schema?)` — column info
  - `db_info()` — server version, database info
- `run()` function that starts the MCP server

### 3. Wire into CLI
- Add `pub mod mcp` to `src/commands/mod.rs`
- Add `Mcp(McpArgs)` variant to Commands enum in `src/main.rs`
- Add dispatch arm in `main()`

### 4. Verify compilation
- `cargo build --features mcp`

## Future Enhancements (not in scope v1)
- SSE/Streamable HTTP transport for remote access
- OAuth2 authorization for remote transport
- Connection pooling (deadpool-postgres)
- More tools: `export_data`, `explain_query`, `pg_dump`-like
- MCP resources (schema definitions, table data)
- MCP prompts (query templates)
