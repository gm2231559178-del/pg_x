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

### 5. Add Streamable HTTP transport (SSE) + optional Bearer token auth
- Add `transport-streamable-http-server` feature to `rmcp` dependency
- Add `axum` HTTP framework (optional, behind `mcp` feature)
- Create `StreamableHttpService` from handler factory via `|| Ok(handler.clone())`
- Mount at `/mcp` using `axum::Router::nest_service`
- Add `/health` endpoint
- Optional `--token` CLI arg enables Bearer token auth middleware on `/mcp`
- Verify: `cargo build --features mcp && cargo run --features mcp -- mcp --transport sse`

### 6. Add OIDC/JWKS token validation
- Add `jsonwebtoken` crate + enable `reqwest` under `mcp` feature
- Add `--oauth-issuer` CLI arg for OIDC provider URL (e.g. Keycloak)
- On startup: auto-discover `jwks_uri` from `/.well-known/openid-configuration`
- Fetch and cache JWKS keys; per-request: validate JWT signature, `exp`, `iss`
- Falls back to first JWK key if `kid` absent in JWT header
- Auth precedence: `--oauth-issuer` > `--token` > none
- Verify: `cargo build --features mcp`

## Future Enhancements (not in scope v2)
- Full OAuth2 authorization server (token endpoint, refresh tokens, client registration)
- JWKS key rotation (re-fetch on validation failure)
- Connection pooling (deadpool-postgres)
- More tools: `export_data`, `explain_query`, `pg_dump`-like
- MCP resources (schema definitions, table data)
- MCP prompts (query templates)
