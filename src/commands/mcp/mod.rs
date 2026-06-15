use anyhow::Result;
use clap::Args;
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars, tool, tool_router,
    transport::stdio,
    ErrorData as McpError, ServiceExt,
};
use serde::Deserialize;

use crate::utils::config::Config;
use crate::utils::db;
use crate::utils::format::pg_cell_to_string;

#[derive(Args, Clone, Debug)]
pub struct McpArgs {
    /// Transport type (stdio or sse)
    #[arg(long, default_value = "stdio")]
    pub transport: String,

    /// Host to bind (SSE transport only)
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port to bind (SSE transport only)
    #[arg(long, default_value_t = 3100)]
    pub port: u16,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryParams {
    pub sql: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTablesParams {
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeTableParams {
    pub table: String,
    pub schema: Option<String>,
}

#[derive(Clone)]
struct PgxMcpHandler {
    url: String,
    tls: bool,
}

impl PgxMcpHandler {
    async fn connect(&self) -> Result<tokio_postgres::Client, McpError> {
        db::connect(&self.url, self.tls).await.map_err(|e| {
            McpError::internal_error(
                "connection_failed",
                Some(serde_json::json!({ "error": e.to_string() })),
            )
        })
    }

    fn fmt_rows(rows: &[tokio_postgres::Row]) -> String {
        if rows.is_empty() {
            return "(0 rows)".to_string();
        }

        let columns = rows[0].columns();
        let col_count = columns.len();
        let mut out = String::new();

        for (i, col) in columns.iter().enumerate() {
            if i > 0 {
                out.push_str(" | ");
            }
            out.push_str(col.name());
        }
        out.push('\n');

        for row in rows {
            for i in 0..col_count {
                if i > 0 {
                    out.push_str(" | ");
                }
                let cell = pg_cell_to_string(row, i);
                if cell == "\0NULL" {
                    out.push_str("NULL");
                } else {
                    out.push_str(&cell);
                }
            }
            out.push('\n');
        }

        out
    }
}

#[tool_router(server_handler)]
impl PgxMcpHandler {
    #[tool(
        description = "Execute a SQL query against the connected database and return formatted results"
    )]
    async fn query(
        &self,
        Parameters(params): Parameters<QueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let client = self.connect().await?;
        let rows = client.query(&params.sql, &[]).await.map_err(|e| {
            McpError::internal_error(
                "query_failed",
                Some(serde_json::json!({ "error": e.to_string(), "sql": params.sql })),
            )
        })?;

        let result = Self::fmt_rows(&rows);
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "List tables in the database, optionally filtered by schema")]
    async fn list_tables(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> Result<CallToolResult, McpError> {
        let client = self.connect().await?;
        let rows = client
            .query(
                "SELECT schemaname, tablename \
                 FROM pg_tables \
                 WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
                   AND ($1 = '' OR schemaname = $1) \
                 ORDER BY schemaname, tablename",
                &[&params.schema.as_deref().unwrap_or("")],
            )
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "query_failed",
                    Some(serde_json::json!({ "error": e.to_string() })),
                )
            })?;

        if rows.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "(no tables found)",
            )]));
        }

        let mut result = String::new();
        for row in &rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            result.push_str(&format!("{}.{}\n", schema, table));
        }

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "Describe columns of a database table")]
    async fn describe_table(
        &self,
        Parameters(params): Parameters<DescribeTableParams>,
    ) -> Result<CallToolResult, McpError> {
        let client = self.connect().await?;
        let schema = params.schema.as_deref().unwrap_or("public");

        let rows = client
            .query(
                "SELECT column_name, data_type, is_nullable, column_default, \
                        character_maximum_length \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 ORDER BY ordinal_position",
                &[&schema, &params.table],
            )
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "query_failed",
                    Some(serde_json::json!({ "error": e.to_string() })),
                )
            })?;

        if rows.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Table {}.{} not found",
                schema, params.table
            ))]));
        }

        let mut result = format!("Table: {}.{}\n\n", schema, params.table);
        result.push_str("Column            Type           Nullable  Default\n");
        result.push_str("----------------- ------------- --------- -------\n");
        for row in &rows {
            let col: String = row.get(0);
            let dtype: String = row.get(1);
            let nullable: String = row.get(2);
            let default: Option<String> = row.get(3);
            let max_len: Option<i32> = row.get(4);
            let type_str = match max_len {
                Some(n) => format!("{}({})", dtype, n),
                None => dtype,
            };
            result.push_str(&format!(
                "{:<18} {:<14} {:<9} {}\n",
                col,
                type_str,
                nullable,
                default.as_deref().unwrap_or(""),
            ));
        }

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "Show database server information including version and current database")]
    async fn db_info(&self) -> Result<CallToolResult, McpError> {
        let client = self.connect().await?;
        let row = client
            .query_one("SELECT version(), current_database()", &[])
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "query_failed",
                    Some(serde_json::json!({ "error": e.to_string() })),
                )
            })?;

        let version: String = row.get(0);
        let db: String = row.get(1);

        let result = format!("Version: {}\nDatabase: {}", version, db);

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "List available connection profiles from pgx config")]
    async fn list_profiles(&self) -> Result<CallToolResult, McpError> {
        let cfg = Config::load().unwrap_or_default();
        if cfg.connections.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "(no profiles configured)",
            )]));
        }

        let mut result = String::new();
        for (name, conn) in &cfg.connections {
            let desc = conn.description.as_deref().unwrap_or("");
            let is_default = cfg.default.as_ref().map(|d| d == name).unwrap_or(false);
            let marker = if is_default { " (default)" } else { "" };
            result.push_str(&format!("{}{}: {}\n", name, marker, desc));
        }

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }
}

pub async fn run(url: String, args: McpArgs, use_tls: bool) -> Result<()> {
    let handler = PgxMcpHandler { url, tls: use_tls };

    match args.transport.as_str() {
        "stdio" => {
            let service = handler.serve(stdio()).await?;
            eprintln!("pgx MCP server ready (stdio transport)");
            service.waiting().await?;
        }
        other => anyhow::bail!("Unsupported transport: {other}. Supported: stdio"),
    }

    Ok(())
}
