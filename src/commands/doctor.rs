use anyhow::Result;
use clap::Args;
use colored::Colorize;
use std::process::Command;

use crate::utils::config::Config;
use crate::utils::db;

/// Diagnose your pgx installation and environment
#[derive(Args, Debug)]
pub struct DoctorArgs;

pub async fn run(
    _args: &DoctorArgs,
    cli_url: Option<String>,
    cli_conn: Option<String>,
) -> Result<()> {
    let mut all_ok = true;

    println!("{}", "── pgx doctor ──".cyan().bold());
    println!();

    // 1. pgx version
    print!("  {} pgx binary ... ", "✔".green());
    println!("{}", env!("CARGO_PKG_VERSION"));

    // 2. psql
    check_psql(&mut all_ok);

    // 3. Config file
    check_config(&mut all_ok);

    // 4. Database connectivity
    check_connection(cli_url, cli_conn, &mut all_ok).await;

    // 5. System libraries
    check_system_deps(&mut all_ok);

    println!();
    if all_ok {
        println!("{} Everything looks good!", "✔".green().bold());
    } else {
        println!("{} Some checks failed (see above).", "✖".red().bold());
    }

    Ok(())
}

fn check_psql(all_ok: &mut bool) {
    print!("  {} psql ... ", "✔".green());
    match Command::new("psql").arg("--version").output() {
        Ok(out) => {
            let ver = String::from_utf8_lossy(&out.stdout).trim().to_string();
            println!("{ver}");
        }
        Err(_) => {
            println!("{}", "not found".red());
            eprintln!("       Install: brew install libpq / apt install postgresql-client");
            *all_ok = false;
        }
    }
}

fn check_config(all_ok: &mut bool) {
    let path = match Config::path() {
        Ok(p) => p,
        Err(e) => {
            println!("  {} config path: {e}", "✖".red());
            *all_ok = false;
            return;
        }
    };

    if !path.exists() {
        println!(
            "  {} config file: {} (not found — using defaults)",
            "ℹ".cyan(),
            path.display().to_string().dimmed()
        );
        return;
    }

    print!(
        "  {} config file: {} ... ",
        "✔".green(),
        path.display().to_string().dimmed()
    );

    match Config::load() {
        Ok(cfg) => {
            println!("OK");
            let conn_count = cfg.connections.len();
            let default = cfg.default.as_deref().unwrap_or("(none)");
            println!("       {} connection(s), default: {default}", conn_count);
            for (name, conn) in &cfg.connections {
                let desc = conn.description.as_deref().unwrap_or("");
                println!("         {name:<20} {}  {desc}", mask_password(&conn.url));
            }
        }
        Err(e) => {
            println!("{}", "INVALID".red());
            eprintln!("       {e}");
            *all_ok = false;
        }
    }
}

async fn check_connection(cli_url: Option<String>, cli_conn: Option<String>, all_ok: &mut bool) {
    // Resolve a URL to test: CLI flag > config default
    let url = {
        let cfg = Config::load().unwrap_or_default();
        match cfg.resolve_from(cli_url, cli_conn) {
            Ok((u, _)) => u,
            Err(_) => {
                println!(
                    "  {} database: no URL provided and no config default set",
                    "ℹ".cyan()
                );
                return;
            }
        }
    };

    let display_url = mask_password(&url);
    print!("  {} connect to {} ... ", "✔".green(), display_url.dimmed());

    match db::connect(&url, false).await {
        Ok(client) => {
            println!("OK");
            // Check version
            if let Ok(row) = client.query_one("SELECT version()", &[]).await {
                let v: String = row.get(0);
                println!("       {v}");
            }
            // Check wal_level
            if let Ok(row) = client.query_one("SHOW wal_level", &[]).await {
                let w: String = row.get(0);
                if w == "logical" {
                    println!("       wal_level = logical  {}", "✔".green());
                } else {
                    println!(
                        "       wal_level = {w}  {} (replication needs 'logical')",
                        "⚠".yellow()
                    );
                }
            }
        }
        Err(e) => {
            println!("{}", "FAILED".red());
            eprintln!("       {e}");
            *all_ok = false;
        }
    }
}

fn check_system_deps(_all_ok: &mut bool) {
    // Kafka: check for librdkafka
    #[cfg(feature = "kafka")]
    {
        print!("  {} librdkafka ... ", "✔".green());
        // Try to detect via pkg-config or ldconfig
        let found = Command::new("ldconfig")
            .arg("-p")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).contains("librdkafka"))
            .unwrap_or(false)
            || Command::new("pkg-config")
                .args(["--exists", "librdkafka"])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);

        if found {
            println!("found");
        } else {
            println!("{}", "not detected".yellow());
            println!("       Install: sudo apt install librdkafka-dev");
        }
    }
}

fn mask_password(url: &str) -> String {
    let mut s = url.to_string();
    if let Some(at) = s.find('@') {
        if let Some(colon) = s[..at].rfind(':') {
            let before = &s[..colon + 1];
            let after = &s[at..];
            s = format!("{before}****{after}");
        }
    }
    s
}
