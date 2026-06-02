use anyhow::Result;
use clap::{Args, Subcommand};
use colored::Colorize;

use crate::utils::config::Config;

/// Manage pgx connection profiles
#[derive(Args, Debug)]
pub struct ProfilesArgs {
    #[command(subcommand)]
    pub command: ProfilesCommands,
}

#[derive(Subcommand, Debug)]
pub enum ProfilesCommands {
    /// List all configured connection profiles
    List,

    /// Show details of a specific connection profile
    Show {
        /// Connection profile name
        name: String,
    },
}

pub fn run(args: &ProfilesArgs) -> Result<()> {
    match &args.command {
        ProfilesCommands::List => cmd_list(),
        ProfilesCommands::Show { name } => cmd_show(name),
    }
}

fn cmd_list() -> Result<()> {
    let cfg = Config::load()?;
    let path = Config::path()?;
    let exists = if path.exists() {
        path.display().to_string()
    } else {
        "(not found, using defaults)".to_string()
    };

    println!("{}", "── Connection Profiles ──".cyan().bold());
    println!("  Config: {}", exists.dimmed());
    println!();

    if cfg.connections.is_empty() {
        println!("  {}", "(no profiles configured)".dimmed());
        println!();
        println!("  Add profiles in ~/.pgx/config.toml:");
        println!(
            "  {}",
            "  [connections.my_db]\n  url = \"postgres://user:pass@host:5432/db\"".dimmed()
        );
        return Ok(());
    }

    for (name, conn) in &cfg.connections {
        let is_default = cfg.default.as_deref() == Some(name);
        let default_tag = if is_default {
            " (default)".green().to_string()
        } else {
            String::new()
        };
        let desc = conn
            .description
            .as_deref()
            .map(|d| format!("  {}", d.dimmed()))
            .unwrap_or_default();
        let has_listen = if conn.listen.is_some() {
            " listen".cyan()
        } else {
            "".dimmed()
        };
        let has_replicate = if conn.replicate.is_some() {
            " replicate".cyan()
        } else {
            "".dimmed()
        };
        println!(
            "  {}{}  {}{}{}",
            name.yellow().bold(),
            default_tag,
            mask_password(&conn.url).dimmed(),
            has_listen,
            has_replicate,
        );
        if !desc.is_empty() {
            println!("    {desc}");
        }
    }

    Ok(())
}

fn cmd_show(name: &str) -> Result<()> {
    let cfg = Config::load()?;
    let conn = cfg
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("No connection profile named '{}'", name))?;

    let is_default = cfg.default.as_deref() == Some(name);

    println!("{}", format!("── Profile: {name} ──").cyan().bold());
    if is_default {
        println!("  {} (default)", "★".green().bold());
    }
    println!();
    println!("  URL:   {}", mask_password(&conn.url).yellow());
    if let Some(ref desc) = conn.description {
        println!("  Desc:  {desc}");
    }
    println!();

    if let Some(ref listen) = conn.listen {
        println!("  {}", "Listen:".cyan().bold());
        if !listen.channels.is_empty() {
            println!("    channels:      {}", listen.channels.join(", "));
        }
        if let Some(n) = listen.max_reconnect_attempts {
            println!("    max_reconn:    {n}");
        }
        if let Some(ms) = listen.reconnect_base_ms {
            println!("    reconnect_ms:  {ms}");
        }
        if let Some(ms) = listen.reconnect_max_ms {
            println!("    reconnect_max: {ms}");
        }
        if let Some(ref sink) = listen.sink {
            println!("    sink:          {sink:#?}");
        }
        println!();
    }

    if let Some(ref repl) = conn.replicate {
        println!("  {}", "Replicate:".cyan().bold());
        if let Some(ref slot) = repl.slot {
            println!("    slot:          {slot}");
        }
        if !repl.publications.is_empty() {
            println!("    publications:  {}", repl.publications.join(", "));
        }
        if !repl.tables.is_empty() {
            println!("    tables:        {}", repl.tables.join(", "));
        }
        if !repl.ops.is_empty() {
            println!("    ops:           {}", repl.ops.join(", "));
        }
        if let Some(v) = repl.temporary {
            println!("    temporary:     {v}");
        }
        if let Some(ref sink) = repl.sink {
            println!("    sink:          {sink:#?}");
        }
    }

    Ok(())
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
