use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;
use std::process::Command;

/// Open an interactive PostgreSQL session via psql
#[derive(Args, Debug)]
pub struct PsqlArgs {
    /// SQL command to execute non-interactively (like psql -c)
    #[arg(short = 'c', long)]
    pub command: Option<String>,

    /// Execute a SQL file (like psql -f)
    #[arg(short = 'f', long)]
    pub file: Option<String>,
}

pub fn run(url: String, args: PsqlArgs) -> Result<()> {
    let psql = which_psql()?;

    let mut cmd = Command::new(&psql);
    cmd.arg(url);

    if let Some(ref c) = args.command {
        cmd.arg("-c").arg(c);
    }
    if let Some(ref f) = args.file {
        cmd.arg("-f").arg(f);
    }

    let status = cmd
        .status()
        .with_context(|| format!("Failed to execute psql: {}", psql))?;

    if !status.success() {
        anyhow::bail!("psql exited with status: {}", status);
    }

    Ok(())
}

fn which_psql() -> Result<String> {
    // Check common locations
    for candidate in &["psql", "/usr/bin/psql", "/usr/local/bin/psql", "/opt/homebrew/bin/psql"] {
        if Command::new(candidate)
            .arg("--version")
            .output()
            .is_ok()
        {
            return Ok(candidate.to_string());
        }
    }
    anyhow::bail!(
        "{} psql not found. Install it via your package manager:\n  \
         brew:  brew install libpq && brew link --force libpq\n  \
         apt:   sudo apt install postgresql-client\n  \
         yum:   sudo yum install postgresql",
        "✖".red(),
    );
}
