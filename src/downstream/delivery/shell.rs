use anyhow::{Context, Result};
use std::collections::HashMap;

/// Shell transport handle shared by the sink seams.
///
/// Hides process spawning behind one `run` method; the caller builds the full
/// child environment (payload, channel, contract overrides).
pub struct Shell {
    command: String,
}

impl Shell {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
        }
    }

    /// Run the command via `sh -c`, with `env` as the child environment.
    /// Errors when the command cannot be spawned or exits non-zero.
    pub async fn run(&self, env: &HashMap<String, String>) -> Result<()> {
        let status = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .envs(env)
            .status()
            .await
            .context("Failed to spawn shell command")?;

        if !status.success() {
            anyhow::bail!(
                "Shell command exited with status: {}",
                status.code().unwrap_or(-1)
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Shell;
    use std::collections::HashMap;

    #[tokio::test]
    async fn runs_command_with_env() {
        let shell = Shell::new("test \"$PGX_PAYLOAD\" = hello");
        let mut env = HashMap::new();
        env.insert("PGX_PAYLOAD".to_string(), "hello".to_string());
        shell.run(&env).await.unwrap();
    }

    #[tokio::test]
    async fn non_zero_exit_is_an_error() {
        let shell = Shell::new("exit 3");
        let err = shell.run(&HashMap::new()).await.unwrap_err();
        assert!(err.to_string().contains("status: 3"), "error: {err}");
    }
}
