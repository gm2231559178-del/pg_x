#[allow(clippy::module_inception)]
pub mod shell {
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use std::collections::HashMap;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent},
        sink::Downstream,
    };

    /// Runs a shell command for every NOTIFY event.
    ///
    /// The raw payload is passed via `PGX_PAYLOAD`, channel via `PGX_CHANNEL`,
    /// and pid via `PGX_PID`.  Any extra environment variables from the
    /// `RoutingSpec.shell_env` map are also injected when using contract mode.
    pub struct ShellDownstream {
        /// The shell command to execute (run via `sh -c <command>`).
        command: String,
        /// Static env vars set at construction time (merged with event-level ones).
        base_env: HashMap<String, String>,
        /// When true, the payload is attempted to be parsed as a ContractMessage.
        contract_mode: bool,
    }

    impl ShellDownstream {
        pub fn new(
            command: impl Into<String>,
            base_env: HashMap<String, String>,
            contract_mode: bool,
        ) -> Self {
            Self {
                command: command.into(),
                base_env,
                contract_mode,
            }
        }
    }

    #[async_trait]
    impl Downstream for ShellDownstream {
        fn name(&self) -> &str {
            "shell"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            let mut env = self.base_env.clone();

            // Always inject envelope vars.
            env.insert("PGX_CHANNEL".to_string(), event.channel.clone());
            env.insert("PGX_PID".to_string(), event.pid.to_string());

            let payload_for_proc;

            if self.contract_mode {
                if let Some(contract) = ContractMessage::try_parse(&event.payload) {
                    // Business data as payload.
                    payload_for_proc = serde_json::to_string(&contract.data)
                        .unwrap_or_else(|_| event.payload.clone());

                    // Inject event-type and schema version.
                    if let Some(et) = &contract.meta.event_type {
                        env.insert("PGX_EVENT_TYPE".to_string(), et.clone());
                    }
                    env.insert(
                        "PGX_SCHEMA_VERSION".to_string(),
                        contract.meta.schema_version.clone(),
                    );

                    // Merge per-message env overrides on top.
                    env.extend(contract.meta.routing.shell_env.clone());
                } else {
                    payload_for_proc = event.payload.clone();
                }
            } else {
                payload_for_proc = event.payload.clone();
            }

            env.insert("PGX_PAYLOAD".to_string(), payload_for_proc);

            // Run the command asynchronously using tokio::process.
            let status = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&self.command)
                .envs(&env)
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
}
