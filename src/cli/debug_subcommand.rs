use clap::Args;
use muzanci_config::collector::Collector;
use muzanci_observability::TError;
use std::path::PathBuf;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::cli::parse::parse_env;
use crate::cli::parse::parse_key_val;
use crate::debug_session::run_debug_session;
use crate::prompt::Intent;
use crate::prompt::prompt_account_selection;
use crate::prompt::prompt_login;
use crate::signal_receiver::SignalReceiver;
use crate::user_config::UserConfig;

#[derive(Args, Debug)]
pub struct DebugSubcommand {
    /// Path to the pipeline config file
    #[arg(long, value_name = "FILE", default_value = "muzan.py")]
    input: PathBuf,

    #[arg(long, value_name = "JOB")]
    job: String,

    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    env: Vec<(String, String)>,
}

impl DebugSubcommand {
    pub async fn run(self, mut user_config: UserConfig) -> Result<(), TError> {
        let DebugSubcommand { input, job, env } = self;

        let env = parse_env(env)?;

        let collector = Collector::new(&env);

        collector
            .evaluate(&input)
            .map_err(|e| TError::new(&format!("failed to evaluate {}:\n{}", input.display(), e)))?;

        let job = collector
            .jobs()
            .into_iter()
            .find(|j| j.name == job)
            .ok_or_else(|| TError::new(&format!("job not found: {job}")))?;

        println!("Found job: {}", job.name);
        println!("Job steps:");

        for step in &job.steps {
            println!("  - {}", step.name);
        }

        let secret = match user_config.fields().personal_access_token_secret.clone() {
            Some(secret) => secret,
            None => {
                let secret = prompt_login(Intent::Debug)?;
                user_config.fields_mut().personal_access_token_secret = Some(secret.to_string());
                user_config.save()?;
                secret
            }
        };

        let account_id = match user_config.fields().account_id {
            Some(account_id) => account_id,
            None => {
                let account_id = prompt_account_selection(&secret).await?;
                user_config.fields_mut().account_id = Some(account_id);
                user_config.save()?;
                account_id
            }
        };

        let debug_client_shutdown = CancellationToken::new();
        let mut join_set = JoinSet::new();

        join_set.spawn({
            let debug_client_shutdown = debug_client_shutdown.clone();
            async move {
                run_debug_session(debug_client_shutdown.clone(), &secret, account_id, job).await
            }
        });

        SignalReceiver::spawn(&mut join_set, debug_client_shutdown.clone());

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("{e}");
                }
                Err(e) => {
                    tracing::error!("Join error: {e}");
                }
            }
        }

        Ok(())
    }
}
