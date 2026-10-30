use clap::Parser;
use clap::Subcommand;
use muzanci_observability::TError;
use muzanci_observability::TracingConfig;
use std::path::PathBuf;

use crate::cli::check_subcommand::CheckSubcommand;
use crate::cli::debug_subcommand::DebugSubcommand;
use crate::cli::show_subcommand::ShowSubcommand;
use crate::cli::user_config::UserConfig;

mod check_subcommand;
mod debug_subcommand;
mod parse;
mod show_subcommand;
mod user_config;

#[derive(Parser, Debug)]
#[command(
    name = "muzanci-cli",
    about = "CLI tool for debugging MuzanCI pipeline configurations and job execution"
)]
pub struct CliCommand {
    #[arg(long, value_name = "FILE", default_value = "muzan.toml", default_value_os_t = UserConfig::default_path(), global = true)]
    config: PathBuf,

    #[command(subcommand)]
    subcommand: CliSubcommand,
}

impl CliCommand {
    pub async fn run(self) -> Result<(), TError> {
        let user_config = self.user_config()?;

        let disable_telemetry = user_config.fields().disable_telemetry.unwrap_or(false)
            || std::env::var("DO_NOT_TRACK").is_ok_and(|v| v == "1");

        if !disable_telemetry {
            muzanci_observability::init_tracing(TracingConfig::default())?;
        }

        match self.subcommand {
            CliSubcommand::Show(subcommand) => subcommand.run(),
            CliSubcommand::Check(subcommand) => subcommand.run(),
            CliSubcommand::Debug(subcommand) => subcommand.run(user_config).await,
        }
    }

    fn user_config(&self) -> Result<UserConfig, TError> {
        let config_path = &self.config;

        if !std::fs::exists(config_path)? {
            if config_path == &UserConfig::default_path() {
                eprintln!("Creating new config file at {}", config_path.display());
                if let Some(parent) = config_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::File::create(config_path)?;
            } else {
                return Err(TError::new("config file not found"));
            }
        }

        eprintln!("Using config file at {}", config_path.display());

        let user_config = UserConfig::from_file(config_path)?;

        Ok(user_config)
    }
}

#[derive(Subcommand, Debug)]
enum CliSubcommand {
    #[command(
        name = "show",
        about = "Prints the dependency graph in the specified format"
    )]
    Show(ShowSubcommand),

    #[command(
        name = "check",
        about = "Checks for syntax errors and cyclical dependencies"
    )]
    Check(CheckSubcommand),

    #[command(name = "debug", about = "Start a debug session")]
    Debug(DebugSubcommand),
}
