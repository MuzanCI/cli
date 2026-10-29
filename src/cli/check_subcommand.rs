use clap::Args;
use muzanci_config::config::Config;
use muzanci_observability::TError;
use std::path::PathBuf;

use crate::cli::parse::parse_env;
use crate::cli::parse::parse_key_val;

#[derive(Args, Debug)]
pub struct CheckSubcommand {
    /// Path to the pipeline config file
    #[arg(long, value_name = "FILE", default_value = "muzan.py")]
    input: PathBuf,

    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    env: Vec<(String, String)>,
}

impl CheckSubcommand {
    pub fn run(self) -> Result<(), TError> {
        let CheckSubcommand { input, env } = self;

        let env = parse_env(env)?;

        let config = Config::from_file(&input, &env)?;

        println!("No syntax errors or dependency cycles detected.");
        println!("Found {} pipelines total.", config.pipelines.len());
        println!("Found {} jobs total.", config.jobs.len());

        Ok(())
    }
}
