use clap::Args;
use clap::ValueEnum;
use muzanci_config::config::Config;
use muzanci_observability::TError;
use serde::Deserialize;
use serde::Serialize;
use std::path::PathBuf;

use crate::cli::parse::parse_env;
use crate::cli::parse::parse_key_val;

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShowFormat {
    Ascii,
    Json,
    Dotgraph,
}

// We implement Display so we can easily print or use the format if needed
impl std::fmt::Display for ShowFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self {
            ShowFormat::Ascii => "ascii",
            ShowFormat::Json => "json",
            ShowFormat::Dotgraph => "dotgraph",
        };
        write!(f, "{}", val)
    }
}

#[derive(Args, Debug)]
pub struct ShowSubcommand {
    /// Path to the pipeline config file
    #[arg(long, value_name = "FILE", default_value = "muzan.py")]
    input: PathBuf,

    /// Format to print the dependency graph in
    #[arg(long, value_enum, value_name = "FORMAT", default_value_t = ShowFormat::Ascii)]
    format: ShowFormat,

    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    env: Vec<(String, String)>,
}

impl ShowSubcommand {
    pub fn run(self) -> Result<(), TError> {
        let ShowSubcommand { input, format, env } = self;

        let env = parse_env(env)?;

        let config = Config::from_file(&input, &env)?;

        let output = match format {
            ShowFormat::Ascii => config.to_ascii_graph(),
            ShowFormat::Json => config.to_json()?,
            ShowFormat::Dotgraph => config.to_dot_graph(),
        };

        println!("{}", output);

        Ok(())
    }
}
