use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use muzanci_config::Config;
use muzanci_config::collector::Collector;
use muzanci_git::GitClient;
use serde::Deserialize;
use serde::Serialize;
use std::path::PathBuf;

use muzanci_config::collector::Env;

mod debug;
mod error;
mod logging;
mod ssh;
mod stdin;

use crate::debug::debug_session::run_debug_session;

#[derive(Parser, Debug)]
#[command(
    name = "muzanci-cli",
    about = "CLI tool for debugging MuzanCI pipeline configurations and job execution"
)]
pub struct CliCommand {
    #[command(subcommand)]
    subcommand: CliSubcommand,
}

impl CliCommand {
    pub fn run(self) -> anyhow::Result<()> {
        match self.subcommand {
            CliSubcommand::Show(args) => run_show(args),
            CliSubcommand::Check(args) => run_check(args),
            CliSubcommand::Debug(args) => run_debug(args),
        }
    }
}

#[derive(Subcommand, Debug)]
enum CliSubcommand {
    #[command(
        name = "show",
        about = "Prints the dependency graph in the specified format"
    )]
    Show(ShowArgs),

    #[command(
        name = "check",
        about = "Checks for syntax errors and cyclical dependencies"
    )]
    Check(CheckArgs),

    #[command(name = "debug", about = "Start a debug session")]
    Debug(DebugArgs),
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShowFormat {
    ASCII,
    JSON,
    DOTGRAPH,
}

// We implement Display so we can easily print or use the format if needed
impl std::fmt::Display for ShowFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self {
            ShowFormat::ASCII => "ascii",
            ShowFormat::JSON => "json",
            ShowFormat::DOTGRAPH => "dotgraph",
        };
        write!(f, "{}", val)
    }
}

#[derive(Args, Debug)]
struct ShowArgs {
    /// Path to the pipeline config file
    #[arg(long, value_name = "FILE", default_value = "muzan.py")]
    input: PathBuf,

    /// Format to print the dependency graph in
    #[arg(long, value_enum, value_name = "FORMAT", default_value_t = ShowFormat::ASCII)]
    format: ShowFormat,

    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    env: Vec<(String, String)>,
}

#[derive(Args, Debug)]
struct CheckArgs {
    /// Path to the pipeline config file
    #[arg(long, value_name = "FILE", default_value = "muzan.py")]
    input: PathBuf,

    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    env: Vec<(String, String)>,
}

#[derive(Args, Debug)]
struct DebugArgs {
    /// Path to the pipeline config file
    #[arg(long, value_name = "FILE", default_value = "muzan.py")]
    input: PathBuf,

    #[arg(long, value_name = "JOB")]
    job: String,

    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    env: Vec<(String, String)>,
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=VALUE: no `=` found in [{s}]"))?;
    let key = s[..pos].trim();
    let value = s[pos + 1..].trim();
    if key.is_empty() {
        return Err(format!("invalid KEY=VALUE: key is empty in [{s}]"));
    }
    Ok((key.to_string(), value.to_string()))
}

fn parse_env(env: Vec<(String, String)>) -> anyhow::Result<Env> {
    let mut env = env
        .into_iter()
        .try_fold(Env::new(), |mut acc, (key, value)| {
            if acc.contains_key(&key) {
                anyhow::bail!("Duplicate env key [{key}]");
            }
            acc.insert(key, value);
            Ok(acc)
        })?;

    if !env.contains_key("GIT_BRANCH") {
        let branch = GitClient::try_default()?.get_branch(&PathBuf::from("."))?;
        env.insert("GIT_BRANCH".to_string(), branch);
    }

    if !env.contains_key("GIT_COMMIT") {
        let commit = GitClient::try_default()?.get_commit(&PathBuf::from("."))?;
        env.insert("GIT_COMMIT".to_string(), commit);
    }

    Ok(env)
}

fn run_show(args: ShowArgs) -> anyhow::Result<()> {
    let ShowArgs { input, format, env } = args;

    let env = parse_env(env)?;

    let config = Config::from_file(&input, &env)?;

    let output = match format {
        ShowFormat::ASCII => config.to_ascii_graph(),
        ShowFormat::JSON => config.to_json()?,
        ShowFormat::DOTGRAPH => config.to_dot_graph(),
    };

    println!("{}", output);

    Ok(())
}

fn run_check(args: CheckArgs) -> anyhow::Result<()> {
    let CheckArgs { input, env } = args;

    let env = parse_env(env)?;

    let config = Config::from_file(&input, &env)?;

    println!("No syntax errors or dependency cycles detected.");
    println!("Found {} pipelines total.", config.pipelines.len());
    println!("Found {} jobs total.", config.jobs.len());

    Ok(())
}

fn run_debug(args: DebugArgs) -> anyhow::Result<()> {
    let _logger_guard = logging::init().unwrap();
    let DebugArgs { input, job, env } = args;

    let env = parse_env(env)?;

    let collector = Collector::new(&env);

    collector
        .evaluate(&input)
        .map_err(|e| anyhow::anyhow!("failed to evaluate {}:\n{}", input.display(), e))?;

    let job = collector
        .jobs()
        .into_iter()
        .find(|j| j.name == job)
        .ok_or_else(|| anyhow::anyhow!("job not found: {job}"))?;

    println!("Found job: {}", job.name);
    println!("Job steps:");

    for step in &job.steps {
        println!("  - {}", step.name);
    }

    run_debug_session(job)
}
