use clap::Parser;
use muzanci_cli::CliCommand;

#[tokio::main]
async fn main() {
    let command = CliCommand::parse();
    if let Err(err) = command.run().await {
        eprintln!("error: {err:#}");
    }
}
