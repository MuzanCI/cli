use clap::Parser;
use muzanci_cli::CliCommand;

fn main() {
    let command = CliCommand::parse();
    if let Err(err) = command.run() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}
