use anyhow::Result;
use clap::Parser;
use reqwest::Url;

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
    #[clap(short, long, parse(from_occurrences), global = true)]
    verbose: usize,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Scrape given website. Host domain must exist in configuration.
    Scrape {
        host: Url,
        #[clap(short, long, default_value_t = 0)]
        /// Takes the next number of pages. Value of 0 will take all pages until the next page
        /// selector cannot
        next: usize,
    },
}

pub async fn execute() -> Result<()> {
    let cli: Cli = Cli::parse();

    if cli.verbose > 0 {
        use std::env::set_var;
        match cli.verbose {
            1 => set_var("RUST_LOG", "info"),
            2 => set_var("RUST_LOG", "debug"),
            _ => set_var("RUST_LOG", "trace"),
        }
        pretty_env_logger::try_init().ok();
    }

    match &cli.command {
        Commands::Scrape { host, next } => {
            println!("host is set to {host} with {next} number of pages");
        }
    }

    Ok(())
}
