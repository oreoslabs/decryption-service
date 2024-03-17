use anyhow::Result;
use clap::Parser;
use decryption_service::worker::start_worker;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// scheduler to connect to
    #[arg(short, long)]
    address: SocketAddr,
    /// worker name
    #[arg(short, long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let env = env_logger::Env::default().filter_or("RUST_LOG", "debug");
    env_logger::init_from_env(env);
    log::info!("scheduler to connect to {}", args.address);
    start_worker(args.address, args.name).await?;
    Ok(())
}
