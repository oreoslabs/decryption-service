use anyhow::Result;
use clap::Parser;
use decryption_service::server::start_server;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// ip:port to listen for worker connection
    #[arg(long, default_value = "0.0.0.0:9002")]
    address: SocketAddr,
    /// ip:port to listen for rest service
    #[arg(long, default_value = "0.0.0.0:9003")]
    rest: SocketAddr,
    /// redis server to connect to
    #[arg(long, default_value = "redis://localhost")]
    redis: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let env = env_logger::Env::default().filter_or("RUST_LOG", "debug");
    env_logger::init_from_env(env);
    log::info!("tcp listening on {}", args.address);
    log::info!("rest listening on {}", args.rest);
    start_server(args.address, args.rest, &args.redis).await?;
    Ok(())
}

