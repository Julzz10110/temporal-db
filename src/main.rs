//! Temporal-DB: Main entry point

use clap::Parser;
use temporal_db::cli::Cli;
use temporal_db::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        temporal_db::cli::Commands::Start { port } => {
            println!("Starting Temporal-DB server on port {}", port);
            // TODO: Start server
            Ok(())
        }
        temporal_db::cli::Commands::Insert { entity, value } => {
            println!("Inserting {} for entity {}", value, entity);
            // TODO: Implement insert
            Ok(())
        }
        temporal_db::cli::Commands::Query { entity, timestamp } => {
            println!("Querying entity {} at {:?}", entity, timestamp);
            // TODO: Implement query
            Ok(())
        }
    }
}
