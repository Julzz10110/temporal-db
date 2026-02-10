//! CLI commands

use clap::{Parser, Subcommand};

/// Temporal-DB CLI
#[derive(Parser)]
#[command(name = "temporal-db")]
#[command(about = "Event-sourced temporal database")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the database server
    Start {
        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,
    },
    /// Insert data
    Insert {
        /// Entity ID
        #[arg(short, long)]
        entity: String,
        /// Value
        #[arg(short, long)]
        value: String,
    },
    /// Query data
    Query {
        /// Entity ID
        #[arg(short, long)]
        entity: String,
        /// Timestamp (AS OF)
        #[arg(short, long)]
        timestamp: Option<String>,
    },
}
