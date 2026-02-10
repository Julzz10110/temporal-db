//! Temporal-DB: Event-sourced temporal database
//!
//! A production-ready, distributed, event-sourced temporal database built in Rust.
//!
//! # Core Concepts
//!
//! - **Events**: Immutable records of changes to data
//! - **Temporal Values**: Values with valid time ranges
//! - **Timelines**: Sequences of events for a single entity
//! - **CRDTs**: Conflict-free replicated data types for distributed consistency
//!
//! # Example
//!
//! ```no_run
//! use temporal_db::prelude::*;
//!
//! # async fn example() -> temporal_db::error::Result<()> {
//! let db = TemporalDB::in_memory()?;
//!
//! // Insert a value
//! db.insert("user:1", "active", Timestamp::now()).await?;
//!
//! // Query at a specific time
//! let value: Option<String> = db.query_as_of("user:1", Timestamp::now()).await?;
//! # Ok(())
//! # }
//! ```

pub mod api;
pub mod cli;
pub mod core;
pub mod crdt;
pub mod distributed;
pub mod error;
pub mod index;
pub mod query;
pub mod storage;

/// Main database type
pub mod db;

/// Prelude module for common imports
pub mod prelude {
    pub use crate::core::*;
    pub use crate::db::TemporalDB;
    pub use crate::error::{Error, Result};
    pub use crate::storage::*;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert!(true);
    }
}
