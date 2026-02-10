//! Error types for Temporal-DB

use thiserror::Error;

/// Result type alias for Temporal-DB operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for Temporal-DB
#[derive(Error, Debug)]
pub enum Error {
    /// Storage-related errors
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Query parsing or execution errors
    #[error("Query error: {0}")]
    Query(String),

    /// CRDT operation errors
    #[error("CRDT error: {0}")]
    Crdt(String),

    /// Distributed system errors
    #[error("Distributed error: {0}")]
    Distributed(String),

    /// Invalid timestamp or time range
    #[error("Temporal error: {0}")]
    Temporal(String),

    /// Index-related errors
    #[error("Index error: {0}")]
    Index(String),

    /// Network/API errors
    #[error("Network error: {0}")]
    Network(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic error wrapper
    #[error("Error: {0}")]
    Other(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<prost::EncodeError> for Error {
    fn from(e: prost::EncodeError) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        Error::Network(e.to_string())
    }
}
