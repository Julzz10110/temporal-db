//! SQL parser for temporal queries

use crate::error::Result;

/// Parsed temporal query
#[derive(Debug, Clone)]
pub struct TemporalQuery {
    /// Query type
    pub query_type: QueryType,
    /// Entity ID filter (if any)
    pub entity_id: Option<String>,
    /// Time range
    pub time_range: Option<TimeRange>,
}

/// Query type
#[derive(Debug, Clone)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
}

/// Time range for temporal queries
#[derive(Debug, Clone)]
pub enum TimeRange {
    AsOf(i64), // Timestamp
    Between { start: i64, end: i64 },
    From(i64), // Start timestamp, open-ended
}

/// Parse a temporal SQL query
pub fn parse_query(_query: &str) -> Result<TemporalQuery> {
    // TODO: Implement actual SQL parsing using nom
    // For now, return a placeholder
    Err(crate::error::Error::Query(
        "SQL parser not yet implemented".to_string(),
    ))
}
