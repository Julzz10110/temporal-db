//! Temporal data types and time handling

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Timestamp representing a point in time with nanosecond precision
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp {
    /// Nanoseconds since Unix epoch
    nanos: i64,
}

impl Timestamp {
    /// Create a timestamp from nanoseconds since Unix epoch
    pub fn from_nanos(nanos: i64) -> Self {
        Self { nanos }
    }

    /// Create a timestamp from microseconds since Unix epoch
    pub fn from_micros(micros: i64) -> Self {
        Self {
            nanos: micros * 1_000,
        }
    }

    /// Create a timestamp from milliseconds since Unix epoch
    pub fn from_millis(millis: i64) -> Self {
        Self {
            nanos: millis * 1_000_000,
        }
    }

    /// Create a timestamp from seconds since Unix epoch
    pub fn from_secs(secs: i64) -> Self {
        Self {
            nanos: secs * 1_000_000_000,
        }
    }

    /// Get current timestamp
    pub fn now() -> Self {
        let now = Utc::now();
        Self {
            nanos: now.timestamp_nanos_opt().unwrap_or(0),
        }
    }

    /// Get nanoseconds since Unix epoch
    pub fn as_nanos(&self) -> i64 {
        self.nanos
    }

    /// Get microseconds since Unix epoch
    pub fn as_micros(&self) -> i64 {
        self.nanos / 1_000
    }

    /// Get milliseconds since Unix epoch
    pub fn as_millis(&self) -> i64 {
        self.nanos / 1_000_000
    }

    /// Get seconds since Unix epoch
    pub fn as_secs(&self) -> i64 {
        self.nanos / 1_000_000_000
    }

    /// Convert to chrono DateTime
    pub fn to_datetime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.as_secs(), (self.nanos % 1_000_000_000) as u32)
            .unwrap_or_else(|| Utc::now())
    }

    /// Add duration in nanoseconds
    pub fn add_nanos(&self, nanos: i64) -> Self {
        Self {
            nanos: self.nanos + nanos,
        }
    }

    /// Subtract duration in nanoseconds
    pub fn sub_nanos(&self, nanos: i64) -> Self {
        Self {
            nanos: self.nanos - nanos,
        }
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_datetime().to_rfc3339())
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Self {
            nanos: dt.timestamp_nanos_opt().unwrap_or(0),
        }
    }
}

/// Time period representing a range or instant
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimePeriod {
    /// Instantaneous point in time
    Instant(Timestamp),
    /// Time range [start, end)
    Range {
        start: Timestamp,
        end: Option<Timestamp>, // None means "until changed" or "forever"
    },
}

impl TimePeriod {
    /// Create an instant period
    pub fn instant(ts: Timestamp) -> Self {
        Self::Instant(ts)
    }

    /// Create a range period
    pub fn range(start: Timestamp, end: Option<Timestamp>) -> Self {
        Self::Range { start, end }
    }

    /// Create an open-ended range (forever)
    pub fn forever(start: Timestamp) -> Self {
        Self::Range { start, end: None }
    }

    /// Check if a timestamp is within this period
    pub fn contains(&self, ts: Timestamp) -> bool {
        match self {
            Self::Instant(t) => *t == ts,
            Self::Range { start, end } => {
                ts >= *start && end.map(|e| ts < e).unwrap_or(true)
            }
        }
    }

    /// Get the start timestamp
    pub fn start(&self) -> Timestamp {
        match self {
            Self::Instant(t) => *t,
            Self::Range { start, .. } => *start,
        }
    }

    /// Get the end timestamp (if any)
    pub fn end(&self) -> Option<Timestamp> {
        match self {
            Self::Instant(_) => None,
            Self::Range { end, .. } => *end,
        }
    }
}

/// Temporal value with valid time and transaction time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalValue<T> {
    /// The actual value
    pub value: T,
    /// Valid time period (when the value is valid in the domain)
    pub valid_time: TimePeriod,
    /// Transaction time (when the value was recorded in the database)
    pub transaction_time: Timestamp,
}

impl<T> TemporalValue<T> {
    /// Create a new temporal value
    pub fn new(
        value: T,
        valid_time: TimePeriod,
        transaction_time: Timestamp,
    ) -> Self {
        Self {
            value,
            valid_time,
            transaction_time,
        }
    }

    /// Create a temporal value valid at a specific instant
    pub fn at_instant(value: T, ts: Timestamp) -> Self {
        Self {
            value,
            valid_time: TimePeriod::instant(ts),
            transaction_time: Timestamp::now(),
        }
    }

    /// Create a temporal value valid from a start time
    pub fn from_time(value: T, start: Timestamp) -> Self {
        Self {
            value,
            valid_time: TimePeriod::forever(start),
            transaction_time: Timestamp::now(),
        }
    }

    /// Create a temporal value valid in a range
    pub fn in_range(value: T, start: Timestamp, end: Option<Timestamp>) -> Self {
        Self {
            value,
            valid_time: TimePeriod::range(start, end),
            transaction_time: Timestamp::now(),
        }
    }

    /// Check if this value is valid at a given timestamp
    pub fn is_valid_at(&self, ts: Timestamp) -> bool {
        self.valid_time.contains(ts)
    }

    /// Get the value
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Get the valid time period
    pub fn valid_time(&self) -> &TimePeriod {
        &self.valid_time
    }

    /// Get the transaction time
    pub fn transaction_time(&self) -> Timestamp {
        self.transaction_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_creation() {
        let ts = Timestamp::now();
        assert!(ts.as_nanos() > 0);

        let ts2 = Timestamp::from_secs(1000);
        assert_eq!(ts2.as_secs(), 1000);
        assert_eq!(ts2.as_millis(), 1_000_000);
    }

    #[test]
    fn test_time_period() {
        let start = Timestamp::from_secs(1000);
        let end = Timestamp::from_secs(2000);
        let period = TimePeriod::range(start, Some(end));

        assert!(period.contains(Timestamp::from_secs(1500)));
        assert!(!period.contains(Timestamp::from_secs(500)));
        assert!(!period.contains(Timestamp::from_secs(2500)));

        let forever = TimePeriod::forever(start);
        assert!(forever.contains(Timestamp::from_secs(10000)));
    }

    #[test]
    fn test_temporal_value() {
        let ts = Timestamp::from_secs(1000);
        let tv = TemporalValue::at_instant("test", ts);

        assert_eq!(tv.value(), &"test");
        assert!(tv.is_valid_at(ts));
        assert!(!tv.is_valid_at(Timestamp::from_secs(2000)));
    }
}
