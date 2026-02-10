//! Temporal index for time-based queries

use crate::core::event::Event;
use crate::core::temporal::Timestamp;
use std::collections::BTreeMap;

/// Temporal index for fast time-based queries
pub struct TemporalIndex {
    /// Map from timestamp to event offsets
    time_index: BTreeMap<Timestamp, Vec<usize>>,
}

impl TemporalIndex {
    pub fn new() -> Self {
        Self {
            time_index: BTreeMap::new(),
        }
    }

    pub fn add_event(&mut self, event: &Event, offset: usize) {
        let timestamp = event.timestamp();
        self.time_index
            .entry(timestamp)
            .or_insert_with(Vec::new)
            .push(offset);
    }

    pub fn find_in_range(&self, start: Timestamp, end: Timestamp) -> Vec<usize> {
        self.time_index
            .range(start..end)
            .flat_map(|(_, offsets)| offsets.iter().cloned())
            .collect()
    }
}

impl Default for TemporalIndex {
    fn default() -> Self {
        Self::new()
    }
}
