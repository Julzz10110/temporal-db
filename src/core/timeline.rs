//! Timeline: sequence of events for an entity

use crate::core::event::Event;
use crate::core::temporal::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Timeline represents the complete history of events for a single entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timeline {
    /// Entity ID this timeline belongs to
    entity_id: String,
    /// Events ordered by timestamp (BTreeMap for ordered iteration)
    events: BTreeMap<Timestamp, Vec<Event>>,
    /// Current version (number of events)
    version: u64,
}

impl Timeline {
    /// Create a new timeline for an entity
    pub fn new(entity_id: String) -> Self {
        Self {
            entity_id,
            events: BTreeMap::new(),
            version: 0,
        }
    }

    /// Get entity ID
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    /// Append an event to the timeline
    pub fn append(&mut self, event: Event) {
        let timestamp = event.timestamp();
        self.events
            .entry(timestamp)
            .or_insert_with(Vec::new)
            .push(event);
        self.version += 1;
    }

    /// Append multiple events
    pub fn append_many(&mut self, events: Vec<Event>) {
        for event in events {
            self.append(event);
        }
    }

    /// Get all events
    pub fn events(&self) -> impl Iterator<Item = &Event> {
        self.events.values().flatten()
    }

    /// Get events in time range [start, end)
    pub fn events_in_range(&self, start: Timestamp, end: Timestamp) -> Vec<&Event> {
        self.events
            .range(start..end)
            .flat_map(|(_, events)| events.iter())
            .collect()
    }

    /// Get events up to a timestamp (inclusive)
    pub fn events_up_to(&self, timestamp: Timestamp) -> Vec<&Event> {
        self.events
            .range(..=timestamp)
            .flat_map(|(_, events)| events.iter())
            .collect()
    }

    /// Get the latest event before or at a timestamp
    pub fn latest_before(&self, timestamp: Timestamp) -> Option<&Event> {
        self.events
            .range(..=timestamp)
            .next_back()
            .and_then(|(_, events)| events.last())
    }

    /// Get the earliest event at or after a timestamp
    pub fn earliest_after(&self, timestamp: Timestamp) -> Option<&Event> {
        self.events
            .range(timestamp..)
            .next()
            .and_then(|(_, events)| events.first())
    }

    /// Get current version
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Check if timeline is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get number of events
    pub fn len(&self) -> usize {
        self.events.values().map(|v| v.len()).sum()
    }

    /// Get first timestamp (if any)
    pub fn first_timestamp(&self) -> Option<Timestamp> {
        self.events.first_key_value().map(|(ts, _)| *ts)
    }

    /// Get last timestamp (if any)
    pub fn last_timestamp(&self) -> Option<Timestamp> {
        self.events.last_key_value().map(|(ts, _)| *ts)
    }

    /// Merge another timeline into this one (for distributed scenarios)
    pub fn merge(&mut self, other: &Timeline) {
        if other.entity_id != self.entity_id {
            return; // Can't merge timelines for different entities
        }

        for (timestamp, events) in &other.events {
            for event in events {
                // Check if event already exists (by ID)
                let exists = self
                    .events
                    .get(timestamp)
                    .map(|evts| evts.iter().any(|e| e.id() == event.id()))
                    .unwrap_or(false);

                if !exists {
                    self.append(event.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::{Event, EventPayload};

    fn create_test_event(ts: Timestamp, entity_id: &str) -> Event {
        let payload = EventPayload::from_json(&serde_json::json!({"value": "test"}))
            .unwrap();
        Event::new("test.event".to_string(), ts, entity_id.to_string(), payload)
    }

    #[test]
    fn test_timeline_creation() {
        let timeline = Timeline::new("entity:1".to_string());
        assert_eq!(timeline.entity_id(), "entity:1");
        assert!(timeline.is_empty());
    }

    #[test]
    fn test_timeline_append() {
        let mut timeline = Timeline::new("entity:1".to_string());
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        timeline.append(create_test_event(ts1, "entity:1"));
        timeline.append(create_test_event(ts2, "entity:1"));

        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline.version(), 2);
    }

    #[test]
    fn test_timeline_range_query() {
        let mut timeline = Timeline::new("entity:1".to_string());
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);
        let ts3 = Timestamp::from_secs(3000);

        timeline.append(create_test_event(ts1, "entity:1"));
        timeline.append(create_test_event(ts2, "entity:1"));
        timeline.append(create_test_event(ts3, "entity:1"));

        let events = timeline.events_in_range(
            Timestamp::from_secs(1500),
            Timestamp::from_secs(2500),
        );
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_timeline_latest_before() {
        let mut timeline = Timeline::new("entity:1".to_string());
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        timeline.append(create_test_event(ts1, "entity:1"));
        timeline.append(create_test_event(ts2, "entity:1"));

        let latest = timeline.latest_before(Timestamp::from_secs(1500));
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().timestamp(), ts1);
    }
}
