//! Event journal: append-only storage for events

use crate::core::event::Event;
use crate::core::temporal::Timestamp;
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// Trait for event journal implementations
#[async_trait]
pub trait EventJournal: Send + Sync {
    /// Append an event to the journal
    async fn append(&mut self, event: Event) -> Result<()>;

    /// Append multiple events atomically
    async fn append_batch(&mut self, events: Vec<Event>) -> Result<()>;

    /// Get events for an entity in a time range
    async fn get_events(
        &self,
        entity_id: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<Event>>;

    /// Get all events for an entity
    async fn get_entity_events(&self, entity_id: &str) -> Result<Vec<Event>>;

    /// Get events by type in a time range
    async fn get_events_by_type(
        &self,
        event_type: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<Event>>;

    /// Get the latest event for an entity before or at a timestamp
    async fn get_latest_event(
        &self,
        entity_id: &str,
        timestamp: Timestamp,
    ) -> Result<Option<Event>>;

    /// Flush pending writes to disk
    async fn flush(&mut self) -> Result<()>;
}

/// In-memory implementation of event journal
pub struct InMemoryJournal {
    /// Map from entity ID to timeline
    timelines: HashMap<String, Vec<Event>>,
    /// Map from event type to events
    events_by_type: HashMap<String, Vec<Event>>,
}

impl InMemoryJournal {
    /// Create a new in-memory journal
    pub fn new() -> Self {
        Self {
            timelines: HashMap::new(),
            events_by_type: HashMap::new(),
        }
    }
}

impl Default for InMemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventJournal for InMemoryJournal {
    async fn append(&mut self, event: Event) -> Result<()> {
        let entity_id = event.entity_id().to_string();
        let event_type = event.event_type().to_string();

        // Add to entity timeline
        self.timelines
            .entry(entity_id)
            .or_insert_with(Vec::new)
            .push(event.clone());

        // Add to type index
        self.events_by_type
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(event);

        Ok(())
    }

    async fn append_batch(&mut self, events: Vec<Event>) -> Result<()> {
        for event in events {
            self.append(event).await?;
        }
        Ok(())
    }

    async fn get_events(
        &self,
        entity_id: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<Event>> {
        let events = self
            .timelines
            .get(entity_id)
            .map(|evts| {
                evts
                    .iter()
                    .filter(|e| {
                        let ts = e.timestamp();
                        ts >= start && ts < end
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(events)
    }

    async fn get_entity_events(&self, entity_id: &str) -> Result<Vec<Event>> {
        Ok(self
            .timelines
            .get(entity_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn get_events_by_type(
        &self,
        event_type: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<Event>> {
        let events = self
            .events_by_type
            .get(event_type)
            .map(|evts| {
                evts
                    .iter()
                    .filter(|e| {
                        let ts = e.timestamp();
                        ts >= start && ts < end
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(events)
    }

    async fn get_latest_event(
        &self,
        entity_id: &str,
        timestamp: Timestamp,
    ) -> Result<Option<Event>> {
        let event = self
            .timelines
            .get(entity_id)
            .and_then(|evts| {
                evts
                    .iter()
                    .filter(|e| e.timestamp() <= timestamp)
                    .max_by_key(|e| e.timestamp())
                    .cloned()
            });

        Ok(event)
    }

    async fn flush(&mut self) -> Result<()> {
        // In-memory journal doesn't need flushing
        Ok(())
    }
}
