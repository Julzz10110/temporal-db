//! Event journal: append-only storage for events

use crate::core::event::Event;
use crate::core::temporal::Timestamp;
use crate::core::timeline::Timeline;
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

/// In-memory implementation of event journal backed by per-entity timelines.
///
/// This keeps events ordered by timestamp and enables efficient temporal queries.
pub struct InMemoryJournal {
    /// Map from entity ID to ordered timeline
    timelines: HashMap<String, Timeline>,
    /// Map from event type to events (for simple filtering by type)
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

        // Add to entity timeline (ordered by timestamp)
        let timeline = self
            .timelines
            .entry(entity_id)
            .or_insert_with(|| Timeline::new(event.entity_id().to_string()));
        timeline.append(event.clone());

        // Add to type index (kept as a flat list for now)
        self.events_by_type
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(event);

        Ok(())
    }

    async fn append_batch(&mut self, events: Vec<Event>) -> Result<()> {
        // For in-memory journal we just reuse append; persistent journals
        // can override this for true batch semantics.
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
            .map(|timeline| {
                timeline
                    .events_in_range(start, end)
                    .into_iter()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(events)
    }

    async fn get_entity_events(&self, entity_id: &str) -> Result<Vec<Event>> {
        let all = self
            .timelines
            .get(entity_id)
            .map(|timeline| timeline.events().cloned().collect())
            .unwrap_or_default();
        Ok(all)
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
            .and_then(|timeline| timeline.latest_before(timestamp).cloned());

        Ok(event)
    }

    async fn flush(&mut self) -> Result<()> {
        // In-memory journal doesn't need flushing
        Ok(())
    }
}
