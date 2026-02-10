//! Event types for event sourcing

use crate::core::temporal::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Event type identifier
pub type EventType = String;

/// Unique event identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId {
    /// UUID of the event
    pub id: Uuid,
}

impl EventId {
    /// Generate a new event ID
    pub fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }

    /// Create from existing UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self { id: uuid }
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// Event metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Event ID
    pub id: EventId,
    /// Event type
    pub event_type: EventType,
    /// Timestamp when event occurred (valid time)
    pub timestamp: Timestamp,
    /// Timestamp when event was recorded (transaction time)
    pub transaction_time: Timestamp,
    /// Entity ID this event belongs to
    pub entity_id: String,
    /// Optional correlation ID for tracing
    pub correlation_id: Option<String>,
    /// Optional causation ID (ID of event that caused this)
    pub causation_id: Option<EventId>,
    /// User/actor who caused this event
    pub actor: Option<String>,
    /// Additional tags for filtering/indexing
    pub tags: Vec<String>,
}

impl EventMetadata {
    /// Create new event metadata
    pub fn new(
        event_type: EventType,
        timestamp: Timestamp,
        entity_id: String,
    ) -> Self {
        Self {
            id: EventId::new(),
            event_type,
            timestamp,
            transaction_time: Timestamp::now(),
            entity_id,
            correlation_id: None,
            causation_id: None,
            actor: None,
            tags: Vec::new(),
        }
    }

    /// Set correlation ID
    pub fn with_correlation_id(mut self, id: String) -> Self {
        self.correlation_id = Some(id);
        self
    }

    /// Set causation ID
    pub fn with_causation_id(mut self, id: EventId) -> Self {
        self.causation_id = Some(id);
        self
    }

    /// Set actor
    pub fn with_actor(mut self, actor: String) -> Self {
        self.actor = Some(actor);
        self
    }

    /// Add tag
    pub fn with_tag(mut self, tag: String) -> Self {
        self.tags.push(tag);
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags.extend(tags);
        self
    }
}

/// Event payload (serialized data)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayload {
    /// Serialized event data (format depends on serialization)
    pub data: Vec<u8>,
    /// Serialization format identifier
    pub format: String,
}

impl EventPayload {
    /// Create payload from serialized data
    pub fn new(data: Vec<u8>, format: String) -> Self {
        Self { data, format }
    }

    /// Create payload from JSON-serializable data
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        let data = serde_json::to_vec(value)?;
        Ok(Self {
            data,
            format: "json".to_string(),
        })
    }

    /// Deserialize from JSON
    pub fn to_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.data)
    }

    /// Create payload from bincode-serialized data
    pub fn from_bincode<T: Serialize>(value: &T) -> Result<Self, bincode::Error> {
        let data = bincode::serialize(value)?;
        Ok(Self {
            data,
            format: "bincode".to_string(),
        })
    }

    /// Deserialize from bincode
    pub fn to_bincode<T: for<'de> Deserialize<'de>>(&self) -> Result<T, bincode::Error> {
        bincode::deserialize(&self.data)
    }
}

/// Complete event with metadata and payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Event metadata
    pub metadata: EventMetadata,
    /// Event payload
    pub payload: EventPayload,
}

impl Event {
    /// Create a new event
    pub fn new(
        event_type: EventType,
        timestamp: Timestamp,
        entity_id: String,
        payload: EventPayload,
    ) -> Self {
        Self {
            metadata: EventMetadata::new(event_type, timestamp, entity_id),
            payload,
        }
    }

    /// Create event with builder pattern
    pub fn builder(
        event_type: EventType,
        timestamp: Timestamp,
        entity_id: String,
        payload: EventPayload,
    ) -> EventBuilder {
        EventBuilder {
            metadata: EventMetadata::new(event_type, timestamp, entity_id),
            payload,
        }
    }

    /// Get event ID
    pub fn id(&self) -> EventId {
        self.metadata.id
    }

    /// Get event type
    pub fn event_type(&self) -> &str {
        &self.metadata.event_type
    }

    /// Get timestamp
    pub fn timestamp(&self) -> Timestamp {
        self.metadata.timestamp
    }

    /// Get entity ID
    pub fn entity_id(&self) -> &str {
        &self.metadata.entity_id
    }

    /// Get payload
    pub fn payload(&self) -> &EventPayload {
        &self.payload
    }
}

/// Builder for events
pub struct EventBuilder {
    metadata: EventMetadata,
    payload: EventPayload,
}

impl EventBuilder {
    /// Set correlation ID
    pub fn correlation_id(mut self, id: String) -> Self {
        self.metadata = self.metadata.with_correlation_id(id);
        self
    }

    /// Set causation ID
    pub fn causation_id(mut self, id: EventId) -> Self {
        self.metadata = self.metadata.with_causation_id(id);
        self
    }

    /// Set actor
    pub fn actor(mut self, actor: String) -> Self {
        self.metadata = self.metadata.with_actor(actor);
        self
    }

    /// Add tag
    pub fn tag(mut self, tag: String) -> Self {
        self.metadata = self.metadata.with_tag(tag);
        self
    }

    /// Add tags
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.metadata = self.metadata.with_tags(tags);
        self
    }

    /// Build the event
    pub fn build(self) -> Event {
        Event {
            metadata: self.metadata,
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let ts = Timestamp::now();
        let payload = EventPayload::from_json(&serde_json::json!({"key": "value"}))
            .unwrap();
        let event = Event::new("test.event".to_string(), ts, "entity:1".to_string(), payload);

        assert_eq!(event.event_type(), "test.event");
        assert_eq!(event.entity_id(), "entity:1");
    }

    #[test]
    fn test_event_builder() {
        let ts = Timestamp::now();
        let payload = EventPayload::from_json(&serde_json::json!({"key": "value"}))
            .unwrap();
        let event = Event::builder("test.event".to_string(), ts, "entity:1".to_string(), payload)
            .actor("user:123".to_string())
            .tag("important".to_string())
            .build();

        assert_eq!(event.metadata.actor, Some("user:123".to_string()));
        assert!(event.metadata.tags.contains(&"important".to_string()));
    }
}
