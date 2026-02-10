//! Main database implementation

use crate::core::event::{Event, EventPayload};
use crate::core::temporal::Timestamp;
use crate::error::{Error, Result};
use crate::storage::{EventJournal, InMemoryJournal};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Main temporal database
pub struct TemporalDB {
    /// Event journal for storing events
    journal: Arc<RwLock<dyn EventJournal>>,
    /// Current state cache (entity_id -> latest value)
    current_state: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl TemporalDB {
    /// Create a new in-memory temporal database
    pub fn in_memory() -> Result<Self> {
        Ok(Self {
            journal: Arc::new(RwLock::new(InMemoryJournal::new())),
            current_state: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Insert a value for an entity at a specific timestamp
    pub async fn insert<V: serde::Serialize>(
        &self,
        entity_id: &str,
        value: V,
        timestamp: Timestamp,
    ) -> Result<()> {
        // Serialize value
        let payload = EventPayload::from_json(&value)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Create event
        let event = Event::new(
            "value.changed".to_string(),
            timestamp,
            entity_id.to_string(),
            payload,
        );

        // Append to journal
        self.journal.write().await.append(event.clone()).await?;

        // Update current state
        self.current_state
            .write()
            .await
            .insert(entity_id.to_string(), event.payload().data.clone());

        Ok(())
    }

    /// Query value at a specific timestamp (AS OF)
    pub async fn query_as_of<V: for<'de> serde::Deserialize<'de>>(
        &self,
        entity_id: &str,
        timestamp: Timestamp,
    ) -> Result<Option<V>> {
        // Get latest event before or at timestamp
        let event = self
            .journal
            .read()
            .await
            .get_latest_event(entity_id, timestamp)
            .await?;

        match event {
            Some(e) => {
                let value: V = e
                    .payload()
                    .to_json()
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Query values in a time range
    pub async fn query_range<V: for<'de> serde::Deserialize<'de>>(
        &self,
        entity_id: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<V>> {
        let events = self
            .journal
            .read()
            .await
            .get_events(entity_id, start, end)
            .await?;

        let mut values = Vec::new();
        for event in events {
            let value: V = event
                .payload()
                .to_json()
                .map_err(|e| Error::Serialization(e.to_string()))?;
            values.push(value);
        }

        Ok(values)
    }

    /// Get current value for an entity
    pub async fn get_current<V: for<'de> serde::Deserialize<'de>>(
        &self,
        entity_id: &str,
    ) -> Result<Option<V>> {
        let state = self.current_state.read().await;
        match state.get(entity_id) {
            Some(data) => {
                let payload = EventPayload::new(data.clone(), "json".to_string());
                let value: V = payload
                    .to_json()
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Get all events for an entity
    pub async fn get_entity_events(&self, entity_id: &str) -> Result<Vec<Event>> {
        self.journal
            .read()
            .await
            .get_entity_events(entity_id)
            .await
    }

    /// Flush pending writes
    pub async fn flush(&self) -> Result<()> {
        self.journal.write().await.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_insert_and_query() {
        let db = TemporalDB::in_memory().unwrap();
        let ts1 = Timestamp::from_secs(1000);

        // Insert value
        db.insert("user:1", "active", ts1).await.unwrap();

        // Query at same time
        let value: Option<String> = db.query_as_of("user:1", ts1).await.unwrap();
        assert_eq!(value, Some("active".to_string()));

        // Query before (should return None)
        let value: Option<String> = db.query_as_of("user:1", Timestamp::from_secs(500)).await.unwrap();
        assert_eq!(value, None);

        // Query after (should return the value)
        let value: Option<String> = db.query_as_of("user:1", Timestamp::from_secs(2000)).await.unwrap();
        assert_eq!(value, Some("active".to_string()));
    }

    #[tokio::test]
    async fn test_multiple_values() {
        let db = TemporalDB::in_memory().unwrap();
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        db.insert("user:1", "active", ts1).await.unwrap();
        db.insert("user:1", "inactive", ts2).await.unwrap();

        // Query at first time
        let value: Option<String> = db.query_as_of("user:1", ts1).await.unwrap();
        assert_eq!(value, Some("active".to_string()));

        // Query at second time
        let value: Option<String> = db.query_as_of("user:1", ts2).await.unwrap();
        assert_eq!(value, Some("inactive".to_string()));

        // Query in between
        let value: Option<String> = db.query_as_of("user:1", Timestamp::from_secs(1500)).await.unwrap();
        assert_eq!(value, Some("active".to_string()));
    }

    #[tokio::test]
    async fn test_range_query() {
        let db = TemporalDB::in_memory().unwrap();
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);
        let ts3 = Timestamp::from_secs(3000);

        db.insert("user:1", "v1", ts1).await.unwrap();
        db.insert("user:1", "v2", ts2).await.unwrap();
        db.insert("user:1", "v3", ts3).await.unwrap();

        // Query range
        let values: Vec<String> = db
            .query_range("user:1", Timestamp::from_secs(1500), Timestamp::from_secs(2500))
            .await
            .unwrap();

        assert_eq!(values.len(), 1);
        assert_eq!(values[0], "v2");
    }
}
