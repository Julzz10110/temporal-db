//! Materialized views for current entity state.
//!
//! This module defines an abstraction over "current state" caches that can
//! be backed by in-memory maps, remote stores, or other implementations.

use crate::core::event::Event;
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

/// Trait for materialized view implementations.
///
/// Implementations are responsible for maintaining the latest value per
/// entity based on the stream of events.
#[async_trait]
pub trait MaterializedView: Send + Sync {
    /// Apply a single event to the view.
    async fn apply_event(&self, event: &Event) -> Result<()>;

    /// Get the raw serialized value for an entity, if present.
    async fn get_current_raw(&self, entity_id: &str) -> Result<Option<Vec<u8>>>;
}

/// Simple in-memory materialized view storing the latest payload per entity.
pub struct InMemoryMaterializedView {
    state: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryMaterializedView {
    /// Create a new, empty materialized view.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl MaterializedView for InMemoryMaterializedView {
    async fn apply_event(&self, event: &Event) -> Result<()> {
        let mut guard = self
            .state
            .write()
            .expect("InMemoryMaterializedView poisoned write lock");
        guard.insert(
            event.entity_id().to_string(),
            event.payload().data.clone(),
        );
        Ok(())
    }

    async fn get_current_raw(&self, entity_id: &str) -> Result<Option<Vec<u8>>> {
        let guard = self
            .state
            .read()
            .expect("InMemoryMaterializedView poisoned read lock");
        Ok(guard.get(entity_id).cloned())
    }
}

