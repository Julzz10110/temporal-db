//! Write-Ahead Log for durability

use crate::core::event::Event;
use crate::error::Result;

/// Write-Ahead Log trait
pub trait WriteAheadLog: Send + Sync {
    /// Append an event to WAL
    fn append(&mut self, event: &Event) -> Result<()>;

    /// Flush WAL to disk
    fn flush(&mut self) -> Result<()>;

    /// Replay events from WAL
    fn replay(&self) -> Result<Vec<Event>>;

    /// Clear WAL (after checkpoint)
    fn clear(&mut self) -> Result<()>;
}

/// In-memory WAL (for testing)
pub struct InMemoryWAL {
    events: Vec<Event>,
}

impl InMemoryWAL {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }
}

impl WriteAheadLog for InMemoryWAL {
    fn append(&mut self, event: &Event) -> Result<()> {
        self.events.push(event.clone());
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<Event>> {
        Ok(self.events.clone())
    }

    fn clear(&mut self) -> Result<()> {
        self.events.clear();
        Ok(())
    }
}
