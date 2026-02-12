//! Persistent event journal backed by WAL + segment files.
//!
//! This module provides a disk-backed implementation of `EventJournal`
//! that writes all events to a write-ahead log and periodically flushes
//! them into immutable segment files managed by `SegmentManager`.

use crate::core::event::Event;
use crate::core::temporal::Timestamp;
use crate::error::Result;
use crate::storage::segment_file::{
    SegmentHeader, SegmentReader, SegmentWriter, MAX_EVENTS_PER_SEGMENT, MAX_SEGMENT_SIZE,
};
use crate::storage::{EventJournal, InMemoryJournal, WriteAheadLog};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Manages creation and rotation of segment files on disk.
pub struct SegmentManager {
    /// Directory where segment files are stored.
    dir: PathBuf,
    /// Currently open segment writer, if any.
    active: Option<SegmentWriter>,
    /// Next segment ID to allocate.
    next_segment_id: u64,
    /// Known segment headers (metadata catalog).
    segments: Vec<SegmentHeader>,
}

impl SegmentManager {
    /// Create a new manager rooted at the given directory.
    pub fn new<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // For now, start from segment ID 1 and ignore any existing files.
        // Later we can scan `dir` and pick the next available ID.
        Ok(Self {
            dir,
            active: None,
            next_segment_id: 1,
            segments: Vec::new(),
        })
    }

    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.dir
            .join(format!("segment-{segment_id:020}.seg"))
    }

    fn open_new_segment(&mut self) -> Result<()> {
        // Use a very wide time range so we don't reject events by timestamp.
        let start = Timestamp::from_nanos(i64::MIN + 1);
        let end = Timestamp::from_nanos(i64::MAX);
        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        let path = self.segment_path(segment_id);
        let writer = SegmentWriter::create(path, segment_id, start, end)?;
        self.active = Some(writer);
        Ok(())
    }

    fn rotate_if_needed(&mut self) -> Result<()> {
        if let Some(writer) = self.active.as_ref() {
            let header = writer.header();
            if header.event_count >= MAX_EVENTS_PER_SEGMENT
                || header.compressed_size as u64 >= MAX_SEGMENT_SIZE
            {
                // Finalize current segment and drop the writer.
                let writer = self.active.take().unwrap();
                let header = writer.finalize()?;
                self.segments.push(header);
            }
        }
        Ok(())
    }

    fn append_event(&mut self, event: Event) -> Result<()> {
        if self.active.is_none() {
            self.open_new_segment()?;
        }
        if let Some(writer) = self.active.as_mut() {
            writer.append(event)?;
        }
        self.rotate_if_needed()?;
        Ok(())
    }

    /// Flush all active data to disk and close the current segment.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.active.take() {
            let header = writer.finalize()?;
            self.segments.push(header);
        }
        Ok(())
    }

    /// List all known segment headers.
    pub fn segments(&self) -> &[SegmentHeader] {
        &self.segments
    }

    /// Read all events from all segments (used for recovery).
    pub fn read_all_events(&self) -> Result<Vec<Event>> {
        let mut all = Vec::new();
        for header in &self.segments {
            let path = self.segment_path(header.segment_id);
            if path.exists() {
                let mut reader = SegmentReader::open(&path)?;
                all.extend(reader.read_events()?);
            }
        }
        Ok(all)
    }
}

/// Disk-backed implementation of `EventJournal` using a WAL and segment files.
///
/// For now, queries are served from an in-memory journal built alongside
/// the WAL/segment writes. On startup, a future constructor can rebuild
/// this state by replaying WAL + segments.
pub struct SegmentedJournal<W: WriteAheadLog> {
    wal: W,
    segment_manager: SegmentManager,
    /// In-memory view used for fast queries.
    in_memory: InMemoryJournal,
    /// Simple index by event type for this journal.
    events_by_type: HashMap<String, Vec<Event>>,
}

impl<W: WriteAheadLog> SegmentedJournal<W> {
    /// Create a new segmented journal rooted at `dir` using the provided WAL.
    pub fn new<P: AsRef<Path>>(dir: P, wal: W) -> Result<Self> {
        let segment_manager = SegmentManager::new(dir)?;
        Ok(Self {
            wal,
            segment_manager,
            in_memory: InMemoryJournal::new(),
            events_by_type: HashMap::new(),
        })
    }

    fn index_event_by_type(&mut self, event: &Event) {
        let ty = event.event_type().to_string();
        self.events_by_type
            .entry(ty)
            .or_insert_with(Vec::new)
            .push(event.clone());
    }

    /// Get list of all segment headers.
    pub fn segments(&self) -> &[SegmentHeader] {
        self.segment_manager.segments()
    }

    /// Read all events from all segments (used for recovery).
    pub fn read_all_events(&self) -> Result<Vec<Event>> {
        self.segment_manager.read_all_events()
    }
}

#[async_trait::async_trait]
impl<W> EventJournal for SegmentedJournal<W>
where
    W: WriteAheadLog + Send + Sync,
{
    async fn append(&mut self, event: Event) -> Result<()> {
        // 1. Write to WAL for durability.
        self.wal.append(&event)?;

        // 2. Append to segment files.
        self.segment_manager.append_event(event.clone())?;

        // 3. Update in-memory indexes for fast queries.
        self.in_memory.append(event.clone()).await?;
        self.index_event_by_type(&event);

        Ok(())
    }

    async fn append_batch(&mut self, events: Vec<Event>) -> Result<()> {
        // For v1, just apply append() in a loop to keep behavior simple.
        for ev in events {
            self.append(ev).await?;
        }
        Ok(())
    }

    async fn get_events(
        &self,
        entity_id: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<Event>> {
        self.in_memory.get_events(entity_id, start, end).await
    }

    async fn get_entity_events(&self, entity_id: &str) -> Result<Vec<Event>> {
        self.in_memory.get_entity_events(entity_id).await
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
                evts.iter()
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
        self.in_memory.get_latest_event(entity_id, timestamp).await
    }

    async fn flush(&mut self) -> Result<()> {
        self.wal.flush()?;
        self.segment_manager.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::{Event, EventPayload};
    use crate::core::temporal::Timestamp;
    use crate::storage::wal::InMemoryWAL;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_segmented_journal_basic() {
        let temp_dir = TempDir::new().unwrap();
        let segments_dir = temp_dir.path().join("segments");
        let wal = InMemoryWAL::new();

        let mut journal = SegmentedJournal::new(&segments_dir, wal).unwrap();

        // Append events
        let payload1 = EventPayload::from_json(&serde_json::json!({"value": "test1"})).unwrap();
        let event1 = Event::new(
            "test.event".to_string(),
            Timestamp::from_secs(1000),
            "entity:1".to_string(),
            payload1,
        );

        let payload2 = EventPayload::from_json(&serde_json::json!({"value": "test2"})).unwrap();
        let event2 = Event::new(
            "test.event".to_string(),
            Timestamp::from_secs(1001),
            "entity:1".to_string(),
            payload2,
        );

        journal.append(event1.clone()).await.unwrap();
        journal.append(event2.clone()).await.unwrap();
        journal.flush().await.unwrap();

        // Query events
        let events = journal
            .get_events(
                "entity:1",
                Timestamp::from_secs(1000),
                Timestamp::from_secs(2000),
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        // Verify segments were created and finalized
        let segments = journal.segment_manager.segments();
        assert!(!segments.is_empty(), "At least one segment should be created");
        // After flush, segment should be finalized and compressed
        // Note: compression happens when buffer is flushed (at 1000 events or on finalize)
        let segment = &segments[0];
        assert_eq!(segment.event_count, 2);
        // Even with 2 events, finalize() should compress the buffer
        assert_ne!(segment.flags & crate::storage::segment_file::FLAG_COMPRESSED, 0,
            "Segment should be compressed after finalize");
        assert_ne!(segment.checksum, 0, "Checksum should be calculated");
    }

    #[tokio::test]
    async fn test_segmented_journal_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let segments_dir = temp_dir.path().join("segments");
        let wal = InMemoryWAL::new();

        let mut journal = SegmentedJournal::new(&segments_dir, wal).unwrap();

        // Add many events to trigger compression
        // Need at least 1000 events to trigger automatic flush_buffer, or rely on finalize()
        for i in 0..100 {
            let payload = EventPayload::from_json(&serde_json::json!({
                "index": i,
                "data": "repetitive data".repeat(20)
            }))
            .unwrap();
            let event = Event::new(
                "test.event".to_string(),
                Timestamp::from_secs(1000 + i as i64),
                format!("entity:{}", i % 10),
                payload,
            );
            journal.append(event).await.unwrap();
        }
        journal.flush().await.unwrap();

        // Verify compression
        let segments = journal.segment_manager.segments();
        assert!(!segments.is_empty(), "At least one segment should be created");
        
        // After flush(), segment should be finalized which triggers compression
        for segment in segments {
            // finalize() should compress even small buffers
            assert_ne!(
                segment.flags & crate::storage::segment_file::FLAG_COMPRESSED,
                0,
                "Segment should be compressed after finalize (flags={})",
                segment.flags
            );
            assert_ne!(segment.checksum, 0, "Checksum should be set");
        }

        // Verify we can read all events back
        let all_events = journal.segment_manager.read_all_events().unwrap();
        assert_eq!(all_events.len(), 100);
    }
}

