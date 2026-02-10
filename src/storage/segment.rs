//! Segment: on-disk storage unit for events

use crate::core::event::Event;
use crate::core::temporal::Timestamp;
use crate::error::Result;

/// Segment header metadata
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    /// Magic number for validation
    pub magic: [u8; 5],
    /// Unique segment ID
    pub segment_id: u64,
    /// Start timestamp of events in this segment
    pub start_time: Timestamp,
    /// End timestamp of events in this segment
    pub end_time: Timestamp,
    /// Number of events in segment
    pub event_count: u32,
    /// Checksum for integrity
    pub checksum: u32,
    /// Flags
    pub flags: u8,
}

impl SegmentHeader {
    /// Magic number: "TEMP0"
    pub const MAGIC: [u8; 5] = *b"TEMP0";
    pub const SIZE: usize = 64;

    /// Create a new segment header
    pub fn new(segment_id: u64, start_time: Timestamp, end_time: Timestamp) -> Self {
        Self {
            magic: Self::MAGIC,
            segment_id,
            start_time,
            end_time,
            event_count: 0,
            checksum: 0,
            flags: 0,
        }
    }
}

/// Segment: container for events on disk
#[derive(Debug)]
pub struct Segment {
    /// Segment header
    pub header: SegmentHeader,
    /// Events in this segment
    pub events: Vec<Event>,
}

impl Segment {
    /// Create a new segment
    pub fn new(segment_id: u64, start_time: Timestamp, end_time: Timestamp) -> Self {
        Self {
            header: SegmentHeader::new(segment_id, start_time, end_time),
            events: Vec::new(),
        }
    }

    /// Add an event to the segment
    pub fn add_event(&mut self, event: Event) -> Result<()> {
        let ts = event.timestamp();
        if ts < self.header.start_time || ts >= self.header.end_time {
            return Err(crate::error::Error::Temporal(format!(
                "Event timestamp {} outside segment range [{}, {})",
                ts.as_nanos(),
                self.header.start_time.as_nanos(),
                self.header.end_time.as_nanos()
            )));
        }

        self.events.push(event);
        self.header.event_count += 1;
        Ok(())
    }
}
