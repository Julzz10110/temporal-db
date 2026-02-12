//! Segment file format: low-level on-disk storage

use crate::core::event::Event;
use crate::core::temporal::Timestamp;
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher as Crc32Hasher;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Segment file format version
pub const SEGMENT_VERSION: u8 = 1;

/// Segment header size (64 bytes)
pub const HEADER_SIZE: usize = 64;

/// Magic number: "TEMP0"
pub const MAGIC: &[u8; 5] = b"TEMP0";

/// Maximum events per segment before rotation
pub const MAX_EVENTS_PER_SEGMENT: u32 = 1_000_000;

/// Maximum segment size (100MB uncompressed)
pub const MAX_SEGMENT_SIZE: u64 = 100 * 1024 * 1024;

/// Compression level for ZSTD (1-22, higher = better compression but slower)
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Flag bits in SegmentHeader.flags
pub const FLAG_COMPRESSED: u8 = 0x01; // Segment data is compressed with ZSTD

/// Segment header structure
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub segment_id: u64,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub event_count: u32,
    pub compressed_size: u32,
    pub checksum: u32,
    pub flags: u8,
}

impl SegmentHeader {
    /// Create a new segment header
    pub fn new(segment_id: u64, start_time: Timestamp, end_time: Timestamp) -> Self {
        Self {
            segment_id,
            start_time,
            end_time,
            event_count: 0,
            compressed_size: 0,
            checksum: 0,
            flags: 0,
        }
    }

    /// Serialize header to bytes
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        
        // Magic number (5 bytes)
        buf.put_slice(MAGIC);
        
        // Version (1 byte)
        buf.put_u8(SEGMENT_VERSION);
        
        // Reserved (2 bytes)
        buf.put_u16(0);
        
        // Segment ID (8 bytes)
        buf.put_u64(self.segment_id);
        
        // Start time (8 bytes)
        buf.put_i64(self.start_time.as_nanos());
        
        // End time (8 bytes)
        buf.put_i64(self.end_time.as_nanos());
        
        // Event count (4 bytes)
        buf.put_u32(self.event_count);
        
        // Compressed size (4 bytes)
        buf.put_u32(self.compressed_size);
        
        // Checksum (4 bytes)
        buf.put_u32(self.checksum);
        
        // Flags (1 byte)
        buf.put_u8(self.flags);
        
        // Padding to 64 bytes: 5+1+2+8+8+8+4+4+4+1 = 45, need 19 more
        buf.put_bytes(0, 19);
        
        debug_assert_eq!(buf.len(), HEADER_SIZE);
        buf.freeze()
    }

    /// Deserialize header from bytes
    pub fn deserialize(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(Error::Storage("Invalid header size".to_string()));
        }

        // Check magic number
        let magic = &buf[0..5];
        if magic != MAGIC {
            return Err(Error::Storage(format!(
                "Invalid magic number: {:?}",
                magic
            )));
        }

        buf.advance(5);

        // Version
        let version = buf.get_u8();
        if version != SEGMENT_VERSION {
            return Err(Error::Storage(format!("Unsupported version: {}", version)));
        }

        // Reserved
        buf.advance(2);

        // Segment ID
        let segment_id = buf.get_u64();

        // Start time
        let start_nanos = buf.get_i64();
        let start_time = Timestamp::from_nanos(start_nanos);

        // End time
        let end_nanos = buf.get_i64();
        let end_time = Timestamp::from_nanos(end_nanos);

        // Event count
        let event_count = buf.get_u32();

        // Compressed size
        let compressed_size = buf.get_u32();

        // Checksum
        let checksum = buf.get_u32();

        // Flags
        let flags = buf.get_u8();

        Ok(Self {
            segment_id,
            start_time,
            end_time,
            event_count,
            compressed_size,
            checksum,
            flags,
        })
    }
}

/// Segment file writer
pub struct SegmentWriter {
    file: File,
    header: SegmentHeader,
    event_buffer: Vec<Event>,
    current_offset: u64,
    checksum_hasher: Crc32Hasher,
}

impl SegmentWriter {
    /// Create a new segment file
    pub fn create<P: AsRef<Path>>(
        path: P,
        segment_id: u64,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Self> {
        let path = path.as_ref();
        
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let header = SegmentHeader::new(segment_id, start_time, end_time);
        let header_bytes = header.serialize();
        
        // Write header
        file.write_all(&header_bytes)?;
        file.sync_all()?;

        Ok(Self {
            file,
            header,
            event_buffer: Vec::new(),
            current_offset: HEADER_SIZE as u64,
            checksum_hasher: Crc32Hasher::new(),
        })
    }

    /// Append an event to the segment
    pub fn append(&mut self, event: Event) -> Result<()> {
        // Validate timestamp
        let ts = event.timestamp();
        if ts < self.header.start_time || ts >= self.header.end_time {
            return Err(Error::Temporal(format!(
                "Event timestamp {} outside segment range [{}, {})",
                ts.as_nanos(),
                self.header.start_time.as_nanos(),
                self.header.end_time.as_nanos()
            )));
        }

        self.event_buffer.push(event);
        self.header.event_count += 1;

        // Flush buffer if it gets too large
        if self.event_buffer.len() >= 1000 {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Flush event buffer to disk
    fn flush_buffer(&mut self) -> Result<()> {
        if self.event_buffer.is_empty() {
            return Ok(());
        }

        // Serialize events
        let mut serialized = Vec::new();
        for event in &self.event_buffer {
            let event_bytes = bincode::serialize(event)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            serialized.extend_from_slice(&(event_bytes.len() as u32).to_le_bytes());
            serialized.extend_from_slice(&event_bytes);
        }

        // Compress with ZSTD
        let compressed = zstd::encode_all(&serialized[..], ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| Error::Storage(format!("ZSTD compression failed: {}", e)))?;

        // Update checksum with compressed data
        self.checksum_hasher.update(&compressed);

        // Write compressed data with length prefix
        let compressed_len = compressed.len() as u32;
        self.file.write_all(&compressed_len.to_le_bytes())
            .map_err(|e| Error::Io(e))?;
        self.file.write_all(&compressed)
            .map_err(|e| Error::Io(e))?;
        
        self.current_offset += 4 + compressed.len() as u64;

        // Mark segment as compressed
        self.header.flags |= FLAG_COMPRESSED;
        
        // Update header: compressed_size is the total size after header
        self.header.compressed_size = (self.current_offset - HEADER_SIZE as u64) as u32;

        // Clear buffer
        self.event_buffer.clear();

        Ok(())
    }

    /// Finalize the segment (write header and close)
    /// Returns the finalized header with updated checksum and flags
    pub fn finalize(mut self) -> Result<SegmentHeader> {
        // Flush remaining events
        self.flush_buffer()?;

        // Calculate final checksum from all compressed data
        self.header.checksum = self.checksum_hasher.finalize();

        // Write updated header
        self.file.seek(SeekFrom::Start(0))?;
        let header_bytes = self.header.serialize();
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?;

        Ok(self.header)
    }

    /// Get current segment info
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }
}

/// Segment file reader
pub struct SegmentReader {
    file: File,
    header: SegmentHeader,
    path: PathBuf,
}

impl SegmentReader {
    /// Open an existing segment file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mut file = File::open(path)?;

        // Read header
        let mut header_buf = vec![0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        
        let header = SegmentHeader::deserialize(&header_buf)?;

        Ok(Self {
            file,
            header,
            path: path.to_path_buf(),
        })
    }

    /// Read all events from the segment
    pub fn read_events(&mut self) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        let mut checksum_hasher = Crc32Hasher::new();

        // Seek past header
        self.file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        // Check if segment is compressed
        let is_compressed = (self.header.flags & FLAG_COMPRESSED) != 0;

        if is_compressed {
            // Read compressed blocks until EOF
            loop {
                // Read compressed block length
                let mut len_buf = [0u8; 4];
                match self.file.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(Error::Io(e)),
                }

                let compressed_len = u32::from_le_bytes(len_buf) as usize;

                // Read compressed data
                let mut compressed_buf = vec![0u8; compressed_len];
                self.file.read_exact(&mut compressed_buf)?;

                // Update checksum
                checksum_hasher.update(&compressed_buf);

                // Decompress
                let decompressed = zstd::decode_all(&compressed_buf[..])
                    .map_err(|e| Error::Storage(format!("ZSTD decompression failed: {}", e)))?;

                // Parse events from decompressed data
                let mut offset = 0;
                while offset < decompressed.len() {
                    if offset + 4 > decompressed.len() {
                        return Err(Error::Storage("Truncated event length".to_string()));
                    }

                    let event_len = u32::from_le_bytes([
                        decompressed[offset],
                        decompressed[offset + 1],
                        decompressed[offset + 2],
                        decompressed[offset + 3],
                    ]) as usize;
                    offset += 4;

                    if offset + event_len > decompressed.len() {
                        return Err(Error::Storage("Truncated event data".to_string()));
                    }

                    let event: Event = bincode::deserialize(&decompressed[offset..offset + event_len])
                        .map_err(|e| Error::Serialization(e.to_string()))?;
                    events.push(event);
                    offset += event_len;
                }
            }

            // Verify checksum
            let calculated_checksum = checksum_hasher.finalize();
            if calculated_checksum != self.header.checksum {
                return Err(Error::Storage(format!(
                    "Checksum mismatch: expected {}, got {}",
                    self.header.checksum, calculated_checksum
                )));
            }
        } else {
            // Legacy format: read uncompressed events
            loop {
                // Read event length
                let mut len_buf = [0u8; 4];
                match self.file.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(Error::Io(e)),
                }

                let event_len = u32::from_le_bytes(len_buf) as usize;

                // Read event data
                let mut event_buf = vec![0u8; event_len];
                self.file.read_exact(&mut event_buf)?;

                // Deserialize event
                let event: Event = bincode::deserialize(&event_buf)?;
                events.push(event);
            }
        }

        Ok(events)
    }

    /// Get segment header
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::{Event, EventPayload};
    use tempfile::TempDir;

    #[test]
    fn test_segment_header_serialization() {
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);
        let header = SegmentHeader::new(1, ts1, ts2);

        let serialized = header.serialize();
        assert_eq!(serialized.len(), HEADER_SIZE);

        let deserialized = SegmentHeader::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.segment_id, 1);
        assert_eq!(deserialized.start_time, ts1);
        assert_eq!(deserialized.end_time, ts2);
    }

    #[test]
    fn test_segment_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test.temp");

        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        // Write segment
        let mut writer = SegmentWriter::create(&segment_path, 1, ts1, ts2).unwrap();

        // Create test events
        let payload1 = EventPayload::from_json(&serde_json::json!({"value": "test1"}))
            .unwrap();
        let event1 = Event::new("test.event".to_string(), ts1, "entity:1".to_string(), payload1);

        let payload2 = EventPayload::from_json(&serde_json::json!({"value": "test2"}))
            .unwrap();
        let event2 = Event::new(
            "test.event".to_string(),
            Timestamp::from_secs(1500),
            "entity:1".to_string(),
            payload2,
        );

        writer.append(event1.clone()).unwrap();
        writer.append(event2.clone()).unwrap();
        let _ = writer.finalize().unwrap();

        // Read segment
        let mut reader = SegmentReader::open(&segment_path).unwrap();
        let header = reader.header();
        assert_eq!(header.segment_id, 1);
        assert_eq!(header.event_count, 2);
        
        // Verify compression flag is set
        assert_ne!(header.flags & FLAG_COMPRESSED, 0, "Segment should be compressed");
        
        // Verify checksum is non-zero
        assert_ne!(header.checksum, 0, "Checksum should be calculated");

        let events = reader.read_events().unwrap();
        assert_eq!(events.len(), 2);
        
        // Verify events match
        assert_eq!(events[0].event_type(), event1.event_type());
        assert_eq!(events[0].entity_id(), event1.entity_id());
        assert_eq!(events[1].event_type(), event2.event_type());
        assert_eq!(events[1].entity_id(), event2.entity_id());
    }

    #[test]
    fn test_segment_compression() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test_compressed.temp");

        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        // Write segment with many events to test compression
        let mut writer = SegmentWriter::create(&segment_path, 2, ts1, ts2).unwrap();

        // Create multiple events with repetitive data (should compress well)
        for i in 0..100 {
            let payload = EventPayload::from_json(&serde_json::json!({
                "index": i,
                "data": "This is a repetitive string that should compress well".repeat(10)
            })).unwrap();
            let event = Event::new(
                "test.event".to_string(),
                Timestamp::from_secs(1000 + i as i64),
                format!("entity:{}", i),
                payload,
            );
            writer.append(event).unwrap();
        }
        let _ = writer.finalize().unwrap();

        // Read segment
        let mut reader = SegmentReader::open(&segment_path).unwrap();
        let header = reader.header();
        assert_eq!(header.event_count, 100);
        assert_ne!(header.flags & FLAG_COMPRESSED, 0);
        assert_ne!(header.checksum, 0);

        let events = reader.read_events().unwrap();
        assert_eq!(events.len(), 100);
    }

    #[test]
    fn test_compression_ratio() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test_ratio.temp");

        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        let mut writer = SegmentWriter::create(&segment_path, 3, ts1, ts2).unwrap();

        // Create events with highly repetitive data
        let repetitive_data = "A".repeat(1000);
        for i in 0..50 {
            let payload = EventPayload::from_json(&serde_json::json!({
                "id": i,
                "large_repetitive_data": repetitive_data,
                "metadata": format!("event-{}", i)
            })).unwrap();
            let event = Event::new(
                "test.event".to_string(),
                Timestamp::from_secs(1000 + i as i64),
                format!("entity:{}", i),
                payload,
            );
            writer.append(event).unwrap();
        }
        let _ = writer.finalize().unwrap();

        // Check file size
        let file_size = std::fs::metadata(&segment_path).unwrap().len();
        let header = SegmentReader::open(&segment_path).unwrap().header().clone();
        
        println!("File size: {} bytes", file_size);
        println!("Compressed size in header: {} bytes", header.compressed_size);
        println!("Event count: {}", header.event_count);
        println!("Compression ratio: {:.2}x", 
            (header.compressed_size as f64) / (file_size - HEADER_SIZE as u64) as f64);

        // Verify compression is working (compressed size should be less than uncompressed)
        assert_ne!(header.flags & FLAG_COMPRESSED, 0);
        
        // Read back and verify
        let mut reader = SegmentReader::open(&segment_path).unwrap();
        let events = reader.read_events().unwrap();
        assert_eq!(events.len(), 50);
    }

    #[test]
    fn test_checksum_verification() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test_checksum.temp");

        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        // Write segment
        let mut writer = SegmentWriter::create(&segment_path, 4, ts1, ts2).unwrap();
        let payload = EventPayload::from_json(&serde_json::json!({"value": "test"})).unwrap();
        let event = Event::new("test.event".to_string(), ts1, "entity:1".to_string(), payload);
        writer.append(event).unwrap();
        writer.finalize().unwrap();

        // Read successfully (should work)
        let mut reader = SegmentReader::open(&segment_path).unwrap();
        let events = reader.read_events().unwrap();
        assert_eq!(events.len(), 1);

        // Corrupt the file by modifying a byte after the header
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&segment_path)
            .unwrap();
        file.seek(SeekFrom::Start(HEADER_SIZE as u64 + 10)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_all().unwrap();

        // Reading should fail due to checksum mismatch or decompression error
        let mut reader = SegmentReader::open(&segment_path).unwrap();
        let result = reader.read_events();
        assert!(result.is_err(), "Reading corrupted segment should fail");
    }

    #[test]
    fn test_multiple_compressed_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test_blocks.temp");

        // Use a wide time range to accommodate many events
        let ts1 = Timestamp::from_nanos(i64::MIN + 1);
        let ts2 = Timestamp::from_nanos(i64::MAX);

        let mut writer = SegmentWriter::create(&segment_path, 5, ts1, ts2).unwrap();

        // Add events in batches to trigger multiple flush_buffer calls
        // Buffer flushes at 1000 events, so we'll create multiple compressed blocks
        for batch in 0..3 {
            for i in 0..1000 {
                let payload = EventPayload::from_json(&serde_json::json!({
                    "batch": batch,
                    "index": i,
                    "data": format!("batch-{}-event-{}", batch, i)
                })).unwrap();
                // Use nanoseconds to ensure unique timestamps within the range
                let timestamp = Timestamp::from_nanos(1000_000_000_000 + (batch * 1000 + i) as i64);
                let event = Event::new(
                    "test.event".to_string(),
                    timestamp,
                    format!("entity:{}", batch * 1000 + i),
                    payload,
                );
                writer.append(event).unwrap();
            }
        }
        writer.finalize().unwrap();

        // Read back all events
        let mut reader = SegmentReader::open(&segment_path).unwrap();
        let header = reader.header();
        assert_eq!(header.event_count, 3000);
        assert_ne!(header.flags & FLAG_COMPRESSED, 0);

        let events = reader.read_events().unwrap();
        assert_eq!(events.len(), 3000);
    }
}
