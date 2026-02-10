//! Write-Ahead Log for durability
//!
//! The WAL guarantees that committed events survive process crashes by
//! recording them in an append-only file before they are flushed into
//! segment files or materialized views.

use crate::core::event::Event;
use crate::error::{Error, Result};
use crc32fast::Hasher as Crc32Hasher;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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

/// On-disk WAL implementation.
///
/// Record format (little-endian):
/// - 4 bytes: CRC32 of payload
/// - 4 bytes: payload length in bytes (N)
/// - N bytes: bincode-serialized `Event`
pub struct FileWAL {
    path: PathBuf,
    file: File,
}

impl FileWAL {
    /// Open (or create) a WAL file at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    /// Open a fresh read handle positioned at the beginning of the WAL.
    fn open_read(&self) -> Result<File> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        f.seek(SeekFrom::Start(0))?;
        Ok(f)
    }

    fn write_record(file: &mut File, event: &Event) -> Result<()> {
        let payload =
            bincode::serialize(event).map_err(|e| Error::Serialization(e.to_string()))?;

        let mut hasher = Crc32Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        let len = payload.len() as u32;

        // [crc32][len][payload]
        file.write_all(&crc.to_le_bytes())?;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&payload)?;

        Ok(())
    }

    fn read_next_record(file: &mut File) -> Result<Option<Event>> {
        let mut header = [0u8; 8];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Clean EOF: no more records.
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }

        let crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

        let mut buf = vec![0u8; len];
        if let Err(e) = file.read_exact(&mut buf) {
            // Truncated record at end of file; treat as logical EOF.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(Error::Io(e));
        }

        let mut hasher = Crc32Hasher::new();
        hasher.update(&buf);
        let actual_crc = hasher.finalize();
        if actual_crc != crc {
            // Corruption detected; stop replay here.
            return Err(Error::Storage("WAL CRC mismatch".to_string()));
        }

        let event: Event =
            bincode::deserialize(&buf).map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(Some(event))
    }
}

impl WriteAheadLog for FileWAL {
    fn append(&mut self, event: &Event) -> Result<()> {
        Self::write_record(&mut self.file, event)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    fn replay(&self) -> Result<Vec<Event>> {
        let mut f = self.open_read()?;
        let mut events = Vec::new();

        loop {
            match Self::read_next_record(&mut f)? {
                Some(ev) => events.push(ev),
                None => break,
            }
        }

        Ok(events)
    }

    fn clear(&mut self) -> Result<()> {
        // Truncate the file and reset write position.
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}
