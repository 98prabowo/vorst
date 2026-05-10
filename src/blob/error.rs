use std::{io, path::PathBuf};

use thiserror::Error;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO failure: {0}")]
    Io(#[from] io::Error),

    // --- Cache & Config Errors ---
    #[error("Invalid storage configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Internal lock poisoned: {0}")]
    CachePoisoned(String),

    // --- Search Errors ---
    #[error("Object {id} not found in segment {segment_id}")]
    ObjectNotFound { id: Uuid, segment_id: Uuid },

    #[error("Object {id} was previously marked as deleted (Tombstone)")]
    ObjectDeleted { id: Uuid },

    // --- Data Integrity Errors ---
    #[error("Out of bounds in segment {id}: offset {offset} + size {size} exceeds capacity")]
    OutOfBounds { id: Uuid, offset: u64, size: u64 },

    #[error("Invalid magic number: expected {expected:#x}, found {found:#x}")]
    InvalidMagic { expected: u32, found: u32 },

    #[error("Segment ID mismatch: expected {expected}, found {found}")]
    IdMismatch { expected: Uuid, found: Uuid },

    #[error("Checksum mismatch for object {id}: stored {expected:#x}, calculated {found:#x}")]
    ChecksumMismatch { id: Uuid, expected: u32, found: u32 },

    #[error("Partial/Torn write detected at offset {offset}")]
    TornWrite { offset: u64 },

    // --- Resource Errors ---
    #[error("Segment capacity reached: cannot ingest {needed} bytes")]
    StorageFull { needed: u64 },

    #[error("Segment {id} is locked by another process at {path}")]
    SegmentLocked { id: Uuid, path: PathBuf },

    #[error("Invalid timestamp in segment header")]
    InvalidTimestamp,

    #[error("Internal coordinator error: {0}")]
    Internal(String),
}

impl Error {
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::OutOfBounds { .. }
                | Self::InvalidMagic { .. }
                | Self::ChecksumMismatch { .. }
                | Self::TornWrite { .. }
        )
    }
}
