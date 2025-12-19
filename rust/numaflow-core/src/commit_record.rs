// Phase 2: Root of Truth - Commit Record
//
// This module implements the authoritative ledger for Enhanced MonoVertex.
// The commit record is the single source of truth for:
// - What data has been processed
// - What state exists  
// - What output is visible
// - Where to resume after crash/restart

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Schema version for CommitRecord format evolution
pub const COMMIT_RECORD_SCHEMA_VERSION: u16 = 1;

/// Commit record status lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitStatus {
    /// State flushed, output MAYBE visible, NOT ack'd to source
    Prepared,
    /// Output visible, safe to ack source
    Committed,
}

/// Source position per source type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourcePosition {
    Kafka { offset: i64 },
    JetStream { sequence: u64 },
    Generator { count: u64 },
}

/// Sink visibility reference (proof of output)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SinkVisibilityRef {
    Kafka {
        transaction_id: String,
        producer_epoch: i32, // Future-proof (not i16)
        partition_offsets: HashMap<i32, i64>,
    },
    S3 {
        manifest_uri: String,
        staging_prefix: String,
        object_count: usize,
    },
    JetStream {
        msg_ids: Vec<String>,
        stream: String,
    },
    Log,
    Blackhole,
}

/// The authoritative ledger entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRecord {
    // Identity & Versioning
    pub generation_id: u64,
    pub epoch_id: u64,
    pub schema_version: u16,
    
    // Source Progress (Authoritative)
    pub source_offsets: HashMap<u32, SourcePosition>, // ✅ u32 partition_id
    
    // Output References
    pub sink_ref: SinkVisibilityRef,
    
    // State Metadata
    pub state_checkpoint_id: u64,
    pub state_keys_count: u64,
    pub state_size_bytes: u64,
    
    // Schema & Watermark (Phase 5)
    pub schema_id: Option<u64>,
    pub watermark: Option<i64>,
  
    // Lifecycle
    pub status: CommitStatus,
    pub created_at: i64,  // Unix timestamp (ms)
    pub committed_at: Option<i64>,
    
    // Integrity
    pub checksum: u32,  // CRC32 over serialized fields (excluding checksum)
}

impl CommitRecord {
    /// Create a new Prepared commit record
    pub fn new_prepared(
        generation_id: u64,
        epoch_id: u64,
        source_offsets: HashMap<u32, SourcePosition>,
        sink_ref: SinkVisibilityRef,
        state_checkpoint_id: u64,
        state_keys_count: u64,
        state_size_bytes: u64,
    ) -> Self {
        let mut record = Self {
            generation_id,
            epoch_id,
            schema_version: COMMIT_RECORD_SCHEMA_VERSION,
            source_offsets,
            sink_ref,
            state_checkpoint_id,
            state_keys_count,
            state_size_bytes,
            schema_id: None,
            watermark: None,
            status: CommitStatus::Prepared,
            created_at: now_ms(),
            committed_at: None,
            checksum: 0, // Computed below
        };
        
        record.checksum = record.compute_checksum();
        record
    }
    
    /// Compute CRC32 checksum over all fields except checksum itself
    pub fn compute_checksum(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        
        // Serialize all fields except checksum
        hasher.update(&self.generation_id.to_le_bytes());
        hasher.update(&self.epoch_id.to_le_bytes());
        hasher.update(&self.schema_version.to_le_bytes());
        
        // source_offsets
        if let Ok(bytes) = bincode::serialize(&self.source_offsets) {
            hasher.update(&bytes);
        }
        
        // sink_ref
        if let Ok(bytes) = bincode::serialize(&self.sink_ref) {
            hasher.update(&bytes);
        }
        
        hasher.update(&self.state_checkpoint_id.to_le_bytes());
        hasher.update(&self.state_keys_count.to_le_bytes());
        hasher.update(&self.state_size_bytes.to_le_bytes());
        
        if let Some(schema_id) = self.schema_id {
            hasher.update(&schema_id.to_le_bytes());
        }
        
        if let Some(watermark) = self.watermark {
            hasher.update(&watermark.to_le_bytes());
        }
        
        hasher.update(&(self.status as u8).to_le_bytes());
        hasher.update(&self.created_at.to_le_bytes());
        
        if let Some(committed_at) = self.committed_at {
            hasher.update(&committed_at.to_le_bytes());
        }
        
        hasher.finalize()
    }
    
    /// Validate checksum (fail-fast on corruption)
    pub fn validate_checksum(&self) -> Result<(), CommitRecordError> {
        let computed = self.compute_checksum();
        if computed != self.checksum {
            return Err(CommitRecordError::Corrupted  {
                expected: self.checksum,
                actual: computed,
                generation_id: self.generation_id,
                epoch_id: self.epoch_id,
            });
        }
        Ok(())
    }
    
    /// Transition to Committed status
    pub fn mark_committed(&mut self) {
        self.status = CommitStatus::Committed;
        self.committed_at = Some(now_ms());
        self.checksum = self.compute_checksum(); // Recompute
    }
}

/// Commit record-specific errors
#[derive(Debug, thiserror::Error)]
pub enum CommitRecordError {
    #[error("Commit record corrupted: expected checksum {expected:08x}, got {actual:08x} (gen={generation_id}, epoch={epoch_id})")]
    Corrupted {
        expected: u32,
        actual: u32,
        generation_id: u64,
        epoch_id: u64,
    },
    
    #[error("No committed epoch found for generation {0}")]
    NoCommittedEpoch(u64),
    
    #[error("Reconciliation failed: {0}")]
    ReconciliationFailed(String),
    
    #[error("Reconciliation timeout")]
    ReconciliationTimeout,
    
    #[error("Invalid sink reference")]
    InvalidSinkRef,
}

/// Get current time in milliseconds
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_commit_record_checksum() {
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });
        
        let record = CommitRecord::new_prepared(
            1, // generation_id
            42, // epoch_id
            offsets,
            SinkVisibilityRef::Log,
            1000, // state_checkpoint_id
            50, // state_keys_count
            4096, // state_size_bytes
        );
        
        // Should validate successfully
        assert!(record.validate_checksum().is_ok());
        
        // Tamper with data
        let mut tampered = record.clone();
        tampered.epoch_id = 43;
        
        // Should fail validation (checksum mismatch)
        assert!(tampered.validate_checksum().is_err());
    }
    
    #[test]
    fn test_commit_record_mark_committed() {
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });
        
        let mut record = CommitRecord::new_prepared(
            1, 42, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
        );
        
        assert_eq!(record.status, CommitStatus::Prepared);
        assert!(record.committed_at.is_none());
        
        record.mark_committed();
        
        assert_eq!(record.status, CommitStatus::Committed);
        assert!(record.committed_at.is_some());
        assert!(record.validate_checksum().is_ok());
    }
}
