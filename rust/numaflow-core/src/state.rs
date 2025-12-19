// Phase 2: State Storage Layer
//
// Provides persistent storage for commit records and user state with
// conditional activation based on enhanced_mode.

use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, DB};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use parking_lot::RwLock;

use crate::commit_record::CommitRecord;
use crate::Result;

/// Column family names
pub const CF_COMMIT_META: &str = "commit_meta";
pub const CF_STATE_DATA: &str = "state_data";

/// Phase 3: Epoch-scoped delta for uncommitted state
#[derive(Debug, Clone)]
struct EpochDelta {
    _epoch_id: u64,
    /// Keys modified in this epoch
    dirty_keys: Vec<Vec<u8>>,
    /// Buffered writes: key -> Some(value) or None (delete)
    buffer: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl EpochDelta {
    fn new(epoch_id: u64) -> Self {
        Self {
            _epoch_id: epoch_id,
            dirty_keys: Vec::new(),
            buffer: HashMap::new(),
        }
    }
    
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if !self.dirty_keys.contains(&key) {
            self.dirty_keys.push(key.clone());
        }
        self.buffer.insert(key, Some(value));
    }
    
    fn delete(&mut self, key: Vec<u8>) {
        if !self.dirty_keys.contains(&key) {
            self.dirty_keys.push(key.clone());
        }
        self.buffer.insert(key, None);
    }
    
    fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.buffer.get(key)
    }
}

/// StateStore manages persistent storage for enhanced MonoVertex
pub struct StateStore {
    db: Arc<DB>,
    enhanced_mode: bool,
    
    /// Phase 3: Epoch-scoped deltas (uncommitted state)
    /// Only exists in memory - never persisted
    epoch_deltas: Arc<RwLock<HashMap<u64, EpochDelta>>>,
}

impl StateStore {
    /// Create a new StateStore
    /// 
    /// If enhanced_mode is false, this returns a minimal store that does nothing.
    /// If enhanced_mode is true, initializes RocksDB with proper column families.
    pub fn new<P: AsRef<Path>>(path: P, enhanced_mode: bool) -> Result<Self> {
        if !enhanced_mode {
            // Standard mode: No-op store (we still need to return something)
            // Use a temporary directory that will be cleaned up
            let temp_path = std::env::temp_dir().join("numaflow_noop_db");
            let mut opts = Options::default();
            opts.create_if_missing(true);
            
            let db = DB::open(&opts, temp_path)
                .map_err(|e| crate::Error::Config(format!("Failed to open no-op DB: {}", e)))?;
            
            return Ok(Self {
                db: Arc::new(db),
                enhanced_mode: false,
                epoch_deltas: Arc::new(RwLock::new(HashMap::new())),
            });
        }

        // Enhanced mode: Full RocksDB with column families
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        // Optimize for SSD
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_max_open_files(1000);
        
        // Column family descriptors
        let cf_commit_meta = ColumnFamilyDescriptor::new(CF_COMMIT_META, Options::default());
        let cf_state_data = ColumnFamilyDescriptor::new(CF_STATE_DATA, Options::default());
        
        let db = DB::open_cf_descriptors(&opts, path, vec![cf_commit_meta, cf_state_data])
            .map_err(|e| crate::Error::Config(format!("Failed to open RocksDB: {}", e)))?;
        
        Ok(Self {
            db: Arc::new(db),
            enhanced_mode: true,
            epoch_deltas: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Write a commit record
    pub fn write_commit_record(&self, record: &CommitRecord) -> Result<()> {
        if !self.enhanced_mode {
            return Ok(()); // No-op in standard mode
        }

        let cf = self.db.cf_handle(CF_COMMIT_META)
            .ok_or_else(|| crate::Error::Config("commit_meta CF not found".to_string()))?;
        
        let key = format_commit_key(record.generation_id, record.epoch_id);
        let value = bincode::serialize(record)
            .map_err(|e| crate::Error::Config(format!("Failed to serialize commit record: {}", e)))?;
        
        self.db.put_cf(&cf, key.as_bytes(), value)
            .map_err(|e| crate::Error::Config(format!("Failed to write commit record: {}", e)))?;
        
        Ok(())
    }

    /// Load the latest committed record for a generation
    ///
    /// ## Iteration Strategy
    ///
    /// We use reverse iteration from the END of the keyspace to find the latest epoch.
    /// This works because commit keys are lexicographically sorted:
    ///   generation_{gen:016x}_epoch_{epoch:016x}
    ///
    /// Reverse iteration guarantees we see highest epoch_id first.
    ///
    /// ## Future Optimization
    ///
    /// For very long-running generations (millions of epochs), maintain a pointer:
    ///   generation_{gen:016x}_latest_committed -> epoch_id
    ///
    /// This would allow O(1) lookup instead of O(log N) reverse scan.
    /// Not implemented yet as premature optimization.
    ///
    /// ## Enhanced Mode Gate
    ///
    /// If enhanced_mode is false, returns None immediately (no records exist).
    pub fn load_latest_committed(&self, generation_id: u64) -> Result<Option<CommitRecord>> {
        if !self.enhanced_mode {
            return Ok(None); // No records in standard mode
        }

        let cf = self.db.cf_handle(CF_COMMIT_META)
            .ok_or_else(|| crate::Error::Config("commit_meta CF not found".to_string()))?;
        
        // Reverse iteration from END to find latest
        // Key format: generation_{gen:016x}_epoch_{epoch:016x}
        // Highest epoch_id appears first in reverse order
        let prefix = format!("generation_{:016x}_", generation_id);
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);
        
        for item in iter {
            let (key, value) = item.map_err(|e| crate::Error::Config(format!("Iterator error: {}", e)))?;
            let key_str = String::from_utf8_lossy(&key);
            
            // Stop if we've gone past this generation's prefix
            if !key_str.starts_with(&prefix) {
                break;
            }
            
            let record: CommitRecord = bincode::deserialize(&value)
                .map_err(|e| crate::Error::Config(format!("Failed to deserialize commit record: {}", e)))?;
            
            // Validate checksum
            record.validate_checksum()
                .map_err(|e| crate::Error::Config(format!("Commit record checksum failed: {}", e)))?;
            
            // Return latest record (Prepared or Committed)
            // Recovery logic handles both statuses appropriately
            return Ok(Some(record));
        }
        
        Ok(None)
    }

    /// Write state data
    pub fn write_state(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.enhanced_mode {
            return Ok(()); // No-op in standard mode
        }

        let cf = self.db.cf_handle(CF_STATE_DATA)
            .ok_or_else(|| crate::Error::Config("state_data CF not found".to_string()))?;
        
        self.db.put_cf(&cf, key, value)
            .map_err(|e| crate::Error::Config(format!("Failed to write state: {}", e)))?;
        
        Ok(())
    }

    /// Read state data
    pub fn read_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.enhanced_mode {
            return Ok(None); // No state in standard mode
        }

        let cf = self.db.cf_handle(CF_STATE_DATA)
            .ok_or_else(|| crate::Error::Config("state_data CF not found".to_string()))?;
        
        self.db.get_cf(&cf, key)
            .map_err(|e| crate::Error::Config(format!("Failed to read state: {}", e)))
    }

    /// Get reference to underlying DB (for advanced operations)
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    /// Check if enhanced mode is active
    pub fn is_enhanced(&self) -> bool {
        self.enhanced_mode
    }

    // ==================== Phase 3: Epoch-Scoped State Methods ====================

    /// Write to current epoch's delta (uncommitted)
    ///
    /// This buffers the write in memory only. It will be flushed to RocksDB
    /// on commit_epoch() or discarded on discard_epoch().
    ///
    /// # Key Invariant
    /// Uncommitted deltas are NEVER persisted - they only live in memory.
    /// This prevents partial epochs from leaking across restarts.
    pub fn put_in_epoch(&self, epoch_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if !self.enhanced_mode {
            return Ok(()); // No-op in standard mode
        }

        let mut deltas = self.epoch_deltas.write();
        let delta = deltas.entry(epoch_id).or_insert_with(|| EpochDelta::new(epoch_id));
        delta.put(key, value);

        Ok(())
    }

    /// Delete from current epoch's delta (uncommitted)
    pub fn delete_in_epoch(&self, epoch_id: u64, key: Vec<u8>) -> Result<()> {
        if !self.enhanced_mode {
            return Ok(()); // No-op in standard mode
        }

        let mut deltas = self.epoch_deltas.write();
        let delta = deltas.entry(epoch_id).or_insert_with(|| EpochDelta::new(epoch_id));
        delta.delete(key);

        Ok(())
    }

    /// Read from epoch delta (with fallback to RocksDB)
    ///
    /// Checks uncommitted delta first, then falls back to persisted state.
    /// This provides read-your-writes consistency within an epoch.
    pub fn get_in_epoch(&self, epoch_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.enhanced_mode {
            return Ok(None); // No state in standard mode
        }

        // Check epoch delta first
        {
            let deltas = self.epoch_deltas.read();
            if let Some(delta) = deltas.get(&epoch_id) {
                if let Some(value_opt) = delta.get(key) {
                    // Found in delta
                    return Ok(value_opt.clone());
                }
            }
        }

        // Fallback to RocksDB (committed state)
        self.read_state(key)
    }

    /// Commit epoch: flush delta to RocksDB atomically
    ///
    /// This is the ONLY way uncommitted state becomes persistent.
    /// Uses WriteBatch for atomicity.
    pub fn commit_epoch(&self, epoch_id: u64) -> Result<()> {
        if !self.enhanced_mode {
            return Ok(()); // No-op in standard mode
        }

        // Remove delta from memory
        let delta = {
            let mut deltas = self.epoch_deltas.write();
            deltas.remove(&epoch_id)
        };

        // If no delta, nothing to commit
        let Some(delta) = delta else {
            return Ok(());
        };

        // Atomic flush to RocksDB
        let cf = self.db.cf_handle(CF_STATE_DATA)
            .ok_or_else(|| crate::Error::Config("state_data CF not found".to_string()))?;

        let mut batch = WriteBatch::default();
        for (key, value_opt) in delta.buffer {
            match value_opt {
                Some(value) => batch.put_cf(&cf, &key, &value),
                None => batch.delete_cf(&cf, &key),
            }
        }

        self.db.write(batch)
            .map_err(|e| crate::Error::Config(format!("Failed to commit epoch {}: {}", epoch_id, e)))?;

        tracing::debug!(
            epoch_id = epoch_id,
            keys_committed = delta.dirty_keys.len(),
            "Epoch delta committed to RocksDB"
        );

        Ok(())
    }

    /// Discard uncommitted epoch
    ///
    /// This removes the epoch delta from memory without persisting.
    /// Used when an epoch fails or is aborted.
    pub fn discard_epoch(&self, epoch_id: u64) {
        let mut deltas = self.epoch_deltas.write();
        if let Some(delta) = deltas.remove(&epoch_id) {
            tracing::debug!(
                epoch_id = epoch_id,
                keys_discarded = delta.dirty_keys.len(),
                "Epoch delta discarded (not committed)"
            );
        }
    }

    /// Get dirty keys for an epoch (for incremental checkpointing)
    pub fn get_dirty_keys(&self, epoch_id: u64) -> Vec<Vec<u8>> {
        let deltas = self.epoch_deltas.read();
        deltas
            .get(&epoch_id)
            .map(|delta| delta.dirty_keys.clone())
            .unwrap_or_default()
    }
}

/// Format commit record key with lexicographic sorting
fn format_commit_key(generation_id: u64, epoch_id: u64) -> String {
    format!("generation_{:016x}_epoch_{:016x}", generation_id, epoch_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_record::{CommitStatus, SinkVisibilityRef};
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[test]
    fn test_state_store_standard_mode() {
        let temp_dir = TempDir::new().unwrap();
        let store = StateStore::new(temp_dir.path(), false).unwrap();
        
        assert!(!store.is_enhanced());
        
        // Operations should be no-ops
        let mut offsets = HashMap::new();
        offsets.insert(0, crate::commit_record::SourcePosition::Kafka { offset: 100 });
        
        let record = CommitRecord::new_prepared(
            1, 42, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
        );
        
        store.write_commit_record(&record).unwrap();
        assert!(store.load_latest_committed(1).unwrap().is_none());
    }

    #[test]
    fn test_state_store_enhanced_mode() {
        let temp_dir = TempDir::new().unwrap();
        let store = StateStore::new(temp_dir.path(), true).unwrap();
        
        assert!(store.is_enhanced());
        
        let mut offsets = HashMap::new();
        offsets.insert(0, crate::commit_record::SourcePosition::Kafka { offset: 100 });
        
        let mut record = CommitRecord::new_prepared(
            1, 42, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
        );
        
        // Write Prepared record
        store.write_commit_record(&record).unwrap();
        
        // Should be returned (changed behavior: returns Prepared for reconciliation)
        let loaded = store.load_latest_committed(1).unwrap().unwrap();
        assert_eq!(loaded.epoch_id, 42);
        assert_eq!(loaded.status, CommitStatus::Prepared);
        
        // Mark as Committed
        record.mark_committed();
        store.write_commit_record(&record).unwrap();
        
        // Should now return Committed version
        let loaded = store.load_latest_committed(1).unwrap().unwrap();
        assert_eq!(loaded.epoch_id, 42);
        assert_eq!(loaded.status, CommitStatus::Committed);
    }

    #[test]
    fn test_commit_key_format() {
        let key = format_commit_key(1, 42);
        assert_eq!(key, "generation_0000000000000001_epoch_000000000000002a");
        
        // Verify lexicographic sorting
        let key1 = format_commit_key(1, 10);
        let key2 = format_commit_key(1, 20);
        let key3 = format_commit_key(1, 100);
        
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn test_state_operations() {
        let temp_dir = TempDir::new().unwrap();
        let store = StateStore::new(temp_dir.path(), true).unwrap();
        
        let key = b"test_key";
        let value = b"test_value";
        
        // Write
        store.write_state(key, value).unwrap();
        
        // Read
        let read = store.read_state(key).unwrap().unwrap();
        assert_eq!(read, value);
        
        // Read non-existent
        assert!(store.read_state(b"nonexistent").unwrap().is_none());
    }
}
