// Phase 3 Part 3: Dirty Index
//
// Tracks which keys/segments were modified since last checkpoint.
// Enables incremental checkpointing (90%+ I/O reduction).

use std::collections::HashSet;
use parking_lot::RwLock;
use std::sync::Arc;

/// Segment identifier for columnar mode (future)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SegmentId {
    pub time_window: u64,
    pub partition: u32,
}

/// Tracks dirty keys for incremental checkpointing
pub struct DirtyIndex {
    /// Keys modified since last checkpoint
    dirty_keys: Arc<RwLock<HashSet<Vec<u8>>>>,
    
    /// Segment-level tracking for columnar mode (future)
    dirty_segments: Arc<RwLock<HashSet<SegmentId>>>,
    
    /// Estimated bytes of dirty state
    dirty_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

impl DirtyIndex {
    /// Create a new dirty index
    pub fn new() -> Self {
        Self {
            dirty_keys: Arc::new(RwLock::new(HashSet::new())),
            dirty_segments: Arc::new(RwLock::new(HashSet::new())),
            dirty_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Mark a key as dirty
    pub fn mark_dirty(&self, key: Vec<u8>, value_size: usize) {
        let mut keys = self.dirty_keys.write();
        keys.insert(key);
        
        // Track bytes
        self.dirty_bytes
            .fetch_add(value_size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Mark multiple keys as dirty (batch)
    pub fn mark_dirty_batch(&self, keys: Vec<(Vec<u8>, usize)>) {
        let mut dirty_keys = self.dirty_keys.write();
        let mut total_bytes = 0;
        
        for (key, value_size) in keys {
            dirty_keys.insert(key);
            total_bytes += value_size;
        }
        
        self.dirty_bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Mark a segment as dirty (for columnar mode)
    #[allow(dead_code)]
    pub fn mark_segment_dirty(&self, segment: SegmentId) {
        let mut segments = self.dirty_segments.write();
        segments.insert(segment);
    }

    /// Get all dirty keys and clear the index
    ///
    /// This is used during checkpoint flush.
    /// Returns keys that need to be persisted to RocksDB.
    pub fn drain_dirty_keys(&self) -> Vec<Vec<u8>> {
        let mut keys = self.dirty_keys.write();
        let result: Vec<_> = keys.drain().collect();
        
        // Reset byte counter
        self.dirty_bytes
            .store(0, std::sync::atomic::Ordering::Relaxed);
        
        result
    }

    /// Get dirty keys without clearing (peek)
    pub fn get_dirty_keys(&self) -> Vec<Vec<u8>> {
        let keys = self.dirty_keys.read();
        keys.iter().cloned().collect()
    }

    /// Get dirty segments and clear
    #[allow(dead_code)]
    pub fn drain_dirty_segments(&self) -> Vec<SegmentId> {
        let mut segments = self.dirty_segments.write();
        segments.drain().collect()
    }

    /// Check if incremental checkpoint is needed
    ///
    /// Returns true if dirty state exceeds threshold.
    pub fn should_checkpoint(&self, threshold_bytes: usize) -> bool {
        let dirty_bytes = self.dirty_bytes.load(std::sync::atomic::Ordering::Relaxed);
        dirty_bytes > threshold_bytes
    }

    /// Get current dirty byte count
    pub fn dirty_byte_count(&self) -> usize {
        self.dirty_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get current dirty key count
    pub fn dirty_key_count(&self) -> usize {
        let keys = self.dirty_keys.read();
        keys.len()
    }

    /// Clear all dirty tracking (after successful checkpoint)
    pub fn clear(&self) {
        let mut keys = self.dirty_keys.write();
        keys.clear();
        
        let mut segments = self.dirty_segments.write();
        segments.clear();
        
        self.dirty_bytes
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if a key is dirty
    pub fn is_dirty(&self, key: &[u8]) -> bool {
        let keys = self.dirty_keys.read();
        keys.contains(key)
    }
}

impl Default for DirtyIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for DirtyIndex {
    fn clone(&self) -> Self {
        Self {
            dirty_keys: Arc::clone(&self.dirty_keys),
            dirty_segments: Arc::clone(&self.dirty_segments),
            dirty_bytes: Arc::clone(&self.dirty_bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_dirty() {
        let index = DirtyIndex::new();
        
        index.mark_dirty(b"key1".to_vec(), 100);
        index.mark_dirty(b"key2".to_vec(), 200);
        
        assert_eq!(index.dirty_key_count(), 2);
        assert_eq!(index.dirty_byte_count(), 300);
        assert!(index.is_dirty(b"key1"));
        assert!(index.is_dirty(b"key2"));
    }

    #[test]
    fn test_mark_dirty_batch() {
        let index = DirtyIndex::new();
        
        let batch = vec![
            (b"key1".to_vec(), 100),
            (b"key2".to_vec(), 200),
            (b"key3".to_vec(), 150),
        ];
        
        index.mark_dirty_batch(batch);
        
        assert_eq!(index.dirty_key_count(), 3);
        assert_eq!(index.dirty_byte_count(), 450);
    }

    #[test]
    fn test_drain_dirty_keys() {
        let index = DirtyIndex::new();
        
        index.mark_dirty(b"key1".to_vec(), 100);
        index.mark_dirty(b"key2".to_vec(), 200);
        
        let keys = index.drain_dirty_keys();
        assert_eq!(keys.len(), 2);
        
        // After drain, should be empty
        assert_eq!(index.dirty_key_count(), 0);
        assert_eq!(index.dirty_byte_count(), 0);
    }

    #[test]
    fn test_should_checkpoint() {
        let index = DirtyIndex::new();
        
        // Below threshold
        index.mark_dirty(b"key1".to_vec(), 100);
        assert!(!index.should_checkpoint(1000));
        
        // Above threshold
        index.mark_dirty(b"key2".to_vec(), 1000);
        assert!(index.should_checkpoint(1000));
    }

    #[test]
    fn test_clear() {
        let index = DirtyIndex::new();
        
        index.mark_dirty(b"key1".to_vec(), 100);
        index.mark_dirty(b"key2".to_vec(), 200);
        
        index.clear();
        
        assert_eq!(index.dirty_key_count(), 0);
        assert_eq!(index.dirty_byte_count(), 0);
    }

    #[test]
    fn test_get_without_clear() {
        let index = DirtyIndex::new();
        
        index.mark_dirty(b"key1".to_vec(), 100);
        
        // Get without clearing
        let keys = index.get_dirty_keys();
        assert_eq!(keys.len(), 1);
        
        // Should still be dirty
        assert_eq!(index.dirty_key_count(), 1);
        assert!(index.is_dirty(b"key1"));
    }

    #[test]
    fn test_duplicate_keys() {
        let index = DirtyIndex::new();
        
        // Mark same key multiple times
        index.mark_dirty(b"key1".to_vec(), 100);
        index.mark_dirty(b"key1".to_vec(), 100);
        index.mark_dirty(b"key1".to_vec(), 100);
        
        // Should only count once for key count
        assert_eq!(index.dirty_key_count(), 1);
        
        // But bytes accumulate (conservative estimate)
        assert_eq!(index.dirty_byte_count(), 300);
    }

    #[test]
    fn test_clone() {
        let index1 = DirtyIndex::new();
        index1.mark_dirty(b"key1".to_vec(), 100);
        
        let index2 = index1.clone();
        
        // Both should see same state (shared Arc)
        assert_eq!(index2.dirty_key_count(), 1);
        assert!(index2.is_dirty(b"key1"));
        
        // Modifications affect both
        index2.mark_dirty(b"key2".to_vec(), 200);
        assert_eq!(index1.dirty_key_count(), 2);
    }
}
