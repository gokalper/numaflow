// Phase 3: State Layout Abstraction
//
// Provides different storage strategies for different operator modes:
// - Compact: Aggregations, group-by (per-key blobs)
// - Columnar: Joins, tables (time-bucketed Arrow batches)

use crate::{Error, Result};
use parking_lot::RwLock;
use rocksdb::{ColumnFamilyRef, DB};
use std::sync::Arc;

/// State layout strategy based on operator mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateLayoutMode {
    /// Compact: Per-key blobs for aggregations
    /// Use case: group-by, reduce, stateful map
    Compact,

    /// Columnar: Time-bucketed batches for joins/tables
    /// Use case: windowed join, temporal table
    /// TODO: Implement in Phase 3.2 if needed
    #[allow(dead_code)]
    Columnar,
}

/// State access trait - abstraction over storage patterns
#[async_trait::async_trait]
pub trait StateAccess: Send + Sync {
    /// Get value for key
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put key-value pair
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Delete key
    async fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    /// Flush dirty keys to persistent storage
    async fn flush_dirty(&mut self, dirty_keys: &[Vec<u8>]) -> Result<()>;

    /// Get all keys (for iteration)
    async fn keys(&self) -> Result<Vec<Vec<u8>>>;
}

/// Compact state layout with hot cache + cold store
///
/// Strategy:
/// - Hot: LRU cache in memory (fast reads)
/// - Cold: RocksDB (persistent)
/// - Write-through: Updates go to both
pub struct CompactStateLayout {
    /// Hot cache (LRU)
    hot_cache: Arc<RwLock<lru::LruCache<Vec<u8>, Vec<u8>>>>,

    /// Cold persistent store (RocksDB)
    db: Arc<DB>,
    cf_name: String,

    /// Cache stats
    cache_hits: Arc<std::sync::atomic::AtomicU64>,
    cache_misses: Arc<std::sync::atomic::AtomicU64>,
}

impl CompactStateLayout {
    /// Create new compact layout
    ///
    /// # Arguments
    /// * `db` - RocksDB instance
    /// * `cf_name` - Column family name for this state
    /// * `cache_size` - Max number of entries in hot cache
    pub fn new(db: Arc<DB>, cf_name: String, cache_size: usize) -> Self {
        Self {
            hot_cache: Arc::new(RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(cache_size).unwrap(),
            ))),
            db,
            cf_name,
            cache_hits: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cache_misses: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Get column family handle
    fn cf_handle(&self) -> Result<ColumnFamilyRef<'_>> {
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| Error::Config(format!("Column family {} not found", self.cf_name)))
    }

    /// Get cache hit rate (for monitoring)
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self
            .cache_misses
            .load(std::sync::atomic::Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

#[async_trait::async_trait]
impl StateAccess for CompactStateLayout {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Try hot cache first
        {
            let mut cache = self.hot_cache.write();
            if let Some(value) = cache.get(key) {
                self.cache_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(Some(value.clone()));
            }
        }

        // Cache miss - read from RocksDB
        self.cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let cf = self.cf_handle()?;
        let value = self.db.get_cf(&cf, key)?;

        // Populate cache
        if let Some(ref v) = value {
            let mut cache = self.hot_cache.write();
            cache.put(key.to_vec(), v.clone());
        }

        Ok(value)
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Write-through: update both cache and RocksDB
        {
            let mut cache = self.hot_cache.write();
            cache.put(key.clone(), value.clone());
        }

        let cf = self.cf_handle()?;
        self.db.put_cf(&cf, &key, &value)?;

        Ok(())
    }

    async fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        // Remove from cache
        {
            let mut cache = self.hot_cache.write();
            cache.pop(&key);
        }

        // Delete from RocksDB
        let cf = self.cf_handle()?;
        self.db.delete_cf(&cf, &key)?;

        Ok(())
    }

    async fn flush_dirty(&mut self, dirty_keys: &[Vec<u8>]) -> Result<()> {
        // For dirty keys, ensure they're in RocksDB
        // (In compact mode, we write-through, so this is a no-op)
        // But we verify consistency

        let cf = self.cf_handle()?;
        let cache = self.hot_cache.read();

        for key in dirty_keys {
            if let Some(value) = cache.peek(key) {
                // Verify it's in RocksDB
                if self.db.get_cf(&cf, key)?.is_none() {
                    // Inconsistency - re-write
                    self.db.put_cf(&cf, key, value)?;
                }
            }
        }

        Ok(())
    }

    async fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let cf = self.cf_handle()?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        let mut result = Vec::new();
        for item in iter {
            let (key, _value) = item?;
            result.push(key.to_vec());
        }

        Ok(result)
    }
}

/// State layout factory
pub struct StateLayoutFactory;

impl StateLayoutFactory {
    /// Create state layout based on mode
    pub fn create(
        mode: StateLayoutMode,
        db: Arc<DB>,
        cf_name: String,
        cache_size: usize,
    ) -> Box<dyn StateAccess> {
        match mode {
            StateLayoutMode::Compact => {
                Box::new(CompactStateLayout::new(db, cf_name, cache_size))
            }
            StateLayoutMode::Columnar => {
                // TODO: Implement in Phase 3.2
                unimplemented!("Columnar mode not yet implemented")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (Arc<DB>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_opts = rocksdb::Options::default();
        let db = DB::open_cf_with_opts(
            &opts,
            temp_dir.path(),
            vec![("test_cf", cf_opts)],
        )
        .unwrap();

        (Arc::new(db), temp_dir)
    }

    #[tokio::test]
    async fn test_compact_layout_put_get() {
        let (db, _temp) = create_test_db();
        let mut layout = CompactStateLayout::new(db, "test_cf".to_string(), 100);

        // Put
        layout
            .put(b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        // Get (should hit cache)
        let value = layout.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Cache hit rate should be > 0
        assert!(layout.cache_hit_rate() > 0.0);
    }

    #[tokio::test]
    async fn test_compact_layout_delete() {
        let (db, _temp) = create_test_db();
        let mut layout = CompactStateLayout::new(db, "test_cf".to_string(), 100);

        layout
            .put(b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();
        layout.delete(b"key1".to_vec()).await.unwrap();

        let value = layout.get(b"key1").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_compact_layout_cache_eviction() {
        let (db, _temp) = create_test_db();
        let mut layout = CompactStateLayout::new(db, "test_cf".to_string(), 2); // Small cache

        // Write 3 keys (exceeds cache)
        layout
            .put(b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();
        layout
            .put(b"key2".to_vec(), b"value2".to_vec())
            .await
            .unwrap();
        layout
            .put(b"key3".to_vec(), b"value3".to_vec())
            .await
            .unwrap();

        // All keys should still be retrievable (from RocksDB)
        assert_eq!(
            layout.get(b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            layout.get(b"key2").await.unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            layout.get(b"key3").await.unwrap(),
            Some(b"value3".to_vec())
        );
    }

    #[tokio::test]
    async fn test_state_layout_factory() {
        let (db, _temp) = create_test_db();

        let layout = StateLayoutFactory::create(
            StateLayoutMode::Compact,
            db.clone(),
            "test_cf".to_string(),
            100,
        );

        // Factory should create working layout
        assert!(layout.get(b"nonexistent").await.unwrap().is_none());
    }
}
