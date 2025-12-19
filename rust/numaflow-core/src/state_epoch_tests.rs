use super::*;
use crate::state::StateStore;
use tempfile::TempDir;

// Phase 3: Epoch delta tests

#[test]
fn test_epoch_delta_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let store = StateStore::new(temp_dir.path(), true).unwrap();
    
    // Write to epoch 1
    store.put_in_epoch(1, b"key1".to_vec(), b"value1".to_vec()).unwrap();
    store.put_in_epoch(1, b"key2".to_vec(), b"value2".to_vec()).unwrap();
    
    // Write to epoch 2
    store.put_in_epoch(2, b"key3".to_vec(), b"value3".to_vec()).unwrap();
    
    // Epoch 1 should see its keys
    assert_eq!(
        store.get_in_epoch(1, b"key1").unwrap(),
        Some(b"value1".to_vec())
    );
    assert_eq!(
        store.get_in_epoch(1, b"key2").unwrap(),
        Some(b"value2".to_vec())
    );
    
    // Epoch 2 should see its keys
    assert_eq!(
        store.get_in_epoch(2, b"key3").unwrap(),
        Some(b"value3".to_vec())
    );
    
    // Epoch 2 should NOT see epoch 1's keys (not committed yet)
    assert_eq!(store.get_in_epoch(2, b"key1").unwrap(), None);
}

#[test]
fn test_epoch_commit() {
    let temp_dir = TempDir::new().unwrap();
    let store = StateStore::new(temp_dir.path(), true).unwrap();
    
    // Write to epoch 1
    store.put_in_epoch(1, b"key1".to_vec(), b"value1".to_vec()).unwrap();
    
    // Not in RocksDB yet
    assert_eq!(store.read_state(b"key1").unwrap(), None);
    
    // Commit epoch
    store.commit_epoch(1).unwrap();
    
    // Now in RocksDB
    assert_eq!(
        store.read_state(b"key1").unwrap(),
        Some(b"value1".to_vec())
    );
    
    // And visible to other epochs
    assert_eq!(
        store.get_in_epoch(2, b"key1").unwrap(),
        Some(b"value1".to_vec())
    );
}

#[test]
fn test_epoch_discard() {
    let temp_dir = TempDir::new().unwrap();
    let store = StateStore::new(temp_dir.path(), true).unwrap();
    
    // Write to epoch 1
    store.put_in_epoch(1, b"key1".to_vec(), b"value1".to_vec()).unwrap();
    
    // Discard epoch (NOT commit)
    store.discard_epoch(1);
    
    // Not in RocksDB
    assert_eq!(store.read_state(b"key1").unwrap(), None);
    
    // Not visible to any epoch
    assert_eq!(store.get_in_epoch(2, b"key1").unwrap(), None);
}

#[test]
fn test_epoch_delete() {
    let temp_dir = TempDir::new().unwrap();
    let store = StateStore::new(temp_dir.path(), true).unwrap();
    
    // Write and commit
    store.put_in_epoch(1, b"key1".to_vec(), b"value1".to_vec()).unwrap();
    store.commit_epoch(1).unwrap();
    
    // Delete in epoch 2
    store.delete_in_epoch(2, b"key1".to_vec()).unwrap();
    
    // Still in RocksDB (epoch 2 not committed)
    assert_eq!(
        store.read_state(b"key1").unwrap(),
        Some(b"value1".to_vec())
    );
    
    // But epoch 2 sees it as deleted
    assert_eq!(store.get_in_epoch(2, b"key1").unwrap(), None);
    
    // Commit epoch 2
    store.commit_epoch(2).unwrap();
    
    // Now deleted from RocksDB
    assert_eq!(store.read_state(b"key1").unwrap(), None);
}

#[test]
fn test_epoch_read_your_writes() {
    let temp_dir = TempDir::new().unwrap();
    let store = StateStore::new(temp_dir.path(), true).unwrap();
    
    // Write to epoch 1
    store.put_in_epoch(1, b"key1".to_vec(), b"v1".to_vec()).unwrap();
    
    // Read immediately (should see uncommitted write)
    assert_eq!(
        store.get_in_epoch(1, b"key1").unwrap(),
        Some(b"v1".to_vec())
    );
    
    // Update same key
    store.put_in_epoch(1, b"key1".to_vec(), b"v2".to_vec()).unwrap();
    
    // Read should see latest uncommitted value
    assert_eq!(
        store.get_in_epoch(1, b"key1").unwrap(),
        Some(b"v2".to_vec())
    );
}

#[test]
fn test_epoch_dirty_keys() {
    let temp_dir = TempDir::new().unwrap();
    let store = StateStore::new(temp_dir.path(), true).unwrap();
    
    // Write 3 keys
    store.put_in_epoch(1, b"key1".to_vec(), b"v1".to_vec()).unwrap();
    store.put_in_epoch(1, b"key2".to_vec(), b"v2".to_vec()).unwrap();
    store.put_in_epoch(1, b"key3".to_vec(), b"v3".to_vec()).unwrap();
    
    // Get dirty keys
    let dirty = store.get_dirty_keys(1);
    assert_eq!(dirty.len(), 3);
    assert!(dirty.contains(&b"key1".to_vec()));
    assert!(dirty.contains(&b"key2".to_vec()));
    assert!(dirty.contains(&b"key3".to_vec()));
}
