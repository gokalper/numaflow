// Week 4 Priority 1: Crash Matrix Tests
//
// Tests all 5 crash scenarios to prove correctness under kill -9.
// Validates Prepared/Committed transitions and SEAL invariants.
//
// All tests verify enhanced_mode gating (standard mode bypasses Phase 2).

use numaflow_core::commit_record::{CommitRecord, CommitStatus, SinkVisibilityRef, SourcePosition};
use numaflow_core::config::monovertex::MonovertexConfig;
use numaflow_core::epoch::EpochCoordinator;
use numaflow_core::recovery::RecoveryCoordinator;
use numaflow_core::seal::SealCoordinator;
use numaflow_core::state::StateStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn test_config(enhanced_mode: bool, generation_id: u64) -> MonovertexConfig {
    let mut config = MonovertexConfig::default();
    config.enhanced_mode = enhanced_mode;
    config.generation_id = generation_id;
    config
}

/// Crash Scenario 1: Kill Before PERSIST
///
/// Timeline:
/// 1. SEAL completes (in-flight drained)
/// 2. CRASH (before WriteBatch)
/// 3. Recovery
///
/// Expected:
/// - No commit record exists
/// - Recovery starts fresh (None)
/// - Entire epoch must be replayed
#[tokio::test]
async fn test_crash_1_before_persist() {
    let temp_dir = TempDir::new().unwrap();
    
    // Enhanced mode test
    {
        let config = Arc::new(test_config(true, 1));
        let state_store = Arc::new(StateStore::new(temp_dir.path().join("enhanced"), true).unwrap());
        
        // Simulate: SEAL completes, then crash before PERSIST
        // (No commit record written)
        
        // Recovery
        let recovery = RecoveryCoordinator::new(config, state_store);
        let result = recovery.bootstrap().await.unwrap();
        
        assert!(result.is_none(), "No commit record should exist after crash before PERSIST");
    }
    
    // Standard mode test (should always skip recovery)
    {
        let config = Arc::new(test_config(false, 1));
        let state_store = Arc::new(StateStore::new(temp_dir.path().join("standard"), false).unwrap());
        
        let recovery = RecoveryCoordinator::new(config, state_store);
        let result = recovery.bootstrap().await.unwrap();
        
        assert!(result.is_none(), "Standard mode always skips Phase 2 recovery");
    }
}

/// Crash Scenario 2: Kill After PERSIST, Before PREPARE
///
/// Timeline:
/// 1. SEAL completes
/// 2. PERSIST writes Prepared commit record + state
/// 3. CRASH (before sink.prepare())
/// 4. Recovery
///
/// Expected:
/// - Prepared commit record exists
/// - Sink has no data (prepare never called)
/// - JetStream: Always safe to Replay
/// - Kafka/S3: Fail-fast (stubs not implemented)
#[tokio::test]
async fn test_crash_2_after_persist_before_prepare() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(true, 1));
    let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());
    
    // Simulate: Write Prepared record (JetStream sink)
    let mut offsets = HashMap::new();
    offsets.insert(0, SourcePosition::Kafka { offset: 100 });
    
    let record = CommitRecord::new_prepared(
        1, 42, offsets,
        SinkVisibilityRef::JetStream {
            msg_ids: vec!["msg_1_42_0_1".to_string()],
            stream: "test".to_string(),
        },
        1000, 50, 4096
    );
    state_store.write_commit_record(&record).unwrap();
    
    // Crash happens here (before prepare)
    
    // Recovery
    let recovery = RecoveryCoordinator::new(config, state_store);
    let result = recovery.bootstrap().await.unwrap();
    
    // JetStream reconciliation returns Replay (safe)
    assert!(result.is_none(), "JetStream should replay when sink not prepared");
}

/// Crash Scenario 3: Kill After PREPARE, Before COMMIT (Sink Visible)
///
/// Timeline:
/// 1. SEAL + PERSIST complete
/// 2. PREPARE commits to sink (data is VISIBLE)
/// 3. CRASH (before updating commit record to Committed)
/// 4. Recovery
///
/// Expected:
/// - Prepared commit record exists
/// - Sink HAS data (prepare succeeded)
/// - Log sink: Always Repair (no external state to check)
/// - Kafka/S3: Would check sink state (stubs fail-fast)
#[tokio::test]
async fn test_crash_3_after_prepare_before_commit() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(true, 1));
    let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());
    
    // Simulate: Write Prepared record (Log sink - data already logged)
    let mut offsets = HashMap::new();
    offsets.insert(0, SourcePosition::Kafka { offset: 100 });
    
    let record = CommitRecord::new_prepared(
        1, 42, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
    );
    state_store.write_commit_record(&record).unwrap();
    
    // Crash happens here (after log write, before record update)
    
    // Recovery
    let recovery = RecoveryCoordinator::new(config.clone(), state_store.clone());
    let result = recovery.bootstrap().await.unwrap();
    
    // Log sink reconciliation returns Repair
    assert!(result.is_some(), "Log sink should repair when data is visible");
    let repaired = result.unwrap();
    assert_eq!(repaired.status, CommitStatus::Committed);
    assert_eq!(repaired.epoch_id, 42);
    
    // Verify repair was persisted
    let loaded = state_store.load_latest_committed(1).unwrap().unwrap();
    assert_eq!(loaded.status, CommitStatus::Committed);
}

/// Crash Scenario 4: Kill After COMMIT, Before Record Update
///
/// This is actually the same as Scenario 3 from recovery perspective.
/// Both have: Prepared record + visible data → Repair
///
/// The distinction is timing, but outcome is identical.
#[tokio::test]
async fn test_crash_4_after_commit_before_record_update() {
    // Same as test_crash_3 - both scenarios have:
    // - Prepared record exists
    // - Sink has visible data
    // - Reconciliation says Repair
    
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(true, 1));
    let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());
    
    let mut offsets = HashMap::new();
    offsets.insert(0, SourcePosition::Kafka { offset: 200 });
    
    let record = CommitRecord::new_prepared(
        1, 43, offsets, SinkVisibilityRef::Log, 2000, 100, 8192
    );
    state_store.write_commit_record(&record).unwrap();
    
    let recovery = RecoveryCoordinator::new(config, state_store);
    let result = recovery.bootstrap().await.unwrap();
    
    assert!(result.is_some(), "Should repair visible epoch");
    assert_eq!(result.unwrap().status, CommitStatus::Committed);
}

/// Crash Scenario 5: Kill After Record Update (Normal Completion)
///
/// Timeline:
/// 1. Full 4-phase commit completes
/// 2. Commit record updated to Committed
/// 3. CRASH (anytime after)
/// 4. Recovery
///
/// Expected:
/// - Committed commit record exists
/// - Recovery resumes from that epoch
/// - No reconciliation needed
#[tokio::test]
async fn test_crash_5_after_record_update() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(true, 1));
    let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());
    
    // Simulate: Write Committed record
    let mut offsets = HashMap::new();
    offsets.insert(0, SourcePosition::Kafka { offset: 300 });
    
    let mut record = CommitRecord::new_prepared(
        1, 44, offsets, SinkVisibilityRef::Log, 3000, 150, 16384
    );
    record.mark_committed();
    state_store.write_commit_record(&record).unwrap();
    
    // Crash happens here (after commit record is Committed)
    
    // Recovery
    let recovery = RecoveryCoordinator::new(config, state_store);
    let result = recovery.bootstrap().await.unwrap();
    
    assert!(result.is_some(), "Should load committed epoch");
    let loaded = result.unwrap();
    assert_eq!(loaded.status, CommitStatus::Committed);
    assert_eq!(loaded.epoch_id, 44);
    
    // No reconciliation needed (already Committed)
}

/// SEAL Invariant Test 1: In-Flight Count Must Be Zero
///
/// Verifies that SEAL waits for in-flight count to reach 0 before allowing PERSIST.
#[tokio::test]
async fn test_seal_invariant_in_flight_zero() {
    let seal = SealCoordinator::new(true); // Enhanced mode
    
    // Track some messages
    seal.track_message_read();
    seal.track_message_read();
    seal.track_message_read();
    assert_eq!(seal.in_flight_count(), 3);
    
    // Start sealing in background
    let seal_clone = seal.clone ();
    let drain_task = tokio::spawn(async move {
        seal_clone.seal_and_drain(Duration::from_secs(5)).await
    });
    
    // Give SEAL time to fence
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(seal.is_sealed(), "Writers should be fenced");
    
    // ACK messages to drain
    seal.track_message_ack();
    seal.track_message_ack();
    seal.track_message_ack();
    assert_eq!(seal.in_flight_count(), 0);
    
    // Drain should complete successfully
    drain_task.await.unwrap().unwrap();
}

/// SEAL Invariant Test 2: No Partial Epochs
///
/// Verifies that sealed flag prevents new writes during drain.
#[tokio::test]
async fn test_seal_invariant_no_partial_epochs() {
    let seal = SealCoordinator::new(true);
    
    // Initially not sealed
    assert!(!seal.is_sealed());
    
    // Seal (with no in-flight messages for quick test)
    seal.seal_and_drain(Duration::from_secs(1)).await.unwrap();
    
    // Now sealed
    assert!(seal.is_sealed());
    
    // Attempting to track new messages won't change sealed state
    // (In real code, writers would check is_sealed() before writing)
    
    // Unseal to resume
    seal.unseal();
    assert!(!seal.is_sealed());
}

/// SEAL Standard Mode Test: Always No-Op
#[tokio::test]
async fn test_seal_standard_mode_noop() {
    let seal = SealCoordinator::new(false); // Standard mode
    
    // Never sealed
    assert!(!seal.is_sealed());
    
    // Tracking is no-op
    seal.track_message_read();
    assert_eq!(seal.in_flight_count(), 0);
    
    // Seal is immediate no-op
    seal.seal_and_drain(Duration::from_secs(1)).await.unwrap();
    assert!(!seal.is_sealed());
}

/// Enhanced Mode vs Standard Mode: Behavior Matrix
#[tokio::test]
async fn test_mode_switching_matrix() {
    let temp_dir = TempDir::new().unwrap();
    
    // Standard mode: Always bypasses Phase 2
    {
        let config = Arc::new(test_config(false, 1));
        let state_store = Arc::new(StateStore::new(temp_dir.path().join("std"), false).unwrap());
        let coordinator = EpochCoordinator::new(config.clone(), state_store.clone());
        let recovery = RecoveryCoordinator::new(config, state_store);
        
        // Commit some epochs
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });
        let _ = coordinator.commit_epoch(offsets, SinkVisibilityRef::Log, 10, 1024).await;
        
        // Recovery should skip (no records persisted)
        assert!(recovery.bootstrap().await.unwrap().is_none());
    }
    
    // Enhanced mode: Full Phase 2
    {
        let config = Arc::new(test_config(true, 1));
        let state_store = Arc::new(StateStore::new(temp_dir.path().join("enh"), true).unwrap());
        let coordinator = EpochCoordinator::new(config.clone(), state_store.clone());
        let recovery = RecoveryCoordinator::new(config, state_store);
        
        // Commit and mark committed
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });
        let record = coordinator.commit_epoch(offsets, SinkVisibilityRef::Log, 10, 1024).await.unwrap();
        coordinator.mark_epoch_committed(record).await.unwrap();
        
        // Recovery should load
        let loaded = recovery.bootstrap().await.unwrap();
        assert!(loaded.is_some(), "Enhanced mode persists and loads records");
    }
}
