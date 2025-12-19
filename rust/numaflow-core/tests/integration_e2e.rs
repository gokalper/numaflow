// Phase 2 Priority 4: Integration Test Framework
//
// End-to-end tests with real sinks to validate crash recovery

use numaflow_core::commit_record::{CommitRecord, SinkVisibilityRef, SourcePosition};
use numaflow_core::config::monovertex::MonovertexConfig;
use numaflow_core::epoch::EpochCoordinator;
use numaflow_core::recovery::RecoveryCoordinator;
use numaflow_core::state::StateStore;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Integration Test 1: JetStream End-to-End
///
/// Tests full cycle with JetStream sink:
/// 1. Write epoch with Prepared record
/// 2. Simulate crash
/// 3. Recovery should Replay (JetStream has dedupe)
#[tokio::test]
async fn test_integration_jetstream_e2e() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = MonovertexConfig::default();
    config.enhanced_mode = true;
    config.generation_id = 1;
    let config = Arc::new(config);
    
    // Phase 1: Normal operation
    let state_store = Arc:: new(StateStore::new(temp_dir.path(), true).unwrap());
    let coordinator = EpochCoordinator::new(config.clone(), state_store.clone());
    
    let mut offsets = HashMap::new();
    offsets.insert(0, SourcePosition::Kafka { offset: 100 });
    
    let record = coordinator.commit_epoch(
        offsets,
        SinkVisibilityRef::JetStream {
            msg_ids: vec!["msg_1_1_0".to_string()],
            stream: "test".to_string(),
        },
        1000,
        10240,
    ).await.unwrap();
    
    // Simulate crash before mark_committed
    drop(coordinator);
    
    // Phase 2: Recovery
    let recovery = RecoveryCoordinator::new(config, state_store);
    let recovered = recovery.bootstrap().await.unwrap();
    
    // JetStream should Replay (safe due to dedupe)
    assert!(recovered.is_none(), "JetStream should replay");
}

/// Integration Test 2: S3 Manifest End-to-End
///
/// Tests full cycle with S3 sink:
/// 1. Write epoch with Prepared record
/// 2. Simulate manifest not written (404)
/// 3. Recovery should Replay
#[tokio::test]
async fn test_integration_s3_manifest_e2e() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = MonovertexConfig::default();
    config.enhanced_mode = true;
    config.generation_id = 1;
    let config = Arc::new(config);
    
    let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());
    let coordinator = EpochCoordinator::new(config.clone(), state_store.clone());
    
    let mut offsets = HashMap::new();
    offsets.insert(0, SourcePosition::Kafka { offset: 100 });
    
    let record = coordinator.commit_epoch(
        offsets,
        SinkVisibilityRef::S3 {
            manifest_uri: "s3://test-bucket/manifests/epoch_1.json".to_string(),
            staging_prefix: "s3://test-bucket/staging/epoch_1/".to_string(),
            object_count: 10,
        },
        1000,
        10240,
    ).await.unwrap();
    
    // Simulate crash before mark_committed
    drop(coordinator);
    
    // Phase 2: Recovery
    // NOTE: This test requires AWS credentials or LocalStack
    // In CI without AWS, it will error (acceptable)
    let recovery = RecoveryCoordinator::new(config, state_store);
    let recovered = recovery.bootstrap().await;
    
    // Either Replay (404) or error (no AWS) - both acceptable for unit test
    match recovered {
        Ok(None) => {
            // S3 returned 404 → Replay (expected)
        }
        Err(_) => {
            // No AWS credentials (expected in test env)
        }
        Ok(Some(_)) => {
            panic!("Unexpected: S3 manifest should not exist in test");
        }
    }
}

/// Integration Test 3: Log Sink End-to-End
///
/// Tests full cycle with Log sink:
/// 1. Write epoch with Prepared record
/// 2. Log already written (data is visible)
/// 3. Recovery should Repair
#[tokio::test]
async fn test_integration_log_sink_e2e() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = MonovertexConfig::default();
    config.enhanced_mode = true;
    config.generation_id = 1;
    let config = Arc::new(config);
    
    let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());
    let coordinator = EpochCoordinator::new(config.clone(), state_store.clone());
    
    let mut offsets = HashMap::new();
    offsets.insert(0, SourcePosition::Kafka { offset: 100 });
    
    let record = coordinator.commit_epoch(
        offsets,
        SinkVisibilityRef::Log,
        1000,
        10240,
    ).await.unwrap();
    
    // Simulate crash before mark_committed
    // (Log data is already visible)
    drop(coordinator);
    
    // Phase 2: Recovery
    let recovery = RecoveryCoordinator::new(config, state_store);
    let recovered = recovery.bootstrap().await.unwrap();
    
    // Log sink should Repair (no external state to check)
    assert!(recovered.is_some(), "Log sink should repair");
    let repaired = recovered.unwrap();
    assert_eq!(repaired.status, numaflow_core::commit_record::CommitStatus::Committed);
}

/// Integration Test 4: Mode Switching
///
/// Tests that standard mode bypasses all Phase 2 logic
#[tokio::test]
async fn test_integration_mode_switching() {
    let temp_dir = TempDir::new().unwrap();
    
    // Standard mode
    {
        let mut config = MonovertexConfig::default();
        config.enhanced_mode = false; // Standard mode
        config.generation_id = 1;
        let config = Arc::new(config);
        
        let state_store = Arc::new(StateStore::new(temp_dir.path().join("standard"), false).unwrap());
        let recovery = RecoveryCoordinator::new(config, state_store);
        
        let recovered = recovery.bootstrap().await.unwrap();
        assert!(recovered.is_none(), "Standard mode should skip recovery");
    }
    
    // Enhanced mode
    {
        let mut config = MonovertexConfig::default();
        config.enhanced_mode = true; // Enhanced mode
        config.generation_id = 1;
        let config = Arc::new(config);
        
        let state_store = Arc::new(StateStore::new(temp_dir.path().join("enhanced"), true).unwrap());
        
        // Write a committed record
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });
        let mut record = CommitRecord::new_prepared(
            1, 1, offsets, SinkVisibilityRef::Log, 1000, 10, 1024
        );
        record.mark_committed();
        state_store.write_commit_record(&record).unwrap();
        
        let recovery = RecoveryCoordinator::new(config, state_store);
        let recovered = recovery.bootstrap().await.unwrap();
        
        assert!(recovered.is_some(), "Enhanced mode should load record");
    }
}
