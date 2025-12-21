// Phase 2: Recovery Bootstrap Module
//
// Handles recovery from crashes by loading the latest commit record
// and reconciling it with sink state.
//
// CRITICAL: All recovery logic is gated on enhanced_mode.
// Standard mode uses default in-memory recovery.

use std::sync::Arc;

use crate::commit_record::{CommitRecord, CommitStatus};
use crate::config::monovertex::MonovertexConfig;
use crate::reconcile::{reconcile_commit_record, ReconcileAction, ReconcileConfig};
use crate::state::StateStore;
use crate::Result;

/// Recovery coordinator
pub struct RecoveryCoordinator {
    config: Arc<MonovertexConfig>,
    state_store: Arc<StateStore>,
    reconcile_config: ReconcileConfig,
}

impl RecoveryCoordinator {
    pub fn new(
        config: Arc<MonovertexConfig>,
        state_store: Arc<StateStore>,
    ) -> Self {
        Self {
            config,
            state_store,
            reconcile_config: ReconcileConfig::default(),
        }
    }

    /// Bootstrap recovery
    ///
    /// If enhanced_mode is false, returns None (standard recovery path).
    /// If enhanced_mode is true, loads latest commit record and reconciles.
    pub async fn bootstrap(&self) -> Result<Option<CommitRecord>> {
        // Enhanced mode gate: Skip if not enhanced
        if !self.config.enhanced_mode {
            tracing::info!("Standard mode: Skipping Phase 2 recovery");
            return Ok(None);
        }

        tracing::info!(
            generation_id = self.config.generation_id,
            "Enhanced mode: Starting Phase 2 recovery"
        );

        // Load latest committed record for this generation
        let record_opt = self.state_store.load_latest_committed(self.config.generation_id)?;

        let Some(record) = record_opt else {
            tracing::info!("No commit record found, starting fresh");
            return Ok(None);
        };

        // Validate checksum
        record.validate_checksum()
            .map_err(|e| crate::Error::Config(format!("Recovery failed: {}", e)))?;

        tracing::info!(
            epoch_id = record.epoch_id,
            status = ?record.status,
            "Loaded commit record"
        );

        // Handle based on status
        match record.status {
            CommitStatus::Committed => {
                // Normal case: last epoch was fully committed
                tracing::info!(epoch_id = record.epoch_id, "Recovery from Committed epoch");
                Ok(Some(record))
            }
            CommitStatus::Prepared => {
                // Crash-after-visible case: need to reconcile
                tracing::warn!(
                    epoch_id = record.epoch_id,
                    "Found Prepared epoch, reconciling sink state"
                );
                
                self.reconcile_prepared_epoch(record).await
            }
        }
    }

    /// Reconcile a Prepared epoch
    ///
    /// Checks sink state and either repairs the record or marks for replay.
    async fn reconcile_prepared_epoch(
        &self,
        mut record: CommitRecord,
    ) -> Result<Option<CommitRecord>> {
        let action = reconcile_commit_record(&record, &self.reconcile_config).await?;

        match action {
            ReconcileAction::Repair => {
                tracing::info!(
                    epoch_id = record.epoch_id,
                    "Sink confirms visibility, repairing commit record"
                );

                // Update record to Committed
                record.mark_committed();
                self.state_store.write_commit_record(&record)?;

                Ok(Some(record))
            }
            ReconcileAction::Replay => {
                tracing::warn!(
                    epoch_id = record.epoch_id,
                    "Sink reports not visible, will replay epoch"
                );

                // Don't return the Prepared record, signal replay
                // Caller should replay from previous committed epoch
                // For now, return None to indicate "start fresh"
                Ok(None)
            }
        }
    }

    /// Check if this is a new generation
    ///
    /// Returns true if:
    /// - No commit records exist for this generation
    /// - Generation ID changed from last run
    pub async fn is_new_generation(&self) -> Result<bool> {
        if !self.config.enhanced_mode {
            return Ok(false); // Standard mode: no generation tracking
        }

        let latest = self.state_store.load_latest_committed(self.config.generation_id)?;
        Ok(latest.is_none())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_record::{SinkVisibilityRef, SourcePosition};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn test_config(enhanced_mode: bool) -> MonovertexConfig {
        MonovertexConfig {
            enhanced_mode,
            generation_id: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_recovery_standard_mode() {
        let temp_dir = TempDir::new().unwrap();
        let config = Arc::new(test_config(false));
        let state_store = Arc::new(StateStore::new(temp_dir.path(), false).unwrap());

        let recovery = RecoveryCoordinator::new(config, state_store);

        // Standard mode: should skip recovery
        let result = recovery.bootstrap().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_recovery_no_records() {
        let temp_dir = TempDir::new().unwrap();
        let config = Arc::new(test_config(true));
        let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());

        let recovery = RecoveryCoordinator::new(config, state_store);

        // No records: should return None (start fresh)
        let result = recovery.bootstrap().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_recovery_committed_epoch() {
        let temp_dir = TempDir::new().unwrap();
        let config = Arc::new(test_config(true));
        let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());

        // Write a committed record
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });

        let mut record = CommitRecord::new_prepared(
            1, 42, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
        );
        record.mark_committed();
        state_store.write_commit_record(&record).unwrap();

        let recovery = RecoveryCoordinator::new(config, state_store);

        // Should load committed record
        let result = recovery.bootstrap().await.unwrap().unwrap();
        assert_eq!(result.epoch_id, 42);
        assert_eq!(result.status, CommitStatus::Committed);
    }

    #[tokio::test]
    async fn test_recovery_prepared_epoch_log_sink() {
        let temp_dir = TempDir::new().unwrap();
        let config = Arc::new(test_config(true));
        let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());

        // Write a Prepared record (Log sink)
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });

        let record = CommitRecord::new_prepared(
            1, 42, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
        );
        state_store.write_commit_record(&record).unwrap();

        let recovery = RecoveryCoordinator::new(config.clone(), state_store.clone());

        // Should repair (Log sink has no external state)
        let result = recovery.bootstrap().await.unwrap();
        assert!(result.is_some(), "Expected repaired record");
        let repaired = result.unwrap();
        assert_eq!(repaired.epoch_id, 42);
        assert_eq!(repaired.status, CommitStatus::Committed);

        // Should be persisted as Committed
        let loaded = state_store.load_latest_committed(1).unwrap().unwrap();
        assert_eq!(loaded.status, CommitStatus::Committed);
    }

    #[tokio::test]
    async fn test_is_new_generation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Arc::new(test_config(true));
        let state_store = Arc::new(StateStore::new(temp_dir.path(), true).unwrap());

        let recovery = RecoveryCoordinator::new(config.clone(), state_store.clone());

        // No records: new generation
        assert!(recovery.is_new_generation().await.unwrap());

        // Write a record
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });
        let mut record = CommitRecord::new_prepared(
            1, 1, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
        );
        record.mark_committed();
        state_store.write_commit_record(&record).unwrap();

        // Has records: not new generation
        assert!(!recovery.is_new_generation().await.unwrap());
    }
}
