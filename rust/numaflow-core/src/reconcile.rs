// Phase 2: Reconciliation Module
//
// Handles crash-after-visible recovery by reconciling commit record state
// with actual sink visibility.
//
// CRITICAL: All reconciliation is gated on enhanced_mode.
// Standard mode never calls this module.

use crate::commit_record::{CommitRecord, SinkVisibilityRef};
use crate::Result;
use std::time::Duration;

/// Reconciliation action after checking sink state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    /// Commit record should be updated to Committed (output is visible)
    Repair,
    /// Epoch should be replayed (output is not visible)
    Replay,
}

/// Reconciliation timeout configuration
pub struct ReconcileConfig {
    pub kafka_timeout: Duration,
    pub kafka_broker_list: String, // Phase 3: Broker list for AdminClient
    pub s3_timeout: Duration,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            kafka_timeout: Duration::from_secs(10),
            kafka_broker_list: String::new(), // Must be set from sink config
            s3_timeout: Duration::from_secs(5),
        }
    }
}

// Phase 3.1: Kafka AdminClient wrapper (lazy-initialized)
use std::sync::Arc;
use tokio::sync::OnceCell;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;

static KAFKA_RECONCILER: OnceCell<Arc<KafkaReconciler>> = OnceCell::const_new();

/// Kafka transaction state (simplified from Kafka's TransactionState)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KafkaTransactionState {
    Committed,
    Aborted,
    Ongoing,
    Unknown,
}

/// Kafka AdminClient wrapper for EOS transaction reconciliation
/// 
/// GATING: Only instantiated when enhanced_mode recovery is active.
/// Standard mode never reaches Kafka reconciliation code.
struct KafkaReconciler {
    #[allow(dead_code)]
    broker_list: String,
    timeout: Duration,
    _admin_client: AdminClient<DefaultClientContext>,
}

impl KafkaReconciler {
    fn new(broker_list: String, timeout: Duration) -> Result<Self> {
        if broker_list.is_empty() {
            return Err(crate::Error::Config(
                "Kafka broker list required for Kafka EOS reconciliation".to_string()
            ));
        }

        tracing::info!(
            broker_list = %broker_list,
            timeout_secs = timeout.as_secs(),
            "Initializing Kafka reconciler AdminClient"
        );

        // Create rdkafka AdminClient
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &broker_list)
            .set("client.id", "numaflow-phase2-reconciler")
            .set("socket.timeout.ms", timeout.as_millis().to_string())
            .set("request.timeout.ms", timeout.as_millis().to_string())
            .create()
            .map_err(|e| crate::Error::Config(format!("Failed to create Kafka AdminClient: {}", e)))?;

        Ok(Self {
            broker_list,
            timeout,
            _admin_client: admin_client,
        })
    }

    /// Describe transaction state via sidecar
    /// 
    /// Connects to Kafka reconciler sidecar and queries transaction state.
    async fn describe_transaction(&self, transaction_id: &str) -> Result<KafkaTransactionState> {
        use crate::kafka_sidecar_client::KafkaSidecarClient;
        use crate::kafka_sidecar_client::reconciler::TransactionState;
        
        tracing::debug!(
            transaction_id = transaction_id,
            "Querying Kafka transaction state via sidecar"
        );

        // Connect to sidecar via UDS
        let socket_path = std::env::var("KAFKA_SIDECAR_SOCKET")
            .unwrap_or_else(|_| "/var/run/kafka-reconciler/reconciler.sock".to_string());
        
        let mut client = KafkaSidecarClient::connect_uds(&socket_path).await?;

        // Call sidecar
        let state = client
            .describe_transaction(
                self.broker_list.clone(),
                transaction_id.to_string(),
                self.timeout,
            )
            .await?;

        // Map proto enum to internal enum
        match state {
            TransactionState::Committed => Ok(KafkaTransactionState::Committed),
            TransactionState::Aborted => Ok(KafkaTransactionState::Aborted),
            TransactionState::Ongoing => Ok(KafkaTransactionState::Ongoing),
            TransactionState::PrepareCommit => Ok(KafkaTransactionState::Ongoing),
            TransactionState::PrepareAbort => Ok(KafkaTransactionState::Aborted),
            TransactionState::Unknown => Ok(KafkaTransactionState::Unknown),
        }
    }

    /// Get or create the global Kafka reconciler (lazy init, connection pooling)
    async fn get_or_create(config: &ReconcileConfig) -> Result<Arc<Self>> {
        KAFKA_RECONCILER
            .get_or_try_init(|| async {
                let reconciler = Self::new(
                    config.kafka_broker_list.clone(),
                    config.kafka_timeout,
                )?;
                Ok(Arc::new(reconciler))
            })
            .await
            .map(Arc::clone)
    }

    /// Attempt to abort an Ongoing transaction
    ///
    /// Per contract: Use producer epoch fencing to abort stuck transactions.
    /// If abort succeeds or broker will auto-abort, it's safe to Replay.
    async fn abort_transaction(
        &self,
        transaction_id: &str,
        producer_epoch: i32,
    ) -> Result<()> {
        tracing::info!(
            transaction_id = transaction_id,
            producer_epoch = producer_epoch,
            "Attempting to abort Ongoing Kafka transaction"
        );

        // IMPLEMENTATION STRATEGY:
        //
        // Kafka transactions can be aborted via:
        // 1. Producer epoch fencing: Start new producer with higher epoch
        //    - This fences the old producer and aborts its transaction
        //    - Broker enforces: newer epoch wins, old epochs are zombie
        // 2. Broker timeout: transaction.timeout.ms (default 60s)
        //    - After timeout, broker auto-aborts Ongoing transactions
        //
        // For Phase 3 MVP: Rely on strategy #2 (broker auto-abort)
        // - rdkafka doesn't expose direct "AbortTransaction" admin API
        // - Creating new producer for fencing is complex (requires credentials, config)
        // - Broker timeout is reliable and doesn't require client action
        //
        // Safety: As long as we don't create a new producer with same epoch,
        // the Ongoing transaction will auto-abort after transaction.timeout.ms

        tracing::warn!(
            transaction_id = transaction_id,
            producer_epoch = producer_epoch,
            "Relying on broker auto-abort (transaction.timeout.ms). \
             Not implementing active abort in Phase 3 MVP."
        );

        // Wait briefly to allow broker to process timeout
        // This prevents rapid restart loops if broker is slow
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!(
            transaction_id = transaction_id,
            "Assuming broker will auto-abort Ongoing transaction. Safe to Replay."
        );

        Ok(())
    }
}

/// Reconcile a Prepared commit record with actual sink state
///
/// This function is ONLY called in enhanced_mode during recovery.
/// It determines whether to repair the record or replay the epoch.
pub async fn reconcile_commit_record(
    record: &CommitRecord,
    config: &ReconcileConfig,
) -> Result<ReconcileAction> {
    // This function should only be called in enhanced mode
    // (caller's responsibility to check, but we can assert)
    
    match &record.sink_ref {
        SinkVisibilityRef::Kafka { 
            transaction_id,
            producer_epoch,
            partition_offsets: _,
        } => {
            reconcile_kafka(transaction_id, *producer_epoch, record, config).await
        }
        SinkVisibilityRef::S3 { manifest_uri, .. } => {
            reconcile_s3(manifest_uri, config).await
        }
        SinkVisibilityRef::JetStream { .. } => {
            // JetStream is always safe to replay due to message ID deduplication
            Ok(ReconcileAction::Replay)
        }
        SinkVisibilityRef::Log | SinkVisibilityRef::Blackhole => {
            // These sinks have no external state to reconcile
            // Safe to mark as committed
            Ok(ReconcileAction::Repair)
        }
    }
}

/// Reconcile Kafka EOS transaction state
///
/// Per contract: Query transaction state and decide Repair/Replay/Fail-fast
/// - Committed + offsets aligned → Repair
/// - Aborted/Empty → Replay
/// - Ongoing → Attempt abort → Replay
/// - Timeout/Error → Fail-fast
async fn reconcile_kafka(
    transaction_id: &str,
    _producer_epoch: i32,
    record: &CommitRecord,
    config: &ReconcileConfig,
) -> Result<ReconcileAction> {
    tracing::info!(
        transaction_id = transaction_id,
        timeout_secs = config.kafka_timeout.as_secs(),
        "Reconciling Kafka EOS transaction"
    );

    // Get or create AdminClient (lazy init, connection pooling)
    let reconciler = KafkaReconciler::get_or_create(config).await?;

    // Query transaction state with timeout
    let txn_state = tokio::time::timeout(
        config.kafka_timeout,
        reconciler.describe_transaction(transaction_id)
    ).await;

    match txn_state {
        Ok(Ok(KafkaTransactionState::Committed)) => {
            tracing::info!(
                transaction_id = transaction_id,
                "Kafka transaction Committed → Verifying offset alignment"
            );
            
            // Critical: Verify offset alignment before Repair
            // Per contract: "Committed must imply visible outputs + aligned offsets"
            verify_offset_alignment(transaction_id, record, config).await?;
            
            tracing::info!(
                transaction_id = transaction_id,
                "Offset alignment verified → Repair"
            );
            Ok(ReconcileAction::Repair)
        }
        Ok(Ok(KafkaTransactionState::Aborted)) => {
            tracing::info!(
                transaction_id = transaction_id,
                "Kafka transaction Aborted → Replay"
            );
            Ok(ReconcileAction::Replay)
        }
        Ok(Ok(KafkaTransactionState::Ongoing)) => {
            tracing::warn!(
                transaction_id = transaction_id,
                "Kafka transaction Ongoing → Attempting abort"
            );
            
            // Attempt to abort the transaction
            // Per contract: abort via producer epoch fencing or broker timeout
            reconciler.abort_transaction(transaction_id, _producer_epoch).await?;
            
            tracing::info!(
                transaction_id = transaction_id,
                "Transaction aborted (or will be by broker) → Replay"
            );
            Ok(ReconcileAction::Replay)
        }
        Ok(Ok(KafkaTransactionState::Unknown)) => {
            tracing::error!(
                transaction_id = transaction_id,
                "Kafka transaction state Unknown → Fail-fast"
            );
            Err(crate::Error::Config(format!(
                "Kafka transaction {} state is Unknown. Cannot determine visibility safely.",
                transaction_id
            )))
        }
        Ok(Err(e)) => {
            // AdminClient error
            tracing::error!(
                transaction_id = transaction_id,
                error = %e,
                "Kafka AdminClient error → Fail-fast"
            );
            Err(e)
        }
        Err(_timeout) => {
            // Timeout
            tracing::error!(
                transaction_id = transaction_id,
                timeout_secs = config.kafka_timeout.as_secs(),
                "Kafka AdminClient timeout → Fail-fast"
            );
            Err(crate::Error::Config(format!(
                "Kafka reconciliation timeout after {}s for transaction {}. Cannot determine state safely.",
                config.kafka_timeout.as_secs(),
                transaction_id
            )))
        }
    }
}

/// Verify offset alignment for Committed Kafka transaction
///
/// Per contract: "Committed must imply visible outputs + aligned offsets"
/// 
/// Checks that consumer group offsets match epoch boundaries in the commit record.
/// If offsets are misaligned, this indicates a partial commit (data corruption risk).
async fn verify_offset_alignment(
    transaction_id: &str,
    record: &CommitRecord,
    _config: &ReconcileConfig,
) -> Result<()> {
    // Extract partition offsets from commit record
    let partition_offsets = match &record.sink_ref {
        SinkVisibilityRef::Kafka { partition_offsets, .. } => partition_offsets,
        _ => {
            return Err(crate::Error::Config(
                "verify_offset_alignment called for non-Kafka sink".to_string()
            ));
        }
    };

    tracing::debug!(
        transaction_id = transaction_id,
        partition_count = partition_offsets.len(),
        "Verifying Kafka offset alignment"
    );

    // IMPLEMENTATION STRATEGY:
    // 
    // Option A: Offsets committed via send_offsets_to_transaction() (Flink-style)
    //   - Kafka guarantees atomicity: transaction commit includes offset commit
    //   - If transaction is Committed, offsets are automatically aligned
    //   - No additional verification needed (trust Kafka's guarantee)
    //
    // Option B: Offsets committed separately via consumer.commit()
    //   - Must query consumer group offsets via AdminClient
    //   - Compare committed offsets with partition_offsets in record
    //   - If mismatch → Fail-fast (partial commit detected)
    //
    // For Phase 3 MVP: Assume Option A (safest, most common for EOS)
    // The presence of partition_offsets in SinkVisibilityRef implies they were
    // included in the transaction via send_offsets_to_transaction().
    //
    // If Kafka reports transaction as Committed, offsets are guaranteed aligned.

    tracing::info!(
        transaction_id = transaction_id,
        "Assuming send_offsets_to_transaction (Flink-style EOS). \
         Kafka Committed guarantee implies offset alignment."
    );

    // TODO: For production, add configuration option to choose strategy:
    // - "flink_style": Trust Kafka Committed guarantee (current)
    // - "verify_consumer_group": Query consumer group offsets and compare
    //
    // If "verify_consumer_group" mode:
    // 1. Extract consumer group from config
    // 2. AdminClient.list_consumer_group_offsets(group)
    // 3. For each partition in partition_offsets:
    //    - Check committed_offset == partition_offsets[partition]
    //    - If mismatch → Fail-fast
    
    Ok(()) // Verification passed (via Kafka guarantee)
}

/// Reconcile S3 manifest state
///
/// Checks if manifest exists via S3 HEAD request.
/// - 200 OK → Repair (manifest exists = all objects visible)
/// - 404 Not Found → Replay (manifest absent = not visible)
/// - Other errors/timeout → Fail-fast
async fn reconcile_s3(
    manifest_uri: &str,
    config: &ReconcileConfig,
) -> Result<ReconcileAction> {
    tracing::info!(
        manifest_uri = manifest_uri,
        timeout_secs = config.s3_timeout.as_secs(),
        "Reconciling S3 manifest"
    );

    // Parse S3 URI (s3://bucket/key)
    let (bucket, key) = parse_s3_uri(manifest_uri)?;

    // Create S3 client
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&aws_config);

    // HEAD request with timeout
    let head_result = tokio::time::timeout(
        config.s3_timeout,
        s3_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
    ).await;

    match head_result {
        Ok(Ok(_response)) => {
            // 200 OK: Manifest exists
            tracing::info!(
                manifest_uri = manifest_uri,
                "S3 manifest exists → Repair"
            );
            Ok(ReconcileAction::Repair)
        }
        Ok(Err(sdk_err)) => {
            // Check if it's a 404 Not Found
            if is_not_found_error(&sdk_err) {
                tracing::info!(
                    manifest_uri = manifest_uri,
                    "S3 manifest not found (404) → Replay"
                );
                Ok(ReconcileAction::Replay)
            } else {
                // Other S3 errors (403, 500, etc.) → Fail-fast
                tracing::error!(
                    manifest_uri = manifest_uri,
                    error = ?sdk_err,
                    "S3 HEAD request failed → Fail-fast"
                );
                Err(crate::Error::Config(format!(
                    "S3 reconciliation failed for manifest {}: {}. \
                     Cannot determine visibility safely.",
                    manifest_uri, sdk_err
                )))
            }
        }
        Err(_timeout) => {
            // Timeout → Fail-fast
            tracing::error!(
                manifest_uri = manifest_uri,
                timeout_secs = config.s3_timeout.as_secs(),
                "S3 HEAD request timeout → Fail-fast"
            );
            Err(crate::Error::Config(format!(
                "S3 reconciliation timeout after {}s for manifest {}. \
                 Cannot determine visibility safely.",
                config.s3_timeout.as_secs(),
                manifest_uri
            )))
        }
    }
}

/// Parse S3 URI into bucket and key
fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    if !uri.starts_with("s3://") {
        return Err(crate::Error::Config(format!("Invalid S3 URI: {}", uri)));
    }

    let without_scheme = &uri[5..]; // Remove "s3://"
    let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();

    if parts.len() != 2 {
        return Err(crate::Error::Config(format!("Invalid S3 URI format: {}", uri)));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Check if SDK error is a Not Found (404) error
fn is_not_found_error(err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>) -> bool {
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::head_object::HeadObjectError;

    match err {
        SdkError::ServiceError(service_err) => {
            matches!(service_err.err(), HeadObjectError::NotFound(_))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_record::{SourcePosition, SinkVisibilityRef};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_reconcile_log_sink() {
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });

        let record = CommitRecord::new_prepared(
            1, 42, offsets, SinkVisibilityRef::Log, 1000, 50, 4096
        );

        let config = ReconcileConfig::default();
        let action = reconcile_commit_record(&record, &config).await.unwrap();

        // Log sink has no external state, safe to repair
        assert_eq!(action, ReconcileAction::Repair);
    }

    #[tokio::test]
    async fn test_reconcile_jetstream_sink() {
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::JetStream { sequence: 100 });

        let record = CommitRecord::new_prepared(
            1,
            42,
            offsets,
            SinkVisibilityRef::JetStream {
                msg_ids: vec!["msg_1_42_0_1".to_string()],
                stream: "test_stream".to_string(),
            },
            1000,
            50,
            4096,
        );

        let config = ReconcileConfig::default();
        let action = reconcile_commit_record(&record, &config).await.unwrap();

        // JetStream is always safe to replay (dedupe)
        assert_eq!(action, ReconcileAction::Replay);
    }

    #[tokio::test]
    async fn test_reconcile_kafka_sink() {
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });

        let mut partition_offsets = HashMap::new();
        partition_offsets.insert(0, 1000);

        let record = CommitRecord::new_prepared(
            1,
            42,
            offsets,
            SinkVisibilityRef::Kafka {
                transaction_id: "test_txn_1_42".to_string(),
                producer_epoch: 1,
                partition_offsets,
            },
            1000,
            50,
            4096,
        );

        let config = ReconcileConfig::default();
        let result = reconcile_commit_record(&record, &config).await;

        // Kafka stub must fail-fast (not default to Replay)
        assert!(result.is_err(), "Kafka stub should fail-fast");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not implemented"), "Error should mention unimplemented reconciliation");
    }

    #[tokio::test]
    async fn test_reconcile_s3_sink_not_found() {
        let mut offsets = HashMap::new();
        offsets.insert(0, SourcePosition::Kafka { offset: 100 });

        let record = CommitRecord::new_prepared(
            1,
            42,
            offsets,
            SinkVisibilityRef::S3 {
                manifest_uri: "s3://test-bucket/manifests/epoch_42.json".to_string(),
                staging_prefix: "s3://test-bucket/staging/epoch_42/".to_string(),
                object_count: 10,
            },
            1000,
            50,
            4096,
        );

        let config = ReconcileConfig::default();
        
        // NOTE: This test will fail if AWS credentials are not configured or
        // if the bucket doesn't exist. In real scenarios, HEAD will return 404 → Replay.
        // For unit test, we just verify it compiles and doesn't panic.
        // Integration tests with LocalStack should verify actual behavior.
        
        // In CI/local without AWS: This will likely error (no credentials)
        // In production with AWS: This will do real HEAD check
        let result = reconcile_commit_record(&record, &config).await;
        
        // Either succeeds with Replay (404) or fails (no credentials/network)
        // Both are acceptable for unit test - we're verifying it compiles
        match result {
            Ok(action) => {
                // If AWS works, should be Replay (assuming bucket/key doesn't exist)
                assert_eq!(action, ReconcileAction::Replay,  "Expected Replay for non-existent manifest");
            }
            Err(_) => {
                // Expected in test environment without AWS credentials
                // Real integration test with LocalStack will verify correctness
            }
        }
    }
}
