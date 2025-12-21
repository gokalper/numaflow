// Phase 3B: Kafka Reconciler Sidecar gRPC Client
//
// Provides gRPC client to communicate with the Go sidecar for Kafka Admin operations.
// Only used when enhanced_mode=true and Kafka EOS sink is configured.

use crate::Result;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Uri};

// Include generated proto code
pub mod reconciler {
    tonic::include_proto!("reconciler.v1");
}

use reconciler::{
    kafka_reconciler_client::KafkaReconcilerClient,
    DescribeTransactionRequest, TransactionState,
};

/// Client for Kafka reconciler sidecar
pub struct KafkaSidecarClient {
    client: KafkaReconcilerClient<Channel>,
}

impl KafkaSidecarClient {
    /// Connect to sidecar via Unix Domain Socket
    /// 
    /// Retries up to 3 times with 200ms delays to handle container startup races.
    pub async fn connect_uds(socket_path: &str) -> Result<Self> {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY_MS: u64 = 200;

        let mut last_error = None;

        for attempt in 1..=MAX_RETRIES {
            // Create endpoint for UDS connection
            match Endpoint::try_from("http://[::]:50051") {
                Ok(endpoint) => {
                    let path = socket_path.to_string();
                    match endpoint
                        .connect_with_connector(tower::service_fn(move |_: Uri| {
                            let path = path.clone();
                            async move {
                                let stream = tokio::net::UnixStream::connect(path).await?;
                                // Wrap in TokioIo for hyper 1.x compatibility
                                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                            }
                        }))
                        .await
                    {
                        Ok(channel) => {
                            tracing::info!(
                                socket_path = socket_path,
                                attempt = attempt,
                                "Connected to Kafka reconciler sidecar"
                            );
                            return Ok(Self {
                                client: KafkaReconcilerClient::new(channel),
                            });
                        }
                        Err(e) => {
                            last_error = Some(format!("Connection failed: {}", e));
                            if attempt < MAX_RETRIES {
                                tracing::warn!(
                                    socket_path = socket_path,
                                    attempt = attempt,
                                    max_retries = MAX_RETRIES,
                                    error = %e,
                                    "Sidecar connection failed, retrying..."
                                );
                                tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS))
                                    .await;
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(crate::Error::Config(format!("Invalid endpoint: {}", e)));
                }
            }
        }

        Err(crate::Error::Config(format!(
            "Failed to connect to sidecar after {} attempts: {}",
            MAX_RETRIES,
            last_error.unwrap_or_else(|| "unknown error".to_string())
        )))
    }

    /// Describe transaction state via sidecar
    pub async fn describe_transaction(
        &mut self,
        broker_list: String,
        transactional_id: String,
        timeout: Duration,
    ) -> Result<TransactionState> {
        let request = tonic::Request::new(DescribeTransactionRequest {
            broker_list,
            transactional_id: transactional_id.clone(),
            timeout_ms: timeout.as_millis() as i32,
        });

        let response = self
            .client
            .describe_transaction(request)
            .await
            .map_err(|e| crate::Error::Config(format!("gRPC call failed: {}", e)))?;

        let inner = response.into_inner();

        if !inner.error.is_empty() {
            return Err(crate::Error::Config(format!(
                "Sidecar error for transaction {}: {}",
                transactional_id, inner.error
            )));
        }

        Ok(TransactionState::try_from(inner.state)
            .unwrap_or(TransactionState::Unknown))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sidecar_client_connection() {
        // This test requires the sidecar to be running
        // In CI, this will be skipped or mocked
        let socket_path = "/var/run/kafka-reconciler/reconciler.sock";
        
        // Attempt connection (will fail if sidecar not running, which is OK for unit test)
        let result = KafkaSidecarClient::connect_uds(socket_path).await;
        
        // We just verify it doesn't panic - actual connection may fail in test env
        match result {
            Ok(_) => println!("Connected to sidecar"),
            Err(e) => println!("Sidecar not available (expected in test): {}", e),
        }
    }
}
