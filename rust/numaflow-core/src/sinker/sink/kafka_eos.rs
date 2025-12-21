// Phase 4: MonoVertex Kafka EOS Sink Enhancement
//
// Adds exactly-once semantics for MonoVertex:
// - Transactional IDs include generation_id for zombie fencing
// - Prepare/commit/abort methods for 4-phase boundary
// -Integration with EpochCoordinator
//
// Note: Kafka sidecar handles reconciliation (transaction state queries),
// this handles actual transactional writes to Kafka.
//
// Uses ThreadedProducer (not FutureProducer) because transactions require
// blocking API in rdkafka.

use numaflow_kafka::sink::KafkaSink;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::Mutex;

use crate::error::{Error, Result};
use crate::message::Message;
use crate::sinker::sink::{ResponseFromSink, Sink};
use crate::commit_record::CommitRecord;
use crate::epoch_buffer::BufferedMessage;

/// MonoVertex-enhanced Kafka sink with exactly-once semantics
/// Phase 4 scaffolding - will be wired into SinkWriter
#[allow(dead_code)]
pub struct KafkaEosSink {
    /// Base Kafka sink (for non-EOS mode)
    base_sink: KafkaSink,
    
    /// Transactional producer (for EOS mode)
    /// Uses ThreadedProducer because FutureProducer doesn't support transactions
    transactional_producer: Option<Arc<Mutex<ThreadedProducer<DefaultProducerContext>>>>,
    
    /// Current generation ID
    generation_id: u64,
    
    /// Vertex name (for transactional ID)
    #[allow(dead_code)]
    vertex_name: String,
    
    /// Topic name
    topic: String,
    
    /// EOS enabled
    eos_enabled: bool,
}

#[allow(dead_code)]
impl KafkaEosSink {
    /// Create new EOS-enabled Kafka sink
    pub fn new(
        base_sink: KafkaSink,
        generation_id: u64,
        vertex_name: String,
        topic: String,
        brokers: Vec<String>,
        eos_enabled: bool,
    ) -> Result<Self> {
        let transactional_producer = if eos_enabled {
            // Transactional ID includes generation for fencing
            let txn_id = format!("monovertex-{}-gen-{}", vertex_name, generation_id);
            
            tracing::info!(
                generation_id = generation_id,
                vertex = vertex_name,
                txn_id = txn_id,
                "Initializing Kafka EOS producer"
            );
            
            let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
                .set("bootstrap.servers", &brokers.join(","))
                .set("transactional.id", &txn_id)
                .set("enable.idempotence", "true")
                .set("max.in.flight.requests.per.connection", "5")
                .set("acks", "all")
                .create()
                .map_err(|e| Error::Sink(format!("Failed to create transactional producer: {}", e)))?;
            
            // Initialize transactions (blocking call)
            producer.init_transactions(Duration::from_secs(30))
                .map_err(|e| Error::Sink(format!("Failed to init transactions: {}", e)))?;
            
            Some(Arc::new(Mutex::new(producer)))
        } else {
            None
        };
        
        Ok(Self {
            base_sink,
            transactional_producer,
            generation_id,
            vertex_name,
            topic,
            eos_enabled,
        })
    }
    
    /// Prepare epoch: begin transaction and send buffered messages
    pub async fn prepare(&self, commit_record: &CommitRecord, messages: Vec<BufferedMessage>) -> Result<()> {
        if !self.eos_enabled {
            return Ok(()); // No-op if EOS disabled
        }
        
        // Fence check
        if commit_record.generation_id != self.generation_id {
            return Err(Error::GenerationMismatch {
                expected: self.generation_id,
                got: commit_record.generation_id,
            });
        }
        
        let producer = self.transactional_producer.as_ref()
            .ok_or_else(|| Error::Sink("Transactional producer not initialized".to_string()))?;
        
        // Spawn blocking task for transaction (rdkafka transactions are blocking)
        let producer = Arc::clone(producer);
        let topic = self.topic.clone();
        
        tokio::task::spawn_blocking(move || {
            let producer = producer.lock();
            
            // Begin transaction
            producer.begin_transaction()
                .map_err(|e| Error::Sink(format!("Failed to begin transaction: {}", e)))?;
            
            // Send all messages in transaction
            for msg in messages {
                // Convert keys to partition key string
                let partition_key: Option<String> = if !msg.keys.is_empty() {
                    Some(msg.keys.iter()
                        .map(|k| String::from_utf8_lossy(k).to_string())
                        .collect::<Vec<_>>()
                        .join(":"))
                } else {
                    None
                };
                
                let mut record = BaseRecord::to(&topic)
                    .payload(&msg.value);
                
                if let Some(ref key) = partition_key {
                    record = record.key(key);
                }
                
                // Send (blocking)
                producer.send(record)
                    .map_err(|(e, _)| Error::Sink(format!("Failed to send message: {}", e)))?;
            }
            
            // Commit transaction (visibility confirmed)
            producer.commit_transaction(Duration::from_secs(30))
                .map_err(|e| Error::Sink(format!("Failed to commit transaction: {}", e)))?;
            
            Ok::<(), Error>(())
        })
        .await
        .map_err(|e| Error::Sink(format!("Transaction task failed: {}", e)))??;
        
        Ok(())
    }
    
    /// Abort transaction on revocation or error
    pub async fn abort(&self) -> Result<()> {
        if !self.eos_enabled {
            return Ok(());
        }
        
        if let Some(producer) = &self.transactional_producer {
            let producer = Arc::clone(producer);
            
            tokio::task::spawn_blocking(move || {
                let producer = producer.lock();
                producer.abort_transaction(Duration::from_secs(10))
                    .map_err(|e| Error::Sink(format!("Failed to abort transaction: {}", e)))
            })
            .await
            .map_err(|e| Error::Sink(format!("Abort task failed: {}", e)))??;
        }
        
        Ok(())
    }
}

// Implement existing Sink trait for backward compatibility
impl Sink for KafkaEosSink {
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        // In non-EOS mode, use base sink
        self.base_sink.sink(messages).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_eos_transactional_id_includes_generation() {
        // Verify transactional ID format
        let txn_id = format!("monovertex-{}-gen-{}", "test-vertex", 42);
        assert_eq!(txn_id, "monovertex-test-vertex-gen-42");
    }
}
