// Phase 3 Part 4: Epoch Output Buffer
//
// Buffers outputs in memory until epoch commits.
// Prevents premature sink writes that could violate exactly-once semantics.
//
// Key Invariant: Outputs NEVER sent to sink before epoch is committed.

use crate::{Error, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Buffered message destined for sink
#[derive(Debug, Clone)]
pub struct BufferedMessage {
    /// Message keys (for Kafka-style sinks)
    pub keys: Vec<Vec<u8>>,
    
    /// Message value
    pub value: Vec<u8>,
    
    /// Optional headers
    pub headers: HashMap<String, Vec<u8>>,
    
    /// Event time (for watermark tracking)
    pub event_time: Option<i64>,
    
    /// Source partition (for watermark tracking and sink routing)
    /// - Kafka/JetStream: Some(partition_id)
    /// - S3/HTTP/Generator: None
    pub partition: Option<u32>,
}

impl BufferedMessage {
    /// Calculate approximate size in bytes
    pub fn size_bytes(&self) -> usize {
        let keys_size: usize = self.keys.iter().map(|k| k.len()).sum();
        let headers_size: usize = self
            .headers
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum();
        
        keys_size + self.value.len() + headers_size
    }
}

/// Buffered outputs for a single epoch
#[derive(Debug)]
pub struct EpochBuffer {
    /// Epoch ID
    pub epoch_id: u64,
    
    /// Generation ID (for fencing)
    pub generation_id: u64,
    
    /// Buffered outputs
    outputs: Vec<BufferedMessage>,
    
    /// Total bytes buffered
    total_bytes: usize,
}

impl EpochBuffer {
    /// Create new epoch buffer
    pub fn new(epoch_id: u64, generation_id: u64) -> Self {
        Self {
            epoch_id,
            generation_id,
            outputs: Vec::new(),
            total_bytes: 0,
        }
    }

    /// Add message to buffer
    pub fn push(&mut self, msg: BufferedMessage) {
        self.total_bytes += msg.size_bytes();
        self.outputs.push(msg);
    }

    /// Get all messages
    pub fn messages(&self) -> &[BufferedMessage] {
        &self.outputs
    }

    /// Get message count
    pub fn count(&self) -> usize {
        self.outputs.len()
    }

    /// Get total bytes
    pub fn bytes(&self) -> usize {
        self.total_bytes
    }

    /// Consume buffer and return messages
    pub fn drain(self) -> Vec<BufferedMessage> {
        self.outputs
    }
}

/// Configuration for output buffer manager
#[derive(Debug, Clone)]
pub struct OutputBufferConfig {
    /// Maximum total bytes across all epochs
    pub max_buffer_bytes: usize,
    
    /// Maximum pending epochs
    pub max_pending_epochs: usize,
    
    /// Per-epoch byte limit
    pub max_epoch_bytes: usize,
}

impl Default for OutputBufferConfig {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 100 * 1024 * 1024, // 100MB
            max_pending_epochs: 10,
            max_epoch_bytes: 20 * 1024 * 1024, // 20MB per epoch
        }
    }
}

/// Multi-epoch output buffer manager
///
/// Manages buffered outputs for multiple in-flight epochs.
/// Provides backpressure when memory limits are reached.
pub struct OutputBufferManager {
    /// Buffers per epoch
    buffers: Arc<RwLock<HashMap<u64, EpochBuffer>>>,
    
    /// Configuration
    config: OutputBufferConfig,
    
    /// Total buffered bytes (across all epochs)
    total_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

impl OutputBufferManager {
    /// Create new output buffer manager
    pub fn new(config: OutputBufferConfig) -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            config,
            total_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Buffer an output for an epoch
    ///
    /// Returns error if memory limits exceeded (backpressure signal).
    pub fn buffer_output(
        &self,
        epoch_id: u64,
        generation_id: u64,
        message: BufferedMessage,
    ) -> Result<()> {
        let msg_size = message.size_bytes();

        // Check global memory limit
        let current_total = self.total_bytes.load(std::sync::atomic::Ordering::Relaxed);
        if current_total + msg_size > self.config.max_buffer_bytes {
            return Err(Error::Backpressure(format!(
                "Output buffer full: {}MB / {}MB. Apply backpressure.",
                current_total / (1024 * 1024),
                self.config.max_buffer_bytes / (1024 * 1024)
            )));
        }

        let mut buffers = self.buffers.write();

        // Check max pending epochs
        if buffers.len() >= self.config.max_pending_epochs && !buffers.contains_key(&epoch_id) {
            return Err(Error::Backpressure(format!(
                "Too many pending epochs: {} / {}",
                buffers.len(),
                self.config.max_pending_epochs
            )));
        }

        // Get or create buffer for this epoch
        let buffer = buffers
            .entry(epoch_id)
            .or_insert_with(|| EpochBuffer::new(epoch_id, generation_id));

        // Check per-epoch limit
        if buffer.bytes() + msg_size > self.config.max_epoch_bytes {
            return Err(Error::Backpressure(format!(
                "Epoch {} buffer full: {}MB / {}MB",
                epoch_id,
                buffer.bytes() / (1024 * 1024),
                self.config.max_epoch_bytes / (1024 * 1024)
            )));
        }

        // Add to buffer
        buffer.push(message);

        // Update total bytes
        self.total_bytes
            .fetch_add(msg_size, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Get buffered messages for an epoch (without removing)
    pub fn get_epoch_buffer(&self, epoch_id: u64) -> Option<Vec<BufferedMessage>> {
        let buffers = self.buffers.read();
        buffers.get(&epoch_id).map(|b| b.messages().to_vec())
    }

    /// Flush epoch: remove buffer and return messages
    ///
    /// This should be called AFTER epoch is committed to RocksDB.
    /// Messages can then be sent to the sink.
    pub fn flush_epoch(&self, epoch_id: u64) -> Option<Vec<BufferedMessage>> {
        let buffer = {
            let mut buffers = self.buffers.write();
            buffers.remove(&epoch_id)
        };

        if let Some(buffer) = buffer {
            // Decrease total bytes
            self.total_bytes
                .fetch_sub(buffer.bytes(), std::sync::atomic::Ordering::Relaxed);

            Some(buffer.drain())
        } else {
            None
        }
    }

    /// Discard uncommitted epoch
    ///
    /// Called when epoch fails or is aborted.
    pub fn discard_epoch(&self, epoch_id: u64) {
        if let Some(buffer) = self.buffers.write().remove(&epoch_id) {
            // Decrease total bytes
            self.total_bytes
                .fetch_sub(buffer.bytes(), std::sync::atomic::Ordering::Relaxed);

            tracing::debug!(
                epoch_id = epoch_id,
                message_count = buffer.count(),
                bytes = buffer.bytes(),
                "Epoch buffer discarded (uncommitted)"
            );
        }
    }

    /// Get total buffered bytes
    pub fn total_buffered_bytes(&self) -> usize {
        self.total_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get number of pending epochs
    pub fn pending_epoch_count(&self) -> usize {
        self.buffers.read().len()
    }

    /// Get buffer usage as fraction (0.0 to 1.0)
    pub fn buffer_usage(&self) -> f64 {
        let current = self.total_buffered_bytes() as f64;
        let max = self.config.max_buffer_bytes as f64;
        current / max
    }

    /// Check if backpressure should be applied
    pub fn should_apply_backpressure(&self, threshold: f64) -> bool {
        self.buffer_usage() > threshold
    }
}

impl Clone for OutputBufferManager {
    fn clone(&self) -> Self {
        Self {
            buffers: Arc::clone(&self.buffers),
            config: self.config.clone(),
            total_bytes: Arc::clone(&self.total_bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_message(size_bytes: usize) -> BufferedMessage {
        BufferedMessage {
            keys: vec![b"key".to_vec()],
            value: vec![0u8; size_bytes],
            headers: HashMap::new(),
            event_time: None,
            partition: None,
        }
    }

    #[test]
    fn test_buffered_message_size() {
        let msg = BufferedMessage {
            keys: vec![b"key1".to_vec(), b"key2".to_vec()],
            value: vec![1, 2, 3, 4, 5],
            headers: HashMap::from([("hdr".to_string(), b"val".to_vec())]),
            event_time: None,
            partition: None,
        };

        // keys: 4 + 4 = 8, value: 5, headers: 3 + 3 = 6, total: 19
        assert_eq!(msg.size_bytes(), 19);
    }

    #[test]
    fn test_epoch_buffer() {
        let mut buffer = EpochBuffer::new(1, 100);

        assert_eq!(buffer.count(), 0);
        assert_eq!(buffer.bytes(), 0);

        buffer.push(create_test_message(100));
        buffer.push(create_test_message(200));

        assert_eq!(buffer.count(), 2);
        assert!(buffer.bytes() > 0);
    }

    #[test]
    fn test_buffer_manager_basic() {
        let config = OutputBufferConfig {
            max_buffer_bytes: 1000,
            max_pending_epochs: 5,
            max_epoch_bytes: 500,
        };

        let manager = OutputBufferManager::new(config);

        // Buffer messages
        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();
        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();

        assert_eq!(manager.pending_epoch_count(), 1);
        assert!(manager.total_buffered_bytes() > 0);
    }

    #[test]
    fn test_buffer_manager_flush() {
        let manager = OutputBufferManager::new(OutputBufferConfig::default());

        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();
        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();

        // Flush epoch
        let messages = manager.flush_epoch(1).unwrap();
        assert_eq!(messages.len(), 2);

        // Should be removed
        assert_eq!(manager.pending_epoch_count(), 0);
        assert_eq!(manager.total_buffered_bytes(), 0);
    }

    #[test]
    fn test_buffer_manager_discard() {
        let manager = OutputBufferManager::new(OutputBufferConfig::default());

        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();

        // Discard instead of flush
        manager.discard_epoch(1);

        assert_eq!(manager.pending_epoch_count(), 0);
        assert_eq!(manager.total_buffered_bytes(), 0);
    }

    #[test]
    fn test_buffer_manager_backpressure() {
        let config = OutputBufferConfig {
            max_buffer_bytes: 300,
            max_pending_epochs: 5,
            max_epoch_bytes: 500,
        };

        let manager = OutputBufferManager::new(config);

        // Fill up buffer
        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();
        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();

        // This should fail (exceeds max_buffer_bytes)
        let result = manager.buffer_output(1, 100, create_test_message(200));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Backpressure(_)));
    }

    #[test]
    fn test_buffer_manager_per_epoch_limit() {
        let config = OutputBufferConfig {
            max_buffer_bytes: 10000,
            max_pending_epochs: 5,
            max_epoch_bytes: 250,
        };

        let manager = OutputBufferManager::new(config);

        // Fill one epoch
        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();
        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();

        // This should fail (exceeds max_epoch_bytes)
        let result = manager.buffer_output(1, 100, create_test_message(100));
        assert!(result.is_err());
    }

    #[test]
    fn test_buffer_manager_max_epochs() {
        let config = OutputBufferConfig {
            max_buffer_bytes: 100000,
            max_pending_epochs: 2,
            max_epoch_bytes: 1000,
        };

        let manager = OutputBufferManager::new(config);

        manager
            .buffer_output(1, 100, create_test_message(10))
            .unwrap();
        manager
            .buffer_output(2, 100, create_test_message(10))
            .unwrap();

        // This should fail (exceeds max_pending_epochs)
        let result = manager.buffer_output(3, 100, create_test_message(10));
        assert!(result.is_err());
    }

    #[test]
    fn test_buffer_usage() {
        let config = OutputBufferConfig {
            max_buffer_bytes: 1000,
            max_pending_epochs: 5,
            max_epoch_bytes: 500,
        };

        let manager = OutputBufferManager::new(config);

        assert_eq!(manager.buffer_usage(), 0.0);

        manager
            .buffer_output(1, 100, create_test_message(500))
            .unwrap();

        // Should be around 50%
        assert!(manager.buffer_usage() > 0.4 && manager.buffer_usage() < 0.6);
    }

    #[test]
    fn test_multi_epoch_isolation() {
        let manager = OutputBufferManager::new(OutputBufferConfig::default());

        manager
            .buffer_output(1, 100, create_test_message(100))
            .unwrap();
        manager
            .buffer_output(2, 100, create_test_message(200))
            .unwrap();

        assert_eq!(manager.pending_epoch_count(), 2);

        // Flush epoch 1
        let msg1 = manager.flush_epoch(1).unwrap();
        assert_eq!(msg1.len(), 1);

        // Epoch 2 should still be there
        assert_eq!(manager.pending_epoch_count(), 1);

        // Flush epoch 2
        let msg2 = manager.flush_epoch(2).unwrap();
        assert_eq!(msg2.len(), 1);

        assert_eq!(manager.pending_epoch_count(), 0);
    }
}
