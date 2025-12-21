// Phase 2: SEAL Strategy
//
// The SEAL phase guarantees epoch boundary correctness by ensuring:
// 1. No new writes enter the epoch
// 2. All in-flight outputs are accounted for
//
// STRATEGY CHOICE: "Fence writers and wait for in-flight count == 0"
//
// Rationale:
// - Clean: No ambiguity about what's in vs out of epoch
// - Simple: Downstream only sees completed epochs
// - Correct: No partial epoch data leaks
//
// Alternative considered and rejected:
// - "Fence writers and drop in-flight" - Would lose data on crash
//
// CRITICAL: Only active in enhanced_mode. Standard mode has no SEAL.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// SEAL coordinator tracks in-flight messages and fences writers
#[derive(Clone)]
pub struct SealCoordinator {
    /// Whether new writes are allowed
    sealed: Arc<AtomicBool>,
    /// Count of messages currently in-flight (between read and ACK)
    in_flight_count: Arc<AtomicU64>,
    /// Enhanced mode flag (SEAL only active if true)
    enhanced_mode: bool,
}

impl SealCoordinator {
    pub fn new(enhanced_mode: bool) -> Self {
        Self {
            sealed: Arc::new(AtomicBool::new(false)),
            in_flight_count: Arc::new(AtomicU64::new(0)),
            enhanced_mode,
        }
    }

    /// Check if writes are allowed
    /// 
    /// If enhanced_mode is false, always returns true (no fencing).
    /// If enhanced_mode is true, returns false when sealed.
    pub fn is_sealed(&self) -> bool {
        if !self.enhanced_mode {
            return false; // Standard mode: never sealed
        }
        self.sealed.load(Ordering::Acquire)
    }

    /// Increment in-flight count when a message is read
    ///
    /// Only tracked in enhanced mode.
    pub fn track_message_read(&self) {
        if !self.enhanced_mode {
            return; // No tracking in standard mode
        }
        self.in_flight_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement in-flight count when a message is ACK'd
    ///
    /// Only tracked in enhanced mode.
    pub fn track_message_ack(&self) {
        if !self.enhanced_mode {
            return; // No tracking in standard mode
        }
        self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current in-flight count
    pub fn in_flight_count(&self) -> u64 {
        if !self.enhanced_mode {
            return 0; // Standard mode: no tracking
        }
        self.in_flight_count.load(Ordering::Relaxed)
    }

    /// Execute SEAL: Fence writers and wait for drain
    ///
    /// If enhanced_mode is false, this is a no-op (returns immediately).
    /// If enhanced_mode is true, sets sealed flag and waits for in-flight == 0.
    ///
    /// Returns Ok(()) when drained, Err if timeout exceeded.
    pub async fn seal_and_drain(&self, drain_timeout: Duration) -> crate::Result<()> {
        if !self.enhanced_mode {
            tracing::debug!("Standard mode: SEAL is no-op");
            return Ok(());
        }

        // Step 1: Fence writers (no new messages enter epoch)
        self.sealed.store(true, Ordering::Release);
        tracing::info!("SEAL: Writers fenced");

        // Step 2: Wait for in-flight to drain
        let drain_result = timeout(drain_timeout, async {
            loop {
                let count = self.in_flight_count();
                if count == 0 {
                    break;
                }
                tracing::debug!(in_flight = count, "SEAL: Waiting for drain");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await;

        match drain_result {
            Ok(_) => {
                tracing::info!("SEAL: Drained successfully");
                Ok(())
            }
            Err(_) => {
                let count = self.in_flight_count();
                tracing::error!(
                    in_flight = count,
                    timeout_secs = drain_timeout.as_secs(),
                    "SEAL: Drain timeout exceeded"
                );
                Err(crate::Error::Config(format!(
                    "SEAL drain timeout: {} messages still in-flight",
                    count
                )))
            }
        }
    }

    /// Unseal (allow writes again)
    ///
    /// Called after epoch is committed to resume processing.
    pub fn unseal(&self) {
        if !self.enhanced_mode {
            return; // No-op in standard mode
        }
        self.sealed.store(false, Ordering::Release);
        tracing::info!("SEAL: Writers unse aled");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seal_standard_mode() {
        let seal = SealCoordinator::new(false);
        
        // Standard mode: never sealed
        assert!(!seal.is_sealed());
        
        // Tracking is no-op
        seal.track_message_read();
        assert_eq!(seal.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_seal_enhanced_mode() {
        let seal = SealCoordinator::new(true);
        
        // Initially not sealed
        assert!(!seal.is_sealed());
        
        // Track some messages
        seal.track_message_read();
        seal.track_message_read();
        assert_eq!(seal.in_flight_count(), 2);
        
        // Start draining in background
        let seal_clone = seal.clone();
        let drain_task = tokio::spawn(async move {
            seal_clone.seal_and_drain(Duration::from_secs(5)).await
        });
        
        // Give SEAL time to fence
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(seal.is_sealed());
        
        // ACK messages to drain
        seal.track_message_ack();
        seal.track_message_ack();
        
        // Drain should complete
        drain_task.await.unwrap().unwrap();
        
        // Unseal
        seal.unseal();
        assert!(!seal.is_sealed());
    }

    #[tokio::test]
    async fn test_seal_timeout() {
        let seal = SealCoordinator::new(true);
        
        // Track a message but don't ACK
        seal.track_message_read();
        
        // SEAL should timeout
        let result = seal.seal_and_drain(Duration::from_millis(100)).await;
        assert!(result.is_err());
    }
}
