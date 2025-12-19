// Phase 3 Part 5: Flush Triggers
//
// Determines when to flush buffered state and outputs.
// Supports multiple triggers: Barrier, Time, Count, Memory pressure.

use std::time::{Duration, Instant};

/// Flush trigger type
#[derive(Debug, Clone)]
pub enum FlushTrigger {
    /// Explicit barrier from control plane
    Barrier,

    /// Time-based: max age of current epoch
    Time(Duration),

    /// Count-based: max messages in current epoch
    Count(usize),

    /// Memory pressure: buffer usage threshold (0.0 to 1.0)
    MemoryPressure(f64),
}

/// Flush policy - determines when to trigger epoch commit
///
/// Supports multiple triggers evaluated in OR fashion.
/// Any trigger condition met → flush.
pub struct FlushPolicy {
    /// Configured triggers
    triggers: Vec<FlushTrigger>,

    /// When current epoch started
    epoch_start: Instant,

    /// Messages processed in current epoch
    message_count: usize,

    /// Last flush time
    last_flush: Instant,
}

impl FlushPolicy {
    /// Create new flush policy
    pub fn new(triggers: Vec<FlushTrigger>) -> Self {
        let now = Instant::now();
        Self {
            triggers,
            epoch_start: now,
            last_flush: now,
            message_count: 0,
        }
    }

    /// Create default policy
    ///
    /// Defaults:
    /// - Time: 10s max epoch age
    /// - Count: 100k messages
    /// - Memory: 80% buffer usage
    pub fn default_policy() -> Self {
        Self::new(vec![
            FlushTrigger::Time(Duration::from_secs(10)),
            FlushTrigger::Count(100_000),
            FlushTrigger::MemoryPressure(0.8),
        ])
    }

    /// Record a processed message
    pub fn record_message(&mut self) {
        self.message_count += 1;
    }

    /// Record multiple messages (batch)
    pub fn record_messages(&mut self, count: usize) {
        self.message_count += count;
    }

    /// Check if flush should be triggered
    ///
    /// # Arguments
    /// * `buffer_usage` - Current buffer usage (0.0 to 1.0)
    /// * `barrier_received` - Whether explicit barrier was received
    ///
    /// Returns true if any trigger condition is met.
    pub fn should_flush(&self, buffer_usage: f64, barrier_received: bool) -> bool {
        // Check each trigger
        for trigger in &self.triggers {
            match trigger {
                FlushTrigger::Barrier => {
                    if barrier_received {
                        return true;
                    }
                }
                FlushTrigger::Time(max_age) => {
                    let epoch_age = self.epoch_start.elapsed();
                    if epoch_age >= *max_age {
                        return true;
                    }
                }
                FlushTrigger::Count(max_count) => {
                    if self.message_count >= *max_count {
                        return true;
                    }
                }
                FlushTrigger::MemoryPressure(threshold) => {
                    if buffer_usage >= *threshold {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Get current epoch age
    pub fn epoch_age(&self) -> Duration {
        self.epoch_start.elapsed()
    }

    /// Get message count in current epoch
    pub fn message_count(&self) -> usize {
        self.message_count
    }

    /// Get time since last flush
    pub fn time_since_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }

    /// Reset for new epoch
    pub fn reset_epoch(&mut self) {
        let now = Instant::now();
        self.epoch_start = now;
        self.last_flush = now;
        self.message_count = 0;
    }

    /// Update policy with new triggers
    pub fn update_triggers(&mut self, triggers: Vec<FlushTrigger>) {
        self.triggers = triggers;
    }
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_barrier_trigger() {
        let policy = FlushPolicy::new(vec![FlushTrigger::Barrier]);

        // No barrier
        assert!(!policy.should_flush(0.0, false));

        // With barrier
        assert!(policy.should_flush(0.0, true));
    }

    #[test]
    fn test_time_trigger() {
        let policy = FlushPolicy::new(vec![FlushTrigger::Time(Duration::from_millis(50))]);

        // Immediately - should not flush
        assert!(!policy.should_flush(0.0, false));

        // Wait for trigger
        thread::sleep(Duration::from_millis(60));

        // Should flush now
        assert!(policy.should_flush(0.0, false));
    }

    #[test]
    fn test_count_trigger() {
        let mut policy = FlushPolicy::new(vec![FlushTrigger::Count(100)]);

        // Below threshold
        policy.record_messages(50);
        assert!(!policy.should_flush(0.0, false));

        // At threshold
        policy.record_messages(50);
        assert!(policy.should_flush(0.0, false));

        // Above threshold
        policy.record_message();
        assert!(policy.should_flush(0.0, false));
    }

    #[test]
    fn test_memory_trigger() {
        let policy = FlushPolicy::new(vec![FlushTrigger::MemoryPressure(0.8)]);

        // Below threshold
        assert!(!policy.should_flush(0.5, false));

        // At threshold
        assert!(policy.should_flush(0.8, false));

        // Above threshold
        assert!(policy.should_flush(0.9, false));
    }

    #[test]
    fn test_multiple_triggers() {
        let mut policy = FlushPolicy::new(vec![
            FlushTrigger::Count(1000),
            FlushTrigger::MemoryPressure(0.8),
        ]);

        // Neither triggered
        policy.record_messages(500);
        assert!(!policy.should_flush(0.5, false));

        // Count triggered
        policy.record_messages(500);
        assert!(policy.should_flush(0.5, false));

        // Reset for memory test
        policy.reset_epoch();

        // Memory triggered (count below threshold)
        policy.record_messages(100);
        assert!(policy.should_flush(0.9, false));
    }

    #[test]
    fn test_default_policy() {
        let policy = FlushPolicy::default();

        // Should have 3 triggers
        assert_eq!(policy.triggers.len(), 3);
    }

    #[test]
    fn test_reset_epoch() {
        let mut policy = FlushPolicy::new(vec![FlushTrigger::Count(100)]);

        policy.record_messages(50);
        assert_eq!(policy.message_count(), 50);

        policy.reset_epoch();
        assert_eq!(policy.message_count(), 0);
    }

    #[test]
    fn test_epoch_age() {
        let policy = FlushPolicy::new(vec![]);

        thread::sleep(Duration::from_millis(10));

        assert!(policy.epoch_age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_update_triggers() {
        let mut policy = FlushPolicy::new(vec![FlushTrigger::Barrier]);

        // Original triggers
        assert_eq!(policy.triggers.len(), 1);

        // Update
        policy.update_triggers(vec![
            FlushTrigger::Time(Duration::from_secs(5)),
            FlushTrigger::Count(1000),
        ]);

        assert_eq!(policy.triggers.len(), 2);
    }

    #[test]
    fn test_or_logic() {
        // All triggers in OR relationship
        let mut policy = FlushPolicy::new(vec![
            FlushTrigger::Barrier,
            FlushTrigger::Count(100),
            FlushTrigger::MemoryPressure(0.8),
        ]);

        // Barrier alone triggers
        assert!(policy.should_flush(0.0, true));

        // Count alone triggers
        policy.record_messages(100);
        assert!(policy.should_flush(0.0, false));

        policy.reset_epoch();

        // Memory alone triggers
        assert!(policy.should_flush(0.9, false));
    }
}
