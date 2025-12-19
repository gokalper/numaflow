// Phase 4: MonoVertex Watermark Management
//
// Manages watermarks for MonoVertex with multiple source partitions.
// Computes global watermark as min across all active partitions.
// Handles idle partition detection with safety guarantees.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::Arc;

use crate::watermark::wmb::Watermark;

/// Configuration for watermark management
#[derive(Clone, Debug)]
pub struct WatermarkConfig {
    /// Timeout for considering a partition idle
    pub idle_timeout: Duration,
    
    /// Whether to enable strict mode (no skipping buffered work)
    pub strict_mode: bool,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(60), // 1 minute
            strict_mode: true,
        }
    }
}

/// Manages watermarks for MonoVertex
///
/// Tracks per-partition watermarks and computes global watermark as
/// the minimum across all active partitions. Handles idle detection
/// with safety rule: no buffered work may be skipped.
pub struct WatermarkManager {
    /// Shared state protected by RwLock
    state: Arc<RwLock<WatermarkState>>,
    
    /// Configuration
    config: WatermarkConfig,
}

struct WatermarkState {
    /// Per-partition watermarks (partition_id -> watermark)
    partition_watermarks: HashMap<u32, Watermark>,
    
    /// Last activity timestamp per partition
    last_activity: HashMap<u32, Instant>,
    
    /// Current global watermark (min across all partitions)
    global_watermark: Watermark,
    
    /// Partitions currently considered idle
    idle_partitions: Vec<u32>,
}

impl WatermarkManager {
    /// Create a new WatermarkManager
    pub fn new(config: WatermarkConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(WatermarkState {
                partition_watermarks: HashMap::new(),
                last_activity: HashMap::new(),
                global_watermark: Watermark::default(),
                idle_partitions: Vec::new(),
            })),
            config,
        }
    }
    
    /// Update watermark for a specific partition
    ///
    /// Marks partition as active and recomputes global watermark.
    pub fn update_partition(&self, partition: u32, watermark: Watermark) {
        let mut state = self.state.write();
        
        state.partition_watermarks.insert(partition, watermark);
        state.last_activity.insert(partition, Instant::now());
        
        // Remove from idle list if present
        state.idle_partitions.retain(|&p| p != partition);
        
        self.recompute_global(&mut state);
        
        tracing::trace!(
            partition = partition,
            watermark = %watermark,
            "Partition watermark updated"
        );
    }
    
    /// Get current global watermark (min across all active partitions)
    pub fn global_watermark(&self) -> Watermark {
        self.state.read().global_watermark.clone()
    }
    
    /// Detect and handle idle partitions
    ///
    /// Partitions idle beyond timeout get their watermark advanced to current time,
    /// ensuring global watermark makes forward progress.
    pub fn detect_idle_partitions(&self) -> Vec<u32> {
        let mut state = self.state.write();
        let now = Instant::now();
        let mut newly_idle = Vec::new();
        
        // Collect idle partitions first to avoid borrow checker issues
        let idle_partitions_to_mark: Vec<u32> = state.last_activity
            .iter()
            .filter_map(|(&partition, &last_activity)| {
                if now.duration_since(last_activity) > self.config.idle_timeout 
                    && !state.idle_partitions.contains(&partition) {
                    Some(partition)
                } else {
                    None
                }
            })
            .collect();
        
        // Now mutate state
        for partition in idle_partitions_to_mark {
            newly_idle.push(partition);
            state.idle_partitions.push(partition);
            
            // Advance idle partition watermark to current time
            let idle_watermark = Utc::now();
            state.partition_watermarks.insert(partition, idle_watermark);
            
            if let Some(&last_activity) = state.last_activity.get(&partition) {
                tracing::info!(
                    partition = partition,
                    idle_duration_secs = now.duration_since(last_activity).as_secs(),
                    "Partition marked as idle, watermark advanced"
                );
            }
        }
        
        if !newly_idle.is_empty() {
            self.recompute_global(&mut state);
        }
        
        newly_idle
    }
    
    /// Mark partition as active (used when partition returns from idle)
    pub fn mark_active(&self, partition: u32) {
        let mut state = self.state.write();
        state.last_activity.insert(partition, Instant::now());
        state.idle_partitions.retain(|&p| p != partition);
        
        tracing::info!(
            partition = partition,
            "Partition marked as active"
        );
    }
    
    /// Get list of currently idle partitions
    pub fn idle_partitions(&self) -> Vec<u32> {
        self.state.read().idle_partitions.clone()
    }
    
    /// Get watermark for a specific partition
    pub fn partition_watermark(&self, partition: u32) -> Option<Watermark> {
        self.state.read().partition_watermarks.get(&partition).cloned()
    }
    
    /// Initialize partitions (typically at startup)
    pub fn initialize_partitions(&self, partitions: Vec<u32>) {
        let mut state = self.state.write();
        let now = Instant::now();
        let initial_watermark = Utc::now();
        
        for partition in partitions {
            state.partition_watermarks.entry(partition)
                .or_insert(initial_watermark.clone());
            state.last_activity.entry(partition)
                .or_insert(now);
        }
        
        self.recompute_global(&mut state);
        
        tracing::info!(
            partition_count = state.partition_watermarks.len(),
            "Watermark partitions initialized"
        );
    }
    
    /// Recompute global watermark (min across all partitions)
    ///
    /// Safety rule: If strict_mode enabled, global watermark can only
    /// advance if ALL partitions have advanced (no skipping buffered work).
    fn recompute_global(&self, state: &mut WatermarkState) {
        if state.partition_watermarks.is_empty() {
            state.global_watermark = Watermark::default();
            return;
        }
        
        // Compute min watermark across all partitions
        let min_watermark = state.partition_watermarks
            .values()
            .min()
            .cloned()
            .unwrap_or(Watermark::default());
        
        // In strict mode, never move backwards
        if self.config.strict_mode && min_watermark < state.global_watermark {
           tracing::warn!(
                old = %state.global_watermark,
                new = %min_watermark,
                "Watermark would move backwards in strict mode - keeping old value"
            );
            return;
        }
        
        // Update global watermark
        let old_watermark = state.global_watermark;
        state.global_watermark = min_watermark;
        
        if state.global_watermark > old_watermark {
            tracing::debug!(
                old = %old_watermark,
                new = %state.global_watermark,
                partition_count = state.partition_watermarks.len(),
                "Global watermark advanced"
            );
        }
    }
    
    /// Check if watermark can advance (no buffered work below watermark)
    ///
    /// This enforces the safety rule: no buffered work may be skipped.
    pub fn can_advance(&self, proposed_watermark: &Watermark, oldest_buffered_time: Option<DateTime<Utc>>) -> bool {
        if !self.config.strict_mode {
            return true;
        }
        
        if let Some(oldest_time) = oldest_buffered_time {
            if *proposed_watermark > oldest_time {
                tracing::warn!(
                    proposed_watermark = %proposed_watermark,
                    oldest_buffered = %oldest_time,
                    "Cannot advance watermark - would skip buffered work"
                );
                return false;
            }
        }
        
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    
    #[test]
    fn test_watermark_min_across_partitions() {
        let manager = WatermarkManager::new(WatermarkConfig::default());
        
        // Initialize 3 partitions
        manager.initialize_partitions(vec![0, 1, 2]);
        
        let now = Utc::now();
        let wm1 = Watermark::new(now);
        let wm2 = Watermark::new(now + chrono::Duration::seconds(10));
        let wm3 = Watermark::new(now + chrono::Duration::seconds(5));
        
        manager.update_partition(0, wm1.clone());
        manager.update_partition(1, wm2);
        manager.update_partition(2, wm3);
        
        // Global should be minimum (wm1)
        let global = manager.global_watermark();
        assert_eq!(global.timestamp, wm1.timestamp);
    }
    
    #[test]
    fn test_idle_detection() {
        let config = WatermarkConfig {
            idle_timeout: Duration::from_millis(100),
            strict_mode: true,
        };
        let manager = WatermarkManager::new(config);
        
        manager.initialize_partitions(vec![0, 1]);
        
        let wm = Watermark::new(Utc::now());
        manager.update_partition(0, wm.clone());
        manager.update_partition(1, wm);
        
        // Wait for idle timeout
        thread::sleep(Duration::from_millis(150));
        
        // Detect idle
        let idle = manager.detect_idle_partitions();
        assert_eq!(idle.len(), 2);
        
        // Check idle list
        let idle_list = manager.idle_partitions();
        assert!(idle_list.contains(&0));
        assert!(idle_list.contains(&1));
    }
    
    #[test]
    fn test_strict_mode_no_backwards() {
        let config = WatermarkConfig {
            idle_timeout: Duration::from_secs(60),
            strict_mode: true,
        };
        let manager = WatermarkManager::new(config);
        
        manager.initialize_partitions(vec![0]);
        
        let now = Utc::now();
        let wm1 = Watermark::new(now);
        let wm2 = Watermark::new(now - chrono::Duration::seconds(10)); // Earlier
        
        manager.update_partition(0, wm1.clone());
        let global1 = manager.global_watermark();
        
        manager.update_partition(0, wm2);
        let global2 = manager.global_watermark();
        
        // Should not move backwards in strict mode
        assert_eq!(global1.timestamp, global2.timestamp);
    }
}
