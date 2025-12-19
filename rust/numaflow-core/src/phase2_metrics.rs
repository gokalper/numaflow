// Phase 2: Metrics for Enhanced MonoVertex
//
// Mandatory observability for Phase 2 recovery and commit operations.
// All metrics are only recorded in enhanced_mode.
//
// Phase 2 TODO: Wire into main prometheus registry during startup
#![allow(dead_code)]

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use std::time::Instant;

/// Labels for Phase 2 metrics
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct Phase2Labels {
    pub generation_id: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RecoveryLabels {
    pub generation_id: String,
    pub path: String, // "committed", "repaired", "replay", "fresh"
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReconcileLabels {
    pub generation_id: String,
    pub sink_type: String, // "kafka", "s3", "jetstream", "log"
    pub action: String,   // "repair", "replay"
}

/// Phase 2 metrics collector
///
/// Only active when enhanced_mode = true.
/// Standard mode never records these metrics.
pub struct Phase2Metrics {
    /// Epoch duration (time from SEAL to commit record update)
    pub epoch_duration_seconds: Family<Phase2Labels, Histogram>,
    
    /// PERSIST phase latency (RocksDB write)
    pub persist_latency_seconds: Family<Phase2Labels, Histogram>,
    
    /// SEAL drain time (wait for in-flight == 0)
    pub seal_drain_seconds: Family<Phase2Labels, Histogram>,
    
    /// Reconciliation time per sink
    pub reconcile_duration_seconds: Family<ReconcileLabels, Histogram>,
    
    /// Recovery path taken
    pub recovery_path_total: Family<RecoveryLabels, Counter>,
    
    /// Current epoch ID
    pub current_epoch_id: Family<Phase2Labels, Gauge>,
    
    /// In-flight message count (for SEAL)
    pub seal_in_flight_count: Family<Phase2Labels, Gauge>,
    
    /// Enhanced mode flag (1 = enhanced, 0 = standard)
    pub enhanced_mode_enabled: Gauge,
}

impl Phase2Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let epoch_duration = Family::<Phase2Labels, Histogram>::new_with_constructor(|| {
            Histogram::new([0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0].into_iter())
        });
        
        let persist_latency = Family::<Phase2Labels, Histogram>::new_with_constructor(|| {
            Histogram::new([0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0].into_iter())
        });
        
        let seal_drain = Family::<Phase2Labels, Histogram>::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0].into_iter())
        });
        
        let reconcile_duration = Family::<ReconcileLabels, Histogram>::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0].into_iter())
        });
        
        let recovery_path = Family::<RecoveryLabels, Counter>::default();
        let current_epoch = Family::<Phase2Labels, Gauge>::default();
        let seal_in_flight = Family::<Phase2Labels, Gauge>::default();
        let enhanced_mode = Gauge::default();
        
        registry.register(
            "numaflow_phase2_epoch_duration_seconds",
            "Time from SEAL to commit record update",
            epoch_duration.clone(),
        );
        
        registry.register(
            "numaflow_phase2_persist_latency_seconds",
            "RocksDB write latency for PERSIST phase",
            persist_latency.clone(),
        );
        
        registry.register(
            "numaflow_phase2_seal_drain_seconds",
            "Time to drain in-flight messages during SEAL",
            seal_drain.clone(),
        );
        
        registry.register(
            "numaflow_phase2_reconcile_duration_seconds",
            "Sink reconciliation time during recovery",
            reconcile_duration.clone(),
        );
        
        registry.register(
            "numaflow_phase2_recovery_path_total",
            "Count of recovery paths taken",
            recovery_path.clone(),
        );
        
        registry.register(
            "numaflow_phase2_current_epoch_id",
            "Current epoch ID being processed",
            current_epoch.clone(),
        );
        
        registry.register(
            "numaflow_phase2_seal_in_flight_count",
            "Number of in-flight messages (tracked for SEAL)",
            seal_in_flight.clone(),
        );
        
        registry.register(
            "numaflow_enhanced_mode_enabled",
            "1 if enhanced mode is active, 0 if standard mode",
            enhanced_mode.clone(),
        );
        
        Self {
            epoch_duration_seconds: epoch_duration,
            persist_latency_seconds: persist_latency,
            seal_drain_seconds: seal_drain,
            reconcile_duration_seconds: reconcile_duration,
            recovery_path_total: recovery_path,
            current_epoch_id: current_epoch,
            seal_in_flight_count: seal_in_flight,
            enhanced_mode_enabled: enhanced_mode,
        }
    }
    
    /// Set enhanced mode flag (call once at startup)
    pub fn set_enhanced_mode(&self, enabled: bool) {
        self.enhanced_mode_enabled.set(if enabled { 1 } else { 0 });
    }
    
    /// Record epoch completion
    pub fn record_epoch_duration(&self, generation_id: u64, start: Instant) {
        let duration = start.elapsed().as_secs_f64();
        let labels = Phase2Labels {
            generation_id: generation_id.to_string(),
        };
        self.epoch_duration_seconds.get_or_create(&labels).observe(duration);
    }
    
    /// Record PERSIST phase latency
    pub fn record_persist_latency(&self, generation_id: u64, start: Instant) {
        let duration = start.elapsed().as_secs_f64();
        let labels = Phase2Labels {
            generation_id: generation_id.to_string(),
        };
        self.persist_latency_seconds.get_or_create(&labels).observe(duration);
    }
    
    /// Record SEAL drain time
    pub fn record_seal_drain(&self, generation_id: u64, start: Instant) {
        let duration = start.elapsed().as_secs_f64();
        let labels = Phase2Labels {
            generation_id: generation_id.to_string(),
        };
        self.seal_drain_seconds.get_or_create(&labels).observe(duration);
    }
    
    /// Record reconciliation duration
    pub fn record_reconcile_duration(
        &self,
        generation_id: u64,
        sink_type: &str,
        action: &str,
        start: Instant,
    ) {
        let duration = start.elapsed().as_secs_f64();
        let labels = ReconcileLabels {
            generation_id: generation_id.to_string(),
            sink_type: sink_type.to_string(),
            action: action.to_string(),
        };
        self.reconcile_duration_seconds.get_or_create(&labels).observe(duration);
    }
    
    /// Increment recovery path counter
    pub fn record_recovery_path(&self, generation_id: u64, path: &str) {
        let labels = RecoveryLabels {
            generation_id: generation_id.to_string(),
            path: path.to_string(),
        };
        self.recovery_path_total.get_or_create(&labels).inc();
    }
    
    /// Update current epoch ID
    pub fn set_current_epoch(&self, generation_id: u64, epoch_id: u64) {
        let labels = Phase2Labels {
            generation_id: generation_id.to_string(),
        };
        self.current_epoch_id.get_or_create(&labels).set(epoch_id as i64);
    }
    
    /// Update in-flight count
    pub fn set_seal_in_flight(&self, generation_id: u64, count: u64) {
        let labels = Phase2Labels {
            generation_id: generation_id.to_string(),
        };
        self.seal_in_flight_count.get_or_create(&labels).set(count as i64);
    }
}

/// Global Phase 2 metrics instance
static PHASE2_METRICS: std::sync::OnceLock<Arc<Phase2Metrics>> = std::sync::OnceLock::new();

/// Initialize Phase 2 metrics (call once at startup)
pub fn init_phase2_metrics(registry: &mut Registry, enhanced_mode: bool) -> Arc<Phase2Metrics> {
    PHASE2_METRICS.get_or_init(|| {
        let metrics = Arc::new(Phase2Metrics::new(registry));
        metrics.set_enhanced_mode(enhanced_mode);
        metrics
    }).clone()
}

/// Get Phase 2 metrics (returns None if not initialized)
pub fn get_phase2_metrics() -> Option<Arc<Phase2Metrics>> {
    PHASE2_METRICS.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metrics_initialization() {
        let mut registry = Registry::default();
        let metrics = Phase2Metrics::new(&mut registry);
        
        metrics.set_enhanced_mode(true);
        assert_eq!(metrics.enhanced_mode_enabled.get(), 1);
        
        metrics.set_enhanced_mode(false);
        assert_eq!(metrics.enhanced_mode_enabled.get(), 0);
    }
    
    #[test]
    fn test_record_metrics() {
        let mut registry = Registry::default();
        let metrics = Phase2Metrics::new(&mut registry);
        
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        metrics.record_epoch_duration(1, start);
        metrics.record_persist_latency(1, start);
        metrics.record_seal_drain(1, start);
        metrics.record_reconcile_duration(1, "kafka", "repair", start);
        metrics.record_recovery_path(1, "committed");
        metrics.set_current_epoch(1, 42);
        metrics.set_seal_in_flight(1, 100);
        
        // Metrics recorded successfully (no panics)
    }
}
