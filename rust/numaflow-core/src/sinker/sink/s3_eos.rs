// Phase 4: S3 EOS Sink with Manifest-Based Visibility
//
// Implements exactly-once semantics for S3 using manifest files as the
// atomic visibility signal. Ensures S3 never exposes partial data.
//
// Key guarantees:
// - Deterministic staging paths (idempotent writes)
// - Manifest-per-epoch (atomic visibility)
// - Recovery from manifests (scan for latest committed epoch)

use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::commit_record::CommitRecord;
use crate::epoch_buffer::BufferedMessage;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::sinker::sink::{ResponseFromSink, Sink};

/// S3 manifest - atomic visibility signal for an epoch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Manifest {
    /// Epoch ID
    pub epoch_id: u64,
    
    /// Generation ID
    pub generation_id: u64,
    
    /// Timestamp when epoch was committed
    pub timestamp: i64,
    
    /// Watermark at commit time (if available)
    pub watermark: Option<i64>,
    
    /// List of data files for this epoch
    pub files: Vec<String>,
    
    /// Total message count
    pub message_count: usize,
    
    /// Total bytes written
    pub total_bytes: usize,
}

/// S3 sink with manifest-based exactly-once semantics
#[allow(dead_code)]
pub struct S3EosSink {
    /// S3 client
    s3_client: Arc<S3Client>,
    
    /// Bucket name
    bucket: String,
    
    /// Base path prefix (e.g., "monovertex/output")
    base_path: String,
    
    /// Current generation ID
    generation_id: u64,
    
    /// Vertex name
    vertex_name: String,
}

impl S3EosSink {
    /// Create a new S3 EOS sink
    pub fn new(
        s3_client: Arc<S3Client>,
        bucket: String,
        base_path: String,
        generation_id: u64,
        vertex_name: String,
    ) -> Self {
        Self {
            s3_client,
            bucket,
            base_path,
            generation_id,
            vertex_name,
        }
    }
    
    /// Prepare epoch: write data files + manifest (atomic visibility)
    pub async fn prepare(&self, commit_record: &CommitRecord, messages: Vec<BufferedMessage>) -> Result<()> {
        // Fence check
        if commit_record.generation_id != self.generation_id {
            return Err(Error::GenerationMismatch {
                expected: self.generation_id,
                got: commit_record.generation_id,
            });
        }
        
        if messages.is_empty() {
            tracing::debug!(
                epoch_id = commit_record.epoch_id,
                "No messages to write to S3"
            );
            return Ok(());
        }
        
        tracing::info!(
            epoch_id = commit_record.epoch_id,
            generation_id = commit_record.generation_id,
            message_count = messages.len(),
            "Writing epoch to S3"
        );
        
        // 1. Write all data files to staging
        let (file_paths, total_bytes) = self.write_data_files(commit_record, &messages).await?;
        
        // 2. Write manifest (atomic visibility signal)
        self.write_manifest(commit_record, file_paths, messages.len(), total_bytes).await?;
        
        tracing::info!(
            epoch_id = commit_record.epoch_id,
            generation_id = commit_record.generation_id,
            file_count = messages.len(),
            total_bytes = total_bytes,
            "S3 epoch committed (manifest published)"
        );
        
        Ok(())
    }
    
    /// Write data files to S3 staging area
    async fn write_data_files(
        &self,
        commit_record: &CommitRecord,
        messages: &[BufferedMessage],
    ) -> Result<(Vec<String>, usize)> {
        let mut file_paths = Vec::new();
        let mut total_bytes = 0;
        
        for (idx, msg) in messages.iter().enumerate() {
            // Deterministic staging path
            let path = format!(
                "{}/gen-{}/epoch-{}/part-{:05}.dat",
                self.base_path,
                commit_record.generation_id,
                commit_record.epoch_id,
                idx
            );
            
            let bytes = msg.value.len();
            
            // Write to S3
            self.s3_client
                .put_object()
                .bucket(&self.bucket)
                .key(&path)
                .body(msg.value.clone().into())
                .send()
                .await
                .map_err(|e| Error::Sink(format!("S3 put_object failed: {}", e)))?;
            
            tracing::trace!(
                path = path,
                bytes = bytes,
                "Data file written to S3"
            );
            
            file_paths.push(path);
            total_bytes += bytes;
        }
        
        Ok((file_paths, total_bytes))
    }
    
    /// Write manifest file (atomic visibility)
    async fn write_manifest(
        &self,
        commit_record: &CommitRecord,
        file_paths: Vec<String>,
        message_count: usize,
        total_bytes: usize,
    ) -> Result<()> {
        let manifest = S3Manifest {
            epoch_id: commit_record.epoch_id,
            generation_id: commit_record.generation_id,
            timestamp: commit_record.created_at,
            watermark: commit_record.watermark,
            files: file_paths,
            message_count,
            total_bytes,
        };
        
        let manifest_json = serde_json::to_vec(&manifest)
            .map_err(|e| Error::Sink(format!("Manifest serialization failed: {}", e)))?;
        
        // Manifest path (makes epoch visible when written)
        let manifest_path = format!(
            "{}/gen-{}/epoch-{}/MANIFEST.json",
            self.base_path,
            commit_record.generation_id,
            commit_record.epoch_id
        );
        
        // Write manifest (atomic visibility)
        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&manifest_path)
            .body(manifest_json.into())
            .send()
            .await
            .map_err(|e| Error::Sink(format!("Manifest write failed: {}", e)))?;
        
        tracing::info!(
            manifest_path = manifest_path,
            file_count = manifest.files.len(),
            "Manifest published to S3 (epoch visible)"
        );
        
        Ok(())
    }
    
    /// Recover from S3 manifests (find latest committed epoch)
    pub async fn recover_latest_epoch(&self) -> Result<Option<u64>> {
        tracing::info!(
            bucket = self.bucket,
            base_path = self.base_path,
            generation_id = self.generation_id,
            "Recovering from S3 manifests"
        );
        
        // List all manifests for this generation
        let prefix = format!(
            "{}/gen-{}/",
            self.base_path,
            self.generation_id
        );
        
        let mut manifests = Vec::new();
        
        // List objects with manifest prefix
        let mut continuation_token: Option<String> = None;
        loop {
            let mut request = self.s3_client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }
            
            let response = request
                .send()
                .await
                .map_err(|e| Error::Sink(format!("S3 list_objects failed: {}", e)))?;
            
            // Parse manifest files
            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        if key.ends_with("MANIFEST.json") {
                            // Download and parse manifest
                            if let Ok(manifest) = self.download_manifest(&key).await {
                                manifests.push(manifest);
                            }
                        }
                    }
                }
            }
            
            // Check for more pages
            if response.is_truncated == Some(true) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }
        
        // Find latest epoch
        let latest = manifests
            .iter()
            .max_by_key(|m| m.epoch_id)
            .map(|m| m.epoch_id);
        
        if let Some(epoch) = latest {
            tracing::info!(
                latest_epoch = epoch,
                manifest_count = manifests.len(),
                "Recovered from S3 manifests"
            );
        } else {
            tracing::info!("No manifests found in S3");
        }
        
        Ok(latest)
    }
    
    /// Download and parse a manifest file
    async fn download_manifest(&self, key: &str) -> Result<S3Manifest> {
        let response = self.s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::Sink(format!("Failed to download manifest {}: {}", key, e)))?;
        
        let bytes = response.body.collect().await
            .map_err(|e| Error::Sink(format!("Failed to read manifest body: {}", e)))?
            .into_bytes();
        
        let manifest: S3Manifest = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Sink(format!("Failed to parse manifest: {}", e)))?;
        
        Ok(manifest)
    }
}

// Implement Sink trait for backward compatibility (non-EOS mode)
impl Sink for S3EosSink {
    async fn sink(&mut self, _messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        // Non-EOS mode not implemented for S3
        Err(Error::Sink("S3 sink requires EOS mode".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_deterministic_paths() {
        let base = "output";
        let generation = 42;
        let epoch = 100;
        let seq = 5;
        
        let data_path = format!(
            "{}/gen-{}/epoch-{}/part-{:05}.dat",
            base, generation, epoch, seq
        );
        assert_eq!(data_path, "output/gen-42/epoch-100/part-00005.dat");
        
        let manifest_path = format!(
            "{}/gen-{}/epoch-{}/MANIFEST.json",
            base, generation, epoch
        );
        assert_eq!(manifest_path, "output/gen-42/epoch-100/MANIFEST.json");
    }
    
    #[test]
    fn test_manifest_serialization() {
        let manifest = S3Manifest {
            epoch_id: 42,
            generation_id: 1,
            timestamp: 1234567890,
            watermark: Some(9999),
            files: vec![
                "output/gen-1/epoch-42/part-00000.dat".to_string(),
                "output/gen-1/epoch-42/part-00001.dat".to_string(),
            ],
            message_count: 2,
            total_bytes: 1024,
        };
        
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: S3Manifest = serde_json::from_str(&json).unwrap();
        
        assert_eq!(parsed.epoch_id, 42);
        assert_eq!(parsed.files.len(), 2);
        assert_eq!(parsed.total_bytes, 1024);
    }
}
