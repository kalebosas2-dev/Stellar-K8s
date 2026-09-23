use std::sync::Arc;
use tokio::sync::Mutex;

/// Trait for cloud storage provider operations.
/// Supports AWS EBS, GCP PD, or local storage providers.
pub trait CloudStorageProvider {
    /// Creates a snapshot of the specified volume.
    async fn create_snapshot(&self, volume_id: String, metadata: u64) -> String;

    /// Deletes a snapshot by ID.
    async fn delete_snapshot(&self, snapshot_id: String);

    /// Restores a volume from a snapshot.
    async fn restore_from_snapshot(&self, snapshot_id: String, target_volume_id: String) -> String;

    /// Lists all snapshots for a given volume or filter.
    async fn list_snapshots(&self, filter: Option<String>) -> Vec<String>;
}

/// Manager for cloud backup operations.
/// Handles retention policies and snapshot lifecycle.
pub struct CloudBackupManager {
    provider: Arc<dyn CloudStorageProvider + Send + Sync>,
    retention_days: u64,
    /// Tracks snapshot metadata for retention enforcement
    snapshots: Arc<Mutex<Vec<SnapshotRecord>>>,
}

#[derive(Debug, Clone)]
struct SnapshotRecord {
    id: String,
    volume_id: String,
    created_at: u64,
    is_valid: bool,
}

impl CloudBackupManager {
    pub fn new(provider: Arc<dyn CloudStorageProvider + Send + Sync>, retention_days: u64) -> Self {
        Self {
            provider,
            retention_days,
            snapshots: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Creates a new snapshot and registers it for retention tracking.
    pub async fn create_snapshot(&self, volume_id: String, metadata: u64) -> String {
        let snapshot_id = self.provider.create_snapshot(volume_id.clone(), metadata).await;
        let record = SnapshotRecord {
            id: snapshot_id.clone(),
            volume_id,
            created_at: metadata,
            is_valid: true,
        };
        let mut snaps = self.snapshots.lock().await;
        snaps.push(record);
        snapshot_id
    }

    /// Enforces retention policy by purging expired snapshot assets.
    pub async fn enforce_retention(&self, current_time: u64) {
        let retention_seconds = self.retention_days * 24 * 60 * 60;
        let mut snaps = self.snapshots.lock().await;
        let expired: Vec<String> = snaps
            .iter()
            .filter(|s| current_time - s.created_at > retention_seconds)
            .map(|s| s.id.clone())
            .collect();

        for id in expired {
            self.provider.delete_snapshot(id.clone()).await;
            snaps.retain(|s| s.id != id);
            println!("Expired snapshot deleted: {}", id);
        }
    }

    /// Restores a volume from a validated snapshot.
    /// Returns the new volume ID upon successful restoration.
    pub async fn restore_from_snapshot(&self, snapshot_id: String, target_volume_id: String) -> String {
        // Validate snapshot exists and is valid
        let snaps = self.snapshots.lock().await;
        let snapshot = snaps.iter().find(|s| s.id == snapshot_id && s.is_valid);
        
        if snapshot.is_none() {
            panic!("Invalid or missing snapshot ID: {}", snapshot_id);
        }

        drop(snaps);

        let new_volume_id = self
            .provider
            .restore_from_snapshot(snapshot_id, target_volume_id)
            .await;
        
        println!("Volume restored from snapshot to: {}", new_volume_id);
        new_volume_id
    }
}