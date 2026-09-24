//! Cloud backup provider adapters (AWS EBS, GCP PD, local) and restore flow.

use crate::backup::snapshot::{Snapshot, SnapshotController};
use std::time::Duration;

/// Supported storage backends for automated backups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudProvider {
    AwsEbs,
    GcpPd,
    Local,
}

impl CloudProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            CloudProvider::AwsEbs => "aws-ebs",
            CloudProvider::GcpPd => "gcp-pd",
            CloudProvider::Local => "local",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "aws" | "aws-ebs" | "ebs" => Some(CloudProvider::AwsEbs),
            "gcp" | "gcp-pd" | "pd" => Some(CloudProvider::GcpPd),
            "local" => Some(CloudProvider::Local),
            _ => None,
        }
    }
}

/// Result of restoring a volume onto a newly provisioned node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    pub snapshot_id: String,
    pub target_volume_id: String,
    pub integrity_ok: bool,
    pub provider: String,
}

/// Cloud backup controller: snapshot → retain → restore.
#[derive(Debug, Clone)]
pub struct CloudBackupController {
    pub provider: CloudProvider,
    pub snapshots: SnapshotController,
}

impl CloudBackupController {
    pub fn new(provider: CloudProvider, retention: Duration) -> Self {
        Self {
            provider,
            snapshots: SnapshotController::new(retention),
        }
    }

    /// Controller loop step: snapshot volume at ledger marker, then prune.
    pub fn run_backup_cycle(
        &mut self,
        volume_id: &str,
        ledger_sequence: u64,
        now_secs: u64,
    ) -> Result<(Snapshot, usize), String> {
        let snap = self.snapshots.create_snapshot(
            volume_id,
            ledger_sequence,
            self.provider.as_str(),
        )?;
        // Provider-specific snapshot API call is represented by tagging provider.
        let purged = self.snapshots.enforce_retention(now_secs);
        Ok((snap, purged))
    }

    /// Instant volume restoration from a validated snapshot for new node provisioning.
    pub fn restore_volume(
        &self,
        snapshot_id: &str,
        target_volume_id: &str,
    ) -> Result<RestoreReport, String> {
        let snap = self
            .snapshots
            .get(snapshot_id)
            .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;

        if !snap.consistent {
            return Err("snapshot failed consistency validation".into());
        }
        if target_volume_id.is_empty() {
            return Err("target_volume_id required".into());
        }

        // Integrity check: ledger marker present and provider matches.
        let integrity_ok = snap.ledger_sequence > 0 && snap.provider == self.provider.as_str();
        if !integrity_ok {
            return Err("database integrity check failed for restored volume".into());
        }

        Ok(RestoreReport {
            snapshot_id: snap.id.clone(),
            target_volume_id: target_volume_id.to_string(),
            integrity_ok,
            provider: self.provider.as_str().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_cycle_and_restore() {
        let mut ctl = CloudBackupController::new(CloudProvider::AwsEbs, Duration::from_secs(86_400));
        let (snap, purged) = ctl
            .run_backup_cycle("pvc-stellar-0", 1_000_042, 1_700_000_000)
            .expect("backup");
        assert_eq!(purged, 0);
        assert_eq!(snap.provider, "aws-ebs");
        assert!(snap.consistent);

        let report = ctl
            .restore_volume(&snap.id, "pvc-stellar-restored")
            .expect("restore");
        assert!(report.integrity_ok);
        assert_eq!(report.target_volume_id, "pvc-stellar-restored");
    }

    #[test]
    fn rejects_unknown_snapshot() {
        let ctl = CloudBackupController::new(CloudProvider::Local, Duration::from_secs(60));
        assert!(ctl.restore_volume("missing", "vol").is_err());
    }

    #[test]
    fn parse_providers() {
        assert_eq!(CloudProvider::parse("aws-ebs"), Some(CloudProvider::AwsEbs));
        assert_eq!(CloudProvider::parse("gcp"), Some(CloudProvider::GcpPd));
        assert_eq!(CloudProvider::parse("local"), Some(CloudProvider::Local));
    }
}
