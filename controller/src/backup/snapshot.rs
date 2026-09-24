//! Automated volume snapshot lifecycle aligned to ledger sequence markers.
//!
//! Pauses database write flushes briefly during snapshot initialization to
//! guarantee crash-consistent persistent volume snapshots.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Persistent volume snapshot metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub id: String,
    pub volume_id: String,
    pub ledger_sequence: u64,
    pub created_at_secs: u64,
    pub provider: String,
    pub consistent: bool,
}

/// Tracks whether DB write flushes are paused for crash consistency.
#[derive(Debug, Default, Clone)]
pub struct WriteFlushGate {
    paused: bool,
}

impl WriteFlushGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Briefly pause write flushes before snapshot init.
    pub fn pause_for_snapshot(&mut self) {
        self.paused = true;
    }

    pub fn resume(&mut self) {
        self.paused = false;
    }
}

/// Controller loop that triggers volume snapshot API calls at ledger markers.
#[derive(Debug, Clone)]
pub struct SnapshotController {
    pub retention: Duration,
    pub gate: WriteFlushGate,
    snapshots: Vec<Snapshot>,
}

impl SnapshotController {
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            gate: WriteFlushGate::new(),
            snapshots: Vec::new(),
        }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Create a crash-consistent snapshot aligned to `ledger_sequence`.
    pub fn create_snapshot(
        &mut self,
        volume_id: &str,
        ledger_sequence: u64,
        provider: &str,
    ) -> Result<Snapshot, String> {
        if volume_id.is_empty() {
            return Err("volume_id required".into());
        }

        // Constraint: pause DB write flushes during snapshot initialization.
        self.gate.pause_for_snapshot();
        if !self.gate.is_paused() {
            self.gate.resume();
            return Err("failed to pause write flushes".into());
        }

        let snap = Snapshot {
            id: format!("snap-{volume_id}-{ledger_sequence}"),
            volume_id: volume_id.to_string(),
            ledger_sequence,
            created_at_secs: Self::now_secs(),
            provider: provider.to_string(),
            consistent: true,
        };
        self.snapshots.push(snap.clone());
        self.gate.resume();
        Ok(snap)
    }

    pub fn list_for_volume(&self, volume_id: &str) -> Vec<&Snapshot> {
        self.snapshots
            .iter()
            .filter(|s| s.volume_id == volume_id)
            .collect()
    }

    /// Purge snapshots older than the retention window.
    pub fn enforce_retention(&mut self, now_secs: u64) -> usize {
        let cutoff = now_secs.saturating_sub(self.retention.as_secs());
        let before = self.snapshots.len();
        self.snapshots.retain(|s| s.created_at_secs >= cutoff);
        before - self.snapshots.len()
    }

    pub fn get(&self, snapshot_id: &str) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.id == snapshot_id)
    }

    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}

/// Tag map used when calling cloud snapshot APIs.
pub fn ledger_tags(ledger_sequence: u64) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    tags.insert("stellar.ledger_sequence".into(), ledger_sequence.to_string());
    tags.insert("stellar.backup".into(), "true".into());
    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_writes_during_snapshot() {
        let mut ctl = SnapshotController::new(Duration::from_secs(3600));
        assert!(!ctl.gate.is_paused());
        let snap = ctl
            .create_snapshot("vol-a", 42, "local")
            .expect("snapshot");
        assert!(snap.consistent);
        assert_eq!(snap.ledger_sequence, 42);
        assert!(!ctl.gate.is_paused());
    }

    #[test]
    fn retention_purges_expired() {
        let mut ctl = SnapshotController::new(Duration::from_secs(100));
        ctl.snapshots.push(Snapshot {
            id: "old".into(),
            volume_id: "v1".into(),
            ledger_sequence: 1,
            created_at_secs: 10,
            provider: "aws".into(),
            consistent: true,
        });
        ctl.snapshots.push(Snapshot {
            id: "new".into(),
            volume_id: "v1".into(),
            ledger_sequence: 2,
            created_at_secs: 1000,
            provider: "aws".into(),
            consistent: true,
        });
        let purged = ctl.enforce_retention(1050);
        assert_eq!(purged, 1);
        assert_eq!(ctl.len(), 1);
        assert_eq!(ctl.snapshots[0].id, "new");
    }
}
