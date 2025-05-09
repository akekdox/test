use std::{collections::HashMap, time::Duration};

use cadence_macros::statsd_gauge;
use tokio::sync::RwLock;

/// Responsible for tracking the lifecycle of each archive as it moves through the ETL pipeline.
pub struct ArchiverPhase {
    dict: RwLock<HashMap<String, Phase>>,
}

impl ArchiverPhase {
    pub fn new() -> Self {
        Self {
            dict: RwLock::new(HashMap::new()),
        }
    }

    pub async fn advance_to(&self, next: Phase, archive_name: &str) {
        let mut guard = self.dict.write().await;
        guard
            .entry(archive_name.to_string())
            .and_modify(|current| *current = current.advance_to(next))
            .or_insert(next);
    }

    pub async fn until_completed(&self, archive_name: &str) {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        loop {
            interval.tick().await;
            {
                let guard = self.dict.read().await;
                if guard
                    .get(archive_name)
                    .map(|p| *p == Phase::Completed)
                    .unwrap_or(false)
                {
                    break;
                }
            }
        }
    }

    pub async fn is_waiting(&self) -> bool {
        let guard = self.dict.read().await;
        guard.values().all(|phase| *phase == Phase::Completed)
    }

    pub async fn cleanup_completed(&self) {
        let mut guard = self.dict.write().await;
        guard.retain(|_, phase| *phase != Phase::Completed);
    }

    pub async fn publish_metrics(&self) {
        // If no archives are being processed, we are in the waiting phase.
        if self.is_waiting().await {
            statsd_gauge!("phase", 0, "phase" => "waiting", "archive" => "none");
            return;
        }

        let guard = self.dict.read().await;
        for (archive_name, phase) in guard.iter() {
            statsd_gauge!("phase", phase.as_value(), "phase" => phase.as_str(), "archive" => archive_name);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    Downloading,
    Decompressing,
    Uploading,
    Completed,
}

impl Phase {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Downloading => "downloading",
            Self::Decompressing => "decompressing",
            Self::Uploading => "uploading",
            Self::Completed => "completed",
        }
    }

    fn as_value(&self) -> u64 {
        match self {
            Self::Downloading => 1,
            Self::Decompressing => 2,
            Self::Uploading => 3,
            Self::Completed => 4,
        }
    }

    fn can_transition_to(&self, next: Phase) -> bool {
        use Phase::*;
        match (*self, next) {
            // Normal flow transitions
            (Downloading, Decompressing) => true,
            (Decompressing, Uploading) => true,
            (Uploading, Completed) => true,
            // All other transitions are invalid
            _ => false,
        }
    }

    fn advance_to(&self, next: Phase) -> Phase {
        if self.can_transition_to(next) {
            next
        } else {
            tracing::error!(
                "Invalid phase transition from {} to {}",
                self.as_str(),
                next.as_str()
            );
            *self
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_phase_transition() {
        let phase = Phase::Downloading;
        let next_phase = phase.advance_to(Phase::Decompressing);
        assert_eq!(next_phase, Phase::Decompressing);
    }

    #[test]
    fn test_invalid_phase_transition() {
        let phase = Phase::Downloading;
        let next_phase = phase.advance_to(Phase::Completed);
        assert_eq!(next_phase, Phase::Downloading);
    }

    #[tokio::test]
    async fn test_multi_archive_phase_tracking() {
        let phase = ArchiverPhase::new();
        let one = "archive_1".to_string();
        let two = "archive_2".to_string();

        phase.advance_to(Phase::Downloading, &one).await;
        phase.advance_to(Phase::Decompressing, &one).await;
        phase.advance_to(Phase::Downloading, &two).await;
        phase.advance_to(Phase::Uploading, &one).await;
        {
            let guard = phase.dict.read().await;
            assert_eq!(guard.get(&one).unwrap(), &Phase::Uploading);
            assert_eq!(guard.get(&two).unwrap(), &Phase::Downloading);
        }

        phase.advance_to(Phase::Decompressing, &two).await;
        phase.advance_to(Phase::Uploading, &two).await;
        phase.advance_to(Phase::Completed, &one).await;
        phase.advance_to(Phase::Completed, &two).await;
        {
            let guard = phase.dict.read().await;
            assert_eq!(guard.get(&one).unwrap(), &Phase::Completed);
            assert_eq!(guard.get(&two).unwrap(), &Phase::Completed);
        }
    }

    #[tokio::test]
    async fn test_cleanup() {
        let phase = ArchiverPhase::new();
        let should_evict = "archive_1".to_string();
        let should_remain = "archive_2".to_string();
        let should_remain_2 = "archive_3".to_string();
        phase.advance_to(Phase::Completed, &should_evict).await;
        phase.advance_to(Phase::Decompressing, &should_remain).await;
        phase.advance_to(Phase::Uploading, &should_remain_2).await;
        phase.cleanup_completed().await;
        {
            let guard = phase.dict.read().await;
            assert_eq!(guard.get(&should_evict), None);
            assert_eq!(guard.get(&should_remain).unwrap(), &Phase::Decompressing);
            assert_eq!(guard.get(&should_remain_2).unwrap(), &Phase::Uploading);
        }
    }

    #[tokio::test]
    async fn test_until_completed() {
        let phase = Arc::new(ArchiverPhase::new());
        phase.advance_to(Phase::Downloading, "archive_1").await;
        let phase_clone = phase.clone();
        let handle = tokio::spawn(async move {
            phase_clone.until_completed("archive_1").await;
        });
        assert!(!handle.is_finished());
        phase.advance_to(Phase::Decompressing, "archive_1").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());
        phase.advance_to(Phase::Uploading, "archive_1").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());
        phase.advance_to(Phase::Completed, "archive_1").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(handle.is_finished());
    }
}
