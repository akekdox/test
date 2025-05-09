use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::Context;
use common::slot_coverage::SlotCoverage;
use helius::Runtime;
use std::collections::HashMap;
use tokio::sync::{mpsc::Sender, RwLock};

use crate::{
    archive_store::{ArchiveKey, ArchiveKeyExt, ArchiveStore},
    config::ArchiverConfig,
    phase::{ArchiverPhase, Phase},
};

use super::pipeline::PipelineMessage;

pub type ArchiveTarPath = PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Represents a range of slots from start_slot (inclusive) to end_slot (inclusive)
pub struct SlotRange {
    pub start_slot: u64,
    pub end_slot: u64,
}

impl SlotRange {
    /// Creates a new SlotRange with the given start and end slots
    pub fn new(start_slot: u64, end_slot: u64) -> Self {
        Self {
            start_slot,
            end_slot,
        }
    }

    /// Checks if a given slot is contained within this range
    pub fn contains(&self, slot: u64) -> bool {
        slot >= self.start_slot && slot <= self.end_slot
    }

    /// Checks if this range is fully covered by any of the provided ranges
    pub fn is_fully_covered_by(&self, ranges: &[&SlotRange]) -> bool {
        let mut current_slot = self.start_slot;

        // Slow and inefficient, but simple and relatively fast for our use case.
        while current_slot <= self.end_slot {
            let slot_covered = ranges.iter().any(|range| range.contains(current_slot));
            if !slot_covered {
                return false;
            }
            current_slot += 1;
        }
        true
    }
}

#[async_trait::async_trait]
pub trait SlotRangeCache: Send + Sync {
    async fn add_range(&self, range: SlotRange) -> anyhow::Result<()>;
    async fn remove_range(&self, range: &SlotRange) -> anyhow::Result<()>;
    async fn is_range_covered(&self, range: &SlotRange) -> anyhow::Result<bool>;
    async fn cleanup_expired(&self) -> anyhow::Result<()>;
}

pub struct InMemorySlotRangeCache {
    ranges: RwLock<HashMap<SlotRange, SystemTime>>,
    expiration_duration: Duration,
}

impl InMemorySlotRangeCache {
    pub fn new(expiration_duration: Duration) -> Self {
        Self {
            ranges: RwLock::new(HashMap::new()),
            expiration_duration,
        }
    }

    pub fn default() -> Self {
        Self::new(Duration::from_secs(6 * 60 * 60)) // 6 hours
    }
}

#[async_trait::async_trait]
impl SlotRangeCache for InMemorySlotRangeCache {
    async fn add_range(&self, range: SlotRange) -> anyhow::Result<()> {
        let mut ranges = self.ranges.write().await;
        ranges.insert(range, SystemTime::now());
        Ok(())
    }

    async fn remove_range(&self, range: &SlotRange) -> anyhow::Result<()> {
        let mut ranges = self.ranges.write().await;
        ranges.remove(range);
        Ok(())
    }

    async fn is_range_covered(&self, range: &SlotRange) -> anyhow::Result<bool> {
        let ranges = self.ranges.read().await;
        let active_ranges: Vec<&SlotRange> = ranges
            .iter()
            .filter(|(_, timestamp)| {
                if let Ok(elapsed) = timestamp.elapsed() {
                    elapsed < self.expiration_duration
                } else {
                    false
                }
            })
            .map(|(range, _)| range)
            .collect();

        Ok(range.is_fully_covered_by(&active_ranges))
    }

    async fn cleanup_expired(&self) -> anyhow::Result<()> {
        let mut ranges = self.ranges.write().await;
        ranges.retain(|_, timestamp| {
            if let Ok(elapsed) = timestamp.elapsed() {
                elapsed < self.expiration_duration
            } else {
                false
            }
        });
        Ok(())
    }
}

pub struct DownloadStageConfig {
    start_slot: Option<u64>,
    end_slot: Option<u64>,
    work_dir: PathBuf,
}

impl From<&ArchiverConfig> for DownloadStageConfig {
    fn from(config: &ArchiverConfig) -> Self {
        DownloadStageConfig {
            start_slot: config.start_slot,
            end_slot: config.end_slot,
            work_dir: PathBuf::from(config.work_dir.clone()),
        }
    }
}

/// Downloads archives from the archive store, decompresses them, and unpacks them.
pub struct DownloadStage<C: SlotCoverage + 'static> {
    archive_store: Arc<Box<dyn ArchiveStore>>,
    phase: Arc<ArchiverPhase>,
    coverage: Arc<C>,
    slot_range_cache: Arc<dyn SlotRangeCache>,
    config: DownloadStageConfig,
}

impl<C: SlotCoverage + 'static> DownloadStage<C> {
    pub fn new(
        archive_store: Arc<Box<dyn ArchiveStore>>,
        phase: Arc<ArchiverPhase>,
        coverage: Arc<C>,
        config: DownloadStageConfig,
    ) -> Self {
        Self {
            archive_store,
            phase,
            coverage,
            slot_range_cache: Arc::new(InMemorySlotRangeCache::default()),
            config,
        }
    }

    // TODO(nick): Replace local mode with E2E test suite.
    pub async fn start(
        self,
        runtime: &mut Runtime,
        download_tx: Sender<PipelineMessage<PathBuf>>,
    ) -> anyhow::Result<()> {
        // Spawn a periodic cleanup task
        let cache = self.slot_range_cache.clone();
        runtime.spawn_critical::<()>("slot-range-cache-cleanup", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(600));
            loop {
                interval.tick().await;
                if let Err(e) = cache.cleanup_expired().await {
                    tracing::error!("Failed to cleanup expired slot ranges: {:#}", e);
                }
            }
        });

        runtime.spawn_critical::<()>("download-stage", async move {
            tracing::info!("Starting download worker in sequential mode");

            loop {
                // Wait for capacity to be available.
                // This will be sequential if the buffer size is 1.
                let Ok(permit) = download_tx.reserve().await else {
                    tracing::error!("Failed to reserve download slot");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };

                let archive_key = match self.get_next_archive().await {
                    Ok(Some(k)) => k,
                    Ok(None) => {
                        tracing::info!("No files available, waiting...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("Error getting next unprocessed archive: {:#}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                match self.download_archive(archive_key.clone()).await {
                    Ok(local_path) => {
                        tracing::info!("Download complete, sending to processor: {}", &archive_key);
                        // Send should never fail unless the channel was closed.
                        let _ = permit.send(PipelineMessage::new(
                            archive_key.into_archive_name(),
                            local_path,
                        ));

                        // Get archive metadata to access slot range
                        if let Ok(metadata) = self
                            .archive_store
                            .get_archive_metadata(archive_key.clone())
                            .await
                        {
                            // Add the slot range to seen ranges
                            let _ = self
                                .slot_range_cache
                                .add_range(SlotRange::new(metadata.start_slot, metadata.end_slot))
                                .await;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error handling next archive: {:#}", e);
                    }
                }
            }
        });

        Ok(())
    }

    async fn download_archive(&self, archive_key: ArchiveKey) -> anyhow::Result<ArchiveTarPath> {
        self.phase
            .advance_to(Phase::Downloading, &archive_key.into_archive_name())
            .await;
        let archive_dir = self
            .archive_store
            .download(self.config.work_dir.clone(), archive_key)
            .await
            .context("Failed to download archive")?;
        Ok(archive_dir)
    }

    async fn get_next_archive(&self) -> anyhow::Result<Option<ArchiveKey>> {
        let (start_slot, end_slot) = (self.config.start_slot, self.config.end_slot);
        tracing::info!(
            "Checking for next unprocessed archive in slot range: {:?}-{:?}",
            start_slot,
            end_slot
        );

        let mut unprocessed_archives = self.archive_store.list_unprocessed_archives().await?;

        // Sort by slot number in reverse order (highest first)
        unprocessed_archives.sort_by(|a, b| b.start_slot.cmp(&a.start_slot));

        // Process archives in order
        for archive in unprocessed_archives {
            tracing::debug!(
                "Checking archive with start slot {}: {}",
                archive.start_slot,
                archive.key
            );

            // Check if slot is within our range
            if !archive.includes_slot_within_range(start_slot, end_slot) {
                tracing::debug!(
                    "Skipping slot {} as it's outside range {:?}-{:?}",
                    archive.start_slot,
                    start_slot,
                    end_slot
                );
                continue;
            }

            let seen_ranges = self
                .slot_range_cache
                .is_range_covered(&SlotRange::new(archive.start_slot, archive.end_slot))
                .await?;

            if seen_ranges {
                tracing::info!(
                    "Skipping archive with slot range {}-{} as it is fully covered by existing ranges",
                    archive.start_slot,
                    archive.end_slot
                );
                continue;
            }

            // Check if slots are already indexed in Clickhouse
            if self
                .is_slot_range_fully_indexed(archive.start_slot, archive.end_slot)
                .await?
            {
                tracing::debug!(
                    "Skipping archive with start slot {} as it exists in Clickhouse",
                    archive.start_slot
                );
                continue;
            }

            tracing::info!("Found eligible archive: {}", archive.key);
            return Ok(Some(archive.key));
        }

        tracing::info!(
            "No eligible archives found in slot range {:?}-{:?}",
            start_slot,
            end_slot
        );
        Ok(None)
    }

    async fn is_slot_range_fully_indexed(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> anyhow::Result<bool> {
        let unique_slots_count = self
            .coverage
            .indexed_slots_in_range(start_slot, end_slot)
            .await
            .context("Failed to get indexed slots in range")?
            .len() as u64;

        // Calculate the expected number of slots and the coverage percentage
        let expected_slots = end_slot - start_slot + 1;
        let coverage_percentage = (unique_slots_count as f64 / expected_slots as f64) * 100.0;
        tracing::info!(
            "Slot range {}-{}: Found {} unique slots ({:.2}% coverage)",
            start_slot,
            end_slot,
            unique_slots_count,
            coverage_percentage
        );
        Ok(coverage_percentage >= 100.0)
    }
}

#[cfg(test)]
mod tests {
    use common::slot_coverage::test::MockSlotCoverage;
    use helius::RuntimeBuilder;
    use tokio::{sync::mpsc, time::timeout};

    use crate::archive_store::{ArchiveMetadata, ArchiveSummary};
    use crate::blockstore::BlockstoreApi;

    use super::*;

    struct MockArchiveStore {
        archives: Vec<ArchiveMetadata>,
    }

    impl MockArchiveStore {
        fn new(archives: Vec<ArchiveMetadata>) -> Self {
            Self { archives }
        }
    }

    #[async_trait::async_trait]
    impl ArchiveStore for MockArchiveStore {
        async fn list_unprocessed_archives(&self) -> anyhow::Result<Vec<ArchiveMetadata>> {
            Ok(self.archives.clone())
        }

        async fn download(
            &self,
            _work_dir: PathBuf,
            archive_key: ArchiveKey,
        ) -> anyhow::Result<ArchiveTarPath> {
            Ok(PathBuf::from(format!(
                "{}.tar.zst",
                archive_key.into_archive_name()
            )))
        }

        async fn summary(&self) -> anyhow::Result<ArchiveSummary> {
            Ok(ArchiveSummary {
                total: 0,
                processed: 0,
                unprocessed: 0,
            })
        }

        async fn get_archive_metadata(
            &self,
            object_key: ArchiveKey,
        ) -> anyhow::Result<ArchiveMetadata> {
            self.archives
                .iter()
                .find(|archive| archive.key == object_key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Archive not found: {}", object_key))
        }

        async fn update_archive_metadata(
            &self,
            _blockstore: Box<dyn BlockstoreApi>,
            _object_key: ArchiveKey,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    async fn assert_message_received(
        rx: &mut mpsc::Receiver<PipelineMessage<PathBuf>>,
        expected_archive_name: &str,
        expected_archive_path: &str,
    ) {
        let PipelineMessage {
            archive_name,
            message: downloaded_archive_path,
        } = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("no timeout")
            .expect("message should be received");
        assert_eq!(archive_name, expected_archive_name);
        assert_eq!(
            downloaded_archive_path,
            PathBuf::from(expected_archive_path)
        );
    }

    async fn assert_no_message_received(rx: &mut mpsc::Receiver<PipelineMessage<PathBuf>>) {
        let out = timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(out.is_err(), "message should not be received");
    }

    #[tokio::test]
    async fn test_download_stage_no_slot_filter() {
        let expected_archives = vec![
            ArchiveMetadata {
                key: "unprocessed/rocksdb_0.tar.zst".to_string(),
                start_slot: 0,
                end_slot: 100,
                data_size: 100,
            },
            ArchiveMetadata {
                key: "unprocessed/rocksdb_200.tar.zst".to_string(),
                start_slot: 200,
                end_slot: 500,
                data_size: 100,
            },
            ArchiveMetadata {
                key: "unprocessed/rocksdb_1000.tar.zst".to_string(),
                start_slot: 1000,
                end_slot: 1100,
                data_size: 100,
            },
        ];
        let config = ArchiverConfig::default();
        let archive_store: Arc<Box<dyn ArchiveStore>> =
            Arc::new(Box::new(MockArchiveStore::new(expected_archives)));
        let mock_coverage = Arc::new(MockSlotCoverage::new(0..=100));
        let phase = Arc::new(ArchiverPhase::new());
        let download_stage = DownloadStage::new(
            archive_store,
            phase,
            mock_coverage,
            DownloadStageConfig::from(&config),
        );

        let mut runtime = RuntimeBuilder::new("test").init().unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        download_stage.start(&mut runtime, tx).await.unwrap();

        // First archive should be the highest slot.
        assert_message_received(&mut rx, "rocksdb_1000", "rocksdb_1000.tar.zst").await;

        // Should move to next archive due to the slot range check, despite the slots not being indexed yet.
        assert_message_received(&mut rx, "rocksdb_200", "rocksdb_200.tar.zst").await;

        // Should find no more archives because all slots have been indexed between 0-100.
        assert_no_message_received(&mut rx).await;
    }

    #[tokio::test]
    async fn test_download_stage_with_start_slot() {
        // Do not expect any archives to be downloaded besides #3.
        let start_slot = 600;
        let expected_archives = vec![
            ArchiveMetadata {
                key: "unprocessed/rocksdb_0.tar.zst".to_string(),
                start_slot: 0,
                end_slot: 100,
                data_size: 100,
            },
            ArchiveMetadata {
                key: "unprocessed/rocksdb_200.tar.zst".to_string(),
                start_slot: 200,
                end_slot: 500,
                data_size: 100,
            },
            ArchiveMetadata {
                key: "unprocessed/rocksdb_1000.tar.zst".to_string(),
                start_slot: 1000,
                end_slot: 1100,
                data_size: 100,
            },
        ];
        let mock_coverage = Arc::new(MockSlotCoverage::new(0..=100));
        let config = ArchiverConfig {
            start_slot: Some(start_slot),
            ..Default::default()
        };
        let archive_store: Arc<Box<dyn ArchiveStore>> =
            Arc::new(Box::new(MockArchiveStore::new(expected_archives)));
        let phase = Arc::new(ArchiverPhase::new());
        let download_stage = DownloadStage::new(
            archive_store,
            phase,
            mock_coverage,
            DownloadStageConfig::from(&config),
        );

        let mut runtime = RuntimeBuilder::new("test").init().unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        download_stage.start(&mut runtime, tx).await.unwrap();

        // First archive should be the highest slot.
        assert_message_received(&mut rx, "rocksdb_1000", "rocksdb_1000.tar.zst").await;

        // No more archives should be downloaded because the start slot is 600.
        assert_no_message_received(&mut rx).await;
    }

    #[tokio::test]
    async fn test_download_stage_with_end_slot() {
        // Do not expect any archives to be downloaded besides #1.
        let end_slot = 150;
        let expected_archives = vec![
            ArchiveMetadata {
                key: "unprocessed/rocksdb_0.tar.zst".to_string(),
                start_slot: 0,
                end_slot: 100,
                data_size: 100,
            },
            ArchiveMetadata {
                key: "unprocessed/rocksdb_200.tar.zst".to_string(),
                start_slot: 200,
                end_slot: 500,
                data_size: 100,
            },
            ArchiveMetadata {
                key: "unprocessed/rocksdb_1000.tar.zst".to_string(),
                start_slot: 1000,
                end_slot: 1100,
                data_size: 100,
            },
        ];
        // Partial coverage of the first archive.
        let mock_coverage = Arc::new(MockSlotCoverage::new(0..=50));
        let config = ArchiverConfig {
            end_slot: Some(end_slot),
            ..Default::default()
        };
        let archive_store: Arc<Box<dyn ArchiveStore>> =
            Arc::new(Box::new(MockArchiveStore::new(expected_archives)));
        let phase = Arc::new(ArchiverPhase::new());
        let download_stage = DownloadStage::new(
            archive_store,
            phase,
            mock_coverage,
            DownloadStageConfig::from(&config),
        );

        let mut runtime = RuntimeBuilder::new("test").init().unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        download_stage.start(&mut runtime, tx).await.unwrap();

        // First archive should be the highest slot.
        assert_message_received(&mut rx, "rocksdb_0", "rocksdb_0.tar.zst").await;
        // No more archives should be downloaded because the end slot is 150.
        assert_no_message_received(&mut rx).await;
    }
}
