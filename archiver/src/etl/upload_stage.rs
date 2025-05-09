use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use cadence_macros::{statsd_count, statsd_gauge};
use common::{
    db::Slot, external_block::ExternalBlock, serialization::BlockContents,
    slot_coverage::SlotCoverage, uploader::Uploader,
};
use helius::Runtime;
use lazy_static::lazy_static;
use solana_transaction_status::ConfirmedBlock;
use tokio::sync::{
    mpsc::{self, Receiver},
    Semaphore,
};

use crate::{
    blockstore::BlockstoreApi,
    config::ArchiverConfig,
    etl::download_stage::SlotRangeCache,
    phase::{ArchiverPhase, Phase},
    util::delete_file_or_directory,
};

use super::{download_stage::{InMemorySlotRangeCache, SlotRange}, pipeline::PipelineMessage};

lazy_static! {
    static ref MAX_CONCURRENT_WRITES: usize =
        std::thread::available_parallelism().unwrap().get() / 2;
}

const MAX_BLOCK_UPLOAD_BATCH_SIZE: usize = 500;

pub struct UploadStageConfig {
    start_slot: Option<Slot>,
    end_slot: Option<Slot>,
    cleanup_files: bool,
}

impl From<&ArchiverConfig> for UploadStageConfig {
    fn from(config: &ArchiverConfig) -> Self {
        Self {
            start_slot: config.start_slot,
            end_slot: config.end_slot,
            cleanup_files: !helius::Environment::is_local().unwrap_or(true),
        }
    }
}

/// The upload stage is responsible for uploading finalized blocks to the DB.
pub struct UploadStage<U: Uploader + 'static, C: SlotCoverage + 'static, E: ExternalBlock + 'static>
{
    uploader: Arc<U>,
    coverage: Arc<C>,
    external_block: Arc<E>,
    phase: Arc<ArchiverPhase>,
    config: UploadStageConfig,
    slot_range_cache: Arc<dyn SlotRangeCache>,
}

impl<U: Uploader + 'static, C: SlotCoverage + 'static, E: ExternalBlock + 'static>
    UploadStage<U, C, E>
{
    pub fn new(
        uploader: Arc<U>,
        coverage: Arc<C>,
        external_block: Arc<E>,
        phase: Arc<ArchiverPhase>,
        config: UploadStageConfig,
    ) -> Self {
        Self {
            uploader,
            coverage,
            external_block,
            phase,
            config,
            slot_range_cache: Arc::new(
                InMemorySlotRangeCache::default(),
            ),
        }
    }

    pub fn start(
        self,
        runtime: &mut Runtime,
        mut blockstore_rx: Receiver<PipelineMessage<Box<dyn BlockstoreApi>>>,
    ) -> anyhow::Result<()> {
        runtime.spawn_critical::<()>("upload-stage", async move {
            while let Some(message) = blockstore_rx.recv().await {
                let PipelineMessage {
                    archive_name,
                    message: blockstore,
                } = message;
                let ledger_path = blockstore.ledger_path().to_owned();

                // Get slot range directly from blockstore
                let slot_range = match (blockstore.first_available_block(), blockstore.max_root()) {
                    (Ok(start), end) => {
                        Some(SlotRange::new(start, end))
                    }
                    _ => None,
                };

                self.phase.advance_to(Phase::Uploading, &archive_name).await;
                match handle_blockstore_upload(
                    &archive_name,
                    blockstore,
                    self.uploader.clone(),
                    self.coverage.clone(),
                    self.external_block.clone(),
                    &self.config,
                )
                .await
                {
                    Ok(_) => {
                        tracing::info!("Successfully uploaded archive {}", archive_name);
                        statsd_count!("upload_stage.archive.upload_success", 1);
                    }
                    Err(e) => {
                        // Remove the slot range from cache on failure to allow retry
                        if let Some(range) = &slot_range {
                            if let Err(cache_err) = self.slot_range_cache.remove_range(range).await
                            {
                                tracing::error!(
                                    "Failed to remove slot range from cache: {:#}",
                                    cache_err
                                );
                            }
                        }
                        tracing::error!("Failed to upload archive {}: {:#}", archive_name, e);
                        statsd_count!("upload_stage.archive.upload_failure", 1);
                    }
                }
                self.phase.advance_to(Phase::Completed, &archive_name).await;

                if self.config.cleanup_files {
                    if let Err(e) = delete_file_or_directory(&ledger_path).await {
                        tracing::warn!("Archive cleanup failed: {}", e);
                    }
                }
            }
        });
        Ok(())
    }
}

async fn handle_blockstore_upload(
    archive_name: &str,
    blockstore: Box<dyn BlockstoreApi>,
    uploader: Arc<impl Uploader + 'static>,
    coverage: Arc<impl SlotCoverage>,
    external_block: Arc<impl ExternalBlock>,
    config: &UploadStageConfig,
) -> anyhow::Result<()> {
    // Take the union of the archiver's start/end slots and the archives start/end slots.
    let start_slot = config
        .start_slot
        .unwrap_or(u64::MAX)
        .min(blockstore.first_available_block()?);
    let end_slot = config.end_slot.unwrap_or(0).max(blockstore.max_root());
    tracing::info!(
        "Uploading slots between {} and {} for archive {}",
        start_slot,
        end_slot,
        archive_name
    );

    // Get all slots that are finalized and skipped, but not yet indexed.
    let (finalized_slots, skipped_slots) =
        get_indexable_slots(&blockstore, coverage, start_slot, end_slot).await?;

    // Handle skipped slots in one shot.
    tracing::info!(
        "Uploading {} skipped slots for archive {}",
        skipped_slots.len(),
        archive_name
    );
    uploader
        .upload_skipped_slots(skipped_slots)
        .await
        .context("Failed to upload skipped slots")?;

    // Spawn an upload task that will upload blocks in parallel.
    // Push finalized blocks to the channel and the uploader task will pick them up.
    // The upload task handle will finish once all blocks have been uploaded.
    tracing::info!(
        "Uploading {} finalized slots for archive {} in parallel",
        finalized_slots.len(),
        archive_name
    );
    let mut progress = UploadProgress::new(archive_name, finalized_slots.len() as u64);
    let (upload_tx, upload_rx) = mpsc::channel(MAX_BLOCK_UPLOAD_BATCH_SIZE);
    let upload_task = spawn_uploader_task(upload_rx, uploader.clone());
    for slot in finalized_slots {
        let Ok(versioned_block) =
            get_block_with_metadata_fallback(slot, &blockstore, external_block.clone()).await
        else {
            tracing::error!("Failed to get block for slot {:#}", slot);
            statsd_count!("upload_stage.block.extract_failure", 1);
            continue;
        };
        let confirmed_block = ConfirmedBlock::from(versioned_block);

        // Should never fail unless the channel is dropped.
        upload_tx
            .send((slot, confirmed_block))
            .await
            .context("Failed to send block to uploader")?;

        progress.increment();
        progress.report_if_needed();
    }

    // Drop the upload tx to signal the upload task to exit.
    drop(upload_tx);
    tokio::time::timeout(Duration::from_secs(10), upload_task)
        .await
        .context("timeout waiting for upload task to complete")?
        .context("Upload task panicked")?;
    Ok(())
}

/// Attempts to fetch a block from the blockstore. If the block does not have metadata,
/// it will attempt to fetch the block from the external source.
///
/// If the external block is also missing metadata, we will log a warning and upload the block
/// regardless.
async fn get_block_with_metadata_fallback(
    slot: Slot,
    blockstore: &Box<dyn BlockstoreApi>,
    external_block: Arc<impl ExternalBlock>,
) -> anyhow::Result<ConfirmedBlock> {
    // Attempt to extract the block from the blockstore.
    match blockstore.get_block_for_slot(slot) {
        Ok(BlockContents::VersionedConfirmedBlock(block)) => {
            statsd_count!("upload_stage.block.extract_success", 1);
            return Ok(ConfirmedBlock::from(block));
        }
        Ok(BlockContents::BlockWithoutMetadata(_)) => {
            tracing::warn!(
                "Slot {} doesn't have metadata, attempting to fetch metadata from external source",
                slot
            );
            statsd_count!("upload_stage.block.missing_metadata", 1);
        }
        Err(e) => {
            tracing::error!(
                "Failed to extract slot {} from archive (will fallback to external source): {:#}",
                slot,
                e
            );
            statsd_count!("upload_stage.block.extract_failure", 1);
        }
    }

    // Fallback to external source.
    let fallback_block = external_block.fetch(slot).await.map_err(|e| {
        tracing::error!("Failed to fetch block from external source: {}", e);
        statsd_count!("upload_stage.block.external_failure", 1);
        e
    })?;

    // Check if fallback block is missing metadata for visibility.
    if fallback_block
        .transactions
        .first()
        .map(|tx| tx.get_status_meta())
        .is_none()
    {
        tracing::warn!(
            "Block {} has no metadata in external source. Uploading it regardless.",
            slot
        );
        statsd_count!("upload_stage.block.external_missing_metadata", 1);
    }

    statsd_count!("upload_stage.block.external_success", 1);
    Ok(fallback_block)
}

async fn get_indexable_slots(
    blockstore: &Box<dyn BlockstoreApi>,
    coverage: Arc<impl SlotCoverage>,
    start_slot: u64,
    end_slot: u64,
) -> anyhow::Result<(Vec<u64>, Vec<u64>)> {
    let indexed_slots = coverage
        .indexed_slots_in_range(start_slot, end_slot)
        .await
        .context("Failed to get indexed slots in range")?
        .into_iter()
        .collect::<HashSet<_>>();
    let (finalized_slots, skipped_slots) = (start_slot..=end_slot)
        .filter(|slot| !indexed_slots.contains(slot))
        .partition(|slot| blockstore.is_root(*slot));
    Ok((finalized_slots, skipped_slots))
}

/// Spawns a task that is responsible for gathering blocks from the channel
/// and uploading them to the database in large batches in parallel.
fn spawn_uploader_task(
    mut upload_rx: Receiver<(Slot, ConfirmedBlock)>,
    uploader: Arc<impl Uploader + 'static>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(*MAX_CONCURRENT_WRITES));
        let mut handles = Vec::new();

        let mut batch = Vec::new();
        while let Some(record) = upload_rx.recv().await {
            batch.push(record);
            if batch.len() == MAX_BLOCK_UPLOAD_BATCH_SIZE {
                // Move the full batch out and spawn an upload task.
                let current_batch = std::mem::take(&mut batch);
                handles.push(spawn_upload_block_batch(
                    current_batch,
                    uploader.clone(),
                    semaphore.clone(),
                ));
            }
        }

        // Upload any remaining blocks.
        if !batch.is_empty() {
            handles.push(spawn_upload_block_batch(
                batch,
                uploader.clone(),
                semaphore.clone(),
            ));
        }

        // Wait for every spawned upload to complete.
        for handle in handles {
            let _ = handle.await;
        }
    })
}

/// Spawns a task that uploads a batch of blocks while recording metrics.
/// Concurrency is limited by the provided semaphore (one permit per upload).
fn spawn_upload_block_batch<U: Uploader + Send + Sync + 'static>(
    batch: Vec<(Slot, ConfirmedBlock)>,
    uploader: Arc<U>,
    semaphore: Arc<Semaphore>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Ensure we do not exceed the parallel-upload limit.
        let _permit = semaphore.acquire_owned().await.expect("semaphore dropped");
        let batch_size = batch.len();
        match uploader.upload_blocks(batch).await {
            Ok(_) => {
                statsd_count!("upload_stage.block.upload_success", batch_size as i64);
            }
            Err(e) => {
                tracing::error!("Failed to upload blocks: {:#}", e);
                statsd_count!("upload_stage.block.upload_failure", batch_size as i64);
            }
        }
    })
}

#[derive(Debug, Clone)]
struct UploadProgress {
    archive_name: String,
    total_slots: u64,
    upload_count: u64,
    last_report_count: u64,
    last_report_time: Instant,
    start_time: Instant,
}

impl UploadProgress {
    fn new(archive_name: &str, total_slots: u64) -> Self {
        Self {
            archive_name: archive_name.to_string(),
            total_slots,
            upload_count: 0,
            last_report_count: 0,
            last_report_time: Instant::now(),
            start_time: Instant::now(),
        }
    }

    fn increment(&mut self) {
        self.upload_count += 1;
    }

    fn report_if_needed(&mut self) {
        let UploadProgress {
            archive_name,
            total_slots,
            upload_count,
            last_report_count,
            last_report_time,
            start_time,
        } = self;

        // Only report every 1000 records.
        if *upload_count % 1000 != 0 {
            return;
        }

        let now = Instant::now();
        let duration = now.duration_since(*last_report_time);
        let uploaded_since_last = *upload_count - *last_report_count;

        // Calculate rates and progress
        let current_rate = if duration.as_secs_f64() >= 0.1 {
            uploaded_since_last as f64 / duration.as_secs_f64()
        } else {
            0.0
        };
        let progress_percentage = (*upload_count as f64 / *total_slots as f64) * 100.0;
        let overall_rate = *upload_count as f64 / start_time.elapsed().as_secs_f64();

        tracing::info!(
            archive = archive_name,
            "Processed {} of {} slots ({:.2}%) of archive {}. Current rate: {:.2} blocks/sec, Overall rate: {:.2} blocks/sec",
            upload_count, total_slots, progress_percentage, archive_name, current_rate, overall_rate,
        );
        statsd_gauge!("ledger_tool.progress_percentage", progress_percentage);
        statsd_gauge!("ledger_tool.overall_rate", overall_rate);
        statsd_gauge!("ledger_tool.total_records", overall_rate);

        // Update the last report time and count.
        self.last_report_time = now;
        self.last_report_count = *upload_count;
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};

    use common::{
        external_block::test::MockExternalBlock, serialization::BlockWithoutMetadata,
        slot_coverage::test::MockSlotCoverage,
    };
    use solana_transaction_status::VersionedConfirmedBlock;

    use super::*;

    struct MockBlockstore {
        rooted_slots: Vec<Slot>,
        missing_metadata_slots: Vec<Slot>,
    }

    impl MockBlockstore {
        fn new(rooted_slots: Vec<Slot>, missing_metadata_slots: Vec<Slot>) -> Self {
            Self {
                rooted_slots,
                missing_metadata_slots,
            }
        }
    }

    impl BlockstoreApi for MockBlockstore {
        fn ledger_path(&self) -> &Path {
            Path::new("test_ledger")
        }

        fn is_root(&self, slot: Slot) -> bool {
            self.rooted_slots.contains(&slot)
        }

        fn max_root(&self) -> Slot {
            self.rooted_slots.last().cloned().unwrap_or(0)
        }

        fn first_available_block(&self) -> anyhow::Result<Slot> {
            Ok(self.rooted_slots.first().cloned().unwrap_or(0))
        }

        fn get_block_for_slot(&self, slot: Slot) -> anyhow::Result<BlockContents> {
            let block = mock_versioned_confirmed_block(slot);
            if self.missing_metadata_slots.contains(&slot) {
                Ok(BlockContents::BlockWithoutMetadata(BlockWithoutMetadata {
                    blockhash: block.blockhash,
                    parent_slot: block.parent_slot,
                    transactions: vec![],
                }))
            } else {
                Ok(BlockContents::VersionedConfirmedBlock(block))
            }
        }
    }

    struct MockUploader {
        blocks_uploaded: Mutex<HashSet<Slot>>,
        skipped_slots: Mutex<HashSet<Slot>>,
    }

    impl MockUploader {
        fn new() -> Self {
            Self {
                blocks_uploaded: Mutex::new(HashSet::new()),
                skipped_slots: Mutex::new(HashSet::new()),
            }
        }

        fn assert_blocks_uploaded(&self, slots: Vec<Slot>) {
            let blocks_uploaded = self.blocks_uploaded.lock().unwrap();
            for slot in slots {
                assert!(
                    blocks_uploaded.contains(&slot),
                    "Block {} was not uploaded",
                    slot
                );
            }
        }

        fn assert_skipped_slots(&self, slots: Vec<Slot>) {
            let skipped_slots = self.skipped_slots.lock().unwrap();
            for slot in slots {
                assert!(
                    skipped_slots.contains(&slot),
                    "Slot {} was not skipped",
                    slot
                );
            }
        }
    }

    #[async_trait::async_trait]
    impl Uploader for MockUploader {
        async fn upload_blocks(&self, blocks: Vec<(Slot, ConfirmedBlock)>) -> anyhow::Result<()> {
            let skipped_slots = self.skipped_slots.lock().unwrap();
            let mut blocks_uploaded = self.blocks_uploaded.lock().unwrap();
            for (slot, _) in blocks {
                if blocks_uploaded.contains(&slot) || skipped_slots.contains(&slot) {
                    panic!(
                        "Attempted to uploaded skipped slot {}, but it was already uploaded",
                        slot
                    );
                }
                blocks_uploaded.insert(slot);
            }
            Ok(())
        }

        async fn upload_skipped_slots(&self, slots: Vec<Slot>) -> anyhow::Result<()> {
            let mut skipped_slots = self.skipped_slots.lock().unwrap();
            let blocks_uploaded = self.blocks_uploaded.lock().unwrap();
            for slot in slots {
                if skipped_slots.contains(&slot) || blocks_uploaded.contains(&slot) {
                    panic!(
                        "Attempted to uploaded skipped slot {}, but it was already uploaded",
                        slot
                    );
                }
                skipped_slots.insert(slot);
            }
            Ok(())
        }
    }

    fn mock_versioned_confirmed_block(slot: Slot) -> VersionedConfirmedBlock {
        VersionedConfirmedBlock {
            blockhash: format!("blockhash-{}", slot),
            previous_blockhash: format!("previous-blockhash-{}", slot),
            parent_slot: slot,
            transactions: vec![],
            rewards: vec![],
            num_partitions: Some(0),
            block_time: None,
            block_height: Some(0),
        }
    }

    #[tokio::test]
    async fn test_handle_blockstore_upload() {
        let archive_name = "test_archive";
        let indexed_slots = vec![];
        let slots_in_archive = (0..10).collect::<Vec<_>>();
        let skipped_slots = vec![3, 8];
        let rooted_slots = slots_in_archive
            .iter()
            .filter(|slot| !skipped_slots.contains(slot))
            .copied()
            .collect();
        let missing_metadata_slots = vec![2, 7];

        // No response from external source slot #7
        let external_blocks = vec![(2 as u64, mock_versioned_confirmed_block(2).into())];

        let blockstore = Box::new(MockBlockstore::new(rooted_slots, missing_metadata_slots));
        let coverage = Arc::new(MockSlotCoverage::new(indexed_slots.into_iter()));
        let external_block = Arc::new(MockExternalBlock::new(external_blocks));
        let uploader = Arc::new(MockUploader::new());
        let config = UploadStageConfig {
            start_slot: None,
            end_slot: None,
            cleanup_files: false,
        };

        handle_blockstore_upload(
            archive_name,
            blockstore,
            uploader.clone(),
            coverage,
            external_block,
            &config,
        )
        .await
        .unwrap();

        // Note that block #7 will be missing because it was not found in the external source.
        uploader.assert_blocks_uploaded(vec![0, 1, 2, 4, 5, 6, 9]);
        uploader.assert_skipped_slots(vec![3, 8]);
    }

    #[tokio::test]
    async fn test_handle_blockstore_high_concurrency() {
        // Intentionally use a number of slots that is not a multiple of the batch size.
        // This will verify that the parallel uploader will await until all blocks have been uploaded.
        let archive_name = "test_archive";
        let indexed_slots = vec![];
        let slots_in_archive = (0..8777).collect::<Vec<_>>();
        let skipped_slots = vec![50, 477];
        let rooted_slots = slots_in_archive
            .iter()
            .filter(|slot| !skipped_slots.contains(slot))
            .copied()
            .collect::<Vec<_>>();
        let missing_metadata_slots = vec![];
        let external_blocks = vec![];
        let blockstore = Box::new(MockBlockstore::new(
            rooted_slots.clone(),
            missing_metadata_slots,
        ));
        let coverage = Arc::new(MockSlotCoverage::new(indexed_slots.into_iter()));
        let external_block = Arc::new(MockExternalBlock::new(external_blocks));
        let uploader = Arc::new(MockUploader::new());
        let config = UploadStageConfig {
            start_slot: None,
            end_slot: None,
            cleanup_files: false,
        };

        handle_blockstore_upload(
            archive_name,
            blockstore,
            uploader.clone(),
            coverage,
            external_block,
            &config,
        )
        .await
        .unwrap();

        uploader.assert_blocks_uploaded(rooted_slots);
        uploader.assert_skipped_slots(skipped_slots);
    }
}
