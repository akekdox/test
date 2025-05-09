use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use common::{external_block::ExternalBlock, slot_coverage::SlotCoverage, uploader::Uploader};
use helius::Runtime;
use tokio::sync::mpsc;

use crate::{
    archive_store::ArchiveStore, blockstore::BlockstoreApi, config::ArchiverConfig,
    decompress::Decompress, phase::ArchiverPhase,
};

use super::{
    decompress_stage::DecompressStage,
    download_stage::{DownloadStage, DownloadStageConfig},
    upload_stage::{UploadStage, UploadStageConfig},
};

pub struct PipelineMessage<T> {
    pub archive_name: String,
    pub message: T,
}

impl<T> PipelineMessage<T> {
    pub fn new(archive_name: String, message: T) -> Self {
        Self {
            archive_name,
            message,
        }
    }
}

/// The ETL pipeline is responsible for downloading, decompressing, and uploading archives.
pub struct Pipeline<U: Uploader + 'static, C: SlotCoverage + 'static, E: ExternalBlock + 'static> {
    download_stage: DownloadStage<C>,
    decompress_stage: DecompressStage,
    upload_stage: UploadStage<U, C, E>,
}

impl<U: Uploader + 'static, C: SlotCoverage + 'static, E: ExternalBlock + 'static>
    Pipeline<U, C, E>
{
    pub fn new(
        archive_store: Arc<Box<dyn ArchiveStore>>,
        tar_unpacker: Arc<Box<dyn Decompress>>,
        uploader: Arc<U>,
        coverage: Arc<C>,
        external_block: Arc<E>,
        phase: Arc<ArchiverPhase>,
        config: ArchiverConfig,
    ) -> Self {
        let work_dir = PathBuf::from(config.work_dir.clone());
        let download_stage = DownloadStage::new(
            archive_store.clone(),
            phase.clone(),
            coverage.clone(),
            DownloadStageConfig::from(&config),
        );
        let decompress_stage = DecompressStage::new(
            archive_store.clone(),
            tar_unpacker,
            phase.clone(),
            work_dir,
            config.enforce_ulimit_nofile,
        );
        let upload_stage = UploadStage::new(
            uploader.clone(),
            coverage.clone(),
            external_block.clone(),
            phase.clone(),
            UploadStageConfig::from(&config),
        );
        Self {
            download_stage,
            decompress_stage,
            upload_stage,
        }
    }

    pub async fn start(self, runtime: &mut Runtime) -> anyhow::Result<()> {
        tracing::info!("Starting ETL pipeline");

        // Consider adjusting the channel sizes if we want to concurrently download or decompress archives.
        let (download_tx, download_rx) = mpsc::channel::<PipelineMessage<PathBuf>>(1);
        let (decompress_tx, decompress_rx) =
            mpsc::channel::<PipelineMessage<Box<dyn BlockstoreApi>>>(1);

        self.download_stage
            .start(runtime, download_tx)
            .await
            .context("Failed to start download stage")?;
        self.decompress_stage
            .start(runtime, download_rx, decompress_tx)
            .context("Failed to start decompress stage")?;
        self.upload_stage
            .start(runtime, decompress_rx)
            .context("Failed to start upload stage")?;
        Ok(())
    }
}
