use anyhow::{Context, Result};
use archiver::archive_store::{ArchiveStore, ArchiveSummary, SolanaArchiveStore};
use archiver::config::ArchiverConfig;
use archiver::decompress::{Decompress, TarUnpacker};
use archiver::phase::ArchiverPhase;
use archiver::{etl, SERVICE_NAME};
use archiver::{init_logger, init_metrics};
use cadence_macros::statsd_gauge;
use common::clickhouse::{ClickhouseClient, ClickhouseClientRegistry};
use common::external_block::BigTableBlock;
use helius::{Runtime, RuntimeBuilder};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::warn;

#[tokio::main]
async fn main() -> Result<()> {
    let args = ArchiverConfig::load()?;
    init_logger().context("Failed to initialize logger")?;
    init_metrics().context("Failed to initialize metrics")?;

    let archive_store: Arc<Box<dyn ArchiveStore>> =
        Arc::new(Box::new(SolanaArchiveStore::try_from(&args)?));
    let phase = Arc::new(ArchiverPhase::new());
    let unpacker: Arc<Box<dyn Decompress>> = Arc::new(Box::new(TarUnpacker::new()));
    let external_block = Arc::new(
        BigTableBlock::new(&args.bigtable_creds_path)
            .await
            .context("Failed to initialize bigtable block")?,
    );

    // The clickhouse client implements both uploader & coverage. We use it for both purposes.
    let ch_client_registry = ClickhouseClientRegistry::try_from(&args.clickhouse)?;
    let ch_client = Arc::new(ClickhouseClient::new(ch_client_registry));
    let uploader = ch_client.clone();
    let coverage = ch_client.clone();

    let mut runtime = RuntimeBuilder::new(SERVICE_NAME)
        .init()
        .context("Failed to initialize runtime")?;

    // Collect and publish metrics based on the ETL pipeline's progress.
    start_heartbeat_job(&mut runtime, phase.clone(), archive_store.clone()).await;

    // The workhorse of the archiver. The pipeline is responsible for downloading, decompressing, and uploading archives.
    etl::Pipeline::new(
        archive_store,
        unpacker,
        uploader,
        coverage,
        external_block,
        phase,
        args,
    )
    .start(&mut runtime)
    .await
    .context("Failed to start ETL pipeline")?;

    runtime.wait_terminated().await?;
    tracing::info!("Shutting down...");

    Ok(())
}

async fn start_heartbeat_job(
    runtime: &mut Runtime,
    phase: Arc<ArchiverPhase>,
    archive_store: Arc<Box<dyn ArchiveStore>>,
) {
    let mut publish_interval = interval(Duration::from_secs(10));
    let mut cleanup_interval = interval(Duration::from_secs(120));
    runtime.spawn::<()>("heartbeat-job", async move {
        loop {
            tokio::select! {
                _ = publish_interval.tick() => {
                    phase.publish_metrics().await;
                    if let Err(e) = update_archive_metrics(archive_store.clone()).await {
                        warn!("Failed to update archive summary metrics: {}", e);
                    }
                }
                _ = cleanup_interval.tick() => {
                    phase.cleanup_completed().await;
                }
            }
        }
    });
}

async fn update_archive_metrics(archive_store: Arc<Box<dyn ArchiveStore>>) -> Result<()> {
    let ArchiveSummary {
        processed,
        unprocessed,
        total,
    } = archive_store.summary().await?;
    statsd_gauge!("summary.processed", processed as f64);
    statsd_gauge!("summary.unprocessed", unprocessed as f64);
    statsd_gauge!("summary.total", total as f64);
    Ok(())
}
