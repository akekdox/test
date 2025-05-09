use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use helius::Runtime;
use solana_ledger::{
    blockstore::{Blockstore, BlockstoreError},
    blockstore_options::{AccessType, BlockstoreOptions, LedgerColumnOptions, ShredStorageType},
};
use tokio::sync::mpsc::{Receiver, Sender};

use super::pipeline::PipelineMessage;

use crate::{
    archive_store::{path_to_archive_key, ArchiveStore},
    blockstore::BlockstoreApi,
    decompress::Decompress,
    phase::{ArchiverPhase, Phase},
};

/// The decompress stage is responsible decompressing and unpacking raw archives into RocksDB tables (Blockstore).
pub struct DecompressStage {
    archive_store: Arc<Box<dyn ArchiveStore>>,
    tar_unpacker: Arc<Box<dyn Decompress>>,
    phase: Arc<ArchiverPhase>,
    work_dir: PathBuf,
    enforce_ulimit_nofile: bool,
}

impl DecompressStage {
    pub fn new(
        archive_store: Arc<Box<dyn ArchiveStore>>,
        tar_unpacker: Arc<Box<dyn Decompress>>,
        phase: Arc<ArchiverPhase>,
        work_dir: PathBuf,
        enforce_ulimit_nofile: bool,
    ) -> Self {
        Self {
            archive_store,
            tar_unpacker,
            phase,
            work_dir,
            enforce_ulimit_nofile,
        }
    }

    pub fn start(
        self,
        runtime: &mut Runtime,
        mut path_rx: Receiver<PipelineMessage<PathBuf>>,
        blockstore_tx: Sender<PipelineMessage<Box<dyn BlockstoreApi>>>,
    ) -> anyhow::Result<()> {
        runtime.spawn_critical::<()>("decompress-stage", async move {
            while let Some(message) = path_rx.recv().await {
                if let Err(e) = self.handle_message(message, &blockstore_tx).await {
                    tracing::error!("[DecompressStage] Error handling archive: {:#}", e);
                }
            }
        });
        Ok(())
    }

    async fn handle_message(
        &self,
        message: PipelineMessage<PathBuf>,
        blockstore_tx: &Sender<PipelineMessage<Box<dyn BlockstoreApi>>>,
    ) -> anyhow::Result<()> {
        let PipelineMessage {
            archive_name,
            message: path,
        } = message;
        self.phase
            .advance_to(Phase::Decompressing, &archive_name)
            .await;
        let out_dir = self.work_dir.join(&archive_name);
        let decompressed_path = self.tar_unpacker.decompress_tar(&path, &out_dir).await?;
        let blockstore = open_blockstore(&decompressed_path, self.enforce_ulimit_nofile)
            .context("Failed to open blockstore")?;

        let _ = self
            .archive_store
            .update_archive_metadata(
                Box::new(
                    open_blockstore(&decompressed_path, self.enforce_ulimit_nofile)
                        .context("Failed to open blockstore")?,
                ),
                path_to_archive_key(path),
            )
            .await;

        blockstore_tx
            .send(PipelineMessage::new(archive_name, Box::new(blockstore)))
            .await
            .context("Blockstore channel closed")?;
        Ok(())
    }
}

pub fn open_blockstore(
    ledger_path: &Path,
    enforce_ulimit_nofile: bool,
) -> Result<Blockstore, BlockstoreError> {
    let access_type = AccessType::PrimaryForMaintenance;
    let shred_storage_type = ShredStorageType::RocksLevel;
    Blockstore::open_with_options(
        ledger_path,
        BlockstoreOptions {
            access_type,
            enforce_ulimit_nofile,
            column_options: LedgerColumnOptions {
                shred_storage_type,
                ..LedgerColumnOptions::default()
            },
            ..BlockstoreOptions::default()
        },
    )
}

#[cfg(test)]
mod tests {
    use helius::RuntimeBuilder;
    use tokio::sync::mpsc;

    use super::*;
    use crate::archive_store::{ArchiveKey, ArchiveKeyExt, ArchiveMetadata, ArchiveSummary};
    use crate::decompress::tests::MockDecompress;
    use crate::etl::download_stage::ArchiveTarPath;

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
            _: Box<dyn BlockstoreApi>,
            _object_key: ArchiveKey,
        ) -> anyhow::Result<()> {
            // Since this is a mock implementation, we'll just return Ok(())
            // In a real implementation, this would update the metadata in the store
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_decompress_stage() {
        let work_dir: PathBuf = PathBuf::from("mock_dir");
        let archive_name = "test_archive".to_string();
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
        let compressed_path = work_dir.join(format!("{}.tar.zst", &archive_name));
        let expected_blockstore_path = work_dir.join(&archive_name);

        let tar_unpacker: Arc<Box<dyn Decompress>> = Arc::new(Box::new(MockDecompress));
        let phase = Arc::new(ArchiverPhase::new());
        let archive_store: Arc<Box<dyn ArchiveStore>> =
            Arc::new(Box::new(MockArchiveStore::new(expected_archives)));
        let decompress_stage =
            DecompressStage::new(archive_store, tar_unpacker, phase, work_dir, false);

        let (download_tx, download_rx) = mpsc::channel::<PipelineMessage<PathBuf>>(1);
        let (blockstore_tx, mut blockstore_rx) =
            mpsc::channel::<PipelineMessage<Box<dyn BlockstoreApi>>>(1);

        let mut runtime = RuntimeBuilder::new("test_runtime").init().unwrap();
        decompress_stage
            .start(&mut runtime, download_rx, blockstore_tx)
            .unwrap();

        download_tx
            .send(PipelineMessage::new(archive_name.clone(), compressed_path))
            .await
            .unwrap();

        let PipelineMessage {
            archive_name,
            message: blockstore,
        } = blockstore_rx.recv().await.unwrap();
        assert_eq!(archive_name, archive_name);
        assert_eq!(
            blockstore.ledger_path().to_owned(),
            expected_blockstore_path
        );
        runtime.cancellation_token().cancel();
    }
}
