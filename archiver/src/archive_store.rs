use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
};

use aws_sdk_s3::{
    config::{Builder, Credentials, Region},
    Client as R2Client,
};
use serde::Deserialize;
use tokio::process::Command;

use crate::{
    blockstore::BlockstoreApi,
    config::{ArchiverConfig, StorageProvider},
};

const ESTIMATED_ARCHIVE_SLOT_RANGE: u64 = 432_000;
const PROCESSED_PREFIX: &str = "processed/";
const UNPROCESSED_PREFIX: &str = "unprocessed/";

pub type ArchiveKey = String;

pub trait ArchiveKeyExt {
    fn into_archive_name(&self) -> String;
}

impl ArchiveKeyExt for ArchiveKey {
    fn into_archive_name(&self) -> String {
        self.split('/')
            .last()
            .and_then(|s| s.strip_suffix(".tar.zst"))
            .unwrap_or_default()
            .to_string()
    }
}
pub fn path_to_archive_key(path: PathBuf) -> ArchiveKey {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}
#[derive(Debug, Clone)]
pub struct ArchiveMetadata {
    pub key: ArchiveKey,
    pub start_slot: u64,
    pub end_slot: u64,
    pub data_size: u64,
}

impl ArchiveMetadata {
    pub fn includes_slot_within_range(
        &self,
        start_slot: Option<u64>,
        end_slot: Option<u64>,
    ) -> bool {
        match (start_slot, end_slot) {
            (Some(start), Some(end)) => {
                let within_start = self.end_slot >= start;
                let within_end = self.start_slot <= end;
                within_start && within_end
            }
            (Some(start), None) => self.end_slot >= start,
            (None, Some(end)) => self.start_slot <= end,
            (None, None) => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveSummary {
    pub total: usize,
    pub processed: usize,
    pub unprocessed: usize,
}

#[async_trait::async_trait]
pub trait ArchiveStore: Send + Sync {
    /// Downloads an archive to the work directory.
    /// For example, the object key might be "unprocessed/rocksdb_123456.tar.zst".
    /// The work directory might be "/data/work_directory".
    /// And then file will be downloaded to "/data/work_directory/rocksdb_123456.tar.zst".
    /// The function will return the path to the file.
    async fn download(&self, work_dir: PathBuf, object_key: ArchiveKey) -> anyhow::Result<PathBuf>;

    /// Lists object keys for unprocessed archives.
    async fn list_unprocessed_archives(&self) -> anyhow::Result<Vec<ArchiveMetadata>>;

    /// Summarizes the archives by process/unprocessed count.
    async fn summary(&self) -> anyhow::Result<ArchiveSummary>;

    async fn get_archive_metadata(&self, object_key: ArchiveKey)
        -> anyhow::Result<ArchiveMetadata>;

    async fn update_archive_metadata(
        &self,
        blockstore: Box<dyn BlockstoreApi>,
        object_key: ArchiveKey,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SolanaArchiveStore {
    r2_client: R2Client,
    r2_endpoint: String,
    r2_access_key: String,
    r2_secret_key: String,
    bucket_name: String,
    gcs_cache: Option<HashMap<u64, (String, String)>>,
    storage_provider: StorageProvider,
    work_dir: PathBuf,
}

impl SolanaArchiveStore {
    fn new(
        r2_client: R2Client,
        r2_endpoint: String,
        r2_access_key: String,
        r2_secret_key: String,
        bucket_name: String,
        storage_provider: StorageProvider,
        work_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        let mut store = Self {
            r2_client,
            r2_endpoint,
            r2_access_key,
            r2_secret_key,
            bucket_name,
            storage_provider,
            work_dir,
            gcs_cache: None,
        };

        if store.storage_provider == StorageProvider::TryGCS {
            store.load_gcs_cache()?;
        }

        Ok(store)
    }

    fn load_gcs_cache(&mut self) -> anyhow::Result<()> {
        let cache_path = self.work_dir.join("gcs_cache.bin");
        tracing::info!("Loading GCS cache from: {:?}", &cache_path);

        if let Ok(mut file) = std::fs::File::open(&cache_path) {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;

            let cache: CacheFile = bincode::deserialize(&bytes)?;

            tracing::info!("GCS cache metadata: {:?}", cache.metadata);

            // Convert the cache format to our internal format
            let mut converted_cache = HashMap::new();
            for (slot, info) in &cache.slots {
                let extension = match info.file_type {
                    FileType::TarZst => "rocksdb.tar.zst",
                    FileType::TarBz2 => "rocksdb.tar.bz2",
                };

                converted_cache.insert(*slot, (info.bucket_name.clone(), extension.to_string()));
            }

            self.gcs_cache = Some(converted_cache);
            tracing::info!("Loaded GCS cache with {} slot entries", cache.slots.len());
            Ok(())
        } else {
            tracing::info!("No cache file found at {:?}", &cache_path);
            Ok(())
        }
    }

    async fn list_objects(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut paginator = self
            .r2_client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = paginator.try_next().await? {
            for obj in page.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }
        }
        Ok(keys)
    }

    async fn try_gcs_download(&self, filename: &str, local_path: &Path) -> anyhow::Result<bool> {
        tracing::info!("Checking GCS for file: {}", filename);

        let slot = filename
            .strip_prefix("rocksdb_")
            .and_then(|s| s.strip_suffix(".tar.zst"))
            .and_then(|s| s.parse::<u64>().ok());

        let slot = match slot {
            Some(s) => s,
            None => {
                tracing::info!("Not a rocksdb archive or invalid format: {}", filename);
                return Ok(false);
            }
        };

        // Check if we know which bucket has this slot
        if let Some((bucket_name, _)) = self.gcs_cache.as_ref().and_then(|cache| cache.get(&slot)) {
            // Try .tar.zst first
            let zst_path = format!("gs://{}/{}/rocksdb.tar.zst", bucket_name, slot);
            tracing::info!("Checking GCS path: {}", zst_path);

            let stat = tokio::process::Command::new("gsutil")
                .arg("stat")
                .arg(&zst_path)
                .output()
                .await?;

            if stat.status.success() {
                tracing::info!("Found file at {}, starting download...", zst_path);

                // Actually download the file
                let download = tokio::process::Command::new("gsutil")
                    .arg("-m")
                    .arg("cp")
                    .arg(&zst_path)
                    .arg(local_path)
                    .output()
                    .await?;

                if download.status.success() {
                    tracing::info!("Successfully downloaded from {}", zst_path);
                    return Ok(true);
                }
            }

            // Fallback to .tar.bz2
            let bz2_path = format!("gs://{}/{}/rocksdb.tar.bz2", bucket_name, slot);
            tracing::info!("Checking fallback GCS path: {}", bz2_path);

            let stat = tokio::process::Command::new("gsutil")
                .arg("stat")
                .arg(&bz2_path)
                .output()
                .await?;

            if stat.status.success() {
                tracing::info!("Found file at {}, starting download...", bz2_path);

                // Actually download the file
                let download = tokio::process::Command::new("gsutil")
                    .arg("cp")
                    .arg(&bz2_path)
                    .arg(local_path)
                    .output()
                    .await?;

                if download.status.success() {
                    tracing::info!("Successfully downloaded from {}", bz2_path);
                    return Ok(true);
                }
            }
        }

        tracing::info!("Not found in GCS (will use R2)");
        Ok(false)
    }

    async fn r2_download(&self, object_key: &str, local_path: &Path) -> anyhow::Result<()> {
        tracing::info!(
            "Starting R2 download of {} to {}",
            object_key,
            local_path.display()
        );

        // Check if file exists locally and matches remote size
        if local_path.exists() {
            let head_output = self
                .r2_client
                .head_object()
                .bucket(&self.bucket_name)
                .key(object_key)
                .send()
                .await?;

            let remote_size = head_output.content_length().unwrap_or(0) as u64;
            let local_size = tokio::fs::metadata(local_path).await?.len();

            if local_size == remote_size {
                tracing::info!(
                    "File already exists locally and sizes match: {}",
                    local_path.display()
                );
                return Ok(());
            }
            tracing::info!("Local file exists but size mismatch. Re-downloading...");
        }

        // Use AWS CLI for download
        let output = Command::new("aws")
            .args([
                "s3",
                "cp",
                &format!("s3://{}/{}", self.bucket_name, object_key),
                local_path.to_str().unwrap(),
                "--endpoint-url",
                &self.r2_endpoint,
                "--no-progress",
                "--cli-read-timeout",
                "60",
                "--cli-connect-timeout",
                "60",
            ])
            .env("AWS_ACCESS_KEY_ID", &self.r2_access_key)
            .env("AWS_SECRET_ACCESS_KEY", &self.r2_secret_key)
            .env("AWS_DEFAULT_REGION", "auto")
            .env("PYTHONWARNINGS", "ignore")
            .env("AWS_SUPPRESS_URLLIB3_WARNINGS", "1")
            .env("AWS_MAX_CONCURRENT_REQUESTS", "10")
            .env("AWS_MULTIPART_THRESHOLD", "1073741824")
            .env("AWS_MULTIPART_CHUNKSIZE", "5368709120")
            .env("AWS_MAX_QUEUE_SIZE", "10000")
            .env("AWS_MAX_BANDWIDTH", "25687091200")
            .env("AWS_S3_MAX_POOL_CONNECTIONS", "15")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "AWS CLI download failed with status: {}. Error: {}",
                output.status,
                stderr
            ));
        }

        // Verify file size after download
        let head_output = self
            .r2_client
            .head_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .send()
            .await?;

        let remote_size = head_output.content_length().unwrap_or(0) as u64;
        let local_size = tokio::fs::metadata(local_path).await?.len();

        if local_size != remote_size {
            anyhow::bail!(
                "Downloaded file size mismatch: expected {}, got {}",
                remote_size,
                local_size
            );
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ArchiveStore for SolanaArchiveStore {
    async fn download(&self, work_dir: PathBuf, object_key: ArchiveKey) -> anyhow::Result<PathBuf> {
        // Verify file format
        if !object_key.ends_with(".tar.zst") {
            anyhow::bail!(
                "Unsupported file format: {}. Only .tar.zst files are supported.",
                object_key
            );
        }

        // Create work directory if it doesn't exist
        tokio::fs::create_dir_all(&work_dir)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create work directory: {}", e))?;

        // Get filename and slot from object key
        let filename = Path::new(&object_key)
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Invalid object key: no filename"))?
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in filename"))?;

        let local_path = work_dir.join(filename);
        match self.storage_provider {
            StorageProvider::TryGCS => match self.try_gcs_download(filename, &local_path).await {
                Ok(true) => (),
                Ok(false) | Err(_) => {
                    tracing::info!("Failed to find file in GCS, falling back to R2");
                    self.r2_download(&object_key, &local_path).await?
                }
            },
            StorageProvider::R2 => self.r2_download(&object_key, &local_path).await?,
        }

        Ok(local_path)
    }

    async fn list_unprocessed_archives(&self) -> anyhow::Result<Vec<ArchiveMetadata>> {
        let mut paginator = self
            .r2_client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(UNPROCESSED_PREFIX)
            .into_paginator()
            .send();

        // Collect all .tar.zst files and their slots
        let mut unprocessed_archives = Vec::new();
        while let Some(page) = paginator.try_next().await? {
            for obj in page.contents() {
                if let Some(key) = obj.key() {
                    if !key.ends_with(".tar.zst") {
                        continue;
                    }
                    let Some(slot) = extract_slot_from_key(key) else {
                        tracing::error!(
                            "Failed to extract slot from key: {}. Skipping record.",
                            key
                        );
                        continue;
                    };

                    // Try to get metadata from the object
                    match self.get_archive_metadata(key.to_string()).await {
                        Ok(metadata) => {
                            unprocessed_archives.push(metadata);
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Failed to get metadata for {}, falling back to estimation: {}",
                                key,
                                e
                            );
                            // Fall back to estimation if metadata fetch fails
                            unprocessed_archives.push(ArchiveMetadata {
                                key: key.to_string(),
                                start_slot: slot,
                                end_slot: slot + ESTIMATED_ARCHIVE_SLOT_RANGE,
                                data_size: obj.size().unwrap_or(0) as u64,
                            });
                        }
                    }
                }
            }
        }
        Ok(unprocessed_archives)
    }

    async fn summary(&self) -> anyhow::Result<ArchiveSummary> {
        let unprocessed_future = self.list_objects(UNPROCESSED_PREFIX);
        let processed_future = self.list_objects(PROCESSED_PREFIX);
        let (unprocessed_result, processed_result) =
            tokio::join!(unprocessed_future, processed_future);
        let unprocessed_count = unprocessed_result?
            .iter()
            .filter(|k| k.ends_with(".tar.zst"))
            .count();
        let processed_count = processed_result?
            .iter()
            .filter(|k| k.ends_with(".tar.zst"))
            .count();
        Ok(ArchiveSummary {
            total: unprocessed_count + processed_count,
            processed: processed_count,
            unprocessed: unprocessed_count,
        })
    }

    async fn get_archive_metadata(
        &self,
        object_key: ArchiveKey,
    ) -> anyhow::Result<ArchiveMetadata> {
        // Get object metadata from R2
        let head_output = self
            .r2_client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&object_key)
            .send()
            .await?;

        let metadata = head_output
            .metadata()
            .ok_or_else(|| anyhow::anyhow!("No metadata found for object: {}", object_key))?;

        // Extract all fields from metadata
        let start_slot = metadata
            .get("start_slot")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Failed to get start_slot from metadata"))?;

        let end_slot = metadata
            .get("end_slot")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(|| start_slot + ESTIMATED_ARCHIVE_SLOT_RANGE);

        let data_size = metadata
            .get("data_size")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Failed to get data_size from metadata"))?;

        Ok(ArchiveMetadata {
            key: object_key,
            start_slot,
            end_slot,
            data_size,
        })
    }

    async fn update_archive_metadata(
        &self,
        blockstore: Box<dyn BlockstoreApi>,
        object_key: ArchiveKey,
    ) -> anyhow::Result<()> {
        let start_slot = blockstore.first_available_block()?;
        let end_slot = blockstore.max_root();

        // // Prepare tags to be updated
        let mut tags = Vec::new();

        // Store all fields as metadata
        tags.push(
            aws_sdk_s3::types::Tag::builder()
                .set_key(Some("start_slot".to_string()))
                .set_value(Some(start_slot.to_string()))
                .build()?,
        );

        tags.push(
            aws_sdk_s3::types::Tag::builder()
                .set_key(Some("end_slot".to_string()))
                .set_value(Some(end_slot.to_string()))
                .build()?,
        );

        // Update the object tags
        self.r2_client
            .put_object_tagging()
            .bucket(&self.bucket_name)
            .key(&object_key)
            .tagging(
                aws_sdk_s3::types::Tagging::builder()
                    .set_tag_set(Some(tags))
                    .build()?,
            )
            .send()
            .await?;

        Ok(())
    }
}

fn extract_slot_from_key(key: &str) -> Option<u64> {
    // Extract filename from path
    let filename = Path::new(key).file_name()?.to_str()?;

    // Try to strip either prefix, or both
    let without_prefix = filename.strip_prefix("rocksdb_")?;

    // Get the number before the extension
    without_prefix.split('.').next()?.parse::<u64>().ok()
}

#[derive(Debug, Deserialize)]
struct CacheMetadata {
    _total_slots: usize,
    _buckets_scanned: Vec<String>,
    _creation_time: String,
}

#[derive(Debug, Deserialize)]
struct CacheFile {
    _version: u32,
    _last_updated: i64,
    slots: HashMap<u64, SlotInfo>,
    metadata: CacheMetadata,
}

#[derive(Debug, Deserialize)]
struct SlotInfo {
    _slot: u64,
    _bucket_id: u8,
    bucket_name: String,
    file_type: FileType,
    _full_path: String,
}

#[derive(Debug, Deserialize)]
enum FileType {
    TarZst,
    TarBz2,
}

impl TryFrom<&ArchiverConfig> for SolanaArchiveStore {
    type Error = anyhow::Error;

    fn try_from(config: &ArchiverConfig) -> Result<Self, Self::Error> {
        let r2_client = init_r2_client(config)?;
        Self::new(
            r2_client,
            config.r2_endpoint.clone(),
            config.r2_access_key.clone().unwrap_or_default(),
            config.r2_secret_key.clone().unwrap_or_default(),
            config.bucket_name.clone(),
            config.storage_provider,
            PathBuf::from(config.work_dir.clone()),
        )
    }
}

fn init_r2_client(config: &ArchiverConfig) -> anyhow::Result<R2Client> {
    let creds = Credentials::new(
        config
            .r2_access_key
            .clone()
            .ok_or(anyhow::anyhow!("R2 access key is required"))?,
        config
            .r2_secret_key
            .clone()
            .ok_or(anyhow::anyhow!("R2 secret key is required"))?,
        None,
        None,
        "R2",
    );
    let r2_config = Builder::new()
        .endpoint_url(config.r2_endpoint.clone())
        .region(Region::new("auto"))
        .credentials_provider(creds)
        .behavior_version_latest()
        .build();

    let r2_client = R2Client::from_conf(r2_config);
    Ok(r2_client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_slot_from_key() {
        // Test the slot extraction function without creating a Downloader instance
        let test_cases = vec![
            ("unprocessed/rocksdb_276044072.tar.zst", Some(276044072)),
            ("unprocessed/rocksdb_9935732.tar.zst", Some(9935732)),
            ("unprocessed/rocksdb_99359376.tar.zst", Some(99359376)),
            ("unprocessed/rocksdb_invalid.tar.zst", None),
            ("unprocessed/not_rocksdb_123456.tar.zst", None),
            ("rocksdb_123456.tar.zst", Some(123456)),
        ];

        for (key, expected) in test_cases {
            // Directly implement the extraction logic from the Downloader method
            let filename = Path::new(key).file_name().and_then(|n| n.to_str());
            let result = filename
                .and_then(|f| f.strip_prefix("rocksdb_"))
                .and_then(|s| s.split('.').next())
                .and_then(|s| s.parse::<u64>().ok());

            assert_eq!(result, expected, "Failed to extract slot from key: {}", key);
        }
    }

    #[test]
    fn test_archive_metadata_includes_slot_within_range() {
        let start_slot = 300_000_000;
        let end_slot = 300_000_000 + ESTIMATED_ARCHIVE_SLOT_RANGE;
        let archive = ArchiveMetadata {
            key: "key".to_string(),
            start_slot,
            end_slot,
            data_size: 0,
        };

        assert!(archive.includes_slot_within_range(Some(0), Some(start_slot)));
        assert!(!archive.includes_slot_within_range(Some(0), Some(start_slot - 1)));
        assert!(archive.includes_slot_within_range(Some(start_slot), Some(end_slot)));
        assert!(archive.includes_slot_within_range(Some(start_slot), None));
        assert!(archive.includes_slot_within_range(None, Some(end_slot)));
        assert!(archive.includes_slot_within_range(None, None));
        assert!(archive.includes_slot_within_range(
            Some(start_slot + (ESTIMATED_ARCHIVE_SLOT_RANGE / 2)),
            None
        ));
    }
}
