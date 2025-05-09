use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::util::delete_file_or_directory;

#[async_trait::async_trait]
pub trait Decompress: Send + Sync {
    /// Decompresses and unpacks a tar.zst file to a destination directory.
    async fn decompress_tar(&self, tar_path: &Path, out_dir: &Path) -> Result<PathBuf>;
}

pub struct TarUnpacker {
    cleanup_source_file: bool,
    use_mbuffer: bool,
}

impl TarUnpacker {
    pub fn new() -> Self {
        Self {
            cleanup_source_file: true,
            use_mbuffer: cfg!(target_os = "linux"),
        }
    }

    pub fn new_with_options(cleanup_source_file: bool, use_mbuffer: bool) -> Self {
        Self {
            cleanup_source_file,
            use_mbuffer,
        }
    }
}

#[async_trait::async_trait]
impl Decompress for TarUnpacker {
    async fn decompress_tar(&self, tar_path: &Path, out_dir: &Path) -> Result<PathBuf> {
        // Verify file format
        if !tar_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid path"))?
            .ends_with(".tar.zst")
        {
            anyhow::bail!(
                "Unsupported compression format: {}. Only .tar.zst files are supported.",
                tar_path.display()
            );
        }

        // Get absolute paths
        let abs_tar_path = tar_path.canonicalize()?;

        // Create destination directory if it does not exist.
        tokio::fs::create_dir_all(out_dir)
            .await
            .context("Failed to create extract directory")?;

        tracing::info!(
            "Extracting {} to {}",
            abs_tar_path.display(),
            out_dir.display()
        );

        let zstdcat_cmd = format!("zstdcat -T0 '{}'", abs_tar_path.display());
        let tar_cmd = format!("tar xf - -m -C '{}'", out_dir.display());
        let cmd = if self.use_mbuffer {
            format!("{} | mbuffer -m 2G | {}", zstdcat_cmd, tar_cmd)
        } else {
            format!("{} | {}", zstdcat_cmd, tar_cmd)
        };
        tracing::info!("Running: {}", cmd);

        // Run the command
        let status = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .current_dir(out_dir)
            .status()
            .await?;

        if !status.success() {
            anyhow::bail!("Extraction command failed with status: {}", status);
        }

        // Delete the source tar.zst file after successful extraction
        if self.cleanup_source_file {
            if let Err(e) = delete_file_or_directory(&abs_tar_path).await {
                tracing::warn!(
                    "Failed to delete source file {}: {}. Continuing anyway...",
                    abs_tar_path.display(),
                    e
                );
            }
        }

        tracing::info!(
            "Successfully extracted tar {} to {}",
            tar_path.display(),
            out_dir.display()
        );
        Ok(out_dir.to_path_buf())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub struct MockDecompress;

    #[async_trait::async_trait]
    impl Decompress for MockDecompress {
        async fn decompress_tar(&self, _tar_path: &Path, out_dir: &Path) -> Result<PathBuf> {
            Ok(out_dir.to_path_buf())
        }
    }
}
