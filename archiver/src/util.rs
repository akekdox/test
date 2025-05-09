use std::{
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};
use tracing::warn;

pub async fn sufficient_space_for_parallel_mode(
    work_dir: PathBuf,
    sequential_threshold_gb: u64,
) -> anyhow::Result<bool> {
    let sequential_threshold_bytes = sequential_threshold_gb * 1024 * 1024 * 1024;

    // Use the work_dir from the struct
    let path = match work_dir.to_str() {
        Some(p) => p,
        None => {
            warn!("Invalid work directory path, falling back to sequential mode");
            return Ok(false);
        }
    };

    let output = match tokio::process::Command::new("df")
        .args(&["-B1", path]) // Get size in bytes
        .output()
        .await
    {
        Ok(o) => o,
        Err(e) => {
            warn!(
                "Failed to execute df command: {}, falling back to sequential mode",
                e
            );
            return Ok(false);
        }
    };

    if !output.status.success() {
        warn!(
            "df command failed with status: {}, falling back to sequential mode",
            output.status
        );
        return Ok(false);
    }

    let output_str = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = output_str.lines().collect();

    if lines.len() < 2 {
        warn!("Unexpected df output format, falling back to sequential mode");
        return Ok(false);
    }

    // Parse the total space from the second line
    // Format is typically: Filesystem 1K-blocks Used Available Use% Mounted on
    // We want the 1K-blocks (total) column, which is index 1
    let fields: Vec<&str> = lines[1].split_whitespace().collect();
    if fields.len() < 2 {
        warn!("Unexpected df output format, falling back to sequential mode");
        return Ok(false);
    }

    let total_bytes = match fields[1].parse::<u64>() {
        Ok(b) => b,
        Err(e) => {
            warn!(
                "Failed to parse total space: {}, falling back to sequential mode",
                e
            );
            return Ok(false);
        }
    };

    // For logging purposes
    let total_gb = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let threshold_gb = sequential_threshold_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    tracing::info!(
        "Disk space check: Total {:.2} GB, Threshold {:.2} GB",
        total_gb,
        threshold_gb
    );

    // Return true if we have sufficient space for parallel mode (total space >= threshold)
    Ok(total_bytes >= sequential_threshold_bytes)
}

pub async fn get_total_disk_space(path: &str) -> anyhow::Result<u64> {
    let output = tokio::process::Command::new("df")
        .args(&["-B1", path]) // Get size in bytes
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow::anyhow!("Failed to execute df command"));
    }

    let output_str = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = output_str.lines().collect();

    if lines.len() < 2 {
        return Err(anyhow::anyhow!("Unexpected df output format"));
    }

    // Parse the total space from the second line
    let fields: Vec<&str> = lines[1].split_whitespace().collect();
    if fields.len() < 2 {
        return Err(anyhow::anyhow!("Unexpected df output format"));
    }

    let total_bytes = fields[1]
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("Failed to parse total space: {}", e))?;

    Ok(total_bytes)
}

pub async fn with_retries<F, Fut, T>(
    operation_name: &str,
    max_retries: u32,
    f: F,
) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut retry_count = 0;
    loop {
        if retry_count > 0 {
            tracing::info!(
                "Retrying {} (attempt {}/{})",
                operation_name,
                retry_count + 1,
                max_retries
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        match f().await {
            Ok(result) => return Ok(result),
            Err(e) if retry_count < max_retries - 1 => {
                tracing::error!("Error during {}: {}", operation_name, e);
                retry_count += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

pub async fn delete_file_or_directory(path: &Path) -> anyhow::Result<()> {
    tracing::info!("Deleting file or directory: {}", path.display());
    if path.exists() {
        if path.is_dir() {
            tokio::fs::remove_dir_all(path).await.map_err(|e| {
                anyhow::anyhow!("Failed to remove directory {}: {}", path.display(), e)
            })?;
        } else {
            tokio::fs::remove_file(path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to remove file {}: {}", path.display(), e))?;
        }
    }
    Ok(())
}
