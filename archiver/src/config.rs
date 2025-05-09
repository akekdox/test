use std::env;

use anyhow::Context;
use figment::{
    providers::{Format, Yaml},
    Figment,
};
use serde::Deserialize;

use common::config::ClickhouseConfig;

#[derive(Clone, Deserialize, Debug, Default)]
pub struct ArchiverConfig {
    pub bucket_name: String,
    pub work_dir: String,
    pub r2_access_key: Option<String>,
    pub r2_secret_key: Option<String>,
    pub r2_endpoint: String,
    pub start_slot: Option<u64>,
    pub end_slot: Option<u64>,
    pub clickhouse: ClickhouseConfig,
    pub storage_provider: StorageProvider,
    pub bigtable_creds_path: String,
    pub enforce_ulimit_nofile: bool,
}

impl ArchiverConfig {
    pub fn load() -> anyhow::Result<ArchiverConfig> {
        let config_path = env::var("ARCHIVER_CONFIG_PATH")
            .unwrap_or_else(|_| format!("./{}/config.yaml", env!("CARGO_PKG_NAME")));
        Figment::from(Yaml::file(config_path))
            .extract::<ArchiverConfig>()
            .context("Failed to load archiver config")
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Default)]
pub enum StorageProvider {
    #[default]
    #[serde(alias = "try-gcs")]
    TryGCS,
    R2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config() -> anyhow::Result<()> {
        let _ = ArchiverConfig::load()?;
        Ok(())
    }
}
