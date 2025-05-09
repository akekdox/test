use anyhow::Context;
use helius::{get_env_or_default, Environment};

pub mod archive_store;
pub mod blockstore;
pub mod config;
pub mod decompress;
pub mod etl;
pub mod phase;
pub mod util;

pub const SERVICE_NAME: &str = "archival_archiver";

pub fn init_logger() -> anyhow::Result<()> {
    let env_filter = get_env_or_default("RUST_LOG", "info")?;
    let subscriber = tracing_subscriber::fmt().with_env_filter(env_filter);
    let _ = if Environment::is_local()? {
        subscriber.compact().try_init()
    } else {
        subscriber.json().try_init()
    };
    Ok(())
}

pub fn init_metrics() -> anyhow::Result<()> {
    let uri = get_env_or_default("METRICS_URI", "127.0.0.1")?;
    let port = get_env_or_default("METRICS_PORT", "7998")?
        .parse()
        .context("Invalid port number provided as METRICS_PORT")?;
    tracing::info!("collecting metrics on: {}:{}", uri, port);

    let socket = std::net::UdpSocket::bind("0.0.0.0:0").context("Binding to 0.0.0.0:0")?;
    socket.set_nonblocking(true)?;
    let sink = cadence::UdpMetricSink::from((uri, port), socket)?;
    let sink = cadence::QueuingMetricSink::from(sink);

    let client = cadence::StatsdClient::builder(SERVICE_NAME, sink)
        .with_error_handler(move |e| tracing::error!("{SERVICE_NAME} statsd metrics error: {e}"))
        .build();
    cadence_macros::set_global_default(client);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{init_logger, init_metrics};

    // Initializes the logger globally for all tests within the crate.
    #[ctor::ctor]
    fn init_test() {
        init_metrics().expect("Failed to initialize metrics");
        if std::env::args().any(|e| e == "--nocapture") {
            init_logger().expect("Failed to initialize logger");
        }
    }
}
