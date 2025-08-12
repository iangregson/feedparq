use std::io::{BufRead, BufReader, Lines};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, fs, path};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use metrics_sqlite::SqliteExporter;
use polars::prelude::*;
use tracing::instrument;
use tracing_subscriber::fmt::time::UtcTime;
use url::Url;

use feedparq::urls_to_lazyframes;

const PATH_DB: &str = "data/metrics.db";
const PATH_CHANNELS_DIR: &str = "config/";
const PATH_OUTPUT_DIR: &str = "data/";
const URN_COUNTERS_RUN_COUNT: &str = "urn:feeqparq:counters.run_count";
const URN_HISTOGRAMS_RUN_DURATION: &str = "urn:feeqparq:histograms.run_duration";
const URN_HISTOGRAMS_FULL_RUN_DURATION: &str = "urn:feeqparq:histograms.full_run_duration";

#[tokio::main]
#[instrument]
async fn main() -> Result<()> {
    // Configure JSON logging
    tracing_subscriber::fmt()
        .json()
        .with_timer(UtcTime::rfc_3339())
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();

    let start = SystemTime::now();
    let fstart: DateTime<Utc> = start.into();
    tracing::info!("Start processing at {}", fstart);

    let metrics_exporter = SqliteExporter::new(
        Duration::from_millis(10),
        Some(Duration::from_secs(60 * 60 * 24)), // 60 sec * 60 min * 24 hours i.e. 1 day
        PATH_DB,
    )
    .with_context(|| "Failed to create SqliteExporter")?;

    metrics_exporter
        .install()
        .with_context(|| "Failed to install SqliteExporter")?;

    let output_path_base = env::var_os("FDPRQ_OUTPUT_DIR")
        .map(path::PathBuf::from)
        .unwrap_or_else(|| path::PathBuf::from(PATH_OUTPUT_DIR));

    let channel_paths = read_directory(path::Path::new(PATH_CHANNELS_DIR))?;
    for channel_path in channel_paths.iter() {
        if let Some(channel_name) = channel_path.file_stem() {
            let start_inner = SystemTime::now();
            let fstart_inner: DateTime<Utc> = start_inner.into();

            tracing::info!(
                "Start processing {} at {}",
                channel_name.to_string_lossy(),
                fstart_inner
            );

            let feed_urls: Vec<Url> = parse_urls_from_file(channel_path)?;
            let lfs = urls_to_lazyframes(feed_urls).await?;
            let lfs = concat(lfs, UnionArgs::default())?;

            let dfs = lfs.collect()?;

            let output_path = output_path_base
                .join(channel_name)
                .with_extension("parquet");

            let mut file = fs::File::create(output_path.clone())?;
            ParquetWriter::new(&mut file).finish(&mut dfs.clone())?;

            let fin_inner = start_inner.duration_since(UNIX_EPOCH)?.as_millis();
            tracing::info!(
                "Finish processing {} at {}",
                channel_name.to_string_lossy(),
                fin_inner
            );
            metrics::histogram!(URN_HISTOGRAMS_RUN_DURATION).record(fin_inner as f64);
        }
    }

    let fin = start.duration_since(UNIX_EPOCH)?.as_millis();
    tracing::info!("Finish processing at {}", fin);
    metrics::histogram!(URN_HISTOGRAMS_FULL_RUN_DURATION).record(fin as f64);
    metrics::counter!(URN_COUNTERS_RUN_COUNT).increment(1);

    Ok(())
}

fn read_directory(path: &path::Path) -> Result<Vec<path::PathBuf>> {
    let file_paths = fs::read_dir(path)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();

    Ok(file_paths)
}

fn read_lines<P>(filename: P) -> Result<Lines<BufReader<fs::File>>>
where
    P: AsRef<path::Path>,
{
    let file = fs::File::open(filename)?;
    Ok(BufReader::new(file).lines())
}

fn parse_urls_from_file<P>(filename: P) -> Result<Vec<Url>>
where
    P: AsRef<path::Path>,
{
    let lines = read_lines(filename)?;
    let urls = lines
        .map_while(Result::ok)
        .filter_map(|line| Url::parse(&line).ok())
        .collect();
    Ok(urls)
}
