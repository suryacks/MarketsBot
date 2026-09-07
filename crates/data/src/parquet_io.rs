use anyhow::{Context, Result};
use arrow::datatypes::FieldRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::fs::File;
use std::path::{Path, PathBuf};

fn tracing_opts() -> TracingOptions {
    TracingOptions::default().allow_null_fields(true)
}

pub fn write_parquet<T: Serialize + DeserializeOwned>(path: impl AsRef<Path>, rows: &[T]) -> Result<()> {
    let path = path.as_ref();
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let fields = Vec::<FieldRef>::from_type::<T>(tracing_opts()).context("tracing arrow schema")?;
    let batch = serde_arrow::to_record_batch(&fields, &rows).context("rows -> arrow")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

pub fn read_parquet<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(1 << 16)
        .build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let rows: Vec<T> = serde_arrow::from_record_batch(&batch).context("arrow -> rows")?;
        out.extend(rows);
    }
    Ok(out)
}

/// All `*.parquet` files under `dir` (recursively), sorted by path.
pub fn list_parquet(dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    fn walk(d: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        if !d.exists() {
            return Ok(());
        }
        for e in std::fs::read_dir(d)? {
            let p = e?.path();
            if p.is_dir() {
                walk(&p, out)?;
            } else if p.extension().is_some_and(|x| x == "parquet") {
                out.push(p);
            }
        }
        Ok(())
    }
    walk(dir.as_ref(), &mut out)?;
    out.sort();
    Ok(out)
}

/// Read and concatenate every parquet file under `dir`.
pub fn read_dir<T: DeserializeOwned>(dir: impl AsRef<Path>) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for p in list_parquet(dir)? {
        out.extend(read_parquet::<T>(&p)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::TradeRow;

    #[test]
    fn roundtrip() {
        let dir = std::env::temp_dir().join(format!("mb-data-test-{}", std::process::id()));
        let p = dir.join("t.parquet");
        let rows = vec![
            TradeRow {
                venue: "kalshi".into(),
                ticker: "X".into(),
                ts_ms: 1,
                yes_px: 5600,
                qty: 100_000,
                taker_yes: true,
                trade_id: "a".into(),
            },
            TradeRow {
                venue: "kalshi".into(),
                ticker: "X".into(),
                ts_ms: 2,
                yes_px: 5700,
                qty: 10_000,
                taker_yes: false,
                trade_id: "b".into(),
            },
        ];
        write_parquet(&p, &rows).unwrap();
        let back: Vec<TradeRow> = read_parquet(&p).unwrap();
        assert_eq!(rows, back);
        let all: Vec<TradeRow> = read_dir(&dir).unwrap();
        assert_eq!(all.len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }
}
