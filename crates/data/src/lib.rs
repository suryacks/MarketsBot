//! On-disk storage: flat row structs ↔ Parquet (readable from Python/polars
//! for research), plus a `Recorder` that buffers live events into hourly files.

pub mod dataset;
pub mod parquet_io;
pub mod recorder;
pub mod rows;

pub use dataset::{DsMarket, DsPrice};
pub use parquet_io::{read_dir, read_parquet, write_parquet};
pub use recorder::Recorder;
pub use rows::*;
