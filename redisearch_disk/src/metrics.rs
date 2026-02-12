//! Metrics collection for RocksDB/SpeeDB and async operations.
//!
//! This module provides structured access to various metrics:
//! - **Column-family metrics**: Specific to individual column families
//! - **Async read metrics**: Statistics from async read pool operations
//! - **Doc table metrics**: Doc table specific metrics (e.g., deleted IDs)
//! - **Index metrics**: Aggregate metrics per index

mod async_read;
mod column_family;
mod doc_table;
mod index;

pub use async_read::AtomicMetrics as AtomicAsyncReadMetrics;
pub use async_read::Metrics as AsyncReadMetrics;
pub use column_family::Metrics as ColumnFamilyMetrics;
pub use doc_table::Metrics as DocTableMetrics;
pub use index::Metrics as IndexMetrics;
