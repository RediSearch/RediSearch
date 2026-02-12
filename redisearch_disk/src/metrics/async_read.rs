//! Async read pool metrics.

use std::ops::AddAssign;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::info_sink::InfoSink;

/// Metrics collected during async read pool operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct Metrics {
    /// Total number of read requests added to the pool.
    pub total_reads_requested: u64,
    /// Number of reads that completed successfully with a document found.
    pub reads_found: u64,
    /// Number of reads that completed but document was not found.
    pub reads_not_found: u64,
    /// Number of reads that failed with an error.
    pub reads_errors: u64,
    /// Number of reads that were filtered out due to document expiration.
    pub reads_expired: u64,
}

impl AddAssign for Metrics {
    fn add_assign(&mut self, other: Self) {
        self.total_reads_requested += other.total_reads_requested;
        self.reads_found += other.reads_found;
        self.reads_not_found += other.reads_not_found;
        self.reads_errors += other.reads_errors;
        self.reads_expired += other.reads_expired;
    }
}

impl Metrics {
    /// Outputs the async read metrics to an info sink.
    pub fn output_to_info_sink(&self, sink: &mut impl InfoSink) {
        sink.add_u64(c"async_total_reads_requested", self.total_reads_requested);
        sink.add_u64(c"async_reads_found", self.reads_found);
        sink.add_u64(c"async_reads_not_found", self.reads_not_found);
        sink.add_u64(c"async_reads_errors", self.reads_errors);
        sink.add_u64(c"async_reads_expired", self.reads_expired);
    }
}

/// Atomic version of async read metrics for lock-free accumulation.
///
/// Used by `DocTable` to accumulate metrics from multiple `AsyncReadPool` instances
/// without requiring a mutex.
#[derive(Debug, Default)]
pub struct AtomicMetrics {
    total_reads_requested: AtomicU64,
    reads_found: AtomicU64,
    reads_not_found: AtomicU64,
    reads_errors: AtomicU64,
    reads_expired: AtomicU64,
}

impl AtomicMetrics {
    /// Atomically accumulates metrics from a `Metrics` instance.
    pub fn accumulate(&self, other: &Metrics) {
        self.total_reads_requested
            .fetch_add(other.total_reads_requested, Ordering::Relaxed);
        self.reads_found
            .fetch_add(other.reads_found, Ordering::Relaxed);
        self.reads_not_found
            .fetch_add(other.reads_not_found, Ordering::Relaxed);
        self.reads_errors
            .fetch_add(other.reads_errors, Ordering::Relaxed);
        self.reads_expired
            .fetch_add(other.reads_expired, Ordering::Relaxed);
    }

    /// Loads the current metrics as a `Metrics` snapshot.
    pub fn load(&self) -> Metrics {
        Metrics {
            total_reads_requested: self.total_reads_requested.load(Ordering::Relaxed),
            reads_found: self.reads_found.load(Ordering::Relaxed),
            reads_not_found: self.reads_not_found.load(Ordering::Relaxed),
            reads_errors: self.reads_errors.load(Ordering::Relaxed),
            reads_expired: self.reads_expired.load(Ordering::Relaxed),
        }
    }
}
