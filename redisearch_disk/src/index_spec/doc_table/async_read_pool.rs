//! Async Document Metadata Read Pool
//!
//! Provides a pool for batching and polling async document metadata reads.
//! This enables the result processor to issue multiple concurrent read requests
//! and poll for results, improving query performance through I/O parallelism.
//!
//! Uses a tokio runtime with an mpsc channel for efficient async task management.

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::{DocTable, DocumentMetadata};
use crate::metrics::AsyncReadMetrics;

/// Executes an async document metadata read and sends the outcome through the channel.
///
/// This function awaits the read future, converts the result into a [`ReadOutcome`],
/// and sends it to the receiver via the provided channel. Send errors are ignored
/// since the receiver may have been dropped if the pool is no longer needed.
///
/// # Arguments
/// * `read_future` - The future that resolves to the document metadata read result
/// * `sender` - Channel sender to communicate the outcome back to the pool
/// * `doc_id` - The document ID being read (included in successful outcomes)
/// * `user_data` - Opaque user data to associate with this read (returned in all outcomes)
async fn read_and_send_outcome(
    read_future: impl std::future::Future<Output = Result<Option<DocumentMetadata>, speedb::Error>>,
    sender: UnboundedSender<ReadOutcome>,
    doc_id: ffi::t_docId,
    user_data: u64,
) {
    let outcome = match read_future.await {
        Ok(Some(dmd)) => ReadOutcome::Found {
            doc_id,
            dmd,
            user_data,
        },
        Ok(None) => ReadOutcome::NotFound { user_data },
        Err(_) => ReadOutcome::Error { user_data },
    };
    // Ignore send errors - receiver may have been dropped
    let _ = sender.send(outcome);
}

/// Process a single outcome from the channel.
/// Returns true if processing should continue, false if stop was requested.
fn process_outcome_static<F, G>(
    outcome: ReadOutcome,
    metrics: &mut AsyncReadMetrics,
    success_callback: &mut F,
    failure_callback: &mut G,
    results_processed: &mut u16,
) -> bool
where
    F: FnMut(ffi::t_docId, DocumentMetadata, u64),
    G: FnMut(u64) -> bool,
{
    match outcome {
        ReadOutcome::Found {
            doc_id,
            dmd,
            user_data,
        } => {
            metrics.reads_found += 1;
            success_callback(doc_id, dmd, user_data);
            *results_processed += 1;
            true // Always continue for success
        }
        ReadOutcome::NotFound { user_data } => {
            metrics.reads_not_found += 1;
            failure_callback(user_data)
        }
        ReadOutcome::Error { user_data } => {
            metrics.reads_errors += 1;
            failure_callback(user_data)
        }
    }
}

/// Mutable context for polling operations, grouping related state to reduce argument count.
struct PollContext<'a, F, G>
where
    F: FnMut(ffi::t_docId, DocumentMetadata, u64),
    G: FnMut(u64) -> bool,
{
    receiver: &'a mut UnboundedReceiver<ReadOutcome>,
    pending_count: &'a mut u16,
    metrics: &'a mut AsyncReadMetrics,
    success_callback: &'a mut F,
    failure_callback: &'a mut G,
    results_processed: u16,
    /// Set to true when failure callback returns false, indicating processing should stop.
    stop_requested: bool,
}

impl<'a, F, G> PollContext<'a, F, G>
where
    F: FnMut(ffi::t_docId, DocumentMetadata, u64),
    G: FnMut(u64) -> bool,
{
    /// Processes a single outcome, updating metrics and invoking the appropriate callback.
    /// Returns true if processing should continue, false if stop was requested.
    fn process_outcome(&mut self, outcome: ReadOutcome) -> bool {
        *self.pending_count -= 1;
        let should_continue = process_outcome_static(
            outcome,
            self.metrics,
            self.success_callback,
            self.failure_callback,
            &mut self.results_processed,
        );
        if !should_continue {
            self.stop_requested = true;
        }
        should_continue
    }
}

/// Collects ready outcomes from the channel and processes them via callbacks.
///
/// This async function handles two phases:
/// 1. **Blocking wait phase**: If `timeout_ms > 0` and there are pending reads,
///    waits up to the timeout for at least one result to become available.
/// 2. **Non-blocking drain phase**: Drains any additional ready results from
///    the channel without waiting.
///
/// # Arguments
/// * `ctx` - Mutable polling context containing receiver, metrics, and callbacks
/// * `timeout_ms` - Maximum time to wait for the first result (0 = non-blocking)
/// * `max_results` - Maximum number of results to process in this call
async fn collect_and_process_ready_outcomes<F, G>(
    ctx: &mut PollContext<'_, F, G>,
    timeout_ms: u32,
    max_results: u16,
) where
    F: FnMut(ffi::t_docId, DocumentMetadata, u64),
    G: FnMut(u64) -> bool,
{
    // Phase 1: If timeout > 0 and we have pending tasks, wait for at least one result
    if timeout_ms > 0
        && *ctx.pending_count > 0
        && ctx.results_processed < max_results
        && !ctx.stop_requested
    {
        get_first_outcome_with_timeout(ctx, timeout_ms).await;
    }

    // Phase 2: Non-blocking drain of any additional ready results
    drain_ready_outcomes(ctx, max_results);
}

/// Gets the first outcome with a timeout.
///
/// Blocks until either an outcome is received or the timeout expires.
/// If an outcome is received, it is processed immediately.
///
/// # Arguments
/// * `ctx` - Mutable polling context
/// * `timeout_ms` - Maximum time to wait in milliseconds
async fn get_first_outcome_with_timeout<F, G>(ctx: &mut PollContext<'_, F, G>, timeout_ms: u32)
where
    F: FnMut(ffi::t_docId, DocumentMetadata, u64),
    G: FnMut(u64) -> bool,
{
    match tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms as u64),
        ctx.receiver.recv(),
    )
    .await
    {
        Ok(Some(outcome)) => {
            ctx.process_outcome(outcome);
        }
        Ok(None) => {
            // Channel closed - should not happen since we hold the sender
        }
        Err(_) => {
            // Timeout expired - no results ready within the time limit
        }
    }
}

/// Drains all immediately available outcomes from the channel without blocking.
///
/// Continues processing outcomes until either:
/// - `max_results` have been processed
/// - No more pending reads remain
/// - The channel is empty or disconnected
///
/// # Arguments
/// * `ctx` - Mutable polling context
/// * `max_results` - Maximum total results to process
fn drain_ready_outcomes<F, G>(ctx: &mut PollContext<'_, F, G>, max_results: u16)
where
    F: FnMut(ffi::t_docId, DocumentMetadata, u64),
    G: FnMut(u64) -> bool,
{
    while ctx.results_processed < max_results && *ctx.pending_count > 0 && !ctx.stop_requested {
        match ctx.receiver.try_recv() {
            Ok(outcome) => {
                ctx.process_outcome(outcome);
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
}

/// Internal message sent through the channel when an async read completes.
enum ReadOutcome {
    /// Document was found with metadata.
    Found {
        doc_id: ffi::t_docId,
        dmd: DocumentMetadata,
        user_data: u64,
    },
    /// Document was not found (deleted or never existed).
    NotFound { user_data: u64 },
    /// Read failed with an error.
    Error { user_data: u64 },
}

/// A pool for managing concurrent async document metadata reads.
///
/// Uses a tokio runtime to spawn async tasks and an mpsc channel to collect results.
/// The pool allows adding async read requests up to a maximum concurrency limit,
/// and polling for completed results. Results that are "not found" or errors
/// are reported as failures via the failure callback.
pub struct AsyncReadPool<'a> {
    doc_table: &'a DocTable,
    runtime: tokio::runtime::Runtime,
    sender: UnboundedSender<ReadOutcome>,
    receiver: UnboundedReceiver<ReadOutcome>,
    max_concurrent: u16,
    /// Number of tasks currently spawned that haven't been received yet.
    pending_count: u16,
    metrics: AsyncReadMetrics,
}

impl<'a> AsyncReadPool<'a> {
    /// Creates a new async read pool.
    ///
    /// # Arguments
    /// * `doc_table` - Reference to the document table for issuing reads
    /// * `max_concurrent` - Maximum number of concurrent pending reads
    ///
    /// # Returns
    /// `Some(pool)` on success, `None` if the tokio runtime could not be created.
    pub fn new(doc_table: &'a DocTable, max_concurrent: u16) -> Option<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .ok()?;
        let (sender, receiver) = mpsc::unbounded_channel();

        Some(Self {
            doc_table,
            runtime,
            sender,
            receiver,
            max_concurrent,
            pending_count: 0,
            metrics: AsyncReadMetrics::default(),
        })
    }

    /// Adds an async read request for the given document ID.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID to read
    /// * `user_data` - Generic user data to associate with this read (returned in ReadResult)
    ///
    /// # Returns
    /// `true` if the request was added, `false` if the pool is at capacity.
    /// When `false` is returned, caller should poll for results before retrying.
    pub fn add_read(&mut self, doc_id: ffi::t_docId, user_data: u64) -> bool {
        if self.pending_count >= self.max_concurrent {
            return false;
        }

        let future = self.doc_table.request_document_metadata_async(doc_id);
        let sender = self.sender.clone();

        self.runtime
            .spawn(read_and_send_outcome(future, sender, doc_id, user_data));

        self.pending_count += 1;
        self.metrics.total_reads_requested += 1;
        true
    }

    /// Polls for ready results.
    ///
    /// # Arguments
    /// * `timeout_ms` - 0 for non-blocking, >0 to wait up to that many milliseconds
    /// * `max_results` - Maximum number of results to process
    /// * `success_callback` - Called for each ready result with (doc_id, dmd, user_data)
    /// * `failure_callback` - Called for each failed read with user_data
    ///
    /// # Returns
    /// Number of pending reads remaining.
    pub fn poll_with_callbacks<F, G>(
        &mut self,
        timeout_ms: u32,
        max_results: u16,
        mut success_callback: F,
        mut failure_callback: G,
    ) -> u16
    where
        F: FnMut(ffi::t_docId, DocumentMetadata, u64),
        G: FnMut(u64) -> bool,
    {
        let mut ctx = PollContext {
            receiver: &mut self.receiver,
            pending_count: &mut self.pending_count,
            metrics: &mut self.metrics,
            success_callback: &mut success_callback,
            failure_callback: &mut failure_callback,
            results_processed: 0,
            stop_requested: false,
        };

        // Use block_on to drive the tokio runtime and receive results
        self.runtime.block_on(collect_and_process_ready_outcomes(
            &mut ctx,
            timeout_ms,
            max_results,
        ));

        *ctx.pending_count
    }
}

impl Drop for AsyncReadPool<'_> {
    fn drop(&mut self) {
        // Accumulate this pool's metrics into the doc table's totals
        self.doc_table.accumulate_async_read_metrics(&self.metrics);
    }
}
