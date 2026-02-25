//! Compaction module for RediSearchDisk.
//!
//! This module contains the delta structures and FFI mechanisms for
//! updating in-memory structures after disk compaction completes.
//!
//! # Architecture Overview
//!
//! The compaction system works as follows:
//! - GC Thread runs in C and calls into Rust
//! - Compaction AND Apply logic runs in Rust
//! - Rust calls FFI functions on the C IndexSpec for:
//!   - Synchronization (acquire/release locks)
//!   - Memory updates (update trie term doc counts, numTerms)
//!
//! # Delta Application Flow
//!
//! 1. Compaction aggregator runs on SpeeDB thread, builds `CompactionDelta`
//! 2. Delta is returned to GC thread after compaction completes
//! 3. GC thread applies delta via FFI calls to update C structures
//! 4. Rust-owned structures (DeletedIds) are updated directly

pub mod apply;
pub mod c_index_spec;
pub mod callback_trait;
pub mod collector;
pub mod delta;

pub use apply::apply_delta;
pub use c_index_spec::{IndexSpecLockGuard, IndexSpecUpdater};
pub use callback_trait::{MergeCallbacks, NoOpCallbacks};
pub use collector::CompactionDeltaCollector;
pub use delta::CompactionDelta;
