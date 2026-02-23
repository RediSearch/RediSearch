mod archived_block;
#[cfg(not(miri))]
mod disk_context;
#[cfg(not(miri))]
mod doc_table;
mod document_flags;
#[cfg(not(miri))]
mod index_spec;
#[cfg(not(miri))]
mod inverted_index;
#[cfg(not(miri))]
mod tag;
#[cfg(not(miri))]
mod term;
#[cfg(not(miri))]
mod throttle;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();
