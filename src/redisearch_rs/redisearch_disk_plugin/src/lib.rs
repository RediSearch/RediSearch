//! The dlopened disk plugin `redisearch_disk.so` (conditional cdylib architecture).
//!
//! `redisearch.so` (the [`redisearch_core`] cdylib) statically contains NO speedb /
//! disk / vecsim code. On RoF it `dlopen`s this sibling `redisearch_disk.so` with
//! `RTLD_NOW | RTLD_GLOBAL` and resolves the `SearchDiskPlugin_{Has,Get,Set}API`
//! entry points (see `redisearch_core::disk_forwarder`).
//!
//! This crate is a thin shell:
//! - It re-exports the superrepo `redisearch_disk` package, which defines the
//!   `SearchDiskPlugin_*` entry points and the whole disk layer (speedb +
//!   vecsim_disk).
//! - It depends on `search_shared`, so under `-C prefer-dynamic` the shared
//!   subgraph (`rqe_iterators::SEARCH_ENTERPRISE_ITERATORS`, allocator / `libstd`)
//!   resolves to the single instance in `libsearch_shared.so` that `redisearch.so`
//!   also binds — letting the plugin register disk iterators that the core's query
//!   engine reads back across the `.so` boundary.
//!
//! Living inside the core Cargo workspace, it is compiled in the SAME build graph
//! as `redisearch_core`, so both link the exact same `search_shared` (one SVH).

// Force a link edge to the umbrella dylib so the shared subgraph binds the single
// `libsearch_shared.so` instance under `-C prefer-dynamic`.
use search_shared as _;

// Pull in the disk layer. The `SearchDiskPlugin_{Has,Get,Set}API` `#[no_mangle]`
// exports live in this crate; `build.rs` keeps them in the cdylib's dynamic symbol
// table via `-Wl,--undefined=...`.
use redisearch_disk as _;
