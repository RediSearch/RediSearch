//! Umbrella `dylib` crate aggregating the shared subgraph for the conditional
//! cdylib disk-plugin architecture.
//!
//! By depending on the shared in-repo crates here and building this crate as a
//! `dylib` (with `-C prefer-dynamic`), the transitive shared crates are linked
//! statically *into* `libsearch_shared.so`. Downstream cdylibs (`redisearch.so`,
//! `redisearch_disk.so`) that also depend on these crates and build
//! `-C prefer-dynamic` resolve them to this single dynamic copy, giving one
//! instance of every shared process-global (and one allocator / `libstd`).

// Re-export so downstream crates can reach the shared globals through the
// umbrella, and so the linker keeps the symbols.
pub use ffi;
pub use index_result;
pub use index_spec;
pub use inverted_index;
pub use query_error;
pub use query_term;
pub use rqe_iterators;

/// Reports whether the shared `rqe_iterators::SEARCH_ENTERPRISE_ITERATORS` is
/// initialised, as observed *through this dylib* (`libsearch_shared.so`).
///
/// Returns `1` when the disk-iterators API has been installed, `0` otherwise.
///
/// This is the read side of the cross-`.so` interop proof: the dlopened disk
/// plugin (`redisearch_disk.so`) installs the shared `SEARCH_ENTERPRISE_ITERATORS`
/// via its `SearchDiskPlugin_SetAPI` entry point, and a C harness calls this
/// function to confirm the write is visible to the single shared instance that
/// `redisearch.so` also binds — proving SVH-matched runtime resolution.
#[unsafe(no_mangle)]
pub extern "C" fn SearchShared_IteratorsApiIsSet() -> std::ffi::c_int {
    std::ffi::c_int::from(rqe_iterators::SEARCH_ENTERPRISE_ITERATORS.get().is_some())
}

/// A shared, allocation-free process global living in `libsearch_shared.so`,
/// used purely to prove cross-`.so` shared-mutable-state visibility without
/// depending on the Redis allocator (which is only initialised inside a live
/// `redis-server`).
///
/// The dlopened disk plugin writes a sentinel here via
/// [`SearchShared_InteropMarkerSet`] and a C harness reads it back via
/// [`SearchShared_InteropMarkerGet`]; if the read observes the plugin's write,
/// both `.so`s bind the single instance in `libsearch_shared.so`.
static INTEROP_MARKER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Write side of the allocation-free interop marker. Stores `value` into the
/// shared `INTEROP_MARKER` in `libsearch_shared.so`. See [`INTEROP_MARKER`].
#[unsafe(no_mangle)]
pub extern "C" fn SearchShared_InteropMarkerSet(value: u64) {
    INTEROP_MARKER.store(value, std::sync::atomic::Ordering::SeqCst);
}

/// Read side of the allocation-free interop marker. Returns the current value of
/// the shared `INTEROP_MARKER` in `libsearch_shared.so`. See [`INTEROP_MARKER`].
#[unsafe(no_mangle)]
pub extern "C" fn SearchShared_InteropMarkerGet() -> u64 {
    INTEROP_MARKER.load(std::sync::atomic::Ordering::SeqCst)
}
