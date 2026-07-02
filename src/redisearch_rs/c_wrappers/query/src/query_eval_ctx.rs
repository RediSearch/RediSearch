/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::QueryEvalCtx`].

use std::ptr::NonNull;

use query_flags::QEFlags;
use rlookup::MetricRequest;
use rqe_core::DocId;
use rqe_iterators::IteratorsConfig;
use search_disk::SearchDiskHandle;

/// Safe wrapper around [`ffi::QueryEvalCtx`].
///
/// The C `QueryEvalCtx` is the shared mutable state threaded through every
/// `Query_Eval*Node` function while converting a parsed query AST into an
/// executable iterator tree.  This wrapper provides typed accessors so that
/// Rust evaluation code can interact with that state without scattering raw
/// pointer dereferences and field accesses across every call site.
///
/// # Mutability
///
/// Most fields are read-only during evaluation, but three are mutated:
///
/// - `tokenId` — monotonically incremented to assign a unique ID to each
///   token iterator created during evaluation.
/// - `numTokens` — incremented when term-expansion nodes (prefix, fuzzy, …)
///   produce additional iterators beyond those counted by the parser.
/// - `notSubtree` — temporarily set to `true` while evaluating the child of
///   a `NOT` node so that descendant `UNION` nodes know they can exit early
///   on the first match. Restored to its previous value afterwards.
pub struct QueryEvalContext(NonNull<ffi::QueryEvalCtx>);

impl QueryEvalContext {
    /// Wrap a raw [`NonNull`] pointer to a [`ffi::QueryEvalCtx`].
    ///
    /// # Safety
    ///
    /// 1. `ptr` must point to a [valid], properly initialised
    ///    [`ffi::QueryEvalCtx`].
    /// 2. All pointer fields within the [`ffi::QueryEvalCtx`] (`sctx`, `opts`,
    ///    `status`, `metricRequestsP`, `docTable`, `config`) and the nested
    ///    `sctx.spec` pointer must themselves be valid, non-null pointers.
    /// 3. The caller must have exclusive access to the pointer for the
    ///    lifetime of the returned [`QueryEvalContext`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn new(ptr: NonNull<ffi::QueryEvalCtx>) -> Self {
        Self(ptr)
    }

    /// Shared reference to the underlying [`ffi::QueryEvalCtx`].
    const fn as_ref(&self) -> &ffi::QueryEvalCtx {
        // SAFETY: invariant (1) of `new`.
        unsafe { self.0.as_ref() }
    }

    /// Exclusive reference to the underlying [`ffi::QueryEvalCtx`].
    const fn as_mut(&mut self) -> &mut ffi::QueryEvalCtx {
        // SAFETY: invariants (1) and (3) of `new`.
        unsafe { self.0.as_mut() }
    }

    /// The raw [`NonNull`] pointer to the underlying [`ffi::QueryEvalCtx`].
    pub const fn as_non_null(&self) -> NonNull<ffi::QueryEvalCtx> {
        self.0
    }

    /// The [`ffi::RedisSearchCtx`] that owns the index being queried.
    pub fn sctx(&self) -> &ffi::RedisSearchCtx {
        // SAFETY: invariant (2) of `new`.
        unsafe { &*self.as_ref().sctx }
    }

    /// The [`ffi::IndexSpec`] being queried.
    pub fn spec(&self) -> &ffi::IndexSpec {
        // SAFETY: invariant (2) of `new` guarantees `sctx.spec` is a valid,
        // non-null pointer.
        unsafe { &*self.sctx().spec }
    }

    /// The search options controlling field masks, scorer, query flags, etc.
    pub const fn opts(&self) -> &ffi::RSSearchOptions {
        // SAFETY: invariant (2) of `new`.
        unsafe { &*self.as_ref().opts }
    }

    /// The [`query_error::QueryError`] accumulator for reporting evaluation
    /// errors and warnings (e.g. max-prefix-expansion limits).
    ///
    /// # Panics
    ///
    /// Panics if the `status` pointer is null, which violates invariant (2)
    /// of [`QueryEvalContext::new`].
    pub fn status(&mut self) -> &mut query_error::QueryError {
        // SAFETY: invariant (2) of `new` guarantees `status` is a valid,
        // non-null pointer, and (3) guarantees exclusive access.
        // `ffi::QueryError` is the opaque representation of
        // `query_error::QueryError` — the `from_opaque_mut_ptr` transmute
        // is safe because both types have identical layout.
        unsafe {
            query_error::QueryError::from_opaque_mut_ptr(
                self.as_mut()
                    .status
                    .cast::<query_error::opaque::OpaqueQueryError>(),
            )
            .expect("status pointer is null")
        }
    }

    /// Double pointer to the metric-requests array.
    ///
    /// The C side appends entries via `array_ensure_append_1`. Rust callers
    /// that create vector iterators use this to register score-field metric
    /// requests.
    pub fn metric_requests_ptr(&self) -> &*mut MetricRequest<'_> {
        // SAFETY: invariant (2) of `new`.
        unsafe { &*self.as_ref().metricRequestsP.cast() }
    }

    /// Allocate the next token ID and return it (post-increment).
    ///
    /// Every token iterator receives a unique ID so that the scoring and
    /// offset-highlight machinery can attribute term frequencies to the
    /// correct query term.
    pub const fn next_token_id(&mut self) -> u32 {
        let inner = self.as_mut();
        let id = inner.tokenId;
        inner.tokenId += 1;
        id
    }

    /// The [`ffi::DocTable`] used to resolve document IDs from key names.
    pub fn doc_table(&self) -> &ffi::DocTable {
        // SAFETY: invariant (2) of `new`.
        unsafe { &*self.as_ref().docTable }
    }

    /// The highest document ID currently assigned in the index.
    ///
    /// In search-on-disk mode (`spec.diskSpec` non-null) the value comes from
    /// the disk index; otherwise it is read from the in-memory
    /// [`DocTable`](ffi::DocTable).
    pub fn max_doc_id(&self) -> DocId {
        // SAFETY: per invariant (1)/(2) of `new`, `spec.diskSpec` is either null
        // or a valid `RedisSearchDiskIndexSpec`.
        let disk = unsafe { SearchDiskHandle::new(self.spec().diskSpec) };
        match disk {
            Some(disk) => disk.max_doc_id(),
            None => self.doc_table().maxDocId,
        }
    }

    /// Request-type flags ([`QEFlags`] bitmask).
    pub fn req_flags(&self) -> QEFlags {
        QEFlags::from_bits(self.as_ref().reqFlags).expect("invalid QEFlags")
    }

    /// The [`IteratorsConfig`] snapshot taken at query start.
    pub fn config(&self) -> &IteratorsConfig {
        // SAFETY: invariant (2) of `new`. `ffi::IteratorsConfig` and
        // `IteratorsConfig` are both `#[repr(C)]` with identical layout —
        // the former is generated by bindgen from the cheadergen output of
        // the latter.
        unsafe { &*self.as_ref().config.cast() }
    }

    /// Whether evaluation is currently inside a `NOT` subtree.
    ///
    /// When `true`, `UNION` nodes may exit early on the first matching child
    /// because the NOT semantics only need to know *whether* a match exists,
    /// not its score.
    pub const fn not_subtree(&self) -> bool {
        self.as_ref().notSubtree
    }

    /// Set the `notSubtree` flag, returning the previous value.
    pub const fn set_not_subtree(&mut self, value: bool) -> bool {
        let inner = self.as_mut();
        let prev = inner.notSubtree;
        inner.notSubtree = value;
        prev
    }
}
