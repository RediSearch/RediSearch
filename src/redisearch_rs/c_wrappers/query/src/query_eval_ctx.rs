/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::QueryEvalCtx`].

use std::{ffi::CStr, ptr::NonNull};

use query_flags::QEFlags;
use rlookup::MetricRequest;
use rqe_core::DocId;
use rqe_iterators::{
    IteratorsConfig,
    not_reducer::TIMEOUT_CHECK_GRANULARITY,
    utils::{AnyTimeoutContext, TimeoutContextBlockedClient},
};
use search_disk::SearchDiskHandle;

use query_types::scorers::{BuiltInScorer, RequestedScorer};

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
/// - `inNotSubTree` — temporarily set to `true` while evaluating the child of
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
    ///    The nested `sctx.spec.diskSpec` pointer may be null (in-memory mode);
    ///    when non-null it must point to a valid
    ///    [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec).
    ///    `bcTimeoutAreq` may be null; when non-null it must point to a valid
    ///    [`AREQ`](ffi::AREQ) that stays valid not just for the lifetime of the returned
    ///    context, but for the lifetime of every timeout context and iterator
    ///    derived from it (e.g. via
    ///    [`build_timeout_context`](QueryEvalContext::build_timeout_context)).
    ///    The `opts.scorerName` pointer may be null (no scorer requested); when
    ///    non-null it must point to a valid NUL-terminated C string that stays
    ///    valid for at least the lifetime of the returned context (read by
    ///    [`scorer`](QueryEvalContext::scorer)).
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

    /// The query-wide default slop (max term distance) for phrase matching.
    ///
    /// A node may override this; a value of `-1` means no phrase constraint.
    pub const fn slop(&self) -> i32 {
        self.opts().slop
    }

    /// Whether the query-wide `INORDER` flag (`Search_InOrder`) is set, forcing
    /// phrase terms to match in order regardless of per-node options.
    pub const fn search_in_order(&self) -> bool {
        self.opts().flags & ffi::RSSearchFlags_Search_InOrder != 0
    }

    /// The scorer this query requested, as a [`RequestedScorer`].
    ///
    /// This reports only the query's own choice; it does **not** apply any
    /// default. A null scorer name is [`Unset`](RequestedScorer::Unset); a
    /// set name resolves to [`BuiltIn`](RequestedScorer::BuiltIn) when it
    /// matches a built-in, otherwise [`Custom`](RequestedScorer::Custom)
    /// carrying the requested name. The caller decides the fallback for each
    /// variant.
    pub fn scorer(&self) -> RequestedScorer<'_> {
        let Some(ptr) = NonNull::new(self.opts().scorerName.cast_mut()) else {
            return RequestedScorer::Unset;
        };
        // SAFETY: invariant (2) of `new` guarantees `opts` is valid and that its
        // `scorerName`, non-null here, points to a valid NUL-terminated C string
        // that stays valid for at least the lifetime of the returned context, and
        // thus of `&self` — which bounds the returned `RequestedScorer`'s borrow.
        let name = unsafe { CStr::from_ptr(ptr.as_ptr()) };
        match BuiltInScorer::from_c_str(name) {
            Some(scorer) => RequestedScorer::BuiltIn(scorer),
            None => RequestedScorer::Custom(name),
        }
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

    /// Raw pointer to the [`ffi::QueryError`] accumulator, for passing to C
    /// functions that report errors into it.
    pub const fn status_ptr(&self) -> *mut ffi::QueryError {
        self.as_ref().status
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
    pub const fn in_not_sub_tree(&self) -> bool {
        self.as_ref().inNotSubTree
    }

    /// Set the `inNotSubTree` flag, returning the previous value.
    pub const fn set_in_not_sub_tree(&mut self, value: bool) -> bool {
        let inner = self.as_mut();
        let prev = inner.inNotSubTree;
        inner.inNotSubTree = value;
        prev
    }

    /// Build the [`AnyTimeoutContext`] a query iterator should use for this
    /// evaluation.
    ///
    /// When a Blocked Client Timeout request is wired into the context
    /// (`bcTimeoutAreq` non-null) the iterator polls that request's timeout
    /// flag. Otherwise the Clock Based Timeout (or [`NoTimeout`], when timeout
    /// checks are skipped or no deadline is set) is derived from `sctx.time`.
    ///
    /// The returned [`AnyTimeoutContext`] is `'static`: when a Blocked Client
    /// Timeout is wired in it holds the `AREQ` as a raw pointer, not a borrow, so
    /// the type system no longer ties it to the request. That validity is now a
    /// runtime precondition (see below), which is why this method is `unsafe`.
    ///
    /// # Safety
    ///
    /// The returned context, and any iterator built from it, must not be used
    /// after the `AREQ` behind `bcTimeoutAreq` is freed.
    ///
    /// A Blocked Client Timeout context holds that `AREQ` as a raw pointer with
    /// no lifetime, so nothing enforces the precondition at compile time:
    /// probing the context calls [`AREQ_CheckTimedOut`](ffi::AREQ_CheckTimedOut)
    /// on the stored pointer. For a [`QueryEvalContext`] built through
    /// [`new`](Self::new), invariant (2) already guarantees `bcTimeoutAreq`
    /// outlives every timeout context and iterator derived from it, so the
    /// caller discharges the precondition simply by not retaining the returned
    /// context beyond the current query. See [`TimeoutContextBlockedClient::new`].
    ///
    /// [`NoTimeout`]: rqe_iterators::utils::NoTimeout
    pub unsafe fn build_timeout_context(&self) -> AnyTimeoutContext {
        match NonNull::new(self.as_ref().bcTimeoutAreq) {
            Some(areq) => {
                // SAFETY: invariant (2) of `new` guarantees a non-null
                // `bcTimeoutAreq` points to a valid `AREQ` that outlives every
                // iterator built from this context; this method's own safety
                // contract requires the caller not to use the returned context
                // past that window — together they satisfy the
                // `TimeoutContextBlockedClient::new` contract.
                let timeout = unsafe { TimeoutContextBlockedClient::new(areq) };
                AnyTimeoutContext::BlockedClient(timeout)
            }
            // No Blocked Client Timeout source: derive the Clock Based Timeout
            // (or `NoTimeout`) from `sctx.time`.
            None => AnyTimeoutContext::from_sctx(self.sctx(), TIMEOUT_CHECK_GRANULARITY),
        }
    }
}
