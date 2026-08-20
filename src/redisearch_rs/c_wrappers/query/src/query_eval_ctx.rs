/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::QueryEvalCtx`].

use std::{
    ffi::{CStr, c_char},
    ptr::NonNull,
};

use c_trie::TermsTrie;
use query_flags::QEFlags;
use rlookup::{MetricRequest, RLookupKey, RLookupKeyHandle};
use rqe_core::DocId;
use rqe_iterators::{
    IteratorsConfig,
    not_reducer::TIMEOUT_CHECK_GRANULARITY,
    utils::{AnyTimeoutContext, TimeoutContextBlockedClient},
};
use search_disk::SearchDiskHandle;

use query_types::scorers::{BuiltInScorer, RequestedScorer};

/// A reservation in the query's metric-request array, as
/// [`QueryEvalContext::add_metric_request`] hands it out.
///
/// Reserving and binding are separate steps because the iterator whose key slot
/// the request points at does not exist yet at reserve time. That split is what
/// this type guards: it is neither [`Copy`] nor [`Clone`] and its index is
/// private, so a reservation can only be bound by the one who made it, and only
/// once. Binding twice would strand the first handle — unreachable from the
/// array, and so never freed.
///
/// Dropping one unbound is legal and expected: a vector node reserves before it
/// knows whether the search will yield a distance to bind, and leaves the
/// request unbound when it does not.
#[derive(Debug)]
pub struct MetricRequestId(usize);

impl MetricRequestId {
    /// Where the reservation sits in
    /// [`metric_requests`](QueryEvalContext::metric_requests).
    ///
    /// Reading the index does not spend the reservation, and no
    /// [`MetricRequestId`] can be rebuilt from one, so this weakens nothing.
    pub const fn index(&self) -> usize {
        self.0
    }
}

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
    ///    `sctx` must additionally stay valid, and at a stable address, for the
    ///    lifetime of every timeout context and iterator derived from this
    ///    context (e.g. via
    ///    [`build_timeout_context`](QueryEvalContext::build_timeout_context)):
    ///    a clock-based timeout context reads `sctx.time.timeout` back on every
    ///    probe rather than capturing it.
    ///    The nested `sctx.spec.terms` pointer — the index's primary terms trie
    ///    — must be valid and non-null: every path that creates an
    ///    [`IndexSpec`](ffi::IndexSpec) installs a terms trie, unconditionally
    ///    and regardless of whether the schema has any text field, before the
    ///    spec can answer a query. Loading a legacy spec is the one path that
    ///    leaves the field null for a while, and it fills it in — or releases
    ///    the half-built spec — before handing the spec out, so no queryable
    ///    spec has a null trie.
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
    ///    The head `metricRequestsP` points *at* must be either null — the
    ///    empty list — or a live `array.h` tracked array of initialised
    ///    [`MetricRequest`]s, that being the only shape carrying the length
    ///    header that [`metric_requests`](QueryEvalContext::metric_requests)
    ///    reads back. A plain allocation holding one [`MetricRequest`] would
    ///    satisfy every other clause here and still make that safe method read
    ///    outside it.
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

    /// Raw pointer to the [`ffi::RedisSearchCtx`], for passing to C functions
    /// that take a `const RedisSearchCtx *`.
    pub const fn sctx_ptr(&self) -> *const ffi::RedisSearchCtx {
        self.as_ref().sctx
    }

    /// The [`ffi::IndexSpec`] being queried.
    pub fn spec(&self) -> &ffi::IndexSpec {
        // SAFETY: invariant (2) of `new` guarantees `sctx.spec` is a valid,
        // non-null pointer.
        unsafe { &*self.sctx().spec }
    }

    /// The index's primary terms trie, the one every term lookup and pattern
    /// expansion walks.
    ///
    /// The trie is always there, per invariant (2) of [`new`](Self::new), which
    /// also says why. The assertion below states that here rather than leaving
    /// each caller to assume it: a spec that broke the invariant would otherwise
    /// be a null dereference in whichever one ran first, with nothing naming
    /// what went wrong.
    ///
    /// `'index` is chosen by the caller and tied to nothing, because the
    /// borrow this hands out is not one the type system can see: the trie is
    /// owned by the spec rather than by the context, so binding it to `&self`
    /// would say the trie is borrowed *from the context* — and then a caller
    /// could not hold it while borrowing the context exclusively, which is
    /// exactly what an expanding node type does across a walk that records
    /// expansions back into the context. Erasing the lifetime is what buys that,
    /// and it is why this is `unsafe`: nothing left in the signature stops the
    /// reference outliving the trie.
    ///
    /// # Safety
    ///
    /// The returned reference must not outlive the query being evaluated. The
    /// spec that owns the trie stays alive for the whole query (invariant (2)
    /// of [`new`](Self::new)), so any `'index` within that span is sound, and
    /// `'static` — or any lifetime reaching past the query — is not.
    pub unsafe fn terms_trie<'index>(&self) -> &'index TermsTrie {
        let terms = self.spec().terms;
        debug_assert!(
            !terms.is_null(),
            "the spec of a query being evaluated must have a terms trie"
        );
        // SAFETY: `terms` is the spec's terms `Trie`: non-null, valid, and
        // neither mutated nor freed for the duration of the query (invariants
        // (1)/(2) of `new`). The caller's own contract above keeps `'index`
        // inside that span.
        unsafe { TermsTrie::from_raw(terms) }
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

    /// Reserve a [`MetricRequest`] for `metric_name`, returning the
    /// [`MetricRequestId`] that binds it.
    ///
    /// The reserved entry has a NULL [`key_handle`](MetricRequest::key_handle)
    /// until [`bind_metric_request_key`](Self::bind_metric_request_key) fills
    /// it in; that is the state the pipeline reads as "the iterator was never
    /// created". An id stays valid for the whole evaluation: appending only
    /// ever grows the array past the existing entries.
    ///
    /// `is_internal` is recorded as [`MetricRequest::is_internal`], which keeps
    /// the metric out of the query response.
    ///
    /// # Safety
    ///
    /// 1. `metric_name` must be non-null and point to a NUL-terminated C string.
    ///    Pipeline construction takes its length with `strlen` and looks the
    ///    name up in the schema, without checking either — so a null or
    ///    unterminated name is a crash or an out-of-bounds read at
    ///    `FT.SEARCH` / `FT.AGGREGATE` time, far from this call.
    /// 2. The request stores `metric_name` as a *borrowed* pointer — nothing
    ///    ever frees it through the array, and the pipeline reads it back long
    ///    after this call. Its owner (for a vector query, the `VectorQuery`,
    ///    hence the AST) must outlive the array.
    pub unsafe fn add_metric_request(
        &mut self,
        metric_name: *const c_char,
        is_internal: bool,
    ) -> MetricRequestId {
        debug_assert!(!metric_name.is_null(), "a metric request must have a name");

        let head = self
            .as_mut()
            .metricRequestsP
            .cast::<*mut MetricRequest<'_>>();
        let request = MetricRequest {
            metric_name,
            key_handle: std::ptr::null_mut(),
            is_internal,
        };

        // SAFETY: invariant (2) of `new` guarantees `metricRequestsP` points to
        // a live head slot, and (3) gives us exclusive access to it.
        let current = unsafe { *head };
        // SAFETY: `current` is a tracked array of `MetricRequest`s (null while
        // the array is still empty, which the callee treats as "allocate"), and
        // `request` is one live, correctly sized element to copy from.
        let grown = unsafe {
            ffi::array_ensure_append_n_func(
                current.cast(),
                (&raw const request).cast_mut().cast(),
                1,
                size_of::<MetricRequest<'_>>() as u16,
            )
        };
        // The append may reallocate, so the new head goes back through the
        // double pointer.
        //
        // SAFETY: as for the read above.
        unsafe { *head = grown.cast() };
        // SAFETY: `grown` is the tracked array the append just returned, so its
        // length header is valid — and non-zero, since it holds `request`.
        MetricRequestId(unsafe { ffi::array_len_func(grown) as usize - 1 })
    }

    /// Allocate the key handle of the request `id` reserved, pointing at
    /// `key_ptr` — the owning iterator's [`RLookupKey`] slot — and return it so
    /// the caller can hand it back to that iterator.
    ///
    /// The handle is allocated with the *module* allocator, because C frees it
    /// with `rm_free` when the AST is destroyed. That allocator does not zero,
    /// so **both** fields are written:
    /// [`key_ptr`](RLookupKeyHandle::key_ptr) as given, and
    /// [`is_valid`](RLookupKeyHandle::is_valid) set. The latter is what
    /// pipeline construction gates the key write on, and the owning iterator
    /// clears it when freed — leaving it at whatever the allocator returned
    /// would drop the metric nondeterministically.
    ///
    /// # Safety
    ///
    /// 1. `id` must come from [`add_metric_request`](Self::add_metric_request)
    ///    on *this* context. Taking it by value is what rules out binding one
    ///    reservation twice, but a [`MetricRequestId`] records no context, so
    ///    which one it came from is still the caller's to get right.
    /// 2. `key_ptr` must be non-null and valid for writes until pipeline
    ///    construction has read the request — *not* merely for as long as the
    ///    owning iterator lives. Pipeline construction writes the resolved key
    ///    through it whenever [`is_valid`](RLookupKeyHandle::is_valid) is still
    ///    set, without checking either pointer, so a null or dangling slot is a
    ///    bad write at `FT.SEARCH` / `FT.AGGREGATE` time, far from this call.
    /// 3. The caller must install the returned handle on the iterator that owns
    ///    `key_ptr` before that iterator can be freed. Clearing
    ///    [`is_valid`](RLookupKeyHandle::is_valid) is the iterator's job, and it
    ///    can only do it through a handle it was given: an uninstalled handle
    ///    keeps the flag set past the death of the slot it points at, which is
    ///    precondition (2) violated by omission rather than by a bad argument.
    ///    Reserving and binding are separate calls because the iterator does not
    ///    exist yet at reserve time — that split is what makes this one easy to
    ///    miss, so it is stated rather than left to the shape of the API.
    /// 4. The metric-request array must outlive every iterator holding one of
    ///    its handles. The handle is freed with the array when the AST is
    ///    destroyed, and an iterator freed after that would clear
    ///    [`is_valid`](RLookupKeyHandle::is_valid) through freed memory.
    pub unsafe fn bind_metric_request_key<'a>(
        &mut self,
        id: MetricRequestId,
        key_ptr: *mut *mut RLookupKey<'a>,
    ) -> *mut RLookupKeyHandle<'a> {
        debug_assert!(
            !key_ptr.is_null(),
            "a bound metric request must have a key slot"
        );
        let idx = id.index();

        let head_slot = self
            .as_mut()
            .metricRequestsP
            .cast::<*mut MetricRequest<'a>>();
        // SAFETY: invariant (2) of `new` guarantees `metricRequestsP` points to
        // a live head slot, and (3) gives us exclusive access to it.
        let head = unsafe { *head_slot };
        // An id proves a reservation happened, but not that it happened *here*,
        // so the bounds check stays. Debug-only, so the length read costs
        // nothing in a release build. An empty list needs no check of its own:
        // `array_len` reports a null head as length zero rather than reaching
        // behind it for a header that is not there.
        #[cfg(debug_assertions)]
        {
            // SAFETY: `head` is either null or the tracked array
            // `add_metric_request` grew, and the length read accepts both.
            let len = unsafe { ffi::array_len_func(head.cast()) } as usize;
            assert!(
                idx < len,
                "`id` must come from `add_metric_request` on this context"
            );
        }

        // Allocated only once the checks above have passed, so a violated
        // contract reports itself instead of leaking the handle on the way out.
        //
        // SAFETY: this Redis API function pointer is set once during module
        // load and never mutated afterwards, so reading it during query
        // evaluation cannot race.
        let alloc = unsafe { redis_module::RedisModule_Alloc }.expect("RedisModule_Alloc unset");
        // SAFETY: the size is non-zero and well within `isize::MAX`.
        let handle = NonNull::new(unsafe { alloc(size_of::<RLookupKeyHandle<'a>>()) })
            .expect("RedisModule_Alloc returned NULL")
            .cast::<RLookupKeyHandle<'a>>()
            .as_ptr();
        // SAFETY: `handle` is a fresh, suitably aligned allocation of the right
        // size, so writing the whole value initialises it — both fields, since
        // the allocator does not zero.
        unsafe {
            handle.write(RLookupKeyHandle {
                key_ptr,
                is_valid: true,
            });
        }

        // SAFETY: invariant (1) makes `idx` an index of a live element, so the
        // offset is in bounds.
        let request = unsafe { head.add(idx) };
        // SAFETY: `request` points at that live, initialised element, which
        // nothing else aliases (invariant (3) of `new`).
        unsafe { (*request).key_handle = handle };

        handle
    }

    /// The metric requests reserved so far, in the order they were reserved.
    ///
    /// The read side of [`add_metric_request`](Self::add_metric_request), which
    /// [`MetricRequestId::index`] indexes into.
    /// The slice borrows this context for as long as it is held, and every
    /// append takes it exclusively, so none can run while it is alive.
    pub fn metric_requests(&self) -> &[MetricRequest<'_>] {
        let head_slot = self
            .as_ref()
            .metricRequestsP
            .cast::<*mut MetricRequest<'_>>();
        // SAFETY: invariant (2) of `new` guarantees `metricRequestsP` points to
        // a live head slot, and gives the head itself the tracked-array shape
        // the length read below depends on.
        let head = unsafe { *head_slot };
        if head.is_null() {
            // The list is a tracked array, whose empty state is a null head
            // with no length header behind it to read.
            return &[];
        }
        // SAFETY: a non-null head points just past the length header of a
        // tracked array, which is what records how many elements follow it.
        let len = unsafe { ffi::array_len_func(head.cast()) } as usize;
        // SAFETY: those `len` elements are initialised `MetricRequest`s, and
        // the array lives as long as the context holding its head — which the
        // returned slice borrows, so nothing can append to it and move it out
        // from under the slice.
        unsafe { std::slice::from_raw_parts(head, len) }
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

    /// Raw mutable pointer to the [`ffi::DocTable`], for passing to C functions
    /// that take a `DocTable *`.
    pub const fn doc_table_mut(&self) -> *mut ffi::DocTable {
        self.as_ref().docTable
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
    /// flag. Otherwise the Clock Based Timeout (or [`NoTimeoutChecker`], when timeout
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
    /// after the `AREQ` behind `bcTimeoutAreq` is freed — nor after `sctx` is
    /// freed or moved, since the clock-based variant reads the deadline out of
    /// it on every probe. No write to `sctx.time.timeout` may overlap a probe;
    /// see [`TimeoutContextDeadline::new`](rqe_iterators::utils::TimeoutContextDeadline::new).
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
    /// [`NoTimeoutChecker`]: rqe_iterators::utils::NoTimeoutChecker
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
            // (or `NoTimeoutChecker`) from `sctx.time`.
            None => {
                let sctx = NonNull::new(self.sctx_ptr().cast_mut()).expect("sctx must be non-null");
                // SAFETY: invariant (2) of `new` guarantees `sctx` stays valid for the lifetime of
                // every timeout context and iterator derived from this one, which is what
                // `from_sctx` needs to read the deadline back on each probe. Writes to the
                // deadline never overlap a probe (see `TimeoutContextDeadline::new`).
                unsafe { AnyTimeoutContext::from_sctx(sctx, TIMEOUT_CHECK_GRANULARITY) }
            }
        }
    }
}
