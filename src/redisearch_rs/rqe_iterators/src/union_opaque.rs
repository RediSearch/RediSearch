/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Dynamic dispatch wrapper over the concrete union variants.
//!
//! [`UnionOpaque`] is the type that sits behind every
//! [`RQEIteratorWrapper`] produced by the
//! FFI `NewUnionIterator` constructor. It holds one of the five concrete
//! union variants and forwards every [`RQEIterator`] call via match dispatch.
//!
//! This module lives in `rqe_iterators` (rather than in the FFI bridge crate)
//! so that [`c2rust::CRQEIterator`](crate::c2rust::CRQEIterator) can recover
//! the wrapper via
//! [`ref_from_header_ptr`](crate::interop::RQEIteratorWrapper::ref_from_header_ptr)
//! and call methods such as [`UnionOpaque::num_children_active`] directly,
//! without going through a C FFI trampoline.

use std::ffi::CStr;
use std::ptr::NonNull;

use ffi::QueryIterator;
use index_result::RSIndexResult;
use query_types::QueryNodeType;
use ref_mode::{Active, Ref, SharedPtr, Suspended};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome, UnionFullFlat,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    union_flat::RawUnionFlat,
    union_heap::RawUnionHeap,
    union_trimmed::RawUnionTrimmed,
};

use index_spec::IndexSpecReadGuard;

/// Enum holding all possible union iterator variants, parameterised over a
/// [`Ref`] mode. See [`UnionVariant`] for the [`Active`] instantiation.
#[repr(C)]
pub enum RawUnionVariant<'query, Rf: Ref, I> {
    FlatFull(RawUnionFlat<'query, Rf, I, false>),
    FlatQuick(RawUnionFlat<'query, Rf, I, true>),
    HeapFull(RawUnionHeap<'query, Rf, I, false>),
    HeapQuick(RawUnionHeap<'query, Rf, I, true>),
    Trimmed(RawUnionTrimmed<'query, Rf, I>),
}

/// Alias for an [`Active`] [`RawUnionVariant`] — the only instantiation
/// with a callable surface today.
pub type UnionVariant<'index, I> = RawUnionVariant<'index, Active<'index>, I>;

// Compile-time proof of invariant 1 on `RawUnionVariant`: for a representative
// concrete child, the `Active` and `Suspended` instantiations are
// layout-identical. Each variant's payload carries its own invariant 1, and the
// per-payload halves are statically enforced by `suspend_child_slot_in_place` /
// `resume_child_slot_in_place`, which is what `RawUnionOpaque`'s transitions
// drive them through; `#[repr(C)]` then puts every payload at the same offset
// under the same tag in both modes. What is asserted here is the whole-enum
// consequence the outer whole-box cast rests on.
//
// There is no per-variant `offset_of!`: naming an enum variant's field in
// `offset_of!` is still unstable.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawUnionVariant<'static, Active<'static>, AChild>;
    type S = RawUnionVariant<'static, Suspended, SChild>;
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, I: RQEIterator<'index>> UnionVariant<'index, I> {
    /// Converts this variant in place to [`UnionVariant::Trimmed`], switching
    /// to unsorted sequential-read mode.
    ///
    /// # Panics
    ///
    /// Panics if the variant has fewer than 3 children.
    pub fn trim(&mut self, limit: usize, asc: bool) {
        // We need ownership of the inner value to call `into_trimmed`.
        // `FlatFull` with an empty Vec is a cheap, valid placeholder that is
        // immediately overwritten on success.
        let placeholder = Self::FlatFull(UnionFullFlat::new(Vec::new()));
        let prev = std::mem::replace(self, placeholder);
        let trimmed = match prev {
            Self::FlatFull(u) => u.into_trimmed(limit, asc),
            Self::FlatQuick(u) => u.into_trimmed(limit, asc),
            Self::HeapFull(u) => u.into_trimmed(limit, asc),
            Self::HeapQuick(u) => u.into_trimmed(limit, asc),
            Self::Trimmed(u) => u.into_trimmed(limit, asc),
        };
        match trimmed {
            Some(t) => *self = Self::Trimmed(t),
            // Should not happen — TrimUnionIterator guards on >= 3 children.
            None => unreachable!("trim called with fewer than 3 children"),
        }
    }
}

// Delegate to the inner variant by shared reference.
macro_rules! delegate_variant_ref {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match &$self.variant {
            UnionVariant::FlatFull(it) => it.$method($($arg),*),
            UnionVariant::FlatQuick(it) => it.$method($($arg),*),
            UnionVariant::HeapFull(it) => it.$method($($arg),*),
            UnionVariant::HeapQuick(it) => it.$method($($arg),*),
            UnionVariant::Trimmed(it) => it.$method($($arg),*),
        }
    };
}

// Delegate to the inner variant by mutable reference.
macro_rules! delegate_variant_ref_mut {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match &mut $self.variant {
            UnionVariant::FlatFull(it) => it.$method($($arg),*),
            UnionVariant::FlatQuick(it) => it.$method($($arg),*),
            UnionVariant::HeapFull(it) => it.$method($($arg),*),
            UnionVariant::HeapQuick(it) => it.$method($($arg),*),
            UnionVariant::Trimmed(it) => it.$method($($arg),*),
        }
    };
}

/// FFI-facing union iterator holding the Rust variant and C-visible metadata
/// (query node type, query string) used by profile printing.
///
/// Parameterised over a [`Ref`] mode — see [`UnionOpaque`] for the
/// [`Active`] instantiation that implements [`RQEIterator`].
#[repr(C)]
pub struct RawUnionOpaque<'query, Rf: Ref, I> {
    pub variant: RawUnionVariant<'query, Rf, I>,
    pub query_node_type: QueryNodeType,
    /// Borrowed C string describing the query (e.g. the search term), or
    /// [`None`] when the union has no associated query string.
    ///
    /// The string is owned by the query AST, not the index; its validity is
    /// tied to the [`Ref`] mode `Rf`, since both the index and the AST outlive
    /// the iterator. In practice the AST is freed only after the entire query
    /// execution pipeline — including all iterators — has been torn down, so
    /// the borrow remains valid for the lifetime of this struct.
    pub query_string: Option<SharedPtr<Rf, CStr>>,
}

/// Alias for an [`Active`] [`RawUnionOpaque`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type UnionOpaque<'index, I> = RawUnionOpaque<'index, Active<'index>, I>;

// Compile-time proof of invariant 1 on `RawUnionOpaque`: for a representative
// concrete child, the `Active` and `Suspended` instantiations are
// layout-identical. Three separate operations rest on it — the `ptr::write` of
// the suspended variant into a slot sized for the active one, the whole-box
// cast in either direction, and the `std::alloc::dealloc` on the abort/error
// path, which frees with `Layout::new::<Self>()` an allocation that was made
// for the active form.
//
// `variant` carries its own invariant 1 (proof above); `query_node_type` is
// `Rf`-free; `query_string` is an `Option<SharedPtr<Rf, CStr>>`, and `SharedPtr`
// is `#[repr(transparent)]` over `NonNull`, so the niche the `Option` folds into
// is the same in both modes.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawUnionOpaque<'static, Active<'static>, AChild>;
    type S = RawUnionOpaque<'static, Suspended, SChild>;
    assert!(offset_of!(A, variant) == offset_of!(S, variant));
    assert!(offset_of!(A, query_node_type) == offset_of!(S, query_node_type));
    assert!(offset_of!(A, query_string) == offset_of!(S, query_string));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, I: RQEIterator<'index>> UnionOpaque<'index, I> {
    /// Set the weight on the union's aggregate result.
    /// Must be called before the first read/skip.
    pub fn set_result_weight(&mut self, weight: f64) {
        if let Some(result) = self.current() {
            result.weight = weight;
        }
    }

    /// Returns the total number of children (including exhausted ones).
    pub const fn num_children_total(&self) -> usize {
        delegate_variant_ref!(self, num_children_total)
    }

    /// Returns the number of currently active (non-exhausted) children.
    pub const fn num_children_active(&self) -> usize {
        delegate_variant_ref!(self, num_children_active)
    }

    /// Returns a shared reference to the child at `idx` (across all children).
    /// Returns [`None`] if the index is out of range.
    pub fn child_at(&self, idx: usize) -> Option<&I> {
        delegate_variant_ref!(self, child_at, idx)
    }

    /// Returns a mutable iterator over all children (including exhausted ones).
    pub fn children_mut(&mut self) -> Box<dyn Iterator<Item = &mut I> + '_> {
        match &mut self.variant {
            UnionVariant::FlatFull(it) => Box::new(it.children_mut()),
            UnionVariant::FlatQuick(it) => Box::new(it.children_mut()),
            UnionVariant::HeapFull(it) => Box::new(it.children_mut()),
            UnionVariant::HeapQuick(it) => Box::new(it.children_mut()),
            UnionVariant::Trimmed(it) => Box::new(it.children_mut()),
        }
    }
}

impl<'index, I: RQEIterator<'index>> RQEIterator<'index> for UnionOpaque<'index, I> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_variant_ref_mut!(self, current)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_variant_ref_mut!(self, read)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_variant_ref_mut!(self, skip_to, doc_id)
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        delegate_variant_ref_mut!(self, revalidate, spec)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        delegate_variant_ref_mut!(self, rewind)
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        delegate_variant_ref!(self, num_estimated)
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        delegate_variant_ref!(self, last_doc_id)
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        delegate_variant_ref!(self, at_eof)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Union
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_variant_ref!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index, I> ProfilePrint for UnionOpaque<'index, I>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        let node_type = self.query_node_type;
        // Union and Geo always print full children even in limited mode —
        // these types have few enough children that collapsing them would lose
        // useful information.
        let print_full =
            !ctx.limited || matches!(node_type, QueryNodeType::Union | QueryNodeType::Geo);

        map.kv_simple_string(c"Type", c"UNION");

        let type_str = match node_type {
            QueryNodeType::Geo => "GEO",
            QueryNodeType::Tag => "TAG",
            QueryNodeType::Union => "UNION",
            QueryNodeType::Fuzzy => "FUZZY",
            QueryNodeType::Prefix => "PREFIX",
            QueryNodeType::Numeric => "NUMERIC",
            QueryNodeType::WildcardQuery => "WILDCARD",
            _ => unreachable!("Invalid type for union"),
        };

        match self.query_string {
            None => {
                let value = std::ffi::CString::new(type_str).unwrap();
                map.kv_simple_string(c"Query type", &value);
            }
            Some(q_str) => {
                let q_str_rust = q_str.get().to_string_lossy();
                let formatted = format!("{type_str} - {q_str_rust}");
                // Use string_buffer (bulk string) instead of simple_string: the
                // query string may contain \r\n which is invalid in RESP Simple
                // Strings.
                map.kv_string_buffer(c"Query type", formatted.as_bytes());
            }
        }

        ctx.print_optional_counters(map);

        let num_children = self.num_children_total();

        if print_full {
            let mut arr = map.kv_array(c"Child iterators");
            for i in 0..num_children {
                if let Some(child) = self.child_at(i) {
                    let mut child_map = arr.map();
                    let mut child_ctx = ctx.child_ctx();
                    child.print_profile(&mut child_map, &mut child_ctx);
                }
            }
        } else {
            let msg = format!("The number of iterators in the union is {num_children}");
            let msg_cstr = std::ffi::CString::new(msg).unwrap();
            map.kv_simple_string(c"Child iterators", &msg_cstr);
        }
    }
}

/// Concrete [`RQEIteratorWrapper`] used to expose a [`UnionOpaque`] to C.
type UnionWrapper<'index> = RQEIteratorWrapper<UnionOpaque<'index, CRQEIterator>>;

/// `ProfileChildren` callback for union iterators.
///
/// Profiles each child in-place via
/// [`CRQEIterator::into_profiled`](crate::c2rust::CRQEIterator::into_profiled),
/// preserving the `UnionOpaque<CRQEIterator>` type so the C-side optimizer and
/// profiler keep seeing the same layout. Returns the same pointer (mutation is
/// in-place).
///
/// # Safety
///
/// `base` must be a valid, owning pointer to a `UnionWrapper` created via
/// [`build_union`].
unsafe extern "C" fn union_profile_children(base: *mut QueryIterator) -> *mut QueryIterator {
    debug_assert!(!base.is_null());
    // SAFETY: caller guarantees `base` is valid and points to a union wrapper.
    let wrapper = unsafe { UnionWrapper::mut_ref_from_header_ptr(base) };
    for child in wrapper.inner.children_mut() {
        // Read the child's owning pointer without consuming the slot; ownership
        // is moved out here and handed back in place below.
        let it = child.as_raw();
        // SAFETY: `it` is a valid, uniquely-owned C iterator; it is consumed
        // here and replaced below, so it is neither leaked nor double-freed.
        let profiled = unsafe { CRQEIterator::new(it) }.into_profiled();
        // `CRQEIterator` is `#[repr(transparent)]` over `NonNull<QueryIterator>`,
        // so a `&mut CRQEIterator` can be viewed as a `*mut *mut QueryIterator`
        // slot for in-place replacement.
        let slot = child as *mut CRQEIterator as *mut *mut QueryIterator;
        // SAFETY: `slot` is a valid, writable pointer; store the profiled
        // iterator back in place.
        unsafe { *slot = profiled.into_raw().as_ptr() };
    }
    base
}

/// Build a union iterator from a `Vec` of already-owned [`CRQEIterator`]
/// children, returning an owning [`NonNull`] pointer to the C-ABI
/// [`QueryIterator`]. Always succeeds: an empty child set reduces to an
/// [`Empty`](crate::empty::Empty) iterator rather than a NULL pointer.
///
/// This variant stores no borrowed data in the returned iterator, so it has no
/// caller preconditions and is safe. To attach a query string for profiling
/// output, use [`build_union_with_q_str`].
///
/// See [`build_union_with_q_str`] for the reduction and variant-selection logic.
pub fn build_union(
    children: Vec<CRQEIterator>,
    quick_exit: bool,
    min_union_iter_heap: usize,
    type_: QueryNodeType,
    weight: f64,
) -> NonNull<QueryIterator> {
    // SAFETY: `q_str` is `None`, so no borrow is stored in the returned
    // iterator; the `build_union_with_q_str` precondition is vacuously satisfied.
    unsafe {
        build_union_with_q_str_opt(
            children,
            quick_exit,
            min_union_iter_heap,
            type_,
            None,
            weight,
        )
    }
}

/// Build a union iterator from a `Vec` of already-owned [`CRQEIterator`]
/// children, attaching `q_str` as the query string shown in profiling output.
/// Returns an owning [`NonNull`] pointer to the C-ABI [`QueryIterator`]; always
/// succeeds (an empty child set reduces to an [`Empty`](crate::empty::Empty)
/// iterator rather than a NULL pointer).
///
/// Applies the union reduction and variant-selection logic of
/// [`new_union_iterator`](crate::union_reducer::new_union_iterator): empty
/// children are removed, a single surviving child
/// is returned directly, and multiple children are placed in a flat or heap
/// union depending on `min_union_iter_heap`. The resulting wrapper carries the
/// [`union_profile_children`] callback so the still-C-driven profiler can
/// recurse into the children.
///
/// Callers with no query string should use the safe [`build_union`] instead.
///
/// # Safety
///
/// `q_str` must stay live and unchanged for as long as the returned iterator
/// exists. The borrow is stored in the [`UnionOpaque`] and read back when the
/// C-driven profiler prints the iterator, but its `'index` lifetime is erased
/// once the iterator is leaked to a raw `*mut QueryIterator`, so the borrow
/// checker cannot enforce this — the caller must guarantee the string outlives
/// the returned iterator.
pub unsafe fn build_union_with_q_str(
    children: Vec<CRQEIterator>,
    quick_exit: bool,
    min_union_iter_heap: usize,
    type_: QueryNodeType,
    q_str: &CStr,
    weight: f64,
) -> NonNull<QueryIterator> {
    // SAFETY: the caller guarantees `q_str` outlives the returned iterator.
    unsafe {
        build_union_with_q_str_opt(
            children,
            quick_exit,
            min_union_iter_heap,
            type_,
            Some(q_str),
            weight,
        )
    }
}

/// Shared implementation of [`build_union`] and [`build_union_with_q_str`].
///
/// # Safety
///
/// `q_str`, when [`Some`], must outlive the returned iterator (see
/// [`build_union_with_q_str`]). `None` imposes no preconditions.
unsafe fn build_union_with_q_str_opt(
    children: Vec<CRQEIterator>,
    quick_exit: bool,
    min_union_iter_heap: usize,
    type_: QueryNodeType,
    q_str: Option<&CStr>,
    weight: f64,
) -> NonNull<QueryIterator> {
    use crate::union_reducer::{NewUnionIterator, new_union_iterator};

    let variant = match new_union_iterator(children, quick_exit, min_union_iter_heap) {
        NewUnionIterator::ReducedEmpty(empty) => {
            let ptr = RQEIteratorWrapper::boxed_new(empty);
            // SAFETY: `boxed_new` uses `Box::into_raw`, which is guaranteed non-null.
            return unsafe { NonNull::new_unchecked(ptr) };
        }
        NewUnionIterator::ReducedSingle(child) => return child.into_raw(),
        NewUnionIterator::Flat(flat) => UnionVariant::FlatFull(flat),
        NewUnionIterator::FlatQuick(flat) => UnionVariant::FlatQuick(flat),
        NewUnionIterator::Heap(heap) => UnionVariant::HeapFull(heap),
        NewUnionIterator::HeapQuick(heap) => UnionVariant::HeapQuick(heap),
    };

    let mut dispatch = UnionOpaque {
        variant,
        query_node_type: type_,
        query_string: q_str.map(SharedPtr::from_ref),
    };
    dispatch.set_result_weight(weight);
    let ptr = RQEIteratorWrapper::boxed_new_inner(dispatch, Some(union_profile_children));
    // SAFETY: `boxed_new_inner` uses `Box::into_raw`, which is guaranteed non-null.
    unsafe { NonNull::new_unchecked(ptr) }
}

impl<'index, I> RQEIteratorBoxed<'index> for UnionOpaque<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawUnionOpaque<'index, Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);

        // Per-variant in-place dispatch, mirroring `resume` below:
        // `RawUnionVariant` itself doesn't implement `RQEIteratorBoxed`, so we
        // match here and drive each payload through the slot helper. Each inner
        // union walks its own children during `suspend`, which is what
        // transitions a dyn-erased `I`'s vtable; a whole-box cast at this level
        // alone would skip those walks and leave the children's vtables stale.
        // Routing through the helper is also what supplies the per-payload
        // `assert_layout_compatible` guard and the abort-on-unwind cover for the
        // moved-out window. The tag is left untouched, and both instantiations
        // declare their variants in the same order, so the cast below lands on
        // the same one.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match, and each payload is a valid, exclusively-owned value that the
        // helper leaves as its suspended counterpart at the same address.
        match unsafe { &mut (*raw).variant } {
            UnionVariant::FlatFull(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            UnionVariant::FlatQuick(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            UnionVariant::HeapFull(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            UnionVariant::HeapQuick(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            UnionVariant::Trimmed(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
        }

        // SAFETY: the variant now holds its suspended form at the same offset
        // under the same tag, and the remaining fields are either `Rf`-free
        // (`query_node_type`) or a borrow of the query AST rather than the index
        // (`query_string`), which the `Active → Suspended` re-typing only
        // weakens. Layout-identical to the suspended form by invariant 1 on
        // `RawUnionOpaque` (const proof above). `Box::from_raw` reuses the same
        // allocation, so the box and every interior address survive the cycle.
        unsafe { Box::from_raw(raw as *mut RawUnionOpaque<'index, Suspended, I::Suspended>) }
    }
}

impl<'query, S> RQESuspendedIterator<'query> for RawUnionOpaque<'query, Suspended, S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = UnionOpaque<'a, S::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);

        // Per-variant in-place dispatch — `RawUnionVariant` itself doesn't impl
        // `RQESuspendedIterator`, so we match here and drive each payload's
        // resume through the slot helper. `RawUnionVariant` is a single
        // `#[repr(C)]` enum parametrized by the ref-mode, so the tag encoding is
        // identical across modes and only the payload bytes change — the
        // whole-box cast below lands on the same variant.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match. On `Unchanged`/`Moved` the helper rewrites the payload as its
        // resumed form; on `Aborted`/`Err` it consumes the payload, leaving it
        // uninitialised (handled below).
        let outcome = match unsafe { &mut (*raw).variant } {
            RawUnionVariant::FlatFull(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            RawUnionVariant::FlatQuick(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            RawUnionVariant::HeapFull(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            RawUnionVariant::HeapQuick(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            RawUnionVariant::Trimmed(_) => {
                // Unreachable for the reason spelled out in `RawUnionTrimmed`'s
                // own `resume`: trimmed unions are only built for
                // `Q_OPT_PARTIAL_RANGE`, whose pipeline never revalidates.
                //
                // Panicking here rather than dispatching the payload through
                // `resume_child_slot_in_place` is deliberate: the helper arms an
                // `AbortOnUnwind` across the dispatched `resume`, so going
                // through it would turn this panic into a `std::process::abort()`
                // while the legacy `revalidate` path — which delegates to
                // `UnionTrimmed::revalidate`'s twin `unreachable!` — unwinds. The
                // two paths must agree on the failure mode.
                unreachable!(
                    "resume is not supported on UnionTrimmed — trimmed unions are not subject to GC"
                );
            }
        };

        // Free the reused allocation when the payload was consumed: only the tag
        // and the `Rf`-free/pointer fields remain live, none of which need
        // dropping (`query_string` is a borrowed `SharedPtr`), so the raw
        // allocation is deallocated directly.
        let free_shell = |raw: *mut Self| {
            // SAFETY: the allocation was made for the *active* form, whose size
            // and alignment match `Self`'s by invariant 1 on `RawUnionOpaque`
            // (const proof above), so `Layout::new::<Self>()` is the layout it
            // was allocated with; the consumed payload must not be dropped, and
            // the remaining fields have no drop glue.
            unsafe { std::alloc::dealloc(raw.cast::<u8>(), std::alloc::Layout::new::<Self>()) };
        };

        match outcome {
            Ok(
                slot_outcome @ (crate::boxed::ResumeSlotOutcome::Unchanged
                | crate::boxed::ResumeSlotOutcome::Moved),
            ) => {
                // SAFETY: the payload holds its resumed form at the same offset
                // and the tag is unchanged; `query_string` is a
                // `#[repr(transparent)]` `SharedPtr` over `NonNull<CStr>`
                // borrowing the query AST (not the index), which outlives the
                // iterator, so its `Suspended → Active<'a>` re-typing is sound;
                // `query_node_type` is `Rf`-free. `Box::from_raw` reuses the
                // same allocation, so the box address — and the FFI's cached
                // `header.current` into the payload's result — stay valid.
                let active =
                    unsafe { Box::from_raw(raw.cast::<UnionOpaque<'a, S::Resumed<'a>>>()) };
                Ok(match slot_outcome {
                    crate::boxed::ResumeSlotOutcome::Unchanged => ResumeOutcome::Ok(active),
                    _ => ResumeOutcome::Moved(active),
                })
            }
            Ok(crate::boxed::ResumeSlotOutcome::Aborted) => {
                free_shell(raw);
                Ok(ResumeOutcome::Aborted)
            }
            Err(e) => {
                free_shell(raw);
                Err(e)
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        match &self.variant {
            RawUnionVariant::FlatFull(it) => {
                <RawUnionFlat<'query, Suspended, S, false> as RQESuspendedIterator<'query>>::last_doc_id(it)
            }
            RawUnionVariant::FlatQuick(it) => {
                <RawUnionFlat<'query, Suspended, S, true> as RQESuspendedIterator<'query>>::last_doc_id(it)
            }
            RawUnionVariant::HeapFull(it) => {
                <RawUnionHeap<'query, Suspended, S, false> as RQESuspendedIterator<'query>>::last_doc_id(it)
            }
            RawUnionVariant::HeapQuick(it) => {
                <RawUnionHeap<'query, Suspended, S, true> as RQESuspendedIterator<'query>>::last_doc_id(it)
            }
            RawUnionVariant::Trimmed(it) => {
                <RawUnionTrimmed<'query, Suspended, S> as RQESuspendedIterator<'query>>::last_doc_id(it)
            }
        }
    }

    fn num_estimated(&self) -> usize {
        match &self.variant {
            RawUnionVariant::FlatFull(it) => {
                <RawUnionFlat<'query, Suspended, S, false> as RQESuspendedIterator<'query>>::num_estimated(it)
            }
            RawUnionVariant::FlatQuick(it) => {
                <RawUnionFlat<'query, Suspended, S, true> as RQESuspendedIterator<'query>>::num_estimated(it)
            }
            RawUnionVariant::HeapFull(it) => {
                <RawUnionHeap<'query, Suspended, S, false> as RQESuspendedIterator<'query>>::num_estimated(it)
            }
            RawUnionVariant::HeapQuick(it) => {
                <RawUnionHeap<'query, Suspended, S, true> as RQESuspendedIterator<'query>>::num_estimated(it)
            }
            RawUnionVariant::Trimmed(it) => {
                <RawUnionTrimmed<'query, Suspended, S> as RQESuspendedIterator<'query>>::num_estimated(it)
            }
        }
    }
}
