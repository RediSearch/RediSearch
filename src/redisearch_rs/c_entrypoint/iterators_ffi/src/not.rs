/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::{self, NonNull};

use ffi::QueryIterator;
use rqe_core::DocId;
use rqe_iterators::{
    NewWildcardIterator, RQEIterator, RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    not::Not,
    not_optimized::NotOptimized,
    not_reducer::{NewNotIterator, TIMEOUT_CHECK_GRANULARITY, new_not_iterator},
    utils::AnyTimeoutContext,
};

type NotFfi<'index> = Not<'index, CRQEIterator, AnyTimeoutContext>;
type NotOptimizedFfi<'index> =
    NotOptimized<'index, NewWildcardIterator<'index>, CRQEIterator, AnyTimeoutContext>;

/// Enum holding both NOT iterator variants with concrete [`CRQEIterator`] child.
///
/// The `NotOptimized` variant is intentionally large because it inlines a
/// `WildcardIterator` to avoid heap allocation. Both variants are
/// long-lived (query lifetime), and the size difference is acceptable.
///
/// `#[repr(C, u8)]` matches [`NotIteratorEnumSuspended`]'s layout so
/// that suspend/resume can ptr::read+ptr::write the variant payload
/// in place — see [`NotIteratorEnumSuspended`] for the heap-stability
/// argument.
#[repr(C, u8)]
#[expect(
    clippy::large_enum_variant,
    reason = "both variants are query-lifetime; boxing would add a needless allocation"
)]
enum NotIteratorEnum<'index> {
    Not(NotFfi<'index>),
    NotOptimized(NotOptimizedFfi<'index>),
}

impl rqe_iterators::profile_print::ProfilePrint for NotIteratorEnum<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
    ) {
        match self {
            Self::Not(it) => it.print_profile(map, ctx),
            Self::NotOptimized(it) => it.print_profile(map, ctx),
        }
    }
}

// Delegate `RQEIterator` to the inner variant.
impl<'index> RQEIterator<'index> for NotIteratorEnum<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut index_result::RSIndexResult<'index>> {
        match self {
            Self::Not(it) => it.current(),
            Self::NotOptimized(it) => it.current(),
        }
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut index_result::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        match self {
            Self::Not(it) => it.read(),
            Self::NotOptimized(it) => it.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        match self {
            Self::Not(it) => it.skip_to(doc_id),
            Self::NotOptimized(it) => it.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &index_spec::IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        match self {
            Self::Not(it) => it.revalidate(spec),
            Self::NotOptimized(it) => it.revalidate(spec),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match self {
            Self::Not(it) => it.rewind(),
            Self::NotOptimized(it) => it.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match self {
            Self::Not(it) => it.num_estimated(),
            Self::NotOptimized(it) => it.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        match self {
            Self::Not(it) => it.last_doc_id(),
            Self::NotOptimized(it) => it.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match self {
            Self::Not(it) => it.at_eof(),
            Self::NotOptimized(it) => it.at_eof(),
        }
    }

    #[inline(always)]
    fn type_(&self) -> rqe_iterators::IteratorType {
        match self {
            Self::Not(it) => it.type_(),
            Self::NotOptimized(it) => it.type_(),
        }
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index> rqe_iterators::interop::ProfileChildren<'index> for NotIteratorEnum<'index> {
    fn profile_children(self) -> Self {
        match self {
            Self::Not(it) => Self::Not(it.profile_children()),
            Self::NotOptimized(it) => Self::NotOptimized(it.profile_children()),
        }
    }
}

/// Build the [`AnyTimeoutContext`] the iterator should use.
///
/// # Safety
///
/// Caller must uphold preconditions 3 and 4 of [`NewNotIterator()`] for the
/// lifetime of the returned context and every iterator built from it.
unsafe fn build_timeout_context(q: NonNull<ffi::QueryEvalCtx>) -> AnyTimeoutContext {
    // SAFETY: caller guarantees q is valid (3).
    let q_ref = unsafe { q.as_ref() };
    let sctx = NonNull::new(q_ref.sctx).expect("q.sctx must be non-null (precondition 4)");
    // SAFETY: precondition 4 keeps `q.sctx` and its request timeout valid and excludes source
    // changes and deadline writes during probes.
    unsafe { AnyTimeoutContext::from_sctx(sctx, TIMEOUT_CHECK_GRANULARITY) }
}
/// Suspended counterpart of [`NotIteratorEnum`].
///
/// Variants hold the [`RQEIteratorBoxed::Suspended`] counterparts of each active
/// variant.
///
/// `#[repr(C, u8)]` matches [`NotIteratorEnum`]'s layout so that
/// suspend/resume can `ptr::read` the variant payload out, drive
/// the inner suspend/resume, and `ptr::write` the result back into
/// the same outer-Box slot — preserving the outer Box's heap
/// allocation across the cycle. The FFI wrapper's `header.current`
/// is a borrowed pointer into the inner iterator's `result.current`
/// slot at a fixed offset within this outer Box; re-allocating the
/// outer Box would leave that pointer dangling.
#[repr(C, u8)]
#[expect(
    clippy::large_enum_variant,
    reason = "matches the layout of the active variant; boxing would needlessly allocate"
)]
enum NotIteratorEnumSuspended<'query> {
    Not(<NotFfi<'query> as RQEIteratorBoxed<'query>>::Suspended),
    NotOptimized(<NotOptimizedFfi<'query> as RQEIteratorBoxed<'query>>::Suspended),
}

impl<'index> RQEIteratorBoxed<'index> for NotIteratorEnum<'index> {
    type Suspended = NotIteratorEnumSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // Preserve the outer Box's heap allocation across the cycle.
        // The FFI wrapper's `header.current` is a borrowed pointer
        // into the inner iterator's `result.current` slot, whose
        // address is at a fixed offset within this outer Box (since
        // the variant payload sits inline per `#[repr(C, u8)]`).
        // Re-allocating the outer Box would shift that offset and
        // leave `header.current` dangling.
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (valid pointer,
        // exclusive ownership). `ptr::read` moves the value out,
        // leaving the slot's bytes typed-but-moved-from; we
        // overwrite via `ptr::write` before reconstituting the Box.
        let active_val = unsafe { ptr::read(raw) };

        let suspended_val =
            match active_val {
                Self::Not(it) => NotIteratorEnumSuspended::Not(
                    *<NotFfi<'index> as RQEIteratorBoxed<'index>>::suspend(Box::new(it)),
                ),
                Self::NotOptimized(it) => NotIteratorEnumSuspended::NotOptimized(
                    *<NotOptimizedFfi<'index> as RQEIteratorBoxed<'index>>::suspend(Box::new(it)),
                ),
            };

        let suspended_raw = raw as *mut NotIteratorEnumSuspended<'index>;
        // SAFETY: `suspended_raw` is the same heap allocation as `raw`,
        // retyped as `NotIteratorEnumSuspended` (layout-compatible by
        // `#[repr(C, u8)]` — see [`NotIteratorEnumSuspended`]). The
        // slot is uninitialised after the earlier `ptr::read`;
        // writing a valid `NotIteratorEnumSuspended` reinitialises it.
        unsafe { ptr::write(suspended_raw, suspended_val) };
        // SAFETY: outer Box reconstituted on the same heap allocation.
        unsafe { Box::from_raw(suspended_raw) }
    }
}

impl<'query> RQESuspendedIterator<'query> for NotIteratorEnumSuspended<'query> {
    type Resumed<'a>
        = NotIteratorEnum<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, rqe_iterators::RQEIteratorError>
    where
        'query: 'a,
    {
        // Mirror of [`NotIteratorEnum::suspend`]: on the recoverable path,
        // preserve the outer Box's heap allocation via `ptr::read` +
        // `ptr::write` instead of `Box::new(NotIteratorEnum::...)` (which would
        // re-allocate and dangle `header.current` / a parent's pointer into the
        // aggregate).
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (valid, exclusive).
        // `ptr::read` moves the suspended value out, leaving the slot
        // logically uninitialised until we either `ptr::write` a resumed value
        // back (recoverable path) or deallocate the raw slot (abort/error path).
        let suspended_val = unsafe { ptr::read(raw) };

        // Forward the inner variant's outcome. `None` means the inner aborted;
        // `Err` propagates a resume error (e.g. timeout). Both non-recoverable
        // cases leave the `raw` slot uninitialised.
        let resumed: Result<Option<(NotIteratorEnum<'a>, bool)>, rqe_iterators::RQEIteratorError> =
            match suspended_val {
                NotIteratorEnumSuspended::Not(s) => {
                    match <_ as RQESuspendedIterator>::resume(Box::new(s), guard) {
                        Err(e) => Err(e),
                        Ok(ResumeOutcome::Aborted) => Ok(None),
                        Ok(ResumeOutcome::Ok(r)) => Ok(Some((NotIteratorEnum::Not(*r), false))),
                        Ok(ResumeOutcome::Moved(r)) => Ok(Some((NotIteratorEnum::Not(*r), true))),
                    }
                }
                NotIteratorEnumSuspended::NotOptimized(s) => {
                    match <_ as RQESuspendedIterator>::resume(Box::new(s), guard) {
                        Err(e) => Err(e),
                        Ok(ResumeOutcome::Aborted) => Ok(None),
                        Ok(ResumeOutcome::Ok(r)) => {
                            Ok(Some((NotIteratorEnum::NotOptimized(*r), false)))
                        }
                        Ok(ResumeOutcome::Moved(r)) => {
                            Ok(Some((NotIteratorEnum::NotOptimized(*r), true)))
                        }
                    }
                }
            };

        match resumed {
            Ok(Some((active_val, moved))) => {
                let active_raw = raw as *mut NotIteratorEnum<'a>;
                // SAFETY: same heap allocation as `raw`, retyped as
                // `NotIteratorEnum<'a>` (layout-compatible by `#[repr(C, u8)]`).
                // The slot was uninitialised after the earlier `ptr::read`;
                // writing a valid `NotIteratorEnum<'a>` reinitialises it.
                unsafe { ptr::write(active_raw, active_val) };
                // SAFETY: outer Box reconstituted on the same heap allocation.
                let active = unsafe { Box::from_raw(active_raw) };
                Ok(if moved {
                    ResumeOutcome::Moved(active)
                } else {
                    ResumeOutcome::Ok(active)
                })
            }
            non_recoverable => {
                // Abort / error: the payload was consumed by the inner `resume`,
                // so the `raw` slot is uninitialised. Free the allocation as raw
                // memory — reconstituting a `Box` would drop the uninitialised
                // slot (UB).
                // SAFETY: `raw` came from `Box::<NotIteratorEnumSuspended>::into_raw`,
                // so it was allocated by the global allocator with this exact
                // layout; its contents were moved out via `ptr::read`.
                unsafe {
                    std::alloc::dealloc(
                        raw.cast::<u8>(),
                        std::alloc::Layout::new::<NotIteratorEnumSuspended>(),
                    );
                }
                match non_recoverable {
                    Ok(None) => Ok(ResumeOutcome::Aborted),
                    Err(e) => Err(e),
                    Ok(Some(_)) => unreachable!("handled by the recoverable arm above"),
                }
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            NotIteratorEnumSuspended::Not(s) => s.last_doc_id(),
            NotIteratorEnumSuspended::NotOptimized(s) => s.last_doc_id(),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            NotIteratorEnumSuspended::Not(s) => s.num_estimated(),
            NotIteratorEnumSuspended::NotOptimized(s) => s.num_estimated(),
        }
    }
}

/// Creates a NOT iterator, choosing between non-optimized and optimized based
/// on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), a simplified
/// iterator is returned directly.
///
/// The request timeout reached through `q.sctx.timeout` selects no timeout,
/// the Blocked Client Timeout, or the Clock Based Timeout. Clock deadlines are
/// read back on every probe so a re-armed deadline is honoured.
///
/// # Safety
///
/// 1. `child` must be null or a valid pointer to a [`QueryIterator`].
///    A null `child` is treated as empty.
/// 2. When non-null, `child` must not be aliased.
/// 3. `q` must be a valid non-null pointer to a [`QueryEvalCtx`](ffi::QueryEvalCtx).
/// 4. `q.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx), which must stay valid and at a stable
///    address for the lifetime of the returned iterator. Its request timeout must also remain
///    valid at a stable address for that lifetime. Timeout source changes and deadline writes
///    may happen only between probes; only the blocked-client flag may change concurrently.
/// 5. `q.sctx.spec` must be a non-null pointer to a valid
///    [`IndexSpec`](ffi::IndexSpec).
/// 6. `q.sctx.spec.rule`, when non-null, must point to a valid
///    [`SchemaRule`](ffi::SchemaRule).
/// 7. When the optimized path is taken, the preconditions of
///    [`rqe_iterators::wildcard::new_wildcard_iterator`] must hold.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNotIterator(
    child: *mut QueryIterator,
    max_doc_id: DocId,
    weight: f64,
    q: *mut ffi::QueryEvalCtx,
) -> *mut QueryIterator {
    let query = NonNull::new(q).expect("q must be non-null");

    // SAFETY: caller upholds preconditions (3, 4).
    let timeout_ctx = unsafe { build_timeout_context(query) };

    // Handle null child: reduce with Empty directly (always becomes wildcard).
    let Some(child_ptr) = NonNull::new(child) else {
        let empty = rqe_iterators::Empty;
        // SAFETY: caller guarantees preconditions (3–7).
        let result = unsafe { new_not_iterator(empty, max_doc_id, weight, timeout_ctx, query) };
        return match result {
            NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
            NewNotIterator::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty),
            // Empty child always reduces; these arms are unreachable.
            NewNotIterator::Not(_) | NewNotIterator::NotOptimized(_) => {
                panic!("Empty not child always reduces")
            }
        };
    };

    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child_ptr) };

    // SAFETY: caller guarantees preconditions (3–7).
    let result = unsafe { new_not_iterator(child, max_doc_id, weight, timeout_ctx, query) };

    match result {
        NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
        NewNotIterator::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty),
        NewNotIterator::Not(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::Not(iter))
        }
        NewNotIterator::NotOptimized(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::NotOptimized(iter))
        }
    }
}
