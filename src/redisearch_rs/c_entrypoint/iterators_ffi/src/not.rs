/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::{self, NonNull};

use ffi::{QueryIterator, t_docId, timespec};
use rqe_iterator_type::IteratorType;
use ffi::ValidateStatus;
use rqe_iterators::{
    BoxedRQEIterator, NewWildcardIterator, RQEIterator, RQEIteratorBoxed, RQESuspendedIterator,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    not::Not,
    not_optimized::NotOptimized,
    not_reducer::{NewNotIterator, new_not_iterator},
};

type NotFfi<'index> = Not<'index, CRQEIterator>;
type NotOptimizedFfi<'index> = NotOptimized<'index, NewWildcardIterator<'index>, CRQEIterator>;

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

impl<'index> NotIteratorEnum<'index> {
    const fn child(&self) -> Option<&CRQEIterator> {
        match self {
            Self::Not(it) => it.child(),
            Self::NotOptimized(it) => it.child(),
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
        doc_id: t_docId,
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
    fn last_doc_id(&self) -> t_docId {
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
enum NotIteratorEnumSuspended {
    Not(<NotFfi<'static> as RQEIteratorBoxed<'static>>::Suspended),
    NotOptimized(<NotOptimizedFfi<'static> as RQEIteratorBoxed<'static>>::Suspended),
}

impl<'index> RQEIteratorBoxed<'index> for NotIteratorEnum<'index> {
    type Suspended = NotIteratorEnumSuspended;

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

        let suspended_val = match active_val {
            Self::Not(it) => NotIteratorEnumSuspended::Not(
                *<NotFfi<'index> as RQEIteratorBoxed<'index>>::suspend(Box::new(it)),
            ),
            Self::NotOptimized(it) => NotIteratorEnumSuspended::NotOptimized(
                *<NotOptimizedFfi<'index> as RQEIteratorBoxed<'index>>::suspend(Box::new(it)),
            ),
        };

        let suspended_raw = raw as *mut NotIteratorEnumSuspended;
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

impl RQESuspendedIterator for NotIteratorEnumSuspended {
    type Resumed<'a> = NotIteratorEnum<'a>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a index_spec::IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Mirror of [`NotIteratorEnum::suspend`]: preserve the outer
        // Box's heap allocation via `ptr::read` + `ptr::write` instead
        // of `Box::new(NotIteratorEnum::...)` (which would re-allocate
        // and dangle `header.current`).
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (valid, exclusive).
        // `ptr::read` moves the suspended value out; we overwrite via
        // `ptr::write` before reconstituting the Box.
        let suspended_val = unsafe { ptr::read(raw) };

        let (active_val, status) = match suspended_val {
            NotIteratorEnumSuspended::Not(s) => {
                let (resumed, status) = <_ as RQESuspendedIterator>::resume(Box::new(s), guard);
                (NotIteratorEnum::Not(*resumed), status)
            }
            NotIteratorEnumSuspended::NotOptimized(s) => {
                let (resumed, status) = <_ as RQESuspendedIterator>::resume(Box::new(s), guard);
                (NotIteratorEnum::NotOptimized(*resumed), status)
            }
        };

        let active_raw = raw as *mut NotIteratorEnum<'a>;
        // SAFETY: same heap allocation as `raw`, retyped as
        // `NotIteratorEnum<'a>` (layout-compatible by `#[repr(C, u8)]`).
        // Slot was uninitialised after the earlier `ptr::read`; writing
        // a valid `NotIteratorEnum<'a>` reinitialises it.
        unsafe { ptr::write(active_raw, active_val) };
        // SAFETY: outer Box reconstituted on the same heap allocation.
        let active = unsafe { Box::from_raw(active_raw) };
        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
        match self {
            NotIteratorEnumSuspended::Not(s) => s.last_doc_id(),
            NotIteratorEnumSuspended::NotOptimized(s) => s.last_doc_id(),
        }
    }
}

/// FFI wrapper for non-reduced NOT iterators ([`NotIteratorEnum`]).
///
/// Used by [`GetNotIteratorChild`] to recover the Rust iterator from a raw
/// [`QueryIterator`] pointer.
type NotIteratorWrapper<'index> = RQEIteratorWrapper<'index, NotIteratorEnum<'index>>;

/// Creates a NOT iterator, choosing between non-optimized and optimized based
/// on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), a simplified
/// iterator is returned directly.
///
/// # Safety
///
/// 1. `child` must be null or a valid pointer to a [`QueryIterator`].
///    A null `child` is treated as empty.
/// 2. When non-null, `child` must not be aliased.
/// 3. `q` must be a valid non-null pointer to a [`QueryEvalCtx`](ffi::QueryEvalCtx).
/// 4. `q.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx).
/// 5. `q.sctx.spec` must be a non-null pointer to a valid
///    [`IndexSpec`](ffi::IndexSpec).
/// 6. `q.sctx.spec.rule`, when non-null, must point to a valid
///    [`SchemaRule`](ffi::SchemaRule).
/// 7. When the optimized path is taken, the preconditions of
///    [`crate::wildcard::NewWildcardIterator_Optimized`] must hold.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNotIterator(
    child: *mut QueryIterator,
    max_doc_id: t_docId,
    weight: f64,
    timeout: timespec,
    q: *mut ffi::QueryEvalCtx,
) -> *mut QueryIterator {
    let query = NonNull::new(q).expect("q must be non-null");

    let (rust_timeout, skip_timeout_checks) = {
        // SAFETY: caller guarantees q is valid (3).
        let q_ref = unsafe { query.as_ref() };
        // SAFETY: caller guarantees q.sctx is valid (4).
        let sctx = unsafe { &*q_ref.sctx };
        if sctx.time.skipTimeoutChecks {
            (std::time::Duration::ZERO, true)
        } else {
            match crate::timespec::duration_from_redis_timespec(timeout) {
                Some(d) => (d, false),
                // Redis sentinel (no timeout) => skip timeout checks
                None => (std::time::Duration::ZERO, true),
            }
        }
    };

    // Handle null child: reduce with Empty directly (always becomes wildcard).
    let Some(child_ptr) = NonNull::new(child) else {
        let empty = rqe_iterators::Empty;
        // SAFETY: caller guarantees preconditions (3–7).
        let result = unsafe {
            new_not_iterator(
                empty,
                max_doc_id,
                weight,
                rust_timeout,
                skip_timeout_checks,
                query,
            )
        };
        return match result {
            NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
            NewNotIterator::ReducedEmpty(empty) => {
                RQEIteratorWrapper::boxed_new(BoxedRQEIterator::new(Box::new(empty)))
            }
            // Empty child always reduces; these arms are unreachable.
            NewNotIterator::Not(_) | NewNotIterator::NotOptimized(_) => {
                panic!("Empty not child always reduces")
            }
        };
    };

    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child_ptr) };

    // SAFETY: caller guarantees preconditions (3–7).
    let result = unsafe {
        new_not_iterator(
            child,
            max_doc_id,
            weight,
            rust_timeout,
            skip_timeout_checks,
            query,
        )
    };

    match result {
        NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
        NewNotIterator::ReducedEmpty(empty) => {
            RQEIteratorWrapper::boxed_new(BoxedRQEIterator::new(Box::new(empty)))
        }
        NewNotIterator::Not(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::Not(iter))
        }
        NewNotIterator::NotOptimized(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::NotOptimized(iter))
        }
    }
}

/// Get the child pointer of a NOT iterator, or NULL if there is no child.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced NOT iterator
///    created via [`NewNotIterator()`]. Must not be called on a reduced
///    (wildcard/empty) iterator returned by [`NewNotIterator()`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetNotIteratorChild(it: *const QueryIterator) -> *const QueryIterator {
    debug_assert!(!it.is_null());
    debug_assert!(
        matches!(
            // SAFETY: Safe thanks to 1
            unsafe { (*it).type_ },
            IteratorType::Not | IteratorType::NotOptimized
        ),
        "Expected a NOT or NOT_OPTIMIZED iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper = unsafe { NotIteratorWrapper::ref_from_header_ptr(it) };
    wrapper
        .inner()
        .child()
        .map(|c| c.as_ref() as *const _)
        .unwrap_or(std::ptr::null())
}
