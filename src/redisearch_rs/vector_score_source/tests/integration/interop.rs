/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The two accessors reach the vector-score key slot through nothing but the
//! C-ABI header, so every assertion here goes through that header rather than
//! through the source it lands on.

use std::{num::NonZeroUsize, ptr::NonNull};

use ffi::QueryIterator;
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use rlookup::{RLookupKey, RLookupKeyHandle};
use rqe_iterators::{FieldExpirationChecker, IdList, interop::RQEIteratorWrapper};
use rqe_iterators_test_utils::MockContext;
use vector_score_source::{
    interop, new_vector_top_k_unfiltered,
    test_utils::{TestIndex, uniform_blob},
};

/// A lowered iterator, released through the header's own `Free` callback.
///
/// Owning it rather than freeing by hand is what keeps a test that unwinds from
/// leaking: `drop` runs during the unwind, which is the safe point a panicking
/// dispatch otherwise leaves no room for.
struct OwnedHeader(Option<NonNull<QueryIterator>>);

impl OwnedHeader {
    fn new(raw: *mut QueryIterator) -> Self {
        Self(Some(
            NonNull::new(raw).expect("boxed_new returns an owning pointer"),
        ))
    }

    fn as_non_null(&self) -> NonNull<QueryIterator> {
        self.0.expect("iterator has already been released")
    }

    /// Release now, for a test that has to observe what freeing did — the
    /// handle invalidation is only visible once the iterator has gone.
    fn free(&mut self) {
        let Some(it) = self.0.take() else {
            return;
        };
        // SAFETY: `it` is a live header from `boxed_new`, taken out of the
        // option so this runs exactly once, and unused afterwards.
        let free = unsafe { it.as_ref() }.Free.expect("Free must be populated");
        // SAFETY: `boxed_new` populates every callback, and ownership passes here.
        unsafe { free(it.as_ptr()) };
    }
}

impl Drop for OwnedHeader {
    fn drop(&mut self) {
        self.free();
    }
}

/// Lower a pure-KNN vector top-k iterator to its header, in the one
/// parameterisation the accessors accept: the production
/// [`FieldExpirationChecker`].
fn header(index: &TestIndex, ctx: &MockContext) -> OwnedHeader {
    // SAFETY: `ctx` owns a zeroed-but-valid `RedisSearchCtx` and `IndexSpec`,
    // and outlives the iterator built here.
    let checker = unsafe {
        FieldExpirationChecker::new(
            ctx.sctx(),
            FieldFilterContext {
                field: FieldMaskOrIndex::Index(0),
                predicate: FieldExpirationPredicate::Default,
            },
            0,
        )
    };
    let k = NonZeroUsize::new(3).expect("3 is non-zero");
    let source = index.source_with_expiration(uniform_blob(0.0, 1), 0, k.get(), 3, checker);
    OwnedHeader::new(RQEIteratorWrapper::boxed_new(new_vector_top_k_unfiltered(
        source, k,
    )))
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn own_key_ref_reaches_the_sources_own_slot() {
    let index = TestIndex::flat(3, 1);
    let ctx = MockContext::new(3, 3);
    let it = header(&index, &ctx);
    // A stand-in for the key the pipeline resolves: never dereferenced.
    let mut key_storage = 0u64;

    // SAFETY: `it` is a live vector top-k iterator, held exclusively here.
    let slot = unsafe { interop::own_key_ref(it.as_non_null()) };
    // SAFETY: `slot` is that iterator's own key slot, live and initialised.
    let key = unsafe { &mut *slot };
    assert!(key.is_null(), "a fresh iterator has no key yet");
    *key = (&raw mut key_storage).cast::<RLookupKey<'_>>();

    // A second dispatch reads the slot back: it belongs to the iterator rather
    // than to the call.
    // SAFETY: as above.
    let slot = unsafe { interop::own_key_ref(it.as_non_null()) };
    // SAFETY: as above.
    let key = unsafe { &mut *slot };
    assert_eq!(
        *key,
        (&raw mut key_storage).cast::<RLookupKey<'_>>(),
        "the slot must survive between dispatches"
    );
    // The stand-in is not a real key: clear it before the iterator drops.
    *key = std::ptr::null_mut();
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn freeing_the_iterator_invalidates_the_handle_it_was_given() {
    let index = TestIndex::flat(3, 1);
    let ctx = MockContext::new(3, 3);
    let mut it = header(&index, &ctx);
    let mut handle = RLookupKeyHandle {
        key_ptr: std::ptr::null_mut(),
        is_valid: true,
    };

    // SAFETY: `it` is a live vector top-k iterator held exclusively, and
    // `handle` outlives it — the iterator is freed just below.
    unsafe { interop::set_key_handle(it.as_non_null(), &raw mut handle) };
    assert!(handle.is_valid, "wiring a handle must not clear it");

    // Released here rather than left to drop: the invalidation is only
    // observable once the iterator has gone.
    it.free();
    assert!(
        !handle.is_valid,
        "freeing the iterator must invalidate its handle"
    );
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
#[should_panic(expected = "expected a vector top-k iterator")]
fn own_key_ref_rejects_an_iterator_of_another_type() {
    let it = OwnedHeader::new(RQEIteratorWrapper::boxed_new(IdList::<true>::new(vec![
        1, 2, 3,
    ])));

    // SAFETY: the header is a live, exclusively-held wrapper reporting its type
    // honestly. Being a vector top-k iterator is not a pre-condition but the
    // documented panic condition, and this asserts it fires: the dispatch reads
    // the type tag and nothing else, so `it` can still free it as the unwind
    // passes through.
    unsafe { interop::own_key_ref(it.as_non_null()) };
}
