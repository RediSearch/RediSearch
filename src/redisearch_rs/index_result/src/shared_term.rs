/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{fmt, ops::Deref, ptr::NonNull, sync::Arc};

use query_term::RSQueryTerm;

/// A reference-counted [`RSQueryTerm`], shared by the iterator that produced it and every
/// record copied from that iterator's results.
///
/// Equivalent to an [`Arc<RSQueryTerm>`], but stored as a pointer to the term itself
/// rather than to the [`Arc`]'s allocation, so C sees a plain `RSQueryTerm *`.
#[repr(transparent)]
pub struct SharedQueryTerm(NonNull<RSQueryTerm>);

// The `Send` and `Sync` impls below hold only while this does.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RSQueryTerm>();
};

// SAFETY: `SharedQueryTerm` is an `Arc<RSQueryTerm>`, which is `Send` because `RSQueryTerm` is
// `Send + Sync`, as asserted above.
unsafe impl Send for SharedQueryTerm {}
// SAFETY: as above, `Arc<RSQueryTerm>` is `Sync` because `RSQueryTerm` is `Send + Sync`.
unsafe impl Sync for SharedQueryTerm {}

impl SharedQueryTerm {
    /// Take ownership of `term`, moving it into a reference-counted allocation.
    pub fn new(term: Box<RSQueryTerm>) -> Self {
        let ptr = Arc::into_raw(Arc::<RSQueryTerm>::from(term));
        // SAFETY: `Arc::into_raw` never returns a null pointer.
        Self(unsafe { NonNull::new_unchecked(ptr.cast_mut()) })
    }
}

impl Clone for SharedQueryTerm {
    fn clone(&self) -> Self {
        // SAFETY: `self.0` comes from `Arc::into_raw` in `Self::new`, and the strong count is at
        // least one while `self` is alive.
        unsafe { Arc::increment_strong_count(self.0.as_ptr()) };
        Self(self.0)
    }
}

impl Drop for SharedQueryTerm {
    fn drop(&mut self) {
        // SAFETY: `self.0` comes from `Arc::into_raw` in `Self::new`, and `self` owns one of the
        // strong references, which this releases exactly once.
        drop(unsafe { Arc::from_raw(self.0.as_ptr()) });
    }
}

impl Deref for SharedQueryTerm {
    type Target = RSQueryTerm;

    fn deref(&self) -> &RSQueryTerm {
        // SAFETY: the term stays allocated while `self` holds a strong reference, and it is never
        // mutated once shared.
        unsafe { self.0.as_ref() }
    }
}

impl fmt::Debug for SharedQueryTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        RSQueryTerm::fmt(self, f)
    }
}
