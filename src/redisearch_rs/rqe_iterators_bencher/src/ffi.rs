/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod bindings {
    #![allow(non_snake_case)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(unsafe_op_in_unsafe_fn)]
    #![allow(improper_ctypes)]
    #![allow(dead_code)]

    use inverted_index::t_docId;

    // Type aliases for C bindings - types without lifetimes for C interop
    pub type RSIndexResult = inverted_index::RSIndexResult<'static, 'static>;
    pub type RSOffsetVector = inverted_index::RSOffsetVector<'static>;

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::{IteratorStatus, ValidateStatus};

/// Simple wrapper around the C `QueryIterator` type.
/// All methods are inlined to avoid the overhead when benchmarking.
pub struct QueryIterator(*mut bindings::QueryIterator);

impl QueryIterator {
    #[inline(always)]
    pub fn new_empty() -> Self {
        Self(unsafe { bindings::NewEmptyIterator() })
    }

    #[inline(always)]
    pub fn num_estimated(&self) -> usize {
        unsafe { (*self.0).NumEstimated.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn at_eof(&self) -> bool {
        unsafe { (*self.0).atEOF }
    }

    #[inline(always)]
    pub fn read(&self) -> IteratorStatus {
        unsafe { (*self.0).Read.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn skip_to(&self, doc_id: u64) -> IteratorStatus {
        unsafe { (*self.0).SkipTo.unwrap()(self.0, doc_id) }
    }

    #[inline(always)]
    pub fn rewind(&self) {
        unsafe { (*self.0).Rewind.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn revalidate(&self) -> ValidateStatus {
        unsafe { (*self.0).Revalidate.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn free(self) {
        unsafe { (*self.0).Free.unwrap()(self.0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bindings::{IteratorStatus_ITERATOR_EOF, ValidateStatus_VALIDATE_OK};

    #[test]
    fn empty_iterator() {
        let it = QueryIterator::new_empty();
        assert_eq!(it.num_estimated(), 0);
        assert!(it.at_eof());

        assert_eq!(it.read(), IteratorStatus_ITERATOR_EOF);
        assert_eq!(it.skip_to(1), IteratorStatus_ITERATOR_EOF);

        it.rewind();
        assert!(it.at_eof());
        assert_eq!(it.read(), IteratorStatus_ITERATOR_EOF);

        assert_eq!(it.revalidate(), ValidateStatus_VALIDATE_OK);

        it.free();
    }
}
