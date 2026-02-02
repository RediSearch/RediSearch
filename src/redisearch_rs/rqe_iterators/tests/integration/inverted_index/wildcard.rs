/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the wildcard inverted index iterator.

use ffi::{IndexFlags_Index_DocIdsOnly, RS_FIELDMASK_ALL, t_docId};
use inverted_index::{RSIndexResult, doc_ids_only::DocIdsOnly};
use rqe_iterators::{RQEIterator, inverted_index::Wildcard};

use crate::inverted_index::utils::{BaseTest, MockContext};

struct WildcardBaseTest {
    test: BaseTest<DocIdsOnly>,
}

impl WildcardBaseTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
        RSIndexResult::virt()
            .doc_id(doc_id)
            .field_mask(RS_FIELDMASK_ALL)
            .frequency(1)
            .weight(1.0)
    }

    fn new(n_docs: u64) -> Self {
        Self {
            test: BaseTest::new(
                IndexFlags_Index_DocIdsOnly,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }

    fn create_iterator(&self) -> Wildcard<'_> {
        let reader = self.test.ii.reader();
        Wildcard::new(reader, self.test.mock_ctx.sctx(), 1.0)
    }
}

#[test]
fn wildcard_read() {
    let test = WildcardBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn wildcard_skip_to() {
    let test = WildcardBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.skip_to(&mut it);
}

#[test]
fn wildcard_empty_index() {
    let ii = inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    let mock_ctx = MockContext::new(0, 0);

    let reader = ii.reader();
    let mut it = Wildcard::new(reader, mock_ctx.sctx(), 1.0);

    // Should immediately be at EOF
    assert!(it.read().expect("read failed").is_none());
    assert!(it.at_eof());
}
