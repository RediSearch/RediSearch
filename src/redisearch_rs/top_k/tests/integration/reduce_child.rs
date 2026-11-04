/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`reduce_child`].

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::{InvertedIndex, doc_ids_only::DocIdsOnly};
use rqe_iterators::{
    Empty, IdList, IteratorType, RQEIterator, Wildcard, inverted_index::Wildcard as InvIdxWildcard,
};
use top_k::{ChildReduction, reduce_child};

#[test]
fn no_child_is_unfiltered() {
    let reduction = reduce_child(None::<Empty>);
    assert!(matches!(reduction, ChildReduction::Unfiltered));
}

#[test]
fn empty_child_reduces_to_empty() {
    let reduction = reduce_child(Some(Empty));
    assert!(matches!(reduction, ChildReduction::Empty));
}

#[test]
fn wildcard_child_is_unfiltered() {
    let reduction = reduce_child(Some(Wildcard::new(10, 1.0)));
    assert!(matches!(reduction, ChildReduction::Unfiltered));
}

#[test]
fn inverted_index_wildcard_child_is_unfiltered() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=3 {
        let record = RSIndexResult::build_virt().doc_id(doc_id).build();
        ii.add_record(&record).expect("failed to add record");
    }
    let child = InvIdxWildcard::new(ii.reader(), 1.0);
    assert_eq!(child.type_(), IteratorType::InvIdxWildcard);

    let reduction = reduce_child(Some(child));
    assert!(matches!(reduction, ChildReduction::Unfiltered));
}

#[test]
fn real_child_is_kept_as_filter() {
    let child = IdList::<true>::new(vec![1, 2, 3]);
    let reduction = reduce_child(Some(child));
    assert!(matches!(reduction, ChildReduction::Filtered(_)));
}
