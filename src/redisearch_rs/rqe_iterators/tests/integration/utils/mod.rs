/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod mock_iterator;
mod wildcard_helper;
pub(crate) use mock_iterator::{Mock, MockData, MockIteratorError, MockRevalidateResult, MockVec};
pub(crate) use wildcard_helper::WildcardHelper;

use ffi::t_docId;
use rqe_iterators::RQEIterator;
use std::collections::BTreeSet;

/// Create a single [`Mock`] child and return it as a boxed trait object
/// together with a handle to its [`MockData`].
pub(crate) fn create_mock_1<const N: usize>(
    doc_ids: [t_docId; N],
) -> (Box<dyn RQEIterator<'static>>, MockData) {
    let mock = Mock::new(doc_ids);
    let data = mock.data();
    (Box::new(mock), data)
}

/// Create two [`Mock`] children and return them as a `Vec` of boxed trait
/// objects together with a two-element array of [`MockData`] handles.
pub(crate) fn create_mock_2<const A: usize, const B: usize>(
    a: [t_docId; A],
    b: [t_docId; B],
) -> (Vec<Box<dyn RQEIterator<'static>>>, [MockData; 2]) {
    let m1 = Mock::new(a);
    let m2 = Mock::new(b);
    let data = [m1.data(), m2.data()];
    let children: Vec<Box<dyn RQEIterator<'static>>> = vec![Box::new(m1), Box::new(m2)];
    (children, data)
}

/// Create three [`Mock`] children and return them as a `Vec` of boxed trait
/// objects together with a three-element array of [`MockData`] handles.
pub(crate) fn create_mock_3<const A: usize, const B: usize, const C: usize>(
    a: [t_docId; A],
    b: [t_docId; B],
    c: [t_docId; C],
) -> (Vec<Box<dyn RQEIterator<'static>>>, [MockData; 3]) {
    let m1 = Mock::new(a);
    let m2 = Mock::new(b);
    let m3 = Mock::new(c);
    let data = [m1.data(), m2.data(), m3.data()];
    let children: Vec<Box<dyn RQEIterator<'static>>> =
        vec![Box::new(m1), Box::new(m2), Box::new(m3)];
    (children, data)
}

/// Create `num_children` [`MockVec`] children whose document-id lists are
/// produced by multiplying each element in `base_result_set` by the child
/// index (1-based).  Returns the children and the sorted, deduplicated
/// expected output.
pub(crate) fn create_union_children(
    num_children: usize,
    base_result_set: &[u64],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<t_docId>) {
    let mut expected = BTreeSet::new();
    let children: Vec<Box<dyn RQEIterator<'static>>> = (1..=num_children)
        .map(|i| {
            let doc_ids: Vec<t_docId> = base_result_set.iter().map(|&x| x * i as u64).collect();
            expected.extend(doc_ids.iter().copied());
            MockVec::new_boxed(doc_ids)
        })
        .collect();
    (children, expected.into_iter().collect())
}
