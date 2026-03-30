/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod mock_iterator;
pub(crate) use mock_iterator::{Mock, MockData, MockIteratorError, MockRevalidateResult, MockVec};

use ffi::t_docId;
use rqe_iterators::RQEIterator;

pub(crate) fn create_mock_1<const N: usize>(
    ids: [t_docId; N],
) -> (Box<dyn RQEIterator<'static>>, MockData) {
    let c = Mock::<N>::new(ids);
    let d = c.data();
    (Box::new(c), d)
}

pub(crate) fn create_mock_2<const N1: usize, const N2: usize>(
    ids1: [t_docId; N1],
    ids2: [t_docId; N2],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<MockData>) {
    let c1 = Mock::<N1>::new(ids1);
    let c2 = Mock::<N2>::new(ids2);
    let d1 = c1.data();
    let d2 = c2.data();
    (vec![Box::new(c1), Box::new(c2)], vec![d1, d2])
}

pub(crate) fn create_mock_3<const N1: usize, const N2: usize, const N3: usize>(
    ids1: [t_docId; N1],
    ids2: [t_docId; N2],
    ids3: [t_docId; N3],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<MockData>) {
    let c1 = Mock::<N1>::new(ids1);
    let c2 = Mock::<N2>::new(ids2);
    let c3 = Mock::<N3>::new(ids3);
    let d1 = c1.data();
    let d2 = c2.data();
    let d3 = c3.data();
    (
        vec![Box::new(c1), Box::new(c2), Box::new(c3)],
        vec![d1, d2, d3],
    )
}

pub(crate) fn create_union_children(
    num_children: usize,
    base_result_set: &[t_docId],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<t_docId>) {
    let mut children: Vec<Box<dyn RQEIterator<'static>>> = Vec::with_capacity(num_children);
    let mut all_ids = Vec::new();
    let mut next_unique_id: t_docId = 10000;

    for i in 0..num_children {
        let mut child_ids = Vec::new();

        for (j, &id) in base_result_set.iter().enumerate() {
            if j % num_children == i || j % 2 == 0 {
                child_ids.push(id);
            }
        }

        for _ in 0..50 {
            child_ids.push(next_unique_id);
            next_unique_id += 1;
        }

        child_ids.sort();
        child_ids.dedup();
        all_ids.extend(child_ids.iter().copied());
        children.push(MockVec::new_boxed(child_ids));
    }

    all_ids.sort();
    all_ids.dedup();
    (children, all_ids)
}
