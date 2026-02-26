/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::id_cases;
use inverted_index::RSResultKind;
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome,
    id_list::{IdListSorted, IdListUnsorted},
};
use rstest_reuse::apply;

#[test]
fn empty_initialization_works() {
    let mut i = IdListSorted::new(vec![]);

    let result = i.current().unwrap();
    assert_eq!(0, result.doc_id);
    assert_eq!(RSResultKind::Virtual, result.kind());

    assert!(i.at_eof());
}

#[test]
#[should_panic(expected = "IDs must be sorted and unique")]
fn unsorted_initialization_of_sorted_variant_panics() {
    let _ = IdListSorted::new(vec![5, 3, 1, 4, 2]);
}

#[test]
fn unsorted_initialization_of_unsorted_variant_works() {
    let mut it = IdListUnsorted::new(vec![5, 3, 1, 4, 2]);

    let result = it.current().unwrap();
    assert_eq!(0, result.doc_id);
    assert_eq!(RSResultKind::Virtual, result.kind());
}

#[test]
#[should_panic(expected = "Can't skip when working with unsorted document ids")]
fn unsorted_variant_cannot_skip() {
    let mut i = IdListUnsorted::new(vec![5, 3, 1, 4, 2]);
    let _ = i.skip_to(3);
}

#[test]
#[should_panic(expected = "IDs must be sorted and unique")]
fn duplicate_initialization() {
    let _ = IdListSorted::new(vec![1, 2, 2, 3, 4]);
}

#[apply(id_cases)]
fn read(#[case] case: &[u64]) {
    let mut it = IdListSorted::new(case.to_vec());

    assert_eq!(it.num_estimated(), case.len());
    assert!(!it.at_eof());

    for expected_id in case.into_iter().copied() {
        assert!(!it.at_eof());
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);

        let result = it.current().unwrap();
        assert_eq!(expected_id, result.doc_id);
        assert_eq!(RSResultKind::Virtual, result.kind());
    }

    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[apply(id_cases)]
#[cfg(not(miri))] // Take too long with Miri, causing CI to timeout
fn skip_to(#[case] case: &[u64]) {
    let mut it = IdListSorted::new(case.to_vec());

    // Read first element
    let first_doc = it.read().unwrap().unwrap();
    let first_id = case[0];
    assert_eq!(first_doc.doc_id, first_id);
    assert_eq!(it.last_doc_id(), first_id);
    assert_eq!(it.at_eof(), Some(&first_id) == case.last());

    // Skip to higher than last doc id: expect EOF, last_doc_id unchanged
    let last = *case.last().unwrap();
    let res = it.skip_to(last + 1); // Expect some EOF status; we only assert observable effects
    assert!(matches!(res, Ok(None)));
    drop(res);
    assert!(it.at_eof());
    assert_eq!(Some(&it.last_doc_id()), case.last());

    // Rewind
    it.rewind();
    assert!(!it.at_eof());

    // probe walks all ids from 1 up to last, probing missing and existing ids
    let mut probe = 1u64;
    for &id in case {
        // Probe all gaps before this id
        while probe < id {
            it.rewind();
            let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(probe) else {
                panic!("probe {probe} -> Expected `Some`");
            };
            assert_eq!(res.doc_id, id);
            // Should land on next existing id
            assert_eq!(it.at_eof(), Some(&id) == case.last());
            assert_eq!(it.last_doc_id(), id);
            probe += 1;
        }
        // Exact match
        it.rewind();
        let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(probe) else {
            panic!("probe {probe} -> Expected `Found`");
        };
        assert_eq!(res.doc_id, id);
        assert_eq!(it.at_eof(), Some(&id) == case.last());
        assert_eq!(it.last_doc_id(), id);
        probe += 1;
    }

    // After consuming all (by reading past end)
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    // Rewind and test direct skips to every existing id
    it.rewind();
    for &id in case {
        let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(id) else {
            panic!("second pass skip_to {id} -> Expected `Found`");
        };
        assert_eq!(res.doc_id, id);
        assert_eq!(it.last_doc_id(), id);
        assert_eq!(it.at_eof(), Some(&id) == case.last());
    }
}

/// Skip between any (ordered) pair of IDs in the list, testing all combinations
#[apply(id_cases)]
fn skip_between_any_pair(#[case] case: &[u64]) {
    if case.len() < 2 {
        return;
    }

    let mut it = IdListSorted::new(case.to_vec());

    for from_idx in 0..case.len() - 1 {
        for to_idx in from_idx + 1..case.len() {
            it.rewind();
            assert_eq!(it.last_doc_id(), 0);
            assert!(!it.at_eof());

            let from_id = case[from_idx];
            let to_id = case[to_idx];

            // Skip to from_id
            let Ok(Some(SkipToOutcome::Found(doc_from))) = it.skip_to(from_id) else {
                panic!("pair ({from_idx},{to_idx}) skip_to({from_id}) expected Found");
            };
            assert_eq!(doc_from.doc_id, from_id);
            assert_eq!(it.last_doc_id(), from_id);
            assert!(!it.at_eof());

            // Skip forward to to_id
            let Ok(Some(SkipToOutcome::Found(doc_to))) = it.skip_to(to_id) else {
                panic!("pair ({from_idx},{to_idx}) skip_to({to_id}) expected Found");
            };
            assert_eq!(doc_to.doc_id, to_id);
            assert_eq!(it.last_doc_id(), to_id);
            assert_eq!(it.at_eof(), Some(&to_id) == case.last());
        }
    }
}

#[apply(id_cases)]
fn rewind(#[case] case: &[u64]) {
    let mut it = IdListSorted::new(case.to_vec());

    // Skip to each doc ID, verify, then rewind and check reset
    for &id in case {
        let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(id) else {
            panic!("skip_to({id}) expected Found");
        };
        assert_eq!(res.doc_id, id);
        assert_eq!(it.last_doc_id(), id);
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());
    }

    // Read all docs sequentially
    for &id in case {
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, id);
        assert_eq!(it.last_doc_id(), id);
    }

    // Read past EOF
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), *case.last().unwrap());

    // Rewind after EOF
    it.rewind();
    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
}

#[test]
fn revalidate() {
    let mut it = IdListSorted::new(vec![1, 2, 3]);
    assert_eq!(
        it.revalidate().expect("revalidate failed"),
        RQEValidateStatus::Ok
    );
}
