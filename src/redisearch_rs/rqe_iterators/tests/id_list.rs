/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{
    id_list::IdList,
    RQEIterator, RQEValidateStatus, SkipToOutcome
};

mod c_mocks;

static CASES: &[&[u64]] = &[
    &[1, 3, 5, 7, 9],
    &[2, 4, 6, 8, 10],
    &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    &[1, 2, 3, 5, 6, 20, 98, 500, 1000],
    &[42],
    &[1000000, 2000000, 3000000],
    &[10, 20, 30, 40, 50],
    &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40],
];

#[test]
#[should_panic(expected = "IDs list must not be empty")]
fn empty_initialization() {
    let _ = IdList::new(vec![]);
}

#[test]
fn read() {
    for (i, &case) in CASES.iter().enumerate() {
        let mut it = IdList::new(case.to_vec());

        assert_eq!(it.num_estimated(), case.len(), "Case {i} has incorrect estimated count");
        assert!(!it.at_eof(), "Case {i} is at EOF before reading");

        for &expected_id in case {
            assert!(!it.at_eof(), "Case {i}");
            let res = it.read();
            assert!(res.is_ok(), "Case {i}, expected {expected_id}");
            let res = res.unwrap();
            assert!(res.is_some(), "Case {i}, expected {expected_id}");
            let res = res.unwrap();
            assert_eq!(res.doc_id, expected_id, "Case {i}");
            drop(res); // Drop the result so we can immutable borrow the iterator (TODO: is this expected?)
            assert_eq!(it.last_doc_id(), expected_id, "Case {i}");
        }

        assert!(it.at_eof(), "Case {i}");
        assert!(matches!(it.read(), Ok(None)), "Case {i}");
        assert!(it.at_eof(), "Case {i}");

        assert!(matches!(it.read(), Ok(None)), "Case {i}");
        assert!(it.at_eof(), "Case {i}");
    }
}

#[test]
fn skip_to() {
    for (ci, &case) in CASES.iter().enumerate() {
        let mut it = IdList::new(case.to_vec());

        // Read first element
        let first_res = it.read();
        assert!(first_res.is_ok(), "Case {ci}");
        let first_res = first_res.unwrap();
        assert!(first_res.is_some(), "Case {ci}");
        let first_doc = first_res.unwrap();
        let first_id = case[0];
        assert_eq!(first_doc.doc_id, first_id, "Case {ci}");
        drop(first_doc); // Drop the result so we can immutable borrow the iterator (TODO: is this expected?)
        assert_eq!(it.last_doc_id(), first_id, "Case {ci}");
        assert!(!it.at_eof(), "Case {ci}");

        // Skip to higher than last doc id: expect EOF, last_doc_id unchanged
        let last = *case.last().unwrap();
        let res = it.skip_to(last + 1); // Expect some EOF status; we only assert observable effects
        assert!(matches!(res, Ok(None)), "Case {ci}");
        drop(res);
        assert!(it.at_eof(), "Case {ci}");
        assert_eq!(it.last_doc_id(), first_id, "Case {ci} last_doc_id changed after EOF skip");

        // Rewind
        it.rewind();
        assert!(!it.at_eof(), "Case {ci} after rewind");

        // probe walks all ids from 1 up to last, probing missing and existing ids
        let mut probe = 1u64;
        for &id in case {
            // Probe all gaps before this id
            while probe < id {
                it.rewind();
                let res = it.skip_to(probe);
                let Ok(Some(SkipToOutcome::NotFound(res))) = res else {
                    panic!("Case {ci} probe {probe} -> Expected `NotFound`, got {res:?}");
                };
                assert_eq!(res.doc_id, id, "Case {ci} probe {probe} expected landing on {id}");
                drop(res);
                // Should land on next existing id
                assert!(!it.at_eof(), "Case {ci} probe {probe} -> unexpected EOF");
                assert_eq!(it.last_doc_id(), id, "Case {ci} probe {probe} expected landing on {id}");
                probe += 1;
            }
            // Exact match
            it.rewind();
            let res = it.skip_to(probe);
            let Ok(Some(SkipToOutcome::Found(res))) = res else {
                panic!("Case {ci} probe {probe} -> Expected `Found`, got {res:?}");
            };
            assert_eq!(res.doc_id, id, "Case {ci} probe {probe} expected landing on {id}");
            drop(res);
            assert!(!it.at_eof(), "Case {ci} exact {id} unexpected EOF");
            assert_eq!(it.last_doc_id(), id, "Case {ci} exact {id}");
            probe += 1;
        }

        // After consuming all (by reading past end)
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof(), "Case {ci} should be at EOF");

        // Rewind and test direct skips to every existing id
        it.rewind();
        for &id in case {
            let res = it.skip_to(id);
            let Ok(Some(SkipToOutcome::Found(res))) = res else {
                panic!("Case {ci} second pass skip_to {id} -> Expected `Found`, got {res:?}");
            };
            assert_eq!(res.doc_id, id, "Case {ci} second pass skip_to {id}");
            drop(res);
            assert_eq!(it.last_doc_id(), id, "Case {ci} second pass skip_to {id}");
            assert!(!it.at_eof(), "Case {ci} premature EOF on second pass id {id}");
        }
    }
}

#[test]
fn rewind() {
    let mut it = Empty::default();

    assert!(matches!(it.read(), Ok(None)));
    assert!(!it.at_eof());

    it.rewind();
    assert!(!it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(!it.at_eof());
}

#[test]
fn revalidate() {
    let mut it = Empty::default();
    assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
}
