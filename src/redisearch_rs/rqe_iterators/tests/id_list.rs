/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use inverted_index::RSResultKind;
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome,
    id_list::{SortedIdList, UnsortedIdList},
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
    &[
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    ],
];

#[test]
fn empty_initialization_works() {
    let mut i = SortedIdList::new(vec![]);

    let result = i.current().unwrap();
    assert_eq!(0, result.doc_id);
    assert_eq!(RSResultKind::Virtual, result.kind());

    assert!(i.at_eof());
}

#[cfg(not(feature = "disable_sort_checks_in_idlist"))]
#[test]
#[should_panic(expected = "IDs must be sorted and unique")]
fn unsorted_initialization_of_sorted_variant_panics() {
    let _ = SortedIdList::new(vec![5, 3, 1, 4, 2]);
}

#[test]
fn unsorted_initialization_of_unsorted_variant_works() {
    let mut it = UnsortedIdList::new(vec![5, 3, 1, 4, 2]);

    let result = it.current().unwrap();
    assert_eq!(0, result.doc_id);
    assert_eq!(RSResultKind::Virtual, result.kind());
}

#[cfg(not(feature = "disable_sort_checks_in_idlist"))]
#[test]
#[should_panic(expected = "Can't skip when working with unsorted document ids")]
fn unsorted_variant_cannot_skip() {
    let mut i = UnsortedIdList::new(vec![5, 3, 1, 4, 2]);
    let _ = i.skip_to(3);
}

#[cfg(not(feature = "disable_sort_checks_in_idlist"))]
#[test]
#[should_panic(expected = "IDs must be sorted and unique")]
fn duplicate_initialization() {
    let _ = SortedIdList::new(vec![1, 2, 2, 3, 4]);
}

#[test]
fn read() {
    for (i, case) in CASES.into_iter().copied().enumerate() {
        let mut it = SortedIdList::new(case.to_vec());

        assert_eq!(
            it.num_estimated(),
            case.len(),
            "Case {i} has incorrect estimated count"
        );
        assert!(!it.at_eof(), "Case {i} is at EOF before reading");

        for expected_id in case.into_iter().copied() {
            assert!(!it.at_eof(), "Case {i}");
            let res = it.read();
            assert!(res.is_ok(), "Case {i}, expected {expected_id}");
            let res = res.unwrap();
            assert!(res.is_some(), "Case {i}, expected {expected_id}");
            let res = res.unwrap();
            assert_eq!(res.doc_id, expected_id, "Case {i}");
            assert_eq!(it.last_doc_id(), expected_id, "Case {i}");

            let result = it.current().unwrap();
            assert_eq!(expected_id, result.doc_id);
            assert_eq!(RSResultKind::Virtual, result.kind());
        }

        assert!(it.at_eof(), "Case {i}");
        assert!(matches!(it.read(), Ok(None)), "Case {i}");
        assert!(it.at_eof(), "Case {i}");

        assert!(matches!(it.read(), Ok(None)), "Case {i}");
        assert!(it.at_eof(), "Case {i}");
    }
}

#[test]
#[cfg(not(miri))] // Take too long with Miri, causing CI to timeout
fn skip_to() {
    for (ci, &case) in CASES.iter().enumerate() {
        let mut it = SortedIdList::new(case.to_vec());

        // Read first element
        let first_res = it.read();
        assert!(first_res.is_ok(), "Case {ci}");
        let first_res = first_res.unwrap();
        assert!(first_res.is_some(), "Case {ci}");
        let first_doc = first_res.unwrap();
        let first_id = case[0];
        assert_eq!(first_doc.doc_id, first_id, "Case {ci}");
        assert_eq!(it.last_doc_id(), first_id, "Case {ci}");
        assert_eq!(it.at_eof(), Some(&first_id) == case.last(), "Case {ci}");

        // Skip to higher than last doc id: expect EOF, last_doc_id unchanged
        let last = *case.last().unwrap();
        let res = it.skip_to(last + 1); // Expect some EOF status; we only assert observable effects
        assert!(matches!(res, Ok(None)), "Case {ci}");
        drop(res);
        assert!(it.at_eof(), "Case {ci}");
        assert_eq!(Some(&it.last_doc_id()), case.last(), "Case {ci}");

        // Rewind
        it.rewind();
        assert!(!it.at_eof(), "Case {ci} after rewind");

        // probe walks all ids from 1 up to last, probing missing and existing ids
        let mut probe = 1u64;
        for &id in case {
            // Probe all gaps before this id
            while probe < id {
                it.rewind();
                let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(probe) else {
                    panic!("Case {ci} probe {probe} -> Expected `Some`");
                };
                assert_eq!(
                    res.doc_id, id,
                    "Case {ci} probe {probe} expected landing on {id}"
                );
                // Should land on next existing id
                assert_eq!(
                    it.at_eof(),
                    Some(&id) == case.last(),
                    "Case {ci} probe {probe} -> unexpected EOF"
                );
                assert_eq!(
                    it.last_doc_id(),
                    id,
                    "Case {ci} probe {probe} expected landing on {id}"
                );
                probe += 1;
            }
            // Exact match
            it.rewind();
            let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(probe) else {
                panic!("Case {ci} probe {probe} -> Expected `Found`");
            };
            assert_eq!(
                res.doc_id, id,
                "Case {ci} probe {probe} expected landing on {id}"
            );
            assert_eq!(
                it.at_eof(),
                Some(&id) == case.last(),
                "Case {ci} exact {id} unexpected EOF"
            );
            assert_eq!(it.last_doc_id(), id, "Case {ci} exact {id}");
            probe += 1;
        }

        // After consuming all (by reading past end)
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof(), "Case {ci} should be at EOF");

        // Rewind and test direct skips to every existing id
        it.rewind();
        for &id in case {
            let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(id) else {
                panic!("Case {ci} second pass skip_to {id} -> Expected `Found`");
            };
            assert_eq!(res.doc_id, id, "Case {ci} second pass skip_to {id}");
            assert_eq!(it.last_doc_id(), id, "Case {ci} second pass skip_to {id}");
            assert_eq!(
                it.at_eof(),
                Some(&id) == case.last(),
                "Case {ci} premature EOF on second pass id {id}"
            );
        }
    }
}

/// Skip between any (ordered) pair of IDs in the list, testing all combinations
#[test]
fn skip_between_any_pair() {
    for (ci, &case) in CASES.iter().filter(|&&case| case.len() >= 2).enumerate() {
        let mut it = SortedIdList::new(case.to_vec());

        for from_idx in 0..case.len() - 1 {
            for to_idx in from_idx + 1..case.len() {
                it.rewind();
                assert_eq!(
                    it.last_doc_id(),
                    0,
                    "Case {ci} pair ({from_idx},{to_idx}) last_doc_id not reset after rewind"
                );
                assert!(
                    !it.at_eof(),
                    "Case {ci} pair ({from_idx},{to_idx}) at EOF after rewind"
                );

                let from_id = case[from_idx];
                let to_id = case[to_idx];

                // Skip to from_id
                let Ok(Some(SkipToOutcome::Found(doc_from))) = it.skip_to(from_id) else {
                    panic!(
                        "Case {ci} pair ({from_idx},{to_idx}) skip_to({from_id}) expected Found"
                    );
                };
                assert_eq!(
                    doc_from.doc_id, from_id,
                    "Case {ci} pair ({from_idx},{to_idx}) wrong doc_from id"
                );
                assert_eq!(
                    it.last_doc_id(),
                    from_id,
                    "Case {ci} pair ({from_idx},{to_idx}) last_doc_id after from_id"
                );
                assert!(
                    !it.at_eof(),
                    "Case {ci} pair ({from_idx},{to_idx}) EOF after from_id"
                );

                // Skip forward to to_id
                let Ok(Some(SkipToOutcome::Found(doc_to))) = it.skip_to(to_id) else {
                    panic!("Case {ci} pair ({from_idx},{to_idx}) skip_to({to_id}) expected Found");
                };
                assert_eq!(
                    doc_to.doc_id, to_id,
                    "Case {ci} pair ({from_idx},{to_idx}) wrong doc_to id"
                );
                assert_eq!(
                    it.last_doc_id(),
                    to_id,
                    "Case {ci} pair ({from_idx},{to_idx}) last_doc_id after to_id"
                );
                assert_eq!(
                    it.at_eof(),
                    Some(&to_id) == case.last(),
                    "Case {ci} pair ({from_idx},{to_idx}) EOF after to_id"
                );
            }
        }
    }
}

#[test]
fn revalidate() {
    let mut it = SortedIdList::new(vec![1, 2, 3]);
    assert_eq!(
        it.revalidate().expect("revalidate failed"),
        RQEValidateStatus::Ok
    );
}
