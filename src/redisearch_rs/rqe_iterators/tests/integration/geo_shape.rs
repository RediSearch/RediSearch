/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::Cell, rc::Rc, time::Duration};

use index_result::{RSIndexResult, RSResultKind};
use rqe_iterators::{
    ExpirationChecker, IteratorType, MemTracker, NoOpChecker, NoTracker, RQEIterator,
    RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    geo_shape::GeoShape,
    utils::{DeadlineTimeoutChecker, NoTimeoutChecker},
};
use rstest_reuse::apply;

use crate::id_cases;

/// Builds a `GeoShape` iterator with no timeout, no expiration, and no memory
/// tracking — the simplest configuration, used by the sorted-id-list tests.
fn plain(ids: Vec<u64>) -> GeoShape<'static, NoTimeoutChecker, NoOpChecker, NoTracker> {
    GeoShape::new(ids, NoTimeoutChecker, NoOpChecker, NoTracker)
}

/// A safe [`MemTracker`] backed by a shared cell, letting a test observe the
/// add/subtract accounting without any raw pointers.
#[derive(Clone, Default)]
struct CellTracker(Rc<Cell<usize>>);

impl CellTracker {
    fn get(&self) -> usize {
        self.0.get()
    }
}

impl MemTracker for CellTracker {
    fn add(&self, bytes: usize) {
        self.0.set(self.0.get() + bytes);
    }

    fn sub(&self, bytes: usize) {
        self.0.set(self.0.get() - bytes);
    }
}

/// An [`ExpirationChecker`] that reports a fixed set of document IDs as expired.
struct MockExpiry {
    has_expiration: bool,
    expired: Vec<u64>,
}

impl ExpirationChecker for MockExpiry {
    fn has_expiration(&self) -> bool {
        self.has_expiration
    }

    fn is_expired(&self, result: &RSIndexResult) -> bool {
        self.expired.contains(&result.doc_id)
    }
}

#[test]
fn type_is_geoshape() {
    let it = plain(vec![1, 2, 3]);
    assert_eq!(it.type_(), IteratorType::GeoShape);
}

#[test]
fn empty_initialization_works() {
    let mut it = plain(vec![]);

    let result = it.current().unwrap();
    assert_eq!(0, result.doc_id);
    assert_eq!(RSResultKind::Virtual, result.kind());

    // Unread rather than past its end, even with nothing to find.
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 0);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
    assert!(it.current().is_none());
}

#[test]
fn unsorted_input_is_sorted_on_construction() {
    let mut it = plain(vec![5, 3, 1, 4, 2]);
    for expected in 1..=5u64 {
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, expected);
    }
    // Still positioned on the last id; the read that runs past it is what EOFs.
    assert!(!it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn duplicate_ids_are_yielded_as_is() {
    // The geometry index does not guarantee unique matches; duplicates must be
    // sorted alongside everything else and yielded one by one. The shared
    // `id_cases` fixture is unique, so this behavior is covered here explicitly.
    let mut it = plain(vec![5, 3, 1, 3]);

    assert_eq!(it.num_estimated(), 4);
    for expected in [1u64, 3, 3, 5] {
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, expected);
    }
    assert!(!it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn skip_to_lands_on_first_of_duplicates() {
    let mut it = plain(vec![1, 3, 3, 5]);

    // Skipping to a duplicated id lands on its first occurrence...
    let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(3) else {
        panic!("expected Found(3)");
    };
    assert_eq!(res.doc_id, 3);
    assert_eq!(it.last_doc_id(), 3);

    // ...and the second copy is still yielded by the next read.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 3);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 5);
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn skip_to_handles_long_duplicate_runs() {
    // IDs spaced 10 apart, each repeated three times. With duplicates present,
    // `skip_to` cannot bound its search by `doc_id - last_doc_id`, so it brackets
    // the match by galloping; this exercises targets that land inside a long run
    // of equal IDs and targets that fall in the gap between runs — neither is
    // reached by the small shared fixtures.
    let ids: Vec<u64> = (1..=100u64)
        .flat_map(|v| {
            let id = v * 10;
            [id, id, id]
        })
        .collect();

    // Target equal to a present ID: lands on the first of its three copies, and
    // the other two are still yielded by the following reads.
    let mut it = plain(ids.clone());
    let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(500) else {
        panic!("expected Found(500)");
    };
    assert_eq!(res.doc_id, 500);
    assert_eq!(it.last_doc_id(), 500);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 500);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 500);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 510);

    // Target in the gap between two runs: lands on the first copy of the next
    // run (NotFound), which is then consumed by the following reads.
    let mut it = plain(ids);
    let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(505) else {
        panic!("expected NotFound landing on 510");
    };
    assert_eq!(res.doc_id, 510);
    assert_eq!(it.last_doc_id(), 510);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 510);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 510);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 520);
}

#[apply(id_cases)]
fn read(#[case] case: &[u64]) {
    let mut it = plain(case.to_vec());

    assert_eq!(it.num_estimated(), case.len());
    assert!(!it.at_eof());

    for expected_id in case.iter().copied() {
        assert!(!it.at_eof());
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);

        let result = it.current().unwrap();
        assert_eq!(expected_id, result.doc_id);
        assert_eq!(RSResultKind::Virtual, result.kind());
    }

    // Sitting on the last id is not EOF; the read that runs past it is.
    assert!(!it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
    assert!(it.current().is_none());
}

#[apply(id_cases)]
#[cfg_attr(miri, ignore = "Takes too long with Miri, causing CI to timeout")]
fn skip_to(#[case] case: &[u64]) {
    let mut it = plain(case.to_vec());

    // Skip past the last doc id: expect EOF, last_doc_id unchanged.
    let last = *case.last().unwrap();
    assert!(matches!(it.skip_to(last + 1), Ok(None)));
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 0);

    it.rewind();

    // Walk every value from 1 up to `last`, probing gaps (NotFound) and exact
    // matches (Found).
    let mut probe = 1u64;
    for &id in case {
        while probe < id {
            it.rewind();
            let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(probe) else {
                panic!("probe {probe} -> Expected NotFound landing on {id}");
            };
            assert_eq!(res.doc_id, id);
            assert!(!it.at_eof(), "still positioned on {id}");
            assert_eq!(it.last_doc_id(), id);
            probe += 1;
        }
        it.rewind();
        let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(probe) else {
            panic!("probe {probe} -> Expected Found");
        };
        assert_eq!(res.doc_id, id);
        assert_eq!(it.last_doc_id(), id);
        probe += 1;
    }
}

#[test]
fn skip_to_from_nonzero_offset() {
    // The `skip_to` test above always rewinds before each probe, so the
    // binary search only ever runs over the full list. Drive it from a
    // non-zero offset instead — the path the intersection engine actually
    // exercises by calling `skip_to`/`read` repeatedly without rewinding.
    let mut it = plain(vec![1, 3, 5, 7, 9]);

    // First skip lands on 3 and advances the offset past it.
    let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(3) else {
        panic!("expected Found(3)");
    };
    assert_eq!(res.doc_id, 3);
    assert_eq!(it.last_doc_id(), 3);

    // Second skip, without rewinding, must binary-search only the unread tail
    // [5, 7, 9] and still find 7.
    let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(7) else {
        panic!("expected Found(7) from a non-zero offset");
    };
    assert_eq!(res.doc_id, 7);
    assert_eq!(it.last_doc_id(), 7);

    // The remaining tail is yielded normally.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 9);
    assert!(matches!(it.read(), Ok(None)));

    // A `read` followed by a `skip_to` that misses must land on the next id in
    // the tail (NotFound), again searching from a non-zero offset.
    let mut it = plain(vec![1, 3, 5, 7, 9]);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(6) else {
        panic!("expected NotFound landing on 7 from a non-zero offset");
    };
    assert_eq!(res.doc_id, 7);
    assert_eq!(it.last_doc_id(), 7);
}

#[apply(id_cases)]
fn rewind(#[case] case: &[u64]) {
    let mut it = plain(case.to_vec());

    for &id in case {
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, id);
    }
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    it.rewind();
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 0);

    let res = it.read().unwrap().unwrap();
    assert_eq!(res.doc_id, case[0]);
}

#[test]
fn expired_documents_are_skipped() {
    let checker = MockExpiry {
        has_expiration: true,
        expired: vec![2, 4],
    };
    let mut it = GeoShape::new(vec![1, 2, 3, 4, 5], NoTimeoutChecker, checker, NoTracker);

    // 2 and 4 are filtered out.
    for expected in [1u64, 3, 5] {
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, expected);
        assert_eq!(it.last_doc_id(), expected);
    }
    assert!(matches!(it.read(), Ok(None)));
}

/// Builds a [`GeoShape`] iterator over `ids` with a fixed set of `expired` docs and
/// expiration gating enabled.
fn with_expired(
    ids: Vec<u64>,
    expired: Vec<u64>,
) -> GeoShape<'static, NoTimeoutChecker, MockExpiry, NoTracker> {
    let checker = MockExpiry {
        has_expiration: true,
        expired,
    };
    GeoShape::new(ids, NoTimeoutChecker, checker, NoTracker)
}

#[test]
fn skip_to_onto_expired_target_advances_to_next_valid() {
    // The geoshape iterator is driven via `skip_to` when it is a non-leading
    // intersection child. An expired match must not be returned: `skip_to` falls
    // back to a scan that advances past the expired run [3, 4] to the next valid
    // doc (5), reported as `NotFound`.
    let mut it = with_expired(vec![1, 2, 3, 4, 5], vec![3, 4]);

    let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(3) else {
        panic!("expected NotFound landing on the next valid doc (5)");
    };
    assert_eq!(res.doc_id, 5);
    assert_eq!(it.last_doc_id(), 5);

    // 5 was already consumed by the fallback scan.
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn skip_to_found_when_target_not_expired() {
    // Expiration is enabled, but the skip target itself is valid: it must still
    // be reported as `Found`, and only the matched entry is checked.
    let mut it = with_expired(vec![1, 2, 3, 4, 5], vec![2, 4]);

    let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(3) else {
        panic!("expected Found(3)");
    };
    assert_eq!(res.doc_id, 3);
    assert_eq!(it.last_doc_id(), 3);
}

#[test]
fn skip_to_onto_expired_run_to_eof() {
    // The only matches at or beyond the target are expired: the fallback scan
    // exhausts the list and the iterator reports EOF without settling on an
    // expired doc.
    let mut it = with_expired(vec![1, 3, 5], vec![5]);

    assert!(matches!(it.skip_to(5), Ok(None)));
    assert!(it.at_eof());
    // `last_doc_id` is untouched: the iterator never settled on a valid match.
    assert_eq!(it.last_doc_id(), 0);
    // The expired candidate cannot leak through the reusable `current` result:
    // past the end there is no current at all.
    assert!(it.current().is_none());
}

#[test]
fn read_to_eof_over_expired_tail_does_not_leak() {
    // The tail [4, 6] is entirely expired, so `read` settles on EOF after the
    // last valid doc (2). The expired ids probed during the final scan must not
    // remain visible through `current`.
    let mut it = with_expired(vec![2, 4, 6], vec![4, 6]);

    assert_eq!(it.read().unwrap().unwrap().doc_id, 2);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
    // The expired tail cannot leak: past the end there is no current at all,
    // while `last_doc_id` still reports the last valid doc.
    assert!(it.current().is_none());
    assert_eq!(it.last_doc_id(), 2);
}

#[test]
fn skip_to_onto_expired_target_does_not_leak_at_eof() {
    // `skip_to` lands on an expired target whose only successors are also
    // expired: the fallback scan reaches EOF. `current` must not surface the
    // expired target (or the expired successors) it probed along the way.
    let mut it = with_expired(vec![1, 3, 5, 7], vec![5, 7]);

    assert!(matches!(it.skip_to(5), Ok(None)));
    assert!(it.at_eof());
    // Neither the expired target nor its expired successors can leak: past the
    // end there is no current at all.
    assert!(it.current().is_none());
    // Nothing valid was ever returned, so `last_doc_id` stays at 0.
    assert_eq!(it.last_doc_id(), 0);
}

#[test]
fn revalidate_is_a_noop_and_preserves_position() {
    // The geoshape iterator keeps no cached expiration state: the checker is
    // consulted on every candidate, so `revalidate` has nothing to refresh. It
    // must report `Ok` and leave the iterator's position untouched.
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let mut it = plain(vec![1, 2, 3]);

    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);

    let status = it
        .revalidate(&mock_ctx.spec_read())
        .expect("revalidate failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    // Position is preserved across revalidate: iteration resumes where it left off.
    assert_eq!(it.last_doc_id(), 1);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 2);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 3);
    assert!(matches!(it.read(), Ok(None)));
}

/// An expired candidate must not survive in `result` when the scan's timeout check
/// returns between iterations: `current` would then report a document the filter
/// exists to hide, and disagree with `last_doc_id`.
#[test]
fn timeout_during_an_expired_run_does_not_leave_the_expired_doc_current() {
    // Granularity 2, so the first `check_timeout` passes and the one after the
    // expired candidate fires — landing the return between two iterations of the
    // scan loop.
    let timeout = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 2);
    let checker = MockExpiry {
        has_expiration: true,
        expired: vec![2],
    };
    let mut it = GeoShape::new(vec![1, 2, 3], timeout, checker, NoTracker);

    // 1 is valid and becomes the position.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);

    // The next read walks onto expired 2 and then times out.
    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));

    assert_eq!(
        it.last_doc_id(),
        1,
        "the expired candidate was never yielded, so the position stays on 1",
    );
    let current = it.current().expect("still positioned on 1");
    assert_eq!(
        current.doc_id, 1,
        "current must agree with last_doc_id, not hold the expired candidate",
    );
}

#[test]
fn timeout_aborts_read() {
    // A deadline already in the past with a granularity of 1 makes the very
    // first probe report a timeout.
    let timeout = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 1);
    let mut it = GeoShape::new(vec![1, 2, 3], timeout, NoOpChecker, NoTracker);

    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
}

#[test]
fn memory_tracking_adds_and_subtracts() {
    let tracker = CellTracker::default();

    {
        let it = GeoShape::new(
            vec![3u64, 1, 2],
            NoTimeoutChecker,
            NoOpChecker,
            tracker.clone(),
        );

        // The tracker reflects exactly what the iterator reports.
        assert_eq!(tracker.get(), it.mem_usage());
        assert!(it.mem_usage() >= 3 * std::mem::size_of::<u64>());
    }

    // Dropping the iterator restores the counter.
    assert_eq!(tracker.get(), 0);
}

#[test]
fn no_tracker_is_a_noop() {
    let it = plain(vec![1, 2, 3]);
    // Three u64s plus the struct overhead.
    assert!(it.mem_usage() >= 3 * std::mem::size_of::<u64>());
}

#[test]
fn geo_shape_upholds_current_contract() {
    use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
    let mut it = plain(vec![10, 20, 30, 50, 80]);
    assert_eq!(assert_current_contract(&mut it), [10, 20, 30, 50, 80]);
    assert_current_contract_via_skip_to(&mut it, 81);
}
