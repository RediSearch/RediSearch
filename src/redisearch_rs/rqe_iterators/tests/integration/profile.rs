/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Duration;

use rqe_iterators::{
    IteratorType, RQEIterator, Wildcard,
    profile::{Profile, ProfileCounters},
};

use crate::utils::{Mock, MockIteratorError, MockRevalidateResult};

#[test]
fn type_() {
    let child = Wildcard::new(10, 1.0);
    let it = Profile::new(child);
    assert_eq!(it.type_(), IteratorType::Profile);
}

#[test]
fn initial_state() {
    let child = Wildcard::new(10, 1.0);
    let profile = Profile::new(child);

    assert_eq!(profile.counters().read, 0);
    assert_eq!(profile.counters().skip_to, 0);
    assert_eq!(profile.counters().eof, false);
    assert_eq!(profile.wall_time_ns(), 0);
}

#[test]
fn profile_read() {
    let child = Wildcard::new(3, 1.0);
    let mut profile = Profile::new(child);

    // Read all docs
    for i in 1..=3 {
        let result = profile.read().unwrap();
        assert!(result.is_some());
        assert_eq!(profile.counters().read, i);
    }

    assert_eq!(profile.counters().skip_to, 0);
    assert!(!profile.counters().eof);

    // Next read returns None -> EOF
    let result = profile.read().unwrap();
    assert!(result.is_none());
    assert!(profile.counters().eof);
}

#[test]
fn initial_read_timed_out() {
    let child = Mock::new([]);
    child
        .data()
        .set_error_at_done(Some(MockIteratorError::TimeoutError(Some(
            Duration::from_secs(1),
        ))));

    let mut profile = Profile::new(child);

    assert!(matches!(
        profile.read(),
        Err(rqe_iterators::RQEIteratorError::TimedOut),
    ));

    assert!(profile.wall_time_ns() >= 1_000_000_000);
}

#[test]
fn profile_skip_to() {
    let child = Wildcard::new(10, 1.0);
    let mut profile = Profile::new(child);

    let _ = profile.skip_to(5);
    assert_eq!(profile.counters().skip_to, 1);
    assert_eq!(profile.counters().read, 0);
    assert!(!profile.counters().eof);

    // Skip beyond range -> EOF
    let result = profile.skip_to(100).unwrap();
    assert!(result.is_none());
    assert_eq!(profile.counters().skip_to, 2);
    assert!(profile.counters().eof);
}

#[test]
fn initial_skip_to_timedout() {
    let child = Mock::new([]);
    child
        .data()
        .set_error_at_done(Some(MockIteratorError::TimeoutError(Some(
            Duration::from_secs(1),
        ))));

    let mut profile = Profile::new(child);

    assert!(matches!(
        profile.skip_to(1),
        Err(rqe_iterators::RQEIteratorError::TimedOut),
    ));

    assert!(profile.wall_time_ns() >= 1_000_000_000);
}

#[test]
fn profile_delegates_to_child() {
    let child = Wildcard::new(10, 2.5);
    let mut profile = Profile::new(child);

    assert_eq!(profile.last_doc_id(), 0);
    assert_eq!(profile.num_estimated(), 10);
    assert!(!profile.at_eof());

    let result = profile.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(result.weight, 2.5);
    assert_eq!(profile.last_doc_id(), 1);

    // Verify current() returns same as what read() returned
    let current = profile.current().unwrap();
    assert_eq!(current.doc_id, 1);
    assert_eq!(current.weight, 2.5);
}

#[test]
fn profile_rewind() {
    let child = Wildcard::new(10, 1.0);
    let mut profile = Profile::new(child);

    // Read some docs
    let _ = profile.read(); // doc 1
    let _ = profile.read(); // doc 2
    assert_eq!(profile.last_doc_id(), 2);
    assert_eq!(profile.counters().read, 2);

    // Rewind
    profile.rewind();

    // Verify reset to beginning
    assert_eq!(profile.last_doc_id(), 0);
    assert!(!profile.at_eof());

    // Can read from start again
    let result = profile.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(profile.counters().read, 3); // counter keeps incrementing
}

#[test]
fn profile_revalidate() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let child = Wildcard::new(10, 1.0);
    let mut profile = Profile::new(child);

    let _ = profile.read(); // doc 1
    let _ = profile.read(); // doc 2

    // Revalidate (Wildcard returns OK)
    let status = profile.revalidate(&*mock_ctx.spec_read());
    assert!(status.is_ok());

    // Verify delegation still works
    assert_eq!(profile.last_doc_id(), 2);
    assert_eq!(profile.current().unwrap().doc_id, 2);
}

// ── ProfileCounters::num_reading_operations tests ───────────────────────

#[test]
fn counters_read_only() {
    let counters = ProfileCounters {
        read: 42,
        skip_to: 0,
        eof: false,
    };
    assert_eq!(counters.num_reading_operations(), 42);
}

#[test]
fn counters_read_and_skip_to() {
    let counters = ProfileCounters {
        read: 10,
        skip_to: 20,
        eof: false,
    };
    assert_eq!(counters.num_reading_operations(), 30);
}

#[test]
fn counters_with_eof_subtracts_one() {
    let counters = ProfileCounters {
        read: 5,
        skip_to: 3,
        eof: true,
    };
    // (5 + 3) - 1 = 7
    assert_eq!(counters.num_reading_operations(), 7);
}

#[test]
fn counters_eof_saturates_at_zero() {
    let counters = ProfileCounters {
        read: 0,
        skip_to: 0,
        eof: true,
    };
    // (0 + 0).saturating_sub(1) = 0
    assert_eq!(counters.num_reading_operations(), 0);
}

#[test]
fn counters_default_is_zero() {
    let counters = ProfileCounters::default();
    assert_eq!(counters.num_reading_operations(), 0);
}

mod via_resume {
    use super::*;
    use rqe_iterators::TypeErasedRQEIterator;
    use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};

    #[test]
    fn profile_revalidate() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Wildcard::new(10, 1.0);
        let mut profile = Box::new(Profile::new(child));

        let _ = profile.read(); // doc 1
        let _ = profile.read(); // doc 2

        // Resume (Wildcard returns OK)
        let mut profile =
            revalidate_via_resume(TypeErasedRQEIterator::new(profile), &mock_ctx.spec_read())
                .expect("resume failed")
                .expect_ok();

        // Verify delegation still works
        assert_eq!(profile.last_doc_id(), 2);
        assert_eq!(profile.current().unwrap().doc_id, 2);
    }

    /// The FFI wrapper caches a raw pointer into the iterator (`header.current`),
    /// so a suspend→resume cycle must reuse the same heap allocation rather than
    /// reallocate. Exercise the concrete (non-type-erased) path and assert the
    /// box address survives both transitions.
    #[test]
    fn profile_resume_preserves_box_address() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Wildcard::new(10, 1.0);
        let mut profile = Box::new(Profile::new(child));

        let _ = profile.read(); // doc 1
        let _ = profile.read(); // doc 2

        let addr_before = &*profile as *const _ as usize;

        let suspended = profile.suspend();
        assert_eq!(
            &*suspended as *const _ as usize, addr_before,
            "suspend must reuse the allocation"
        );

        let active = match suspended
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Ok(it) | ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Aborted => panic!("wildcard child should not abort"),
        };

        assert_eq!(
            &*active as *const _ as usize, addr_before,
            "resume must reuse the same allocation (FFI caches a pointer into it)"
        );
        assert_eq!(active.last_doc_id(), 2);
    }

    /// Regression test for a `Profile` wrapping a *type-erased* child — the
    /// common case, since composites and the FFI hand `Profile` a
    /// `TypeErasedRQEIterator`. `Profile::suspend` must drive the child's own
    /// `suspend` (dispatching through the child's `dyn` vtable) rather than
    /// blindly reinterpreting the child slot: a whole-box cast leaves the
    /// child's *active* vtable in place while the type claims it is suspended,
    /// so the later `resume` would dispatch through the wrong vtable (UB). The
    /// earlier tests only cover a concrete child, for which the whole-box cast
    /// happens to be sound.
    #[test]
    fn profile_over_type_erased_child_round_trips() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = TypeErasedRQEIterator::new(Box::new(Wildcard::new(10, 1.0)));
        let mut profile = Box::new(Profile::new(child));

        let _ = profile.read(); // doc 1
        let _ = profile.read(); // doc 2

        let addr_before = &*profile as *const _ as usize;

        let suspended = profile.suspend();
        let mut active = match suspended
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Ok(it) | ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Aborted => panic!("wildcard child should not abort"),
        };

        // The outer allocation is reused across the cycle...
        assert_eq!(
            &*active as *const _ as usize, addr_before,
            "resume must reuse the same allocation (FFI caches a pointer into it)"
        );
        // ...and the type-erased child resumed at the right position and still
        // reads correctly (would be corrupt if dispatched via the wrong vtable).
        assert_eq!(active.last_doc_id(), 2);
        assert_eq!(active.current().unwrap().doc_id, 2);
        assert_eq!(active.read().unwrap().unwrap().doc_id, 3);
    }

    /// When the profiled child's revalidation is unrecoverable, `resume` must
    /// report `Aborted` and tear the reused allocation down *without* dropping
    /// the already-consumed child slot — exercising the
    /// `dealloc_after_child_gone` teardown path. A leak, double-drop, or use of
    /// the moved-from child in that path is what this guards against.
    #[test]
    fn profile_resume_aborts_when_child_validation_fails() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([1, 2, 3]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Abort);
        let mut profile = Box::new(Profile::new(child));

        let _ = profile.read(); // doc 1

        let suspended = profile.suspend();
        let outcome = suspended
            .resume(&mock_ctx.spec_read())
            .expect("resume must not surface an error for an aborting child");
        assert!(
            matches!(outcome, ResumeOutcome::Aborted),
            "an unrecoverable child must abort the whole profile",
        );
    }

    /// As [`profile_resume_aborts_when_child_validation_fails`], but for the
    /// realistic case of a *type-erased* child: the abort teardown must run
    /// correctly when the consumed child slot holds an erased suspended
    /// iterator rather than a concrete one.
    #[test]
    fn profile_over_type_erased_child_aborts_on_validation_failure() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let mock = Mock::new([1, 2, 3]);
        mock.data()
            .set_revalidate_result(MockRevalidateResult::Abort);
        let child = TypeErasedRQEIterator::new(Box::new(mock));
        let mut profile = Box::new(Profile::new(child));

        let _ = profile.read(); // doc 1

        let suspended = profile.suspend();
        let outcome = suspended
            .resume(&mock_ctx.spec_read())
            .expect("resume must not surface an error for an aborting child");
        assert!(
            matches!(outcome, ResumeOutcome::Aborted),
            "an unrecoverable type-erased child must abort the whole profile",
        );
    }
}
