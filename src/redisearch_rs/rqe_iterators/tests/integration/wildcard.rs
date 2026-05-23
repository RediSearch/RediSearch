/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{IteratorType, RQEIterator, SkipToOutcome, wildcard::Wildcard};
use rqe_iterators_test_utils::ContractChecker;

/// Helper macro to assert skip_to result with expected doc_id
/// This preserves the call site location in test failures
macro_rules! assert_skip_to_found {
    ($result:expr, $target_doc_id:expr) => {
        assert!($result.is_ok());

        let outcome = $result.unwrap();
        assert!(outcome.is_some());

        if let Some(SkipToOutcome::Found(doc)) = outcome {
            assert_eq!(doc.doc_id, $target_doc_id);
        } else {
            panic!("Expected Found outcome, got {:?}", outcome);
        }
    };
}

#[test]
fn type_() {
    let it = ContractChecker::new(Wildcard::new(10, 1.0));
    assert_eq!(it.type_(), IteratorType::Wildcard);
}

#[test]
fn initial_state() {
    let it = ContractChecker::new(Wildcard::new(10, 5.));

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 10);
}

#[test]
fn read_with_post_call_mutate() {
    // small test to ensure such mutations are possible
    // for iterators which wrap Wildcard.
    let mut it = ContractChecker::new(Wildcard::new(2, 0.));

    for step in 1..=2 {
        let result = it.read().unwrap().unwrap();
        // iterators do not reset between calls
        // so important to not do something like this test,
        // where you accumulate!!! Instead you want to assign these properties (always)
        // such that they have the value you expect. Here be dragons.
        assert_eq!(result.weight, if step == 1 { 0. } else { 42. });
        result.weight += 42.;
        assert_eq!(result.weight, if step == 1 { 42. } else { 84. });
    }
}

#[test]
fn read_sequential() {
    let weight = 0.5;
    let mut it = ContractChecker::new(Wildcard::new(5, weight));

    // Read all documents sequentially
    for expected_id in 1..=5 {
        let result = it.read();
        let result = result.unwrap();
        let doc = result.unwrap();
        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(doc.weight, weight);
        assert_eq!(it.last_doc_id(), expected_id);
        assert_eq!(it.current().unwrap().doc_id, expected_id);

        // Positioned on a result, so not at EOF, including on the last id.
        assert!(!it.at_eof());
    }

    // After reading all docs, next read should return None
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());

    // Reading again should still return None
    let result = it.read().unwrap();
    assert!(result.is_none());
}

#[test]
fn skip_to_valid_targets() {
    let mut it = ContractChecker::new(Wildcard::new(10, 5.));

    // Test skipping to middle
    let result = it.skip_to(5);
    assert_skip_to_found!(result, 5);
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().unwrap().doc_id, 5);
    assert!(!it.at_eof());

    // Test skipping to last document
    let result = it.skip_to(10);
    assert_skip_to_found!(result, 10);
    assert_eq!(it.last_doc_id(), 10);
    assert_eq!(it.current().unwrap().doc_id, 10);
    // Still positioned on doc 10; the read that runs past it is what flips EOF.
    assert!(!it.at_eof());

    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
    assert!(it.current().is_none());
}

#[test]
fn skip_to_beyond_range() {
    let mut it = ContractChecker::new(Wildcard::new(10, 1.));

    let result = it.skip_to(11); // Beyond range

    let outcome = result.unwrap();
    assert!(outcome.is_none());
    assert!(it.at_eof());

    // The skip carried no result, so it left no position behind: nothing was
    // ever yielded, and `last_doc_id` still says so.
    assert_eq!(it.last_doc_id(), 0);
    assert!(it.current().is_none());

    // Subsequent reads should return None
    let result = it.read().unwrap();
    assert!(result.is_none());
}

#[test]
fn skip_to_beyond_range_keeps_the_last_yielded_position() {
    let mut it = ContractChecker::new(Wildcard::new(10, 1.));

    for expected_id in 1..=3 {
        assert_eq!(it.read().unwrap().unwrap().doc_id, expected_id);
    }

    // Overshooting reports EOF without adopting a position the iterator never
    // handed out — `top_id` least of all, which a parent would read as "doc 10
    // is this iterator's current result".
    assert!(matches!(it.skip_to(11), Ok(None)));
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 3);
    assert!(it.current().is_none());

    // Exhaustion is recorded on its own, so it holds even though the position
    // (3) is still below `top_id` and a forward probe is still well-formed.
    assert!(matches!(it.skip_to(4), Ok(None)));
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 3);

    // Only a rewind revives it.
    it.rewind();
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 0);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
}

#[test]
fn rewind() {
    let mut it = ContractChecker::new(Wildcard::new(10, 7.2));

    // Read some documents
    for _i in 1..=3 {
        let result = it.read().unwrap();
        assert!(result.is_some());
    }

    assert_eq!(it.last_doc_id(), 3);
    assert_eq!(it.current().unwrap().doc_id, 3);

    // Rewind
    it.rewind();

    // Check state after rewind
    assert_eq!(it.last_doc_id(), 0);
    assert_eq!(it.current().unwrap().doc_id, 0);
    assert!(!it.at_eof());

    // Should be able to read from beginning again
    let result = it.read().unwrap();
    let doc = result.unwrap();

    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);
    assert_eq!(it.current().unwrap().doc_id, 1);
}

#[test]
fn read_after_skip() {
    let mut it = ContractChecker::new(Wildcard::new(10, 3.));

    // Skip to middle
    let result = it.skip_to(5);
    assert_skip_to_found!(result, 5);
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().unwrap().doc_id, 5);

    // Continue reading sequentially from 6 to 10
    for expected_id in 6..=10 {
        let result = it.read().unwrap();
        let doc = result.unwrap();

        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
        assert_eq!(it.current().unwrap().doc_id, expected_id);
    }

    // After reading all remaining docs, should return EOF
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());
}

#[test]
fn skip_to_after_eof() {
    let mut it = ContractChecker::new(Wildcard::new(10, 3.7));

    // First, move to EOF by skipping beyond range
    let result = it.skip_to(11);
    assert!(result.is_ok());
    assert!(it.at_eof());

    // Try to skip to a valid target while at EOF. The overshoot above left the
    // position at 0, so this is a well-formed forward probe — it just finds
    // nothing, because exhaustion holds until a rewind.
    let result = it.skip_to(5);
    let outcome = result.unwrap();
    assert!(outcome.is_none());
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 0);
}

#[test]
fn zero_documents() {
    let mut it = ContractChecker::new(Wildcard::new(0, 3.7));

    // Unread rather than past its end: `at_eof()` only flips once a read has
    // actually found nothing, even for an iterator that has nothing to find.
    assert!(!it.at_eof(), "top_id=0 has not run past its end yet");
    assert_eq!(it.last_doc_id(), 0, "last_doc_id should be 0");
    assert_eq!(it.current().unwrap().doc_id, 0, "current().id should be 0");
    assert_eq!(it.num_estimated(), 0, "num_estimated should be 0");

    // Read should return None
    let result = it.read();
    let outcome = result.unwrap();
    assert!(outcome.is_none());
    assert!(it.at_eof(), "the read ran past the end");
    assert!(it.current().is_none());

    // Skip should return None
    let result = it.skip_to(1);
    let outcome = result.expect("skip_to(1) should succeed");
    assert!(
        outcome.is_none(),
        "skip_to(1) should return Ok(None) for empty iterator"
    );
}

/// The one test still driving the bare iterator, because its subject is
/// *Wildcard's own* `debug_assert` that a skip goes strictly forward: it fires
/// on a call that breaks `skip_to`'s caller-side precondition, which
/// `ContractChecker` now catches one step earlier, so a wrapped version would
/// pin the checker's panic instead of Wildcard's.
///
/// Probing the position the iterator already holds is the boundary case — a
/// strictly backward probe trips the same single assertion, so one test covers
/// both.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "assertion failed: self.last_doc_id() < doc_id")]
fn skip_to_same_position() {
    let mut it = Wildcard::new(10, 0.5);

    // Skip to position 5
    let result = it.skip_to(5);
    assert!(result.is_ok());
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().unwrap().doc_id, 5);

    // Try to skip to the same position, should panic
    let _ = it.skip_to(5);
}

#[test]
fn wildcard_upholds_current_contract() {
    use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
    let mut it = ContractChecker::new(Wildcard::new(5, 1.0));
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}

/// Suspend/resume coverage for [`OptimizedWildcard`], the enum that stands in
/// for the `existingDocs`-backed wildcard once the encoding is known.
///
/// [`OptimizedWildcard`] adds no behaviour of its own — every method forwards
/// to the inverted-index wildcard inside, which `inverted_index::wildcard`
/// already covers. What it adds is a whole-box reinterpretation between itself
/// and [`OptimizedWildcardSuspended`], so these tests are about the enum's
/// *layout* surviving the round trip, per variant.
mod via_resume {
    use ffi::IndexFlags_Index_DocIdsOnly;
    use index_result::RSIndexResult;
    use inverted_index::{
        DecodedBy, Encoder, InvertedIndex, doc_ids_only::DocIdsOnly, opaque,
        opaque::OpaqueEncoding, raw_doc_ids_only::RawDocIdsOnly,
    };
    use rqe_core::{DocId, RS_FIELDMASK_ALL};
    use rqe_iterators::{
        RQEIterator, RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome,
        inverted_index::Wildcard as InvIdxWildcard,
        wildcard::{OptimizedWildcard, OptimizedWildcardSuspended},
    };
    use rqe_iterators_test_utils::MockContext;

    const DOC_IDS: [DocId; 5] = [1, 3, 5, 7, 9];

    /// An `existingDocs` inverted index wired into a [`MockContext`]'s spec.
    ///
    /// The wiring is the point: resume opens with an identity check against
    /// `spec.existingDocs`, so a fixture that leaves the spec's pointer null
    /// (like `optional_optimized`'s `WildcardIndex`) can only ever produce
    /// `Aborted`. The index is held behind a raw pointer so the spec's view of
    /// it and the reader's view of it are derived from the same provenance.
    struct ExistingDocs {
        ctx: MockContext,
        index: *mut opaque::InvertedIndex,
    }

    impl Drop for ExistingDocs {
        fn drop(&mut self) {
            self.ctx
                .spec_write()
                .set_existing_docs_ptr(std::ptr::null_mut());
            // SAFETY: `index` came from `Box::into_raw` in `new` and every
            // borrow handed out is tied to `&self`, so none outlives this.
            unsafe { drop(Box::from_raw(self.index)) };
        }
    }

    fn populate<E: Encoder>(ii: &mut InvertedIndex<E>) {
        for doc_id in DOC_IDS {
            let record = RSIndexResult::build_virt()
                .doc_id(doc_id)
                .field_mask(RS_FIELDMASK_ALL)
                .frequency(1)
                .build();
            ii.add_record(&record).expect("failed to add record");
        }
    }

    impl ExistingDocs {
        fn new(index: opaque::InvertedIndex) -> Self {
            let ctx = MockContext::new(0, 0);
            let index = Box::into_raw(Box::new(index));
            ctx.spec_write().set_existing_docs_ptr(index.cast());
            Self { ctx, index }
        }

        fn doc_ids_only() -> Self {
            let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
            populate(&mut ii);
            Self::new(opaque::InvertedIndex::DocIdsOnly(ii))
        }

        fn raw_doc_ids_only() -> Self {
            let mut ii = InvertedIndex::<RawDocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
            populate(&mut ii);
            Self::new(opaque::InvertedIndex::RawDocIdsOnly(ii))
        }

        /// The typed index the spec's `existingDocs` points at.
        fn typed<E>(&self) -> &InvertedIndex<E>
        where
            E: DecodedBy + OpaqueEncoding<Storage = InvertedIndex<E>>,
        {
            // SAFETY: `index` is a live, exclusively-owned allocation for as
            // long as `self` is; the returned borrow is tied to `&self`.
            E::from_opaque(unsafe { &*self.index })
        }

        /// Garbage-collect `doc_id` out of the index, the way the fork GC does.
        ///
        /// Takes `&self` so it can run while an iterator built from
        /// [`typed`](Self::typed) is still alive — which is the whole point of
        /// the scenario, and also why the caller has to be miri-ignored: the
        /// exclusive access below overlaps a shared borrow that outlives it.
        /// The existing `inverted_index::wildcard` revalidation tests take the
        /// same shortcut for the same reason.
        fn remove_document<E>(&self, doc_id: DocId)
        where
            E: Encoder + DecodedBy + OpaqueEncoding<Storage = InvertedIndex<E>>,
        {
            // SAFETY: `index` is a live, exclusively-owned allocation; see the
            // aliasing caveat above for the borrow that overlaps this one.
            let ii = E::from_mut_opaque(unsafe { &mut *self.index });
            let delta = ii
                .scan_gc(
                    |d| d != doc_id,
                    None::<fn(&RSIndexResult, &inverted_index::RepairContext<'_>)>,
                )
                .expect("scan GC failed")
                .expect("no GC scan delta");
            assert_eq!(ii.apply_gc(delta).entries_removed, 1);
        }
    }

    /// E: the round trip, for both encodings.
    ///
    /// The `matches!` assertions are the interesting ones: they check that the
    /// whole-box cast landed on the *same* variant, which is what a disagreeing
    /// tag encoding between the two enums would break, and it can disagree per
    /// variant — so covering only the first would prove little.
    ///
    /// It is worth being precise about what this does *not* establish. The
    /// guarantee that the encodings agree comes from `#[repr(C, u8)]` on the
    /// pair, not from here: with the attribute removed, `repr(Rust)` happens to
    /// pick the same layout on the current toolchain and target, and this test
    /// still passes (checked under miri). It is a regression test for the
    /// *cast* — the addresses, the tag, and the position — and it will catch a
    /// layout divergence if one ever materialises, but the attribute is what
    /// stops one from materialising in the first place.
    #[rstest::rstest]
    #[case::doc_ids_only(false)]
    #[case::raw_doc_ids_only(true)]
    fn resume_round_trip_per_variant(#[case] raw: bool) {
        let fixture = if raw {
            ExistingDocs::raw_doc_ids_only()
        } else {
            ExistingDocs::doc_ids_only()
        };
        let guard = fixture.ctx.spec_read();
        let mut it = Box::new(if raw {
            OptimizedWildcard::RawDocIdsOnly(InvIdxWildcard::new(
                fixture.typed::<RawDocIdsOnly>().reader(),
                1.0,
            ))
        } else {
            OptimizedWildcard::DocIdsOnly(InvIdxWildcard::new(
                fixture.typed::<DocIdsOnly>().reader(),
                1.0,
            ))
        });

        assert_eq!(it.read().unwrap().unwrap().doc_id, DOC_IDS[0]);
        let box_addr = &*it as *const _ as usize;

        let suspended = it.suspend();
        assert_eq!(
            &*suspended as *const _ as usize, box_addr,
            "suspend must reuse the allocation",
        );
        let tag_survived = if raw {
            matches!(&*suspended, OptimizedWildcardSuspended::RawDocIdsOnly(_))
        } else {
            matches!(&*suspended, OptimizedWildcardSuspended::DocIdsOnly(_))
        };
        assert!(tag_survived, "suspend must land on the same variant");
        assert_eq!(RQESuspendedIterator::last_doc_id(&*suspended), DOC_IDS[0]);

        let mut active = match suspended.resume(&guard).expect("resume failed") {
            ResumeOutcome::Ok(a) => a,
            ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
            ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
        };
        assert_eq!(
            &*active as *const _ as usize, box_addr,
            "resume must reuse the allocation",
        );
        let tag_survived = if raw {
            matches!(&*active, OptimizedWildcard::RawDocIdsOnly(_))
        } else {
            matches!(&*active, OptimizedWildcard::DocIdsOnly(_))
        };
        assert!(tag_survived, "resume must land on the same variant");
        assert_eq!(active.read().unwrap().unwrap().doc_id, DOC_IDS[1]);
    }

    /// F1: the GC nulled `existingDocs` out from under the iterator.
    #[test]
    fn resume_aborts_when_existing_docs_is_nulled() {
        let fixture = ExistingDocs::doc_ids_only();
        let mut it = Box::new(OptimizedWildcard::DocIdsOnly(InvIdxWildcard::new(
            fixture.typed::<DocIdsOnly>().reader(),
            1.0,
        )));
        assert_eq!(it.read().unwrap().unwrap().doc_id, DOC_IDS[0]);

        fixture
            .ctx
            .spec_write()
            .set_existing_docs_ptr(std::ptr::null_mut());

        // Taken after the write, as the production sequence does: the read lock
        // is re-acquired once the GC's write lock has been released.
        let guard = fixture.ctx.spec_read();
        assert!(matches!(
            it.suspend().resume(&guard).expect("resume failed"),
            ResumeOutcome::Aborted
        ));
    }

    /// F2: the GC replaced `existingDocs` with a fresh allocation, so the
    /// reader's pointer is stale even though the spec's is not null.
    #[test]
    fn resume_aborts_when_existing_docs_is_replaced() {
        let fixture = ExistingDocs::doc_ids_only();
        let mut it = Box::new(OptimizedWildcard::DocIdsOnly(InvIdxWildcard::new(
            fixture.typed::<DocIdsOnly>().reader(),
            1.0,
        )));
        assert_eq!(it.read().unwrap().unwrap().doc_id, DOC_IDS[0]);

        let replacement = Box::into_raw(Box::new(opaque::InvertedIndex::DocIdsOnly(
            InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
        )));
        let original = fixture.ctx.spec_read().existing_docs_ptr();
        fixture
            .ctx
            .spec_write()
            .set_existing_docs_ptr(replacement.cast());

        // Taken after the write — see `resume_aborts_when_existing_docs_is_nulled`.
        let guard = fixture.ctx.spec_read();
        assert!(matches!(
            it.suspend().resume(&guard).expect("resume failed"),
            ResumeOutcome::Aborted
        ));

        fixture.ctx.spec_write().set_existing_docs_ptr(original);
        // SAFETY: `replacement` came from `Box::into_raw` and the spec no
        // longer points at it.
        unsafe { drop(Box::from_raw(replacement)) };
    }

    /// F3: the document the iterator sits on is collected while it is
    /// suspended, so the resume re-seeks past it and reports `Moved`.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "GC mutates the index through `&mut` while the reader's pointer into it is still live, which Stacked Borrows rejects"
    )]
    fn resume_reports_moved_when_the_current_document_is_collected() {
        let fixture = ExistingDocs::doc_ids_only();
        let mut it = Box::new(OptimizedWildcard::DocIdsOnly(InvIdxWildcard::new(
            fixture.typed::<DocIdsOnly>().reader(),
            1.0,
        )));
        assert_eq!(it.read().unwrap().unwrap().doc_id, DOC_IDS[0]);
        assert_eq!(it.read().unwrap().unwrap().doc_id, DOC_IDS[1]);

        let suspended = it.suspend();
        fixture.remove_document::<DocIdsOnly>(DOC_IDS[1]);

        let guard = fixture.ctx.spec_read();
        let mut active = match suspended.resume(&guard).expect("resume failed") {
            ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Ok(_) => panic!("expected Moved, got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };
        assert_eq!(
            active
                .current()
                .expect("a moved iterator answers `current`")
                .doc_id,
            DOC_IDS[2],
            "the resume settles on the next surviving document",
        );
        assert_eq!(active.read().unwrap().unwrap().doc_id, DOC_IDS[3]);
    }
}
