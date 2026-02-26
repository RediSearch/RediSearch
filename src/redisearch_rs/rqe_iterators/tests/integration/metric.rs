/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{
    RQEIterator, RQEValidateStatus,
    metric::{MetricSortedById, MetricSortedByScore},
};

#[test]
#[should_panic(expected = "assertion failed: ids.len() == metric_data.len()")]
fn test_metric_creation_panic() {
    let ids = vec![1, 3, 5, 7, 9];
    let metric_data = vec![0.1, 0.3, 0.5, 0.7];
    let _ = MetricSortedById::new(ids, metric_data);
}

#[test]
fn test_metric_creation() {
    let ids = vec![1, 3, 5, 7, 9];
    let metric_data = vec![0.1, 0.3, 0.5, 0.7, 0.9];
    let mut metric = MetricSortedById::new(ids.clone(), metric_data.clone());

    // Test that the metric was created with correct data
    assert_eq!(metric.num_estimated(), ids.len());

    // test current is correctly init based on child (idList)
    assert_eq!(metric.current().unwrap().doc_id, 0);
}

#[test]
fn score_variant_can_handle_unsorted_ids() {
    let ids = vec![5, 3, 1, 4, 2];
    assert!(!ids.is_sorted());
    let metric_data = vec![0.1, 0.3, 0.5, 0.7, 0.9];
    let _ = MetricSortedByScore::new(ids, metric_data);
}

#[test]
#[should_panic(expected = "Can't skip when working with unsorted document ids")]
fn score_variant_cannot_skip() {
    let ids = vec![5, 3, 1, 4, 2];
    let metric_data = vec![0.1, 0.3, 0.5, 0.7, 0.9];
    let mut i = MetricSortedByScore::new(ids, metric_data);
    let _ = i.skip_to(3);
}

#[cfg(not(miri))]
mod not_miri {
    use crate::id_cases;
    use ffi::RSValue_Number_Get;
    use inverted_index::RSResultKind;
    use rqe_iterators::{RQEIterator, SkipToOutcome, metric::MetricSortedById};
    use rstest_reuse::apply;

    #[apply(id_cases)]
    fn read(#[case] case: &[u64]) {
        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it = MetricSortedById::new(case.to_vec(), metric_data.clone());

        assert_eq!(it.num_estimated(), case.len());
        assert!(!it.at_eof());

        for (j, &expected_id) in case.iter().enumerate() {
            assert!(!it.at_eof());
            let res = it.read().unwrap().unwrap();
            assert_eq!(res.doc_id, expected_id);
            assert_eq!(res.kind(), RSResultKind::Metric);
            assert_eq!(res.as_numeric(), Some(metric_data[j]));

            assert!(!res.metrics.is_null());
            let metrics = unsafe { *res.metrics };
            assert!(metrics.key.is_null());

            let metric_val = unsafe { RSValue_Number_Get(metrics.value) };
            assert_eq!(metric_val, metric_data[j]);
            assert_eq!(it.last_doc_id(), expected_id);
        }

        assert!(it.at_eof());
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[apply(id_cases)]
    fn skip_to(#[case] case: &[u64]) {
        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it = MetricSortedById::new(case.to_vec(), metric_data.clone());

        // Read first element
        let first_doc = it.read().unwrap().unwrap();
        let first_id = case[0];
        assert_eq!(first_doc.doc_id, first_id);
        assert_eq!(first_doc.kind(), RSResultKind::Metric);
        assert_eq!(first_doc.as_numeric().unwrap(), metric_data[0]);

        assert!(!first_doc.metrics.is_null());
        let metrics = unsafe { *first_doc.metrics };
        assert!(metrics.key.is_null());

        let metric_val = unsafe { RSValue_Number_Get(metrics.value) };
        assert_eq!(metric_val, metric_data[0]);
        assert_eq!(it.last_doc_id(), first_id);
        assert_eq!(it.current().unwrap().doc_id, first_id);
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
        for (j, &id) in case.iter().enumerate() {
            // Probe all gaps before this id
            while probe < id {
                it.rewind();
                let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(probe) else {
                    panic!("probe {probe} -> Expected `Some`");
                };
                assert_eq!(res.doc_id, id);
                assert_eq!(res.kind(), RSResultKind::Metric);
                assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

                assert!(!res.metrics.is_null());
                let metrics = unsafe { *res.metrics };
                assert!(metrics.key.is_null());

                let metric_val = unsafe { RSValue_Number_Get(metrics.value) };
                assert_eq!(metric_val, metric_data[j]);
                // Should land on next existing id
                assert_eq!(it.at_eof(), Some(&id) == case.last());
                assert_eq!(it.last_doc_id(), id);
                assert_eq!(it.current().unwrap().doc_id, id);
                probe += 1;
            }
            // Exact match
            it.rewind();
            let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(probe) else {
                panic!("probe {probe} -> Expected `Found`");
            };
            assert_eq!(res.doc_id, id);
            assert_eq!(res.kind(), RSResultKind::Metric);
            assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

            assert!(!res.metrics.is_null());
            let metrics = unsafe { *res.metrics };
            assert!(metrics.key.is_null());

            let metric_val = unsafe { RSValue_Number_Get(metrics.value) };
            assert_eq!(metric_val, metric_data[j]);
            assert_eq!(it.at_eof(), Some(&id) == case.last());
            assert_eq!(it.last_doc_id(), id);
            assert_eq!(it.current().unwrap().doc_id, id);
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
            assert_eq!(it.current().unwrap().doc_id, id);
            assert_eq!(it.at_eof(), Some(&id) == case.last());
        }
    }

    /// Skip between any (ordered) pair of IDs in the list, testing all combinations
    #[apply(id_cases)]
    fn skip_between_any_pair(#[case] case: &[u64]) {
        if case.len() < 2 {
            return;
        }

        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it = MetricSortedById::new(case.to_vec(), metric_data);

        for from_idx in 0..case.len() - 1 {
            for to_idx in from_idx + 1..case.len() {
                it.rewind();
                assert_eq!(it.last_doc_id(), 0);
                assert_eq!(it.current().unwrap().doc_id, 0);
                assert!(!it.at_eof());

                let from_id = case[from_idx];
                let to_id = case[to_idx];

                // Skip to from_id
                let Ok(Some(SkipToOutcome::Found(doc_from))) = it.skip_to(from_id) else {
                    panic!("pair ({from_idx},{to_idx}) skip_to({from_id}) expected Found");
                };
                assert_eq!(doc_from.doc_id, from_id);
                assert_eq!(it.last_doc_id(), from_id);
                assert_eq!(it.current().unwrap().doc_id, from_id);
                assert!(!it.at_eof());

                // Skip forward to to_id
                let Ok(Some(SkipToOutcome::Found(doc_to))) = it.skip_to(to_id) else {
                    panic!("pair ({from_idx},{to_idx}) skip_to({to_id}) expected Found");
                };
                assert_eq!(doc_to.doc_id, to_id);
                assert_eq!(it.last_doc_id(), to_id);
                assert_eq!(it.current().unwrap().doc_id, to_id);
                assert_eq!(it.at_eof(), Some(&to_id) == case.last());
            }
        }
    }

    #[apply(id_cases)]
    fn rewind(#[case] case: &[u64]) {
        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it = MetricSortedById::new(case.to_vec(), metric_data.clone());

        // Skip to each doc ID, verify, then rewind and check reset
        for (j, &id) in case.iter().enumerate() {
            let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(id) else {
                panic!("skip_to({id}) expected Found");
            };
            assert_eq!(res.doc_id, id);
            assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

            assert!(!res.metrics.is_null());
            let metrics = unsafe { *res.metrics };
            assert!(metrics.key.is_null());
            let metric_val = unsafe { RSValue_Number_Get(metrics.value) };
            assert_eq!(metric_val, metric_data[j]);
            assert_eq!(it.last_doc_id(), id);
            it.rewind();
            assert_eq!(it.last_doc_id(), 0);
            assert!(!it.at_eof());
        }

        // Read all docs sequentially
        for (j, &id) in case.iter().enumerate() {
            let res = it.read().unwrap().unwrap();
            assert_eq!(res.doc_id, id);
            assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

            assert!(!res.metrics.is_null());
            let metrics = unsafe { *res.metrics };
            assert!(metrics.key.is_null());
            let metric_val = unsafe { RSValue_Number_Get(metrics.value) };
            assert_eq!(metric_val, metric_data[j]);
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
}

#[test]
fn revalidate() {
    let metric_data = vec![0.1, 0.2, 0.3];
    let mut it = MetricSortedById::new(vec![1, 2, 3], metric_data);
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
}

#[test]
fn metric_type_returns_vector_distance() {
    let it = MetricSortedById::new(vec![1], vec![0.5]);
    assert_eq!(
        it.metric_type(),
        rqe_iterators::metric::MetricType::VectorDistance
    );
}

#[test]
fn key_mut_ref_initially_null() {
    let mut it = MetricSortedById::new(vec![1], vec![0.5]);
    assert!(it.key_mut_ref().is_null());
}

#[cfg(not(miri))]
#[test]
fn set_handle_non_null_invalidates_on_drop() {
    use ffi::RLookupKeyHandle;

    let mut handle = RLookupKeyHandle {
        key_ptr: std::ptr::null_mut(),
        is_valid: true,
    };
    let handle_ptr: *mut RLookupKeyHandle = &mut handle;

    {
        let mut it = MetricSortedById::new(vec![1], vec![0.5]);
        // SAFETY: handle_ptr points to a valid, stack-allocated RLookupKeyHandle.
        unsafe { it.set_handle(handle_ptr) };
        // it is dropped here
    }

    // After drop, the handle should be invalidated
    assert!(!handle.is_valid);
}
