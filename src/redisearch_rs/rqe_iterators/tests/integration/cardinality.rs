/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_types::QueryNodeType;
use rqe_iterators::{
    Intersection, RQEIterator, RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome,
    UnionFullFlat, UnionFullHeap, UnionOpaque, UnionQuickFlat, UnionQuickHeap, UnionTrimmed,
    UnionVariant, id_list::IdListSorted, not::Not, not_optimized::NotOptimized, optional::Optional,
    optional_optimized::OptionalOptimized, utils::NoTimeoutChecker,
};
use rqe_iterators_test_utils::MockContext;

use crate::utils::WildcardHelper;

fn list(ids: impl IntoIterator<Item = u64>) -> IdListSorted<'static> {
    IdListSorted::new(ids.into_iter().collect::<Vec<_>>())
}

fn independent_children() -> Vec<IdListSorted<'static>> {
    vec![list((1..=100).filter(|id| id % 2 == 0)), list(1..=50)]
}

fn count<'index>(it: &mut impl RQEIterator<'index>) -> usize {
    let mut count = 0;
    while it.read().unwrap().is_some() {
        count += 1;
    }
    count
}

fn close(actual: f64, expected: f64) {
    assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
}

#[derive(Clone, Copy, Debug)]
enum UnionKind {
    FlatFull,
    FlatQuick,
    HeapFull,
    HeapQuick,
}

fn union_variant(
    kind: UnionKind,
    children: Vec<IdListSorted<'static>>,
) -> UnionVariant<'static, IdListSorted<'static>> {
    match kind {
        UnionKind::FlatFull => UnionVariant::FlatFull(UnionFullFlat::new(children)),
        UnionKind::FlatQuick => UnionVariant::FlatQuick(UnionQuickFlat::new(children)),
        UnionKind::HeapFull => UnionVariant::HeapFull(UnionFullHeap::new(children)),
        UnionKind::HeapQuick => UnionVariant::HeapQuick(UnionQuickHeap::new(children)),
    }
}

#[test]
fn intersection_estimates_independent_selectivity_without_changing_the_bound() {
    let mut it = Intersection::new(independent_children(), 1.0, false);
    close(it.estimated_matches(100), 25.0);
    assert_eq!(it.num_estimated(), 50);
    assert_eq!(count(&mut it), 25);
    close(it.estimated_matches(100), 25.0);
    it.rewind();
    close(it.estimated_matches(100), 25.0);
}

#[rstest::rstest]
#[case::correlated((1..=100).filter(|id| id % 2 == 0).collect(), 50)]
#[case::disjoint((1..=100).filter(|id| id % 2 != 0).collect(), 0)]
fn intersection_predictions_do_not_prune_results(
    #[case] second: Vec<u64>,
    #[case] actual_count: usize,
) {
    let mut it = Intersection::new(
        vec![list((1..=100).filter(|id| id % 2 == 0)), list(second)],
        1.0,
        false,
    );
    close(it.estimated_matches(100), 25.0);
    assert_eq!(count(&mut it), actual_count);
    assert_eq!(it.num_estimated(), 50);
}

#[rstest::rstest]
#[case::flat_full(UnionKind::FlatFull)]
#[case::flat_quick(UnionKind::FlatQuick)]
#[case::heap_full(UnionKind::HeapFull)]
#[case::heap_quick(UnionKind::HeapQuick)]
fn union_estimates_include_exhausted_children(#[case] kind: UnionKind) {
    let mut it = UnionOpaque {
        variant: union_variant(kind, independent_children()),
        query_node_type: QueryNodeType::Union,
        query_string: None,
    };
    close(it.estimated_matches(100), 75.0);
    assert_eq!(it.num_estimated(), 100);
    assert!(it.skip_to(60).unwrap().is_some());
    close(it.estimated_matches(100), 75.0);
    count(&mut it);
    close(it.estimated_matches(100), 75.0);
    it.rewind();
    assert_eq!(count(&mut it), 75);
}

#[rstest::rstest]
#[case::flat_full(UnionKind::FlatFull)]
#[case::flat_quick(UnionKind::FlatQuick)]
#[case::heap_full(UnionKind::HeapFull)]
#[case::heap_quick(UnionKind::HeapQuick)]
fn numeric_union_keeps_disjoint_range_estimates_additive(#[case] kind: UnionKind) {
    let it = UnionOpaque {
        variant: union_variant(kind, vec![list(1..=50), list(51..=100)]),
        query_node_type: QueryNodeType::Numeric,
        query_string: None,
    };
    close(it.estimated_matches(100), 100.0);
    close(it.estimated_matches(75), 75.0);
}

#[rstest::rstest]
#[case::flat_full(UnionKind::FlatFull)]
#[case::flat_quick(UnionKind::FlatQuick)]
#[case::heap_full(UnionKind::HeapFull)]
#[case::heap_quick(UnionKind::HeapQuick)]
fn union_estimates_survive_suspension(
    #[case] kind: UnionKind,
    #[values(QueryNodeType::Union, QueryNodeType::Numeric)] origin: QueryNodeType,
) {
    let ctx = MockContext::new(0, 0);
    let mut it = Box::new(UnionOpaque {
        variant: union_variant(kind, independent_children()),
        query_node_type: origin,
        query_string: None,
    });
    let expected = it.estimated_matches(100);
    assert!(it.read().unwrap().is_some());
    let suspended = it.suspend();
    close(
        RQESuspendedIterator::estimated_matches(&*suspended, 100),
        expected,
    );
    let guard = ctx.spec_read();
    let ResumeOutcome::Ok(resumed) = suspended.resume(&guard).unwrap() else {
        panic!("owned ID lists must resume unchanged");
    };
    close(resumed.estimated_matches(100), expected);
}

#[test]
fn nested_compounds_use_child_predictions() {
    let children = vec![
        Intersection::new(independent_children(), 1.0, false),
        Intersection::new(independent_children(), 1.0, false),
    ];
    let union = UnionFullFlat::new(children);
    close(union.estimated_matches(100), 43.75);
    assert_eq!(union.num_estimated(), 100);
}

#[test]
fn empty_and_single_child_compounds_handle_population_limits() {
    let empty = Intersection::new(Vec::<IdListSorted<'static>>::new(), 1.0, false);
    close(empty.estimated_matches(100), 0.0);
    let empty = UnionFullFlat::new(Vec::<IdListSorted<'static>>::new());
    close(empty.estimated_matches(100), 0.0);
    let one = Intersection::new(vec![list(1..=50)], 1.0, false);
    close(one.estimated_matches(100), 50.0);
    close(one.estimated_matches(10), 10.0);
    close(one.estimated_matches(0), 0.0);
    let one = UnionFullHeap::new(vec![list(1..=50)]);
    close(one.estimated_matches(100), 50.0);
    close(one.estimated_matches(10), 10.0);
    close(one.estimated_matches(0), 0.0);
}

#[test]
fn not_removes_child_prediction_from_population() {
    let child = Intersection::new(independent_children(), 1.0, false);
    let mut it = Not::new(child, 100, 1.0, NoTimeoutChecker);
    close(it.estimated_matches(100), 75.0);
    assert_eq!(it.num_estimated(), 100);
    assert_eq!(count(&mut it), 75);
    close(it.estimated_matches(0), 0.0);
}

#[test]
fn optimized_not_uses_the_live_wildcard_population() {
    let helper = WildcardHelper::new(&[1, 2, 3, 4, 5]);
    let wildcard = helper.create_wildcard();
    let mut it = NotOptimized::new(wildcard, list([2, 4]), 100, 1.0, NoTimeoutChecker);
    close(it.estimated_matches(100), 3.0);
    assert_eq!(count(&mut it), 3);
}

#[test]
fn optional_keeps_population_independent_of_the_child() {
    let mut it = Optional::new(100, 1.0, list([2, 4]));
    close(it.estimated_matches(100), 100.0);
    assert_eq!(count(&mut it), 100);
    close(it.estimated_matches(30), 30.0);
}

#[test]
fn optimized_optional_uses_the_live_wildcard_population() {
    let helper = WildcardHelper::new(&[1, 2, 3, 4, 5]);
    let wildcard = helper.create_wildcard();
    let mut it = OptionalOptimized::new(wildcard, list([2, 4]), 100, 1.0);
    close(it.estimated_matches(100), 5.0);
    assert_eq!(count(&mut it), 5);
}

#[test]
fn trimmed_union_estimates_only_the_selected_window() {
    let mut it = UnionTrimmed::new(vec![list(1..=20), list(21..=40), list(41..=60)], 10, true);
    close(it.estimated_matches(100), 40.0);
    assert_eq!(count(&mut it), 40);
    close(it.estimated_matches(100), 40.0);
    it.rewind();
    close(it.estimated_matches(100), 40.0);
}

#[rstest::rstest]
#[case::present(Some(Intersection::new(independent_children(), 1.0, false)), 25.0)]
#[case::empty(None, 0.0)]
fn wrappers_preserve_predictions_across_type_erased_suspension(
    #[case] child: Option<Intersection<'static, IdListSorted<'static>>>,
    #[case] expected: f64,
) {
    use rqe_iterators::{TypeErasedRQEIterator, maybe_empty::MaybeEmpty, profile::Profile};

    let ctx = MockContext::new(0, 0);
    let child = child.map_or_else(MaybeEmpty::new_empty, MaybeEmpty::new);
    let it = Box::new(TypeErasedRQEIterator::new(Box::new(Profile::new(child))));
    close(it.estimated_matches(100), expected);
    assert_eq!(it.last_doc_id(), 0);
    let suspended = it.suspend();
    close(
        RQESuspendedIterator::estimated_matches(&*suspended, 100),
        expected,
    );
    let guard = ctx.spec_read();
    let ResumeOutcome::Ok(mut resumed) = suspended.resume(&guard).unwrap() else {
        panic!("owned ID lists must resume unchanged");
    };
    close(resumed.estimated_matches(100), expected);
    assert_eq!(count(&mut resumed), expected as usize);
}

#[test]
fn a_zero_prediction_does_not_establish_exhaustion() {
    let mut iterator = Intersection::new((0..200).map(|_| list([1])).collect(), 1.0, false);
    assert_eq!(iterator.estimated_matches(1_000), 0.0);
    assert_eq!(iterator.num_estimated(), 1);
    assert!(!iterator.at_eof());
    assert_eq!(iterator.read().unwrap().unwrap().doc_id, 1);
    assert!(iterator.read().unwrap().is_none());
}

fn check_suspended_estimates<'query, I: RQEIteratorBoxed<'query>>(
    iterator: I,
    context: &MockContext,
    expected: [(usize, f64); 3],
) {
    for (population, count) in expected {
        close(iterator.estimated_matches(population), count);
    }
    let suspended = Box::new(iterator).suspend();
    for (population, count) in expected {
        close(
            RQESuspendedIterator::estimated_matches(&*suspended, population),
            count,
        );
    }
    let guard = context.spec_read();
    let ResumeOutcome::Ok(resumed) = suspended.resume(&guard).unwrap() else {
        panic!("unchanged children must resume unchanged");
    };
    for (population, count) in expected {
        close(resumed.estimated_matches(population), count);
    }
}

#[test]
fn not_and_optional_preserve_estimates_across_suspension() {
    let context = MockContext::new(0, 0);
    check_suspended_estimates(
        Not::new(list([2, 4]), 100, 1.0, NoTimeoutChecker),
        &context,
        [(100, 98.0), (2, 0.0), (0, 0.0)],
    );
    check_suspended_estimates(
        Optional::new(100, 1.0, list([2, 4])),
        &context,
        [(100, 100.0), (2, 2.0), (0, 0.0)],
    );
    check_suspended_estimates(
        NotOptimized::new(
            crate::utils::Mock::new([1, 2, 3, 4, 5]),
            list([2, 4]),
            100,
            1.0,
            NoTimeoutChecker,
        ),
        &context,
        [(100, 3.0), (2, 0.0), (0, 0.0)],
    );
    check_suspended_estimates(
        OptionalOptimized::new(
            crate::utils::Mock::new([1, 2, 3, 4, 5]),
            list([2, 4]),
            100,
            1.0,
        ),
        &context,
        [(100, 5.0), (2, 2.0), (0, 0.0)],
    );
}
