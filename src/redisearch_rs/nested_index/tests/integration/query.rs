/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::Bound::{self, Excluded, Included, Unbounded};

use nested_index::query::{Branch, Clause, Expr, PlanError, Predicate, TerminalFilter, plan};
use nested_index::{
    BucketLevel, BucketRange, Constraint, FloatCompression, Level, NestedIndex, TagLevel,
    TerminalKind,
};
use proptest::prelude::*;

use crate::util::{materials, priced};

const COLOR: usize = 0;
const SIZE: usize = 1;
const PRICE: usize = 2;

fn tags<'a>(level: usize, values: &[&'a str]) -> Expr<'a> {
    Expr::Clause(Clause {
        level,
        predicate: Predicate::TagIn(values.iter().map(|v| v.as_bytes()).collect()),
    })
}

const fn prefix(level: usize, prefix: &str) -> Expr<'_> {
    Expr::Clause(Clause {
        level,
        predicate: Predicate::TagPrefix(prefix.as_bytes()),
    })
}

const fn range(level: usize, start: Bound<f64>, end: Bound<f64>) -> Expr<'static> {
    Expr::Clause(Clause {
        level,
        predicate: Predicate::Range { start, end },
    })
}

fn not(expr: Expr<'_>) -> Expr<'_> {
    Expr::Not(Box::new(expr))
}

fn tag_in<'a>(values: &[&'a str]) -> Constraint<'a> {
    Constraint::TagIn(values.iter().map(|v| v.as_bytes()).collect())
}

const ANY_PRICE: TerminalFilter<'static> = TerminalFilter::Numeric {
    start: Unbounded,
    end: Unbounded,
};

fn plan_priced<'a>(expr: &Expr<'a>) -> Vec<Branch<'a>> {
    plan(&priced(None), expr, 16).unwrap()
}

#[test]
fn a_full_prefix_constrains_every_level() {
    let expr = Expr::And(vec![
        tags(COLOR, &["red"]),
        tags(SIZE, &["M"]),
        range(PRICE, Unbounded, Excluded(30.0)),
    ]);
    assert_eq!(
        plan_priced(&expr),
        [Branch {
            routing: vec![tag_in(&["red"]), tag_in(&["M"])],
            terminal: TerminalFilter::Numeric {
                start: Unbounded,
                end: Excluded(30.0),
            },
        }]
    );
}

#[test]
fn a_level_without_a_clause_is_any() {
    assert_eq!(
        plan_priced(&tags(SIZE, &["M"])),
        [Branch {
            routing: vec![Constraint::Any, tag_in(&["M"])],
            terminal: ANY_PRICE,
        }]
    );
}

#[test]
fn clauses_on_one_level_intersect() {
    let expr = Expr::And(vec![
        tags(COLOR, &["red", "blue", "green"]),
        tags(COLOR, &["blue", "red"]),
        not(tags(COLOR, &["red"])),
        range(PRICE, Included(1.0), Included(10.0)),
        range(PRICE, Excluded(1.0), Unbounded),
    ]);
    assert_eq!(
        plan_priced(&expr),
        [Branch {
            routing: vec![tag_in(&["blue"]), Constraint::Any],
            terminal: TerminalFilter::Numeric {
                start: Excluded(1.0),
                end: Included(10.0),
            },
        }]
    );
}

#[test]
fn negations_on_one_level_add_up() {
    let expr = Expr::And(vec![
        not(tags(COLOR, &["red"])),
        not(tags(COLOR, &["blue"])),
    ]);
    assert_eq!(
        plan_priced(&expr)[0].routing[0],
        Constraint::TagNotIn(vec![b"blue", b"red"])
    );
}

#[test]
fn contradictory_branches_are_dropped() {
    let cases = [
        Expr::And(vec![tags(COLOR, &["red"]), tags(COLOR, &["blue"])]),
        Expr::And(vec![tags(COLOR, &["red"]), not(tags(COLOR, &["red"]))]),
        Expr::And(vec![prefix(COLOR, "re"), prefix(COLOR, "bl")]),
        range(PRICE, Excluded(5.0), Excluded(5.0)),
        Expr::And(vec![
            range(PRICE, Included(1.0), Included(2.0)),
            range(PRICE, Included(3.0), Unbounded),
        ]),
        Expr::Or(Vec::new()),
    ];
    for expr in &cases {
        assert_eq!(plan_priced(expr), [], "{expr:?}");
    }
}

#[test]
fn prefixes_narrow_tags_and_each_other() {
    let narrower = Expr::And(vec![prefix(COLOR, "r"), prefix(COLOR, "re")]);
    assert_eq!(
        plan_priced(&narrower)[0].routing[0],
        Constraint::TagPrefix(b"re")
    );
    let filtered = Expr::And(vec![tags(COLOR, &["red", "blue"]), prefix(COLOR, "r")]);
    assert_eq!(plan_priced(&filtered)[0].routing[0], tag_in(&["red"]));
}

#[test]
fn an_or_becomes_one_branch_per_alternative() {
    // (red | blue) (M | price < 5): four conjunctions.
    let expr = Expr::And(vec![
        Expr::Or(vec![tags(COLOR, &["red"]), tags(COLOR, &["blue"])]),
        Expr::Or(vec![
            tags(SIZE, &["M"]),
            range(PRICE, Unbounded, Excluded(5.0)),
        ]),
    ]);
    let branches = plan_priced(&expr);
    assert_eq!(branches.len(), 4);
    assert_eq!(branches[0].routing, [tag_in(&["red"]), tag_in(&["M"])]);
    assert_eq!(branches[3].routing, [tag_in(&["blue"]), Constraint::Any]);
}

#[test]
fn not_is_pushed_down_to_the_clauses() {
    // NOT (red AND M) = NOT red OR NOT M.
    let expr = not(Expr::And(vec![tags(COLOR, &["red"]), tags(SIZE, &["M"])]));
    let branches = plan_priced(&expr);
    assert_eq!(branches.len(), 2);
    assert_eq!(branches[0].routing[0], Constraint::TagNotIn(vec![b"red"]));
    assert_eq!(branches[1].routing[1], Constraint::TagNotIn(vec![b"M"]));
    // A double negation cancels.
    assert_eq!(
        plan_priced(&not(not(tags(COLOR, &["red"])))),
        plan_priced(&tags(COLOR, &["red"]))
    );
}

#[test]
fn a_negated_range_is_below_or_above_it() {
    let branches = plan_priced(&not(range(PRICE, Included(1.0), Excluded(5.0))));
    let terminals: Vec<_> = branches.into_iter().map(|b| b.terminal).collect();
    assert_eq!(
        terminals,
        [
            TerminalFilter::Numeric {
                start: Unbounded,
                end: Excluded(1.0)
            },
            TerminalFilter::Numeric {
                start: Included(5.0),
                end: Unbounded
            },
        ]
    );
    // Outside an unbounded range is nothing.
    assert_eq!(plan_priced(&not(range(PRICE, Unbounded, Unbounded))), []);
}

#[test]
fn a_range_on_a_bucket_level_becomes_a_bucket_range() {
    let index = NestedIndex::new(
        vec![Level::Buckets(
            BucketLevel::new(vec![2020.0, 2022.0, 2024.0]).unwrap(),
        )],
        TerminalKind::Numeric(FloatCompression::Off),
    )
    .unwrap();
    let branches = plan(&index, &range(0, Included(2020.0), Excluded(2022.0)), 16).unwrap();
    assert_eq!(
        branches[0].routing,
        [Constraint::BucketRange(BucketRange {
            start: Included(2020.0),
            end: Excluded(2022.0)
        })]
    );
}

#[test]
fn a_tag_terminal_gets_the_tag_condition() {
    let expr = Expr::And(vec![tags(COLOR, &["red"]), tags(1, &["steel"])]);
    let branches = plan(&materials(), &expr, 16).unwrap();
    assert_eq!(
        branches[0].terminal,
        TerminalFilter::Tag(tag_in(&["steel"]))
    );
    let open = plan(&materials(), &tags(COLOR, &["red"]), 16).unwrap();
    assert_eq!(open[0].terminal, TerminalFilter::Tag(Constraint::Any));
}

#[test]
fn invalid_groups_are_rejected() {
    let index = priced(None);
    let cases = [
        (tags(3, &["x"]), PlanError::NoSuchLevel { level: 3 }),
        (
            range(COLOR, Unbounded, Unbounded),
            PlanError::PredicateMismatch { level: COLOR },
        ),
        (
            tags(PRICE, &["x"]),
            PlanError::PredicateMismatch { level: PRICE },
        ),
        (not(prefix(COLOR, "r")), PlanError::NegatedPrefix),
        (
            Expr::And(vec![not(tags(COLOR, &["red"])), prefix(COLOR, "r")]),
            PlanError::NegatedPrefix,
        ),
        // The same, in the other order.
        (
            Expr::And(vec![prefix(COLOR, "r"), not(tags(COLOR, &["red"]))]),
            PlanError::NegatedPrefix,
        ),
    ];
    for (expr, expected) in cases {
        assert_eq!(plan(&index, &expr, 16), Err(expected), "{expr:?}");
    }
}

#[test]
fn too_many_alternatives_are_rejected() {
    let three = || {
        Expr::Or(vec![
            tags(COLOR, &["a"]),
            tags(COLOR, &["b"]),
            tags(COLOR, &["c"]),
        ])
    };
    // 3 × 3 = 9 alternatives.
    let expr = Expr::And(vec![three(), three()]);
    assert!(plan(&priced(None), &expr, 9).is_ok());
    assert_eq!(
        plan(&priced(None), &expr, 8),
        Err(PlanError::TooManyBranches { max: 8 })
    );
}

// Property: an element matches the group exactly when it matches one of its branches.

const VALUES: [&str; 4] = ["a", "ab", "b", "c"];

#[derive(Debug, Clone, Copy)]
struct Element {
    color: &'static str,
    size: &'static str,
    price: f64,
}

fn element() -> impl Strategy<Value = Element> {
    (
        prop::sample::select(&VALUES[..3]),
        prop::sample::select(&VALUES[..3]),
        (0..8).prop_map(|p| f64::from(p) / 2.0),
    )
        .prop_map(|(color, size, price)| Element { color, size, price })
}

fn bound() -> impl Strategy<Value = Bound<f64>> {
    prop_oneof![
        Just(Unbounded),
        (0..4).prop_map(|v| Included(f64::from(v))),
        (0..4).prop_map(|v| Excluded(f64::from(v))),
    ]
}

fn clause() -> impl Strategy<Value = Expr<'static>> {
    prop_oneof![
        (
            0..2usize,
            prop::collection::vec(prop::sample::select(&VALUES[..]), 1..3)
        )
            .prop_map(|(level, values)| tags(level, &values)),
        (0..2usize, prop::sample::select(&["", "a", "b"][..]))
            .prop_map(|(level, p)| prefix(level, p)),
        (bound(), bound()).prop_map(|(start, end)| range(PRICE, start, end)),
    ]
}

fn expr() -> impl Strategy<Value = Expr<'static>> {
    clause().prop_recursive(3, 12, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(Expr::And),
            prop::collection::vec(inner.clone(), 0..3).prop_map(Expr::Or),
            inner.prop_map(not),
        ]
    })
}

const fn level_value(element: &Element, level: usize) -> &'static str {
    if level == COLOR {
        element.color
    } else {
        element.size
    }
}

fn in_range(value: f64, start: Bound<f64>, end: Bound<f64>) -> bool {
    (match start {
        Included(lo) => value >= lo,
        Excluded(lo) => value > lo,
        Unbounded => true,
    }) && (match end {
        Included(hi) => value <= hi,
        Excluded(hi) => value < hi,
        Unbounded => true,
    })
}

/// The group's meaning on one element, read straight off the expression.
fn matches(expr: &Expr<'_>, element: &Element) -> bool {
    match expr {
        Expr::Clause(Clause { level, predicate }) => match predicate {
            Predicate::TagIn(values) => values.contains(&level_value(element, *level).as_bytes()),
            Predicate::TagPrefix(p) => level_value(element, *level).as_bytes().starts_with(p),
            Predicate::Range { start, end } => in_range(element.price, *start, *end),
        },
        Expr::And(children) => children.iter().all(|c| matches(c, element)),
        Expr::Or(children) => children.iter().any(|c| matches(c, element)),
        Expr::Not(child) => !matches(child, element),
    }
}

fn satisfies(constraint: &Constraint<'_>, value: &str) -> bool {
    let value = value.as_bytes();
    match constraint {
        Constraint::Any => true,
        Constraint::TagIn(values) => values.contains(&value),
        Constraint::TagNotIn(values) => !values.contains(&value),
        Constraint::TagPrefix(p) => value.starts_with(p),
        Constraint::BucketRange(_) => unreachable!("the index has tag levels only"),
    }
}

fn in_branch(branch: &Branch<'_>, element: &Element) -> bool {
    let TerminalFilter::Numeric { start, end } = branch.terminal else {
        unreachable!("the index has a numeric terminal")
    };
    satisfies(&branch.routing[COLOR], element.color)
        && satisfies(&branch.routing[SIZE], element.size)
        && in_range(element.price, start, end)
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Each case is slow under Miri, which runs a handful of them.
        cases: if cfg!(miri) { 2 } else { 512 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn an_element_matches_the_group_exactly_when_it_matches_a_branch(
        expr in expr(),
        elements in prop::collection::vec(element(), 1..8),
    ) {
        let branches = match plan(&priced(None), &expr, usize::MAX) {
            Ok(branches) => branches,
            // A prefix under a negation is not plannable; nothing to compare.
            Err(PlanError::NegatedPrefix) => return Ok(()),
            Err(other) => panic!("{other}"),
        };
        for element in &elements {
            prop_assert_eq!(
                matches(&expr, element),
                branches.iter().any(|branch| in_branch(branch, element)),
                "{:?} on {:?}, planned as {:?}", expr, element, branches
            );
        }
    }
}

#[test]
fn an_unconstrained_group_is_one_open_branch() {
    // `@items:( )` cannot be written, but an empty AND is every element.
    let index = NestedIndex::new(
        vec![Level::Tag(TagLevel::new())],
        TerminalKind::Numeric(FloatCompression::Off),
    )
    .unwrap();
    assert_eq!(
        plan(&index, &Expr::And(Vec::new()), 16).unwrap(),
        [Branch {
            routing: vec![Constraint::Any],
            terminal: ANY_PRICE,
        }]
    );
}
