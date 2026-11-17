/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_SAME_ELEMENT → element filter.
//!
//! The fixture indexes [`COMPANIES`] the way a NESTED group of two TAG fields and one NUMERIC
//! field does: tag entries that record element numbers, a numeric index, and per-element
//! numbers in each document's sorting vector.
//!
//! Disabled under Miri: [`TestContext`] calls into the C library, which Miri cannot execute.
#![cfg(not(miri))]

use std::{ffi::CString, ptr};

use inverted_index::NumericFilter;
use query::mock::{MockQueryNode, TokenNodeType};
use query_error::QueryErrorCode;
use query_eval::{Config, EvalResult, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::QueryNodeType;
use rqe_core::DocId;
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::{
    ContractChecker, GlobalGuard, TestContext, assert_current_contract, with_c_globals_locked,
};
use sorting_vector::RSSortingVector;
use value::{ElementNumbers, SharedValue};

/// One element of a company's array: its key, its reporting period, and its value.
type Element = (&'static str, &'static str, Option<f64>);

/// Document 1. Its headcount is 90: the value above 100 belongs to the revenue element.
const SMALL: DocId = 1;
/// Document 2.
const LARGE: DocId = 2;
/// Document 3. Its first headcount element has no value.
const NULL: DocId = 3;

/// The elements of each document, in document id order.
const COMPANIES: [&[Element]; 3] = [
    &[
        ("headcount", "fy0", Some(90.0)),
        ("revenue", "fy0", Some(500.0)),
    ],
    &[
        ("headcount", "fy0", Some(150.0)),
        ("revenue", "fy0", Some(50.0)),
    ],
    &[
        ("headcount", "fy1", None),
        ("revenue", "fy0", Some(700.0)),
        ("headcount", "fy0", Some(300.0)),
    ],
];

/// The fields that [`GroupFixture`] adds to its spec, in the order of the spec.
#[derive(Clone, Copy)]
enum Field {
    /// The TAG field of the group that holds an element's key.
    Key,
    /// The TAG field of the group that holds an element's reporting period.
    Period,
    /// The NUMERIC field of the group.
    Value,
    /// A NUMERIC field that belongs to no group.
    OutsideNumber,
    /// A TAG field that belongs to no group. It holds what [`Field::Key`] holds.
    OutsideTag,
    /// A TAG field of another group. It holds what [`Field::Key`] holds.
    OtherGroupTag,
}

/// The position in the spec's groups of the group under test, and of another one.
const GROUP: u16 = 0;
const OTHER_GROUP: u16 = 1;

impl Field {
    const ALL: [(Self, &[u8]); 6] = [
        (Self::Key, b"k"),
        (Self::Period, b"r"),
        (Self::Value, b"v"),
        (Self::OutsideNumber, b"n"),
        (Self::OutsideTag, b"t"),
        (Self::OtherGroupTag, b"o"),
    ];

    /// The position of the field in the spec, whose first field is the context's own.
    const fn index(self) -> ffi::t_fieldIndex {
        self as ffi::t_fieldIndex + 1
    }

    const fn is_numeric(self) -> bool {
        matches!(self, Self::Value | Self::OutsideNumber)
    }

    const fn group(self) -> Option<u16> {
        match self {
            Self::Key | Self::Period | Self::Value => Some(GROUP),
            Self::OtherGroupTag => Some(OTHER_GROUP),
            Self::OutsideNumber | Self::OutsideTag => None,
        }
    }

    /// The field in the spec of `context`.
    fn spec(self, context: &TestContext) -> &mut ffi::FieldSpec {
        // SAFETY: `context.spec` is a valid, exclusively-owned spec.
        let fields = unsafe { (*context.spec).fields };
        // SAFETY: the spec holds its own field and those of `Field::ALL`.
        let field = unsafe { fields.add(self.index() as usize) };
        // SAFETY: the field is valid, and the fixture refers to one field at a time.
        unsafe { field.as_mut() }.expect("the spec has fields")
    }
}

/// A child of the same-element node.
enum Condition {
    /// `@field:{a | b}` on a TAG field.
    Tags {
        field: Field,
        tags: Vec<&'static str>,
        weight: f64,
    },
    /// `(min, max]` on a NUMERIC field.
    Range { field: Field, min: f64, max: f64 },
    /// An OR of conditions.
    Either {
        alternatives: Vec<Condition>,
        weight: f64,
    },
    /// A text term, which the parser never puts in a group.
    Term(&'static str),
}

/// `@field:{tag}`.
fn tag_of(field: Field, tag: &'static str) -> Condition {
    Condition::Tags {
        field,
        tags: vec![tag],
        weight: 1.0,
    }
}

/// `@k:{tag}`.
fn key(tag: &'static str) -> Condition {
    tag_of(Field::Key, tag)
}

/// `@r:{tag}`.
fn period(tag: &'static str) -> Condition {
    tag_of(Field::Period, tag)
}

/// `@v:[(min max]`.
const fn value(min: f64, max: f64) -> Condition {
    Condition::Range {
        field: Field::Value,
        min,
        max,
    }
}

/// Whether the node is evaluated under a NOT, where only the set of documents matters.
#[derive(Clone, Copy)]
enum Negation {
    Outside,
    Inside,
}

/// Owns everything that the evaluation of a same-element node borrows.
struct GroupFixture {
    _guard: GlobalGuard,
    /// Declared before the [`TestContext`] it borrows from, so it drops first.
    ctx: QueryEvalContext,
    _context: TestContext,
    /// The nodes under evaluation and the filters they point to.
    nodes: Vec<MockQueryNode>,
    #[expect(
        clippy::vec_box,
        reason = "the nodes point to the filters, which must not move"
    )]
    filters: Vec<Box<NumericFilter>>,
}

impl GroupFixture {
    fn new(negation: Negation) -> Self {
        let _guard = GlobalGuard;
        let mut context = TestContext::tag(std::iter::empty());

        // A later `add_field` can move the fields, so they are set up once all are added.
        for (_, name) in Field::ALL {
            context.add_field(name);
        }
        for (field, _) in Field::ALL {
            let fs = field.spec(&context);
            let (type_, sortable) = if field.is_numeric() {
                (
                    ffi::FieldType_INDEXFLD_T_NUMERIC,
                    ffi::FieldSpecOptions_FieldSpec_Sortable,
                )
            } else {
                (ffi::FieldType_INDEXFLD_T_TAG, 0)
            };
            fs.set_types(type_);
            if let Some(group) = field.group() {
                fs.set_options(ffi::FieldSpecOptions_FieldSpec_NestedMember | sortable);
                fs.nestedGroup = group;
            }
        }
        Field::Value.spec(&context).sortIdx = 0;

        for (doc, elements) in COMPANIES.iter().enumerate() {
            let doc_id = context.add_document(&format!("company{doc}"));
            assert_eq!(doc_id, doc as DocId + 1);
            for keys in [Field::Key, Field::OutsideTag, Field::OtherGroupTag] {
                index_tags(keys.spec(&context), doc_id, elements.iter().map(|e| e.0));
            }
            index_tags(
                Field::Period.spec(&context),
                doc_id,
                elements.iter().map(|e| e.1),
            );
            let numbers: Vec<Option<f64>> = elements.iter().map(|e| e.2).collect();
            index_numbers(&context, Field::Value.spec(&context), doc_id, &numbers);
            context.set_sorting_vector(doc_id, stored_numbers(&numbers));
        }

        // SAFETY: `context.qctx()` is a valid, exclusively-owned `QueryEvalCtx`, which
        // upholds the `QueryEvalContext::new` invariants.
        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };
        ctx.set_in_not_sub_tree(matches!(negation, Negation::Inside));

        Self {
            _guard,
            ctx,
            _context: context,
            nodes: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Build the node of `condition`, keeping it and what it points to alive in the fixture.
    fn build(&mut self, condition: &Condition) -> *mut ffi::RSQueryNode {
        let node = match condition {
            Condition::Tags {
                field,
                tags,
                weight,
            } => {
                let tokens: Vec<_> = tags
                    .iter()
                    .map(|tag| MockQueryNode::with_redis_token(TokenNodeType::Token, tag))
                    .collect();
                let mut node = MockQueryNode::new(QueryNodeType::Tag);
                node.opts_mut().weight = *weight;
                node.set_tag_field_index(field.index());
                node.set_children(&tokens.iter().map(MockQueryNode::as_ptr).collect::<Vec<_>>());
                self.nodes.extend(tokens);
                node
            }
            Condition::Range { field, min, max } => {
                let mut filter = Box::new(NumericFilter {
                    min: *min,
                    max: *max,
                    min_inclusive: false,
                    max_inclusive: true,
                    field_index: field.index(),
                    ..Default::default()
                });
                let mut node = MockQueryNode::new(QueryNodeType::Numeric);
                node.opts_mut().weight = 1.0;
                node.set_numeric_filter(&raw mut *filter);
                self.filters.push(filter);
                node
            }
            Condition::Either {
                alternatives,
                weight,
            } => {
                let children: Vec<_> = alternatives.iter().map(|c| self.build(c)).collect();
                let mut node = MockQueryNode::new(QueryNodeType::Union);
                node.opts_mut().weight = *weight;
                node.set_children(&children);
                node
            }
            Condition::Term(term) => MockQueryNode::with_redis_token(TokenNodeType::Token, term),
        };
        let ptr = node.as_ptr();
        self.nodes.push(node);
        ptr
    }

    /// Evaluate the same-element node of `conditions` into its iterator, or `None` when the
    /// evaluation built no iterator.
    fn iterator(&mut self, conditions: &[Condition]) -> Option<EvalResult<'_>> {
        let children: Vec<_> = conditions.iter().map(|c| self.build(c)).collect();
        let mut same_element = MockQueryNode::new(QueryNodeType::SameElement);
        same_element.opts_mut().weight = 1.0;
        same_element.set_same_element_group(GROUP);
        same_element.set_children(&children);
        let node = same_element.as_non_null();
        self.nodes.push(same_element);

        // SAFETY: the node and the children built above are valid, live nodes, exclusively
        // borrowed for the call. Their tokens come from `with_redis_token`.
        let node = unsafe { QueryNodeMut::new(node) };
        Some(eval_node(&mut self.ctx, node, Config::default())?.into_boxed())
    }

    /// The documents that the same-element node of `conditions` yields and the type of its
    /// iterator, or `None` when the evaluation built no iterator.
    fn eval(&mut self, conditions: &[Condition]) -> Option<(Vec<DocId>, IteratorType)> {
        let mut it = ContractChecker::new(self.iterator(conditions)?);
        let type_ = it.type_();
        Some((assert_current_contract(&mut it), type_))
    }

    /// The documents that the same-element node of `conditions` yields.
    fn matches(&mut self, conditions: &[Condition]) -> Vec<DocId> {
        let (doc_ids, type_) = self.eval(conditions).expect("the group is evaluated");
        assert_eq!(type_, IteratorType::ElementFilter);
        doc_ids
    }
}

/// Index `tags`, one per array element of document `doc_id`, in the tag index of `field`.
fn index_tags(field: &mut ffi::FieldSpec, doc_id: DocId, tags: impl Iterator<Item = &'static str>) {
    let tags: Vec<CString> = tags.map(|tag| CString::new(tag).unwrap()).collect();
    let mut values: Vec<_> = tags.iter().map(|tag| tag.as_ptr()).collect();
    let elements: Vec<u32> = (0..tags.len() as u32).collect();
    // SAFETY: an all-zero `IndexStats` is valid; see `tag::index_values`.
    let mut stats: ffi::IndexStats = unsafe { std::mem::zeroed() };
    // `TagIndex_Ensure` reads and bumps a non-atomic id counter.
    with_c_globals_locked(|| {
        // SAFETY: `field` is a valid tag field of a spec that is not on disk.
        let index = unsafe { ffi::TagIndex_Ensure(field, ptr::null_mut(), false) };
        let index_ctx = ffi::TagIndexIndexCtx {
            batch: ptr::null_mut(),
            values: values.as_mut_ptr(),
            n: values.len(),
            docId: doc_id,
            hasFieldExpiration: false,
            stats: &mut stats,
            elements: elements.as_ptr(),
        };
        // SAFETY: `index` is the valid memory-mode index just ensured, and `index_ctx`
        // points to `n` live strings and element numbers.
        let indexed = unsafe { ffi::TagIndex_Index(ptr::null_mut(), index, &index_ctx) };
        assert!(indexed);
        // SAFETY: as above.
        unsafe { ffi::TagIndex_Commit(index, values.as_mut_ptr(), values.len(), &mut stats) };
    });
}

/// Index the numbers that the elements of document `doc_id` hold, in the numeric index of
/// `field`.
fn index_numbers(
    context: &TestContext,
    field: &mut ffi::FieldSpec,
    doc_id: DocId,
    numbers: &[Option<f64>],
) {
    // SAFETY: `context.spec` is a valid, exclusively-owned spec.
    let spec = unsafe { &mut *context.spec };
    // SAFETY: `spec` owns the numeric field `field`.
    let tree = unsafe { rqe_iterators::open_numeric_or_geo_index(spec, field, true, true) }
        .expect("the numeric index is created");
    for (i, number) in numbers.iter().flatten().enumerate() {
        tree.add(doc_id, *number, false, i > 0, 0);
    }
}

/// The sorting vector of a document whose elements hold `numbers` for [`Field::Value`].
fn stored_numbers(numbers: &[Option<f64>]) -> RSSortingVector {
    let (elements, present): (Vec<u32>, Vec<f64>) = numbers
        .iter()
        .enumerate()
        .filter_map(|(element, number)| Some((element as u32, (*number)?)))
        .unzip();
    // What the value shows is not read here.
    let shown = SharedValue::new_num(present[0]);
    let mut sorting_vector = RSSortingVector::new(1);
    sorting_vector
        .try_insert_val(
            0,
            ElementNumbers::store(&elements, &present, numbers.len() as u32, shown),
        )
        .expect("slot 0 exists");
    sorting_vector
}

#[test]
fn tag_and_range_match_on_one_element() {
    let mut fixture = GroupFixture::new(Negation::Outside);

    assert_eq!(
        fixture.matches(&[key("headcount"), value(100.0, f64::INFINITY)]),
        [LARGE, NULL]
    );
    assert_eq!(
        fixture.matches(&[key("headcount"), value(f64::NEG_INFINITY, 100.0)]),
        [SMALL]
    );
    // `SMALL` has a headcount and a value above 400, in different elements.
    assert!(
        fixture
            .matches(&[key("headcount"), value(400.0, f64::INFINITY)])
            .is_empty()
    );
}

#[test]
fn tags_only_match_on_one_element() {
    let mut fixture = GroupFixture::new(Negation::Outside);

    assert_eq!(fixture.matches(&[key("headcount"), period("fy1")]), [NULL]);
    // `fy1` is the period of a headcount element only.
    assert!(fixture.matches(&[key("revenue"), period("fy1")]).is_empty());
}

#[test]
fn ranges_only_match_on_one_element() {
    let mut fixture = GroupFixture::new(Negation::Outside);

    // `SMALL` has a value above 100 and a value up to 200, in different elements.
    assert_eq!(
        fixture.matches(&[value(100.0, f64::INFINITY), value(f64::NEG_INFINITY, 200.0)]),
        [LARGE]
    );
}

#[test]
fn tags_and_two_ranges_match_on_one_element() {
    let mut fixture = GroupFixture::new(Negation::Outside);

    assert_eq!(
        fixture.matches(&[
            key("headcount"),
            period("fy0"),
            value(100.0, f64::INFINITY),
            value(f64::NEG_INFINITY, 200.0),
        ]),
        [LARGE]
    );
}

/// An enclosing intersection orders its children by their estimate times this weight, which
/// for the group is that of the iterator that names its candidates.
#[test]
fn sort_weight_is_that_of_the_candidates() {
    /// The sort weight of an intersection of two iterators, and of one tag iterator.
    const INTERSECTION_OF_TWO: f64 = 1.0 / 2.0;
    const ONE_TAG: f64 = 1.0;
    let mut fixture = GroupFixture::new(Negation::Outside);

    let two_tags = fixture
        .iterator(&[key("headcount"), period("fy0")])
        .expect("the group is evaluated");
    assert_eq!(
        two_tags.intersection_sort_weight(false),
        INTERSECTION_OF_TWO
    );
    drop(two_tags);

    let one_tag = fixture
        .iterator(&[key("headcount"), value(100.0, f64::INFINITY)])
        .expect("the group is evaluated");
    assert_eq!(one_tag.intersection_sort_weight(false), ONE_TAG);
}

#[test]
fn tag_that_no_document_has_matches_nothing() {
    let mut fixture = GroupFixture::new(Negation::Outside);

    let (doc_ids, type_) = fixture
        .eval(&[key("profit"), value(100.0, f64::INFINITY)])
        .expect("the group is evaluated");
    assert!(doc_ids.is_empty());
    assert_eq!(type_, IteratorType::Empty);
}

/// A shape that the parser never produces leaves no iterator and the error.
#[test]
fn unexpected_condition_is_an_error() {
    let above_100 = || value(100.0, f64::INFINITY);
    let either = |alternatives| Condition::Either {
        alternatives,
        weight: 1.0,
    };
    let cases = [
        (
            "a text term",
            vec![key("headcount"), Condition::Term("hello")],
        ),
        (
            "a range on a field outside the group",
            vec![
                key("headcount"),
                Condition::Range {
                    field: Field::OutsideNumber,
                    min: 0.0,
                    max: 1.0,
                },
            ],
        ),
        (
            "a tag on a field outside the group",
            vec![tag_of(Field::OutsideTag, "headcount"), above_100()],
        ),
        (
            "a tag on a field of another group",
            vec![tag_of(Field::OtherGroupTag, "headcount"), above_100()],
        ),
        (
            "a tag on a field of another group under an OR",
            vec![
                either(vec![
                    key("revenue"),
                    tag_of(Field::OtherGroupTag, "headcount"),
                ]),
                above_100(),
            ],
        ),
        (
            "a range under an OR",
            vec![key("headcount"), either(vec![period("fy0"), above_100()])],
        ),
        ("no condition", vec![]),
    ];
    for (name, conditions) in cases {
        let mut fixture = GroupFixture::new(Negation::Outside);

        assert!(fixture.eval(&conditions).is_none(), "{name}");
        let status = fixture.ctx.status();
        assert_eq!(status.code(), QueryErrorCode::Generic, "{name}");
        assert_eq!(
            status.public_message().and_then(|m| m.to_str().ok()),
            Some("Unexpected condition in a NESTED group"),
            "{name}"
        );
    }
}

/// `NULL` matches only through its revenue element, the second alternative: its headcount
/// elements hold nothing or 300. A union that stops at the first alternative that matches
/// the document reports the headcount elements only.
fn revenue_above_600(either: Condition) -> [Condition; 2] {
    [either, value(600.0, f64::INFINITY)]
}

/// `(@k:{headcount} | @k:{revenue})`.
fn either_key(weight: f64) -> Condition {
    Condition::Either {
        alternatives: vec![key("headcount"), key("revenue")],
        weight,
    }
}

/// `@k:{headcount | revenue}`.
fn any_key(weight: f64) -> Condition {
    Condition::Tags {
        field: Field::Key,
        tags: vec!["headcount", "revenue"],
        weight,
    }
}

/// Also where a union may otherwise stop at its first match: under a NOT, and with weight 0.
#[test]
fn union_in_group_reports_every_alternative() {
    let cases = [
        ("in a plain query", Negation::Outside, 1.0),
        ("under a NOT", Negation::Inside, 1.0),
        ("with weight 0", Negation::Outside, 0.0),
    ];
    for (name, negation, weight) in cases {
        let mut fixture = GroupFixture::new(negation);

        assert_eq!(
            fixture.matches(&revenue_above_600(either_key(weight))),
            [NULL],
            "an OR of tag conditions {name}"
        );
        assert_eq!(
            fixture.matches(&revenue_above_600(any_key(weight))),
            [NULL],
            "a tag condition of two tags {name}"
        );
    }
}
