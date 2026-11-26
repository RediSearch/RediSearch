/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![cfg(not(miri))]

use std::ffi::{CStr, CString};
use std::ptr;

use inverted_index::NumericFilter;
use nested_index::{
    Element, FloatCompression, IndexParams, NestedIndex, Tag, TagLevel, TerminalKind, TerminalValue,
};
use query::mock::{MockQueryNode, TokenNodeType};
use query_error::QueryErrorCode;
use query_eval::{Config, EvalResult, NestedLimits, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::QueryNodeType;
use rqe_core::{DocId, FieldIndex};
use rqe_iterators::RQEIterator;
use rqe_iterators_test_utils::{
    ContractChecker, GlobalGuard, TestContext, assert_current_contract,
};

/// A sub-field of the fixture's NESTED field.
struct SubField {
    name: &'static CStr,
    numeric: bool,
}

const COLOR: SubField = SubField {
    name: c"color",
    numeric: false,
};
const SIZE: SubField = SubField {
    name: c"size",
    numeric: false,
};
const PRICE: SubField = SubField {
    name: c"price",
    numeric: true,
};
const MATERIAL: SubField = SubField {
    name: c"material",
    numeric: false,
};

/// One element: its routing keys, then its terminal value.
enum Value {
    Tag(String),
    Number(f64),
}

/// A spec with one NESTED field, `items`, whose index holds `docs`.
struct NestedFixture {
    _guard: GlobalGuard,
    /// Declared before the [`TestContext`] it borrows from, so it drops first.
    ctx: QueryEvalContext,
    /// Owns the spec; its teardown frees the field's definition and index, as
    /// `FieldSpec_Cleanup` does for any NESTED field.
    context: TestContext,
    field_index: FieldIndex,
}

impl NestedFixture {
    /// `sub_fields` in `INDEX` order: every one but the last routes on a tag level.
    #[expect(
        clippy::multiple_unsafe_ops_per_block,
        reason = "one SAFETY argument covers building the C field by hand"
    )]
    fn new(sub_fields: &[SubField], docs: &[(DocId, Vec<Vec<Value>>)]) -> Self {
        let _guard = GlobalGuard::default();
        let mut context = TestContext::tag(std::iter::empty());
        let field = context.add_field(b"items");

        let (routing, terminal) = sub_fields.split_at(sub_fields.len() - 1);
        let kind = if terminal[0].numeric {
            TerminalKind::Numeric(FloatCompression::Off)
        } else {
            TerminalKind::Tag
        };
        let levels = routing.iter().map(|_| TagLevel::new()).collect();
        let mut index = NestedIndex::new(levels, kind).unwrap();
        for (doc_id, elements) in docs {
            let keys: Vec<Vec<&[u8]>> = elements
                .iter()
                .map(|values| {
                    values[..values.len() - 1]
                        .iter()
                        .map(|value| match value {
                            Value::Tag(tag) => tag.as_bytes(),
                            Value::Number(_) => unreachable!("routing values are tags"),
                        })
                        .collect()
                })
                .collect();
            let elements: Vec<Element<'_>> = elements
                .iter()
                .zip(&keys)
                .map(|(values, path)| Element {
                    path,
                    value: match values.last().unwrap() {
                        Value::Tag(tag) => TerminalValue::Tag(Tag::new(tag.as_bytes()).unwrap()),
                        Value::Number(n) => TerminalValue::Numeric(*n),
                    },
                })
                .collect();
            index
                .index_document(*doc_id, &elements, IndexParams { max_depth_range: 0 })
                .unwrap();
        }

        // SAFETY: the allocators are installed by the test harness. The definition is
        // allocated as `FieldSpec_InitNested` allocates it, and its strings as the parser's,
        // because the spec's teardown frees them with the module allocator.
        unsafe {
            let calloc = redis_module::RedisModule_Calloc.unwrap();
            let strdup = redis_module::RedisModule_Strdup.unwrap();
            let def = calloc(1, size_of::<ffi::NestedDef>()).cast::<ffi::NestedDef>();
            assert!(!def.is_null());
            for (i, sub) in sub_fields.iter().enumerate() {
                let path = CString::new(format!("$.{}", sub.name.to_str().unwrap())).unwrap();
                let slot = &mut (*def).subFields[i];
                slot.name = strdup(sub.name.as_ptr());
                slot.path = strdup(path.as_ptr());
                slot.type_ = if sub.numeric {
                    ffi::FieldType_INDEXFLD_T_NUMERIC
                } else {
                    ffi::FieldType_INDEXFLD_T_TAG
                };
                (*def).levels[i] = i as u8;
            }
            (*def).numSubFields = sub_fields.len() as u8;
            (*def).numLevels = sub_fields.len() as u8;

            let fs = &mut *field.as_ptr();
            fs.set_types(ffi::FieldType_INDEXFLD_T_NESTED);
            // Both members are written: field creation left TAG defaults in the union.
            fs.__bindgen_anon_1.nestedOpts.def = def;
            fs.__bindgen_anon_1.nestedOpts.nestedIndex =
                Box::into_raw(Box::new(index)).cast::<ffi::NestedIndex>();
        }
        // SAFETY: `field` was just added to the spec and is not borrowed elsewhere.
        let field_index = unsafe { field.as_ref() }.index;
        // SAFETY: `context.qctx()` is a valid, exclusively-owned `QueryEvalCtx`.
        let ctx = unsafe { QueryEvalContext::new(context.qctx()) };
        Self {
            _guard,
            ctx,
            context,
            field_index,
        }
    }

    /// `color` and `size` tags routing to numeric `price` terminals.
    fn priced(docs: &[(DocId, Vec<Vec<Value>>)]) -> Self {
        Self::new(&[COLOR, SIZE, PRICE], docs)
    }

    /// Arms the request timeout with a deadline already in the past.
    fn expire_deadline(&mut self) {
        self.context.set_search_time(EXPIRED_DEADLINE, false);
    }

    /// The doc ids `group` matches, or `None` when evaluation yields no iterator.
    fn eval(&mut self, group: &mut Query) -> Option<Vec<DocId>> {
        self.eval_with(group, Config::default())
    }

    /// As [`eval`](Self::eval), under `config`.
    fn eval_with(&mut self, group: &mut Query, config: Config) -> Option<Vec<DocId>> {
        // SAFETY: the root and every node under it are live and owned by `group`, which is
        // borrowed exclusively for the call; tag tokens are allocated as the parser allocates
        // them (`MockQueryNode::with_redis_token`).
        let node = unsafe { QueryNodeMut::new(group.root()) };
        let evaluated = eval_node(&mut self.ctx, node, config)?;
        let mut it: ContractChecker<EvalResult<'_>> = ContractChecker::new(evaluated.into_boxed());
        assert_current_contract(&mut it);
        it.rewind();
        let mut ids = Vec::new();
        while let Some(result) = it.read().expect("read must not fail") {
            ids.push(result.doc_id);
        }
        Some(ids)
    }

    /// The error evaluating `group` set, after asserting it yields no iterator.
    fn error(&mut self, group: &mut Query) -> (QueryErrorCode, String) {
        self.error_with(group, Config::default())
    }

    /// As [`error`](Self::error), under `config`.
    fn error_with(&mut self, group: &mut Query, config: Config) -> (QueryErrorCode, String) {
        assert!(
            self.eval_with(group, config).is_none(),
            "an erroneous group yields no iterator"
        );
        let status = self.ctx.status();
        (
            status.code(),
            status
                .private_message()
                .map(|m| m.to_string_lossy().into_owned())
                .unwrap_or_default(),
        )
    }
}

fn tags(values: &[&str]) -> Vec<Value> {
    values.iter().map(|v| Value::Tag(v.to_string())).collect()
}

fn priced(color: &str, size: &str, price: f64) -> Vec<Value> {
    vec![
        Value::Tag(color.to_string()),
        Value::Tag(size.to_string()),
        Value::Number(price),
    ]
}

/// A query tree of mock nodes, kept alive together since parents point at children.
#[derive(Default)]
struct Query {
    nodes: Vec<MockQueryNode>,
    #[expect(
        clippy::vec_box,
        reason = "the nodes point at the filters, which must not move as the vector grows"
    )]
    filters: Vec<Box<NumericFilter>>,
}

impl Query {
    fn push(&mut self, node: MockQueryNode) -> *mut ffi::RSQueryNode {
        let ptr = node.as_ptr();
        self.nodes.push(node);
        ptr
    }

    fn root(&self) -> ptr::NonNull<ffi::RSQueryNode> {
        self.nodes.last().expect("a query has a root").as_non_null()
    }

    fn token(&mut self, value: &str) -> *mut ffi::RSQueryNode {
        self.push(MockQueryNode::with_redis_token(TokenNodeType::Token, value))
    }

    /// `@<sub>:{<children>}` inside a group on `field`.
    fn tag(
        &mut self,
        field: FieldIndex,
        sub: u8,
        children: &[*mut ffi::RSQueryNode],
    ) -> *mut ffi::RSQueryNode {
        let mut node = MockQueryNode::new(QueryNodeType::Tag);
        node.set_tag_field_index(field);
        node.set_sub_field(sub);
        node.set_children(children);
        self.push(node)
    }

    fn tag_values(&mut self, field: FieldIndex, sub: u8, values: &[&str]) -> *mut ffi::RSQueryNode {
        let children: Vec<_> = values.iter().map(|v| self.token(v)).collect();
        self.tag(field, sub, &children)
    }

    fn affix(&mut self, value: &str, prefix: bool, suffix: bool) -> *mut ffi::RSQueryNode {
        let mut node = MockQueryNode::with_redis_token(TokenNodeType::Prefix, value);
        node.set_prefix_mode(prefix, suffix);
        self.push(node)
    }

    /// `@<sub>:[min max]`, both ends included.
    fn range(&mut self, field: FieldIndex, sub: u8, min: f64, max: f64) -> *mut ffi::RSQueryNode {
        self.range_with(field, sub, (min, true), (max, true))
    }

    /// `@<sub>:[min max]`, each end given with whether it is included.
    fn range_with(
        &mut self,
        field: FieldIndex,
        sub: u8,
        (min, min_inclusive): (f64, bool),
        (max, max_inclusive): (f64, bool),
    ) -> *mut ffi::RSQueryNode {
        let mut filter = Box::new(NumericFilter {
            min,
            max,
            min_inclusive,
            max_inclusive,
            field_index: field,
            ..Default::default()
        });
        let mut node = MockQueryNode::new(QueryNodeType::Numeric);
        node.set_numeric_filter(&mut *filter as *mut NumericFilter);
        node.set_sub_field(sub);
        self.filters.push(filter);
        self.push(node)
    }

    fn combine(
        &mut self,
        type_: QueryNodeType,
        children: &[*mut ffi::RSQueryNode],
    ) -> *mut ffi::RSQueryNode {
        let mut node = MockQueryNode::new(type_);
        node.set_children(children);
        self.push(node)
    }

    /// The group `@items:( <inner> )`, which becomes the root.
    fn group(&mut self, field: FieldIndex, inner: *mut ffi::RSQueryNode) {
        let mut node = MockQueryNode::new(QueryNodeType::Nested);
        node.set_nested_field_index(field);
        node.set_children(&[inner]);
        self.push(node);
    }
}

/// doc 1: red M at 10, blue L at 50. doc 2: red L at 50, blue M at 10.
fn paired() -> Vec<(DocId, Vec<Vec<Value>>)> {
    vec![
        (1, vec![priced("red", "m", 10.0), priced("blue", "l", 50.0)]),
        (2, vec![priced("red", "l", 50.0), priced("blue", "m", 10.0)]),
    ]
}

#[test]
fn a_group_matches_the_values_of_one_element() {
    let mut fixture = NestedFixture::priced(&paired());
    let field = fixture.field_index;
    let mut q = Query::default();
    let color = q.tag_values(field, 0, &["RED"]);
    let size = q.tag_values(field, 1, &["m"]);
    let and = q.combine(QueryNodeType::Phrase, &[color, size]);
    q.group(field, and);
    // The tag value is folded as the sub-field stores it.
    assert_eq!(fixture.eval(&mut q), Some(vec![1]));
}

#[test]
fn a_range_on_the_terminal_and_an_or_across_sub_fields() {
    let mut fixture = NestedFixture::priced(&paired());
    let field = fixture.field_index;
    let mut q = Query::default();
    let red = q.tag_values(field, 0, &["red"]);
    let cheap = q.range(field, 2, 0.0, 20.0);
    let red_and_cheap = q.combine(QueryNodeType::Phrase, &[red, cheap]);
    let large = q.tag_values(field, 1, &["l"]);
    let or = q.combine(QueryNodeType::Union, &[red_and_cheap, large]);
    q.group(field, or);
    assert_eq!(fixture.eval(&mut q), Some(vec![1, 2]));

    let mut q = Query::default();
    let blue = q.tag_values(field, 0, &["blue"]);
    let cheap = q.range(field, 2, 0.0, 20.0);
    let and = q.combine(QueryNodeType::Phrase, &[blue, cheap]);
    q.group(field, and);
    assert_eq!(fixture.eval(&mut q), Some(vec![2]));
}

#[test]
fn a_tag_terminal_reads_listed_excluded_and_prefixed_values() {
    let docs = vec![
        (1, vec![tags(&["red", "steel"]), tags(&["blue", "wood"])]),
        (2, vec![tags(&["red", "wood"]), tags(&["red", "stone"])]),
    ];
    let mut fixture = NestedFixture::new(&[COLOR, MATERIAL], &docs);
    let field = fixture.field_index;

    // No condition on the terminal: every value of the reached terminal.
    let mut q = Query::default();
    let blue = q.tag_values(field, 0, &["blue"]);
    q.group(field, blue);
    assert_eq!(fixture.eval(&mut q), Some(vec![1]));

    // A negated value on the terminal.
    let mut q = Query::default();
    let red = q.tag_values(field, 0, &["red"]);
    let steel = q.tag_values(field, 1, &["steel"]);
    let not_steel = q.combine(QueryNodeType::Not, &[steel]);
    let and = q.combine(QueryNodeType::Phrase, &[red, not_steel]);
    q.group(field, and);
    assert_eq!(fixture.eval(&mut q), Some(vec![2]));

    // A prefix on the terminal.
    let mut q = Query::default();
    let st = q.affix("st", true, false);
    let materials = q.tag(field, 1, &[st]);
    q.group(field, materials);
    assert_eq!(fixture.eval(&mut q), Some(vec![1, 2]));
}

#[test]
fn a_prefix_on_the_terminal_follows_the_prefix_settings() {
    let docs = vec![
        (1, vec![tags(&["red", "steel"])]),
        (2, vec![tags(&["red", "stone"])]),
        (3, vec![tags(&["blue", "straw"])]),
    ];
    let mut fixture = NestedFixture::new(&[COLOR, MATERIAL], &docs);
    let field = fixture.field_index;
    let group = |q: &mut Query, prefix: &str| {
        let affix = q.affix(prefix, true, false);
        let materials = q.tag(field, 1, &[affix]);
        q.group(field, materials);
    };
    let config = |min_term_prefix, max_prefix_expansions| Config {
        min_term_prefix,
        max_prefix_expansions,
        ..Config::default()
    };

    // Shorter than the minimum: nothing, without an error.
    let mut q = Query::default();
    group(&mut q, "s");
    assert_eq!(fixture.eval_with(&mut q, config(2, 100)), None);
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);

    // The cap counts the values one prefix opens across every terminal it reaches: red's two,
    // then blue's one.
    let mut q = Query::default();
    group(&mut q, "st");
    assert_eq!(fixture.eval_with(&mut q, config(2, 3)), Some(vec![1, 2, 3]));
    assert!(
        !fixture
            .ctx
            .status()
            .warnings()
            .reached_max_prefix_expansions()
    );
    let mut q = Query::default();
    group(&mut q, "st");
    let capped = fixture
        .eval_with(&mut q, config(2, 2))
        .expect("the values within the cap are read");
    assert_eq!(capped.len(), 2, "{capped:?}");
    assert!(
        fixture
            .ctx
            .status()
            .warnings()
            .reached_max_prefix_expansions()
    );
}

#[test]
fn a_multi_word_tag_value_is_one_value() {
    let docs = vec![
        (1, vec![priced("dark red", "m", 1.0)]),
        (2, vec![priced("dark", "m", 1.0)]),
    ];
    let mut fixture = NestedFixture::priced(&docs);
    let field = fixture.field_index;
    let mut q = Query::default();
    let dark = q.token("dark");
    let red = q.token("red");
    let phrase = q.combine(QueryNodeType::Phrase, &[dark, red]);
    let color = q.tag(field, 0, &[phrase]);
    q.group(field, color);
    assert_eq!(fixture.eval(&mut q), Some(vec![1]));
}

#[test]
fn a_stopword_only_clause_matches_nothing() {
    let mut fixture = NestedFixture::priced(&paired());
    let field = fixture.field_index;
    let mut q = Query::default();
    let null = q.push(MockQueryNode::new(QueryNodeType::Null));
    q.group(field, null);
    assert_eq!(fixture.eval(&mut q), None);
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
}

#[test]
fn unsupported_clauses_are_errors() {
    let mut fixture = NestedFixture::priced(&paired());
    let field = fixture.field_index;

    // A suffix wildcard.
    let mut q = Query::default();
    let suffix = q.affix("ed", false, true);
    let color = q.tag(field, 0, &[suffix]);
    q.group(field, color);
    let (code, message) = fixture.error(&mut q);
    assert_eq!(code, QueryErrorCode::Syntax);
    assert!(
        message.contains("supported inside a NESTED group"),
        "{message}"
    );
    fixture.ctx.status().clear();

    // A text term.
    let mut q = Query::default();
    let term = q.token("red");
    q.group(field, term);
    let (code, _) = fixture.error(&mut q);
    assert_eq!(code, QueryErrorCode::Syntax);
}

/// A [`Config`] with `limits` lowered, so a small index reaches them.
fn limited(limits: impl FnOnce(&mut NestedLimits)) -> Config {
    let mut config = Config::default();
    limits(&mut config.nested_limits);
    config
}

#[test]
fn each_group_limit_fails_the_query_and_names_itself() {
    // Three terminals under size M: red, blue and green; each opens one numeric range.
    let docs = vec![
        (1, vec![priced("red", "m", 1.0)]),
        (2, vec![priced("blue", "m", 2.0)]),
        (3, vec![priced("green", "m", 3.0)]),
    ];
    let mut fixture = NestedFixture::priced(&docs);
    let field = fixture.field_index;
    let group = |q: &mut Query| {
        let m = q.tag_values(field, 1, &["m"]);
        q.group(field, m);
    };
    let cases: [(Config, &str); 2] = [
        (limited(|l| l.max_terminals = 2), "more than 2 terminals"),
        (limited(|l| l.max_readers = 2), "more than 2 posting lists"),
    ];
    for (config, expected) in cases {
        let mut q = Query::default();
        group(&mut q);
        let (code, message) = fixture.error_with(&mut q, config);
        assert_eq!(code, QueryErrorCode::Limit, "{expected}");
        assert!(message.contains(expected), "{message}");
        fixture.ctx.status().clear();
    }
    // At the limits, the group is answered.
    let mut q = Query::default();
    group(&mut q);
    let config = limited(|l| {
        l.max_terminals = 3;
        l.max_readers = 3;
    });
    assert_eq!(fixture.eval_with(&mut q, config), Some(vec![1, 2, 3]));
}

/// A `CLOCK_MONOTONIC_RAW` deadline one second after boot: in the past for any process that can
/// run this test.
const EXPIRED_DEADLINE: ffi::timespec = ffi::timespec {
    tv_sec: 1,
    tv_nsec: 0,
};

#[test]
fn a_walk_past_the_deadline_fails_the_query_with_a_timeout() {
    // An open color level over more colors than the walk enters between two clock reads, and a
    // size no element has: the walk enters every color and reaches no terminal, so only the
    // timeout bounds it.
    let docs: Vec<_> = (0..500)
        .map(|doc| (doc + 1, vec![priced(&format!("c{doc}"), "m", 1.0)]))
        .collect();
    let mut fixture = NestedFixture::priced(&docs);
    let field = fixture.field_index;
    let group = |q: &mut Query| {
        let xl = q.tag_values(field, 1, &["xl"]);
        q.group(field, xl);
    };

    let mut q = Query::default();
    group(&mut q);
    assert_eq!(fixture.eval(&mut q), None, "no element has size XL");

    fixture.expire_deadline();
    let mut q = Query::default();
    group(&mut q);
    let (code, message) = fixture.error_with(&mut q, Config::default());
    assert_eq!(code, QueryErrorCode::TimedOut);
    assert!(message.contains("timed out"), "{message}");
}

#[test]
fn too_many_alternatives_fail_the_query() {
    let mut fixture = NestedFixture::priced(&paired());
    let field = fixture.field_index;
    let mut q = Query::default();
    let red = q.tag_values(field, 0, &["red"]);
    let m = q.tag_values(field, 1, &["m"]);
    let or = q.combine(QueryNodeType::Union, &[red, m]);
    q.group(field, or);
    let (code, message) = fixture.error_with(&mut q, limited(|l| l.max_branches = 1));
    assert_eq!(code, QueryErrorCode::Limit);
    assert!(message.contains("more than 1 alternatives"), "{message}");
}

#[test]
#[expect(
    clippy::multiple_unsafe_ops_per_block,
    reason = "one SAFETY argument covers taking the index out of the field"
)]
fn a_field_without_an_index_matches_nothing() {
    let mut fixture = NestedFixture::priced(&paired());
    let field = fixture.field_index;
    // SAFETY: the field belongs to the fixture's spec, and nothing reads it concurrently. The
    // index is freed here, so the spec's teardown, which frees a null index as a no-op, does
    // not free it again.
    unsafe {
        let spec = fixture
            .ctx
            .sctx()
            .spec
            .as_ref()
            .expect("the fixture has a spec");
        let fs = &mut *spec.fields.add(field as usize);
        let index = fs.__bindgen_anon_1.nestedOpts.nestedIndex;
        drop(Box::from_raw(index.cast::<NestedIndex>()));
        fs.__bindgen_anon_1.nestedOpts.nestedIndex = ptr::null_mut();
    }
    let mut q = Query::default();
    let red = q.tag_values(field, 0, &["red"]);
    q.group(field, red);
    assert_eq!(fixture.eval(&mut q), None);
}

#[test]
fn an_excluded_infinite_end_leaves_out_infinite_values() {
    let docs = vec![
        (1, vec![priced("red", "m", f64::INFINITY)]),
        (2, vec![priced("red", "m", 5.0)]),
        (3, vec![priced("red", "m", f64::NEG_INFINITY)]),
    ];
    let mut fixture = NestedFixture::priced(&docs);
    let field = fixture.field_index;
    let cases = [
        ((0.0, true), (f64::INFINITY, false), vec![2]),
        ((0.0, true), (f64::INFINITY, true), vec![1, 2]),
        ((f64::NEG_INFINITY, false), (10.0, true), vec![2]),
        ((f64::NEG_INFINITY, true), (10.0, true), vec![2, 3]),
    ];
    for (start, end, expected) in cases {
        let mut q = Query::default();
        let price = q.range_with(field, 2, start, end);
        q.group(field, price);
        assert_eq!(fixture.eval(&mut q), Some(expected), "{start:?} {end:?}");
    }
}
