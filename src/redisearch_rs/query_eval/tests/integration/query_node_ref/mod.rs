/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod mock;

use std::ops::Bound;

use inverted_index::NumericFilter;
use mock::MockQueryNode;
use query_eval::{QueryNode, QueryNodeRef, WildcardMode};
use query_node_type::QueryNodeType;

#[test]
fn node_type_returns_discriminant() {
    let mock = MockQueryNode::new(QueryNodeType::Token);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    assert_eq!(node.node_type(), QueryNodeType::Token);
}

#[test]
fn node_type_reflects_each_variant() {
    for ty in [
        QueryNodeType::Phrase,
        QueryNodeType::Union,
        QueryNodeType::Numeric,
        QueryNodeType::Not,
        QueryNodeType::Optional,
        QueryNodeType::Geo,
        QueryNodeType::Geometry,
        QueryNodeType::Prefix,
        QueryNodeType::Ids,
        QueryNodeType::Wildcard,
        QueryNodeType::Tag,
        QueryNodeType::Fuzzy,
        QueryNodeType::LexRange,
        QueryNodeType::Vector,
        QueryNodeType::WildcardQuery,
        QueryNodeType::Null,
        QueryNodeType::Missing,
    ] {
        let mock = MockQueryNode::new(ty);
        let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
        assert_eq!(node.node_type(), ty, "mismatch for {ty}");
    }
}

#[test]
fn opts_returns_header() {
    let mut mock = MockQueryNode::new(QueryNodeType::Token);
    mock.opts_mut().max_slop = 7;
    mock.opts_mut().weight = 2.5;
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    assert_eq!(node.opts().max_slop, 7);
    assert_eq!(node.opts().weight, 2.5);
}

#[test]
fn num_children_zero_for_leaf() {
    let mock = MockQueryNode::new(QueryNodeType::Token);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    assert_eq!(node.num_children(), 0);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn num_children_counts_correctly() {
    let child1 = MockQueryNode::new(QueryNodeType::Token);
    let child2 = MockQueryNode::new(QueryNodeType::Numeric);
    let mut parent = MockQueryNode::new(QueryNodeType::Phrase);
    parent.set_children(&[child1.as_ptr(), child2.as_ptr()]);

    let node = unsafe { QueryNodeRef::new(parent.as_non_null()) };
    assert_eq!(node.num_children(), 2);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn child_returns_correct_node() {
    let child1 = MockQueryNode::new(QueryNodeType::Token);
    let child2 = MockQueryNode::new(QueryNodeType::Numeric);
    let child1_ptr = child1.as_ptr();
    let child2_ptr = child2.as_ptr();

    let mut parent = MockQueryNode::new(QueryNodeType::Union);
    parent.set_children(&[child1_ptr, child2_ptr]);

    let node = unsafe { QueryNodeRef::new(parent.as_non_null()) };
    let c0 = node.child(0);
    let c1 = node.child(1);
    assert_eq!(c0.node_type(), QueryNodeType::Token);
    assert_eq!(c1.node_type(), QueryNodeType::Numeric);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn children_iterator() {
    let child1 = MockQueryNode::new(QueryNodeType::Geo);
    let child2 = MockQueryNode::new(QueryNodeType::Tag);
    let child3 = MockQueryNode::new(QueryNodeType::Fuzzy);

    let mut parent = MockQueryNode::new(QueryNodeType::Phrase);
    parent.set_children(&[child1.as_ptr(), child2.as_ptr(), child3.as_ptr()]);

    let node = unsafe { QueryNodeRef::new(parent.as_non_null()) };
    let types: Vec<_> = node.children().map(|c| c.node_type()).collect();
    assert_eq!(
        types,
        vec![QueryNodeType::Geo, QueryNodeType::Tag, QueryNodeType::Fuzzy]
    );
}

#[test]
fn children_iterator_empty_for_leaf() {
    let mock = MockQueryNode::new(QueryNodeType::Wildcard);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    assert_eq!(node.children().count(), 0);
}

#[test]
fn as_enum_phrase() {
    let mut mock = MockQueryNode::new(QueryNodeType::Phrase);
    mock.set_phrase_exact(1);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    let QueryNode::Phrase { exact } = node.as_enum() else {
        panic!("expected Phrase");
    };
    assert!(exact);
}

#[test]
fn as_enum_unit_variants() {
    let cases: &[(QueryNodeType, fn(&QueryNode) -> bool)] = &[
        (QueryNodeType::Union, |q| matches!(q, QueryNode::Union)),
        (QueryNodeType::Not, |q| matches!(q, QueryNode::Not)),
        (QueryNodeType::Optional, |q| {
            matches!(q, QueryNode::Optional)
        }),
        (QueryNodeType::Wildcard, |q| {
            matches!(q, QueryNode::Wildcard)
        }),
        (QueryNodeType::Null, |q| matches!(q, QueryNode::Null)),
    ];
    for &(ty, check) in cases {
        let mock = MockQueryNode::new(ty);
        let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
        let variant = node.as_enum();
        assert!(check(&variant), "unexpected variant for {ty}");
    }
}

#[test]
fn as_enum_token() {
    let mock = MockQueryNode::new(QueryNodeType::Token);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    let QueryNode::Token { tok } = node.as_enum() else {
        panic!("expected Token");
    };
    assert!(tok.str_.is_null());
}

#[test]
fn as_enum_numeric() {
    let mut nf = NumericFilter {
        min: 1.0,
        max: 10.0,
        min_inclusive: true,
        max_inclusive: false,
        ..NumericFilter::default()
    };
    let mut mock = MockQueryNode::new(QueryNodeType::Numeric);
    mock.set_numeric_filter(&raw mut nf);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    let QueryNode::Numeric { nf } = node.as_enum() else {
        panic!("expected Numeric");
    };
    assert_eq!(nf.min, 1.0);
    assert_eq!(nf.max, 10.0);
    assert!(nf.min_inclusive);
    assert!(!nf.max_inclusive);
}

#[test]
fn as_enum_prefix() {
    for (pfx, sfx, expected) in [
        (true, false, WildcardMode::Prefix),
        (false, true, WildcardMode::Suffix),
        (true, true, WildcardMode::Contains),
    ] {
        let mut mock = MockQueryNode::new(QueryNodeType::Prefix);
        mock.set_prefix_mode(pfx, sfx);
        let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
        let QueryNode::Prefix { mode, .. } = node.as_enum() else {
            panic!("expected Prefix");
        };
        assert_eq!(mode, expected);
    }
}

#[test]
fn as_enum_lex_range_unbounded() {
    let mock = MockQueryNode::new(QueryNodeType::LexRange);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    let QueryNode::LexRange { begin, end } = node.as_enum() else {
        panic!("expected LexRange");
    };
    assert_eq!(begin, Bound::Unbounded);
    assert_eq!(end, Bound::Unbounded);
}

#[test]
fn as_enum_lex_range_inclusive() {
    let mut begin_str = *b"a\0";
    let mut end_str = *b"z\0";
    let mut mock = MockQueryNode::new(QueryNodeType::LexRange);
    mock.set_lex_range(
        begin_str.as_mut_ptr().cast(),
        true,
        end_str.as_mut_ptr().cast(),
        true,
    );
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    let QueryNode::LexRange { begin, end } = node.as_enum() else {
        panic!("expected LexRange");
    };
    assert!(matches!(begin, Bound::Included(_)));
    assert!(matches!(end, Bound::Included(_)));
}

#[test]
fn as_enum_lex_range_exclusive() {
    let mut begin_str = *b"a\0";
    let mut end_str = *b"z\0";
    let mut mock = MockQueryNode::new(QueryNodeType::LexRange);
    mock.set_lex_range(
        begin_str.as_mut_ptr().cast(),
        false,
        end_str.as_mut_ptr().cast(),
        false,
    );
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    let QueryNode::LexRange { begin, end } = node.as_enum() else {
        panic!("expected LexRange");
    };
    assert!(matches!(begin, Bound::Excluded(_)));
    assert!(matches!(end, Bound::Excluded(_)));
}

#[test]
fn as_enum_all_payload_variants() {
    for ty in [
        QueryNodeType::Geo,
        QueryNodeType::Geometry,
        QueryNodeType::Ids,
        QueryNodeType::Tag,
        QueryNodeType::Fuzzy,
        QueryNodeType::Vector,
        QueryNodeType::WildcardQuery,
        QueryNodeType::Missing,
    ] {
        let mock = MockQueryNode::new(ty);
        let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
        let variant = node.as_enum();
        let matches = matches!(
            variant,
            QueryNode::Geo { .. }
                | QueryNode::Geometry { .. }
                | QueryNode::Ids { .. }
                | QueryNode::Tag { .. }
                | QueryNode::Fuzzy { .. }
                | QueryNode::Vector { .. }
                | QueryNode::WildcardQuery { .. }
                | QueryNode::Missing { .. }
        );
        assert!(matches, "unexpected variant for {ty}");
    }
}
