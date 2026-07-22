/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use inverted_index::NumericFilter;
use query::{QueryNode, QueryNodeRef, WildcardMode, mock::MockQueryNode};
use query_types::QueryNodeType;

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
    assert!(tok.as_bytes().is_none());
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

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn query_node_mut_narrows_child_field_masks() {
    // A phrase node with mask 0b0110 and two children with broader masks.
    // Narrowing each child against the parent intersects the masks in place,
    // and `as_ref` still exposes the child for read-only evaluation.
    let mut child1 = MockQueryNode::new(QueryNodeType::Token);
    child1.opts_mut().field_mask = 0b1111;
    let mut child2 = MockQueryNode::new(QueryNodeType::Numeric);
    child2.opts_mut().field_mask = 0b1010;

    let mut parent = MockQueryNode::new(QueryNodeType::Phrase);
    parent.opts_mut().field_mask = 0b0110;
    parent.set_children(&[child1.as_ptr(), child2.as_ptr()]);

    let node = unsafe { QueryNodeRef::new(parent.as_non_null()) };
    // SAFETY: `node` is the sole wrapper to this subtree for the duration of
    // the test; no other wrapper or derived reference is live.
    let mut node = unsafe { node.as_mut() };
    assert_eq!(node.num_children(), 2);
    let mask = node.opts().field_mask;

    {
        let mut c0 = node.child_mut(0);
        c0.and_field_mask(mask);
        assert_eq!(c0.opts().field_mask, 0b0110); // 0b1111 & 0b0110
        assert_eq!(c0.as_ref().node_type(), QueryNodeType::Token);
    }
    {
        let mut c1 = node.child_mut(1);
        c1.and_field_mask(mask);
        assert_eq!(c1.opts().field_mask, 0b0010); // 0b1010 & 0b0110
        assert_eq!(c1.as_ref().node_type(), QueryNodeType::Numeric);
    }

    // The parent's own mask is untouched.
    assert_eq!(node.opts().field_mask, 0b0110);
}

#[test]
#[should_panic(expected = "out of bounds")]
fn query_node_mut_child_out_of_bounds_panics() {
    let mock = MockQueryNode::new(QueryNodeType::Wildcard);
    let node = unsafe { QueryNodeRef::new(mock.as_non_null()) };
    // SAFETY: sole wrapper to a leaf node; no other wrapper or reference live.
    let mut node = unsafe { node.as_mut() };
    let _ = node.child_mut(0);
}
