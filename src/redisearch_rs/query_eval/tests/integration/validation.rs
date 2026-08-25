/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Contract tests for the pure Rust query-validation traversal.

use std::ptr;

use index_spec::IndexSpec;
use query::{
    QueryNodeRef,
    mock::{MockQueryNode, TokenNodeType},
};
use query_error::{QueryError, QueryErrorCode};
use query_eval::check_is_valid;
use query_types::{QASTValidationFlags, QASTValidationFlagsSet, QueryNodeFlags, QueryNodeType};

fn spec_with_fields(fields: &mut [ffi::FieldSpec]) -> ffi::IndexSpec {
    // SAFETY: all-zero is a valid baseline for the C POD type.
    let mut spec: ffi::IndexSpec = unsafe { std::mem::zeroed() };
    spec.flags = 0x80000;
    spec.fields = fields.as_mut_ptr();
    spec.numFields = fields.len().try_into().unwrap();
    spec
}

fn text_field(options: u32) -> ffi::FieldSpec {
    // SAFETY: all-zero is a valid baseline for the C POD type.
    let mut field: ffi::FieldSpec = unsafe { std::mem::zeroed() };
    field.set_types(1);
    field.set_options(options);
    field.ftId = 0;
    field
}

fn validate(
    node: &MockQueryNode,
    spec: &ffi::IndexSpec,
    opts: &mut ffi::RSSearchOptions,
    status: &mut QueryError,
    flags: QASTValidationFlagsSet,
) -> bool {
    // SAFETY: the test-owned node and spec remain live for the call.
    let root = unsafe { QueryNodeRef::new(node.as_non_null()) };
    let spec = unsafe { IndexSpec::from_raw(ptr::from_ref(spec)) };
    check_is_valid(root, spec, opts, status, flags)
}

#[test]
fn explicit_weight_is_rejected_before_node_validation() {
    let mut fields = [text_field(0)];
    let spec = spec_with_fields(&mut fields);
    let mut node = MockQueryNode::new(QueryNodeType::Token);
    node.opts_mut().explicit_weight = true;
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(!validate(
        &node,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlags::NoWeight.into()
    ));
    assert_eq!(status.code(), QueryErrorCode::WeightNotAllowed);
}

#[test]
fn hybrid_main_vector_is_exempt_from_vector_and_weight_restrictions() {
    let mut fields = [text_field(0)];
    let spec = spec_with_fields(&mut fields);
    let mut node = MockQueryNode::new(QueryNodeType::Vector);
    node.opts_mut().explicit_weight = true;
    node.opts_mut().flags |= QueryNodeFlags::HybridVectorSubqueryNode;
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();
    let flags =
        QASTValidationFlagsSet::from(QASTValidationFlags::NoWeight) | QASTValidationFlags::NoVector;

    assert!(validate(&node, &spec, &mut opts, &mut status, flags));
    assert_eq!(status.code(), QueryErrorCode::Ok);
}

#[test]
fn empty_token_sets_error_but_validation_returns_success() {
    let mut fields = [text_field(0)];
    let spec = spec_with_fields(&mut fields);
    let mut node = MockQueryNode::with_token(TokenNodeType::Token, b"");
    node.opts_mut().field_mask = 1;
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(validate(
        &node,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlagsSet::empty()
    ));
    assert_eq!(status.code(), QueryErrorCode::Syntax);
}

#[test]
fn tag_context_mutates_options_and_accepts_indexed_empty_token() {
    let mut fields = [text_field(0x100)];
    let spec = spec_with_fields(&mut fields);
    let mut child = MockQueryNode::with_token(TokenNodeType::Token, b"");
    child.opts_mut().field_mask = 1;
    let mut tag = MockQueryNode::new(QueryNodeType::Tag);
    tag.set_tag_field(ptr::from_ref(&fields[0]));
    tag.set_children(&[child.as_ptr()]);
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(validate(
        &tag,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlagsSet::empty()
    ));
    assert_ne!(opts.flags & QueryNodeFlags::IsTag as u32, 0);
    assert_ne!(opts.flags & QueryNodeFlags::IndexesEmpty as u32, 0);
    assert_eq!(status.code(), QueryErrorCode::Ok);
}
