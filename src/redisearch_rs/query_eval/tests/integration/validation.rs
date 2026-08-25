/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Contract tests for the pure Rust query-validation traversal.

use std::{
    ptr,
    sync::{Mutex, MutexGuard},
};

use index_spec::IndexSpec;
use inverted_index::NumericFilter;
use query::{
    QueryNodeRef,
    mock::{MockQueryNode, TokenNodeType},
};
use query_error::{QueryError, QueryErrorCode};
use query_eval::check_is_valid;
use query_types::{QASTValidationFlags, QASTValidationFlagsSet, QueryNodeFlags, QueryNodeType};

static VALIDATION_LOCK: Mutex<()> = Mutex::new(());

fn validation_lock() -> MutexGuard<'static, ()> {
    VALIDATION_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct FlexModeGuard(bool);

impl FlexModeGuard {
    fn enable() -> Self {
        // SAFETY: callers hold `VALIDATION_LOCK`, serialising all validation
        // tests that read the process-wide setting.
        let previous = unsafe { ffi::RSGlobalConfig.simulateInFlex };
        // SAFETY: protected by `VALIDATION_LOCK` until this guard is dropped.
        unsafe { ffi::RSGlobalConfig.simulateInFlex = true };
        Self(previous)
    }
}

impl Drop for FlexModeGuard {
    fn drop(&mut self) {
        // SAFETY: the owning test still holds `VALIDATION_LOCK` while guards
        // are dropped in reverse declaration order.
        unsafe { ffi::RSGlobalConfig.simulateInFlex = self.0 };
    }
}

fn spec_with_fields(fields: &mut [ffi::FieldSpec], rule: &mut ffi::SchemaRule) -> ffi::IndexSpec {
    // SAFETY: all-zero is a valid baseline for the C POD type.
    let mut spec: ffi::IndexSpec = unsafe { std::mem::zeroed() };
    spec.flags = 0x80000;
    spec.fields = fields.as_mut_ptr();
    spec.numFields = fields.len().try_into().unwrap();
    spec.rule = ptr::from_mut(rule);
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
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn explicit_weight_is_rejected_before_node_validation() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
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
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn hybrid_main_vector_is_exempt_from_vector_and_weight_restrictions() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
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
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn ordinary_vector_is_rejected_when_vectors_are_disallowed() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
    let node = MockQueryNode::new(QueryNodeType::Vector);
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(!validate(
        &node,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlags::NoVector.into()
    ));
    assert_eq!(status.code(), QueryErrorCode::VectorNotAllowed);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn hybrid_vector_exemption_does_not_propagate_to_nested_vector() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
    let child = MockQueryNode::new(QueryNodeType::Vector);
    let mut vector = MockQueryNode::new(QueryNodeType::Vector);
    vector.opts_mut().flags |= QueryNodeFlags::HybridVectorSubqueryNode;
    vector.set_children(&[child.as_ptr()]);
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(!validate(
        &vector,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlags::NoVector.into()
    ));
    assert_eq!(status.code(), QueryErrorCode::VectorNotAllowed);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn empty_token_sets_error_but_validation_returns_success() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
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
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn tag_context_mutates_options_and_accepts_indexed_empty_token() {
    let _guard = validation_lock();
    let mut fields = [text_field(0x100)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
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

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn ordinary_index_fast_path_skips_node_restrictions() {
    let _guard = validation_lock();
    let mut fields = [];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let mut spec = spec_with_fields(&mut fields, &mut rule);
    spec.flags = 0;
    let mut node = MockQueryNode::new(QueryNodeType::Token);
    node.opts_mut().explicit_weight = true;
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(validate(
        &node,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlags::NoWeight.into()
    ));
    assert_eq!(status.code(), QueryErrorCode::Ok);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn hybrid_exemption_does_not_propagate_to_children() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
    let mut child = MockQueryNode::new(QueryNodeType::Token);
    child.opts_mut().explicit_weight = true;
    let mut vector = MockQueryNode::new(QueryNodeType::Vector);
    vector.opts_mut().explicit_weight = true;
    vector.opts_mut().flags |= QueryNodeFlags::HybridVectorSubqueryNode;
    vector.set_children(&[child.as_ptr()]);
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();
    let flags =
        QASTValidationFlagsSet::from(QASTValidationFlags::NoWeight) | QASTValidationFlags::NoVector;

    assert!(!validate(&vector, &spec, &mut opts, &mut status, flags));
    assert_eq!(status.code(), QueryErrorCode::WeightNotAllowed);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn invalid_numeric_range_separates_public_and_private_details() {
    let _guard = validation_lock();
    let mut field = text_field(0);
    let field_name = b"pri\xffce\0";
    // SAFETY: the returned hidden strings own copies of the field name.
    field.fieldName =
        unsafe { ffi::NewHiddenString(field_name.as_ptr().cast(), field_name.len() - 1, true) };
    // SAFETY: the returned hidden strings own copies of the field name.
    field.fieldPath =
        unsafe { ffi::NewHiddenString(field_name.as_ptr().cast(), field_name.len() - 1, true) };
    let mut fields = [field];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
    let mut filter = NumericFilter {
        field_spec: ptr::from_ref(&fields[0]),
        min: 2.0,
        max: 1.0,
        ..NumericFilter::default()
    };
    let mut node = MockQueryNode::new(QueryNodeType::Numeric);
    node.set_numeric_filter(&mut filter);
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(!validate(
        &node,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlagsSet::empty()
    ));
    assert_eq!(status.code(), QueryErrorCode::Syntax);
    assert_eq!(
        status.public_message().unwrap().to_bytes(),
        b"Invalid numeric range (min > max)"
    );
    assert!(
        status
            .private_message()
            .unwrap()
            .to_bytes()
            .ends_with(b": @pri\xffce:[2.000000 1.000000]")
    );

    // SAFETY: both strings were allocated above and are no longer borrowed.
    unsafe {
        ffi::HiddenString_Free(fields[0].fieldName, true);
        ffi::HiddenString_Free(fields[0].fieldPath, true);
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn undefined_order_rejects_top_level_slop_for_selected_text_field() {
    let _guard = validation_lock();
    let mut field = text_field(0x80);
    let field_name = b"ti\xfftle\0";
    // SAFETY: the returned hidden strings own copies of the field name.
    field.fieldName =
        unsafe { ffi::NewHiddenString(field_name.as_ptr().cast(), field_name.len() - 1, true) };
    // SAFETY: the returned hidden strings own copies of the field name.
    field.fieldPath =
        unsafe { ffi::NewHiddenString(field_name.as_ptr().cast(), field_name.len() - 1, true) };
    let mut fields = [field];
    // SAFETY: all-zero is a valid baseline; setting `type_` to one selects JSON.
    let mut rule: ffi::SchemaRule = unsafe { std::mem::zeroed() };
    rule.type_ = document::DocumentType::Json;
    let mut spec = spec_with_fields(&mut fields, &mut rule);
    spec.flags |= 0x20000;
    let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
    phrase.opts_mut().field_mask = 1;
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts: ffi::RSSearchOptions = unsafe { std::mem::zeroed() };
    opts.slop = 1;
    let mut status = QueryError::default();

    assert!(!validate(
        &phrase,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlagsSet::empty()
    ));
    assert_eq!(status.code(), QueryErrorCode::BadOrderOption);
    assert_eq!(
        status.public_message().unwrap().to_bytes(),
        b"slop/inorder are not supported for field with undefined ordering"
    );
    assert!(
        status
            .private_message()
            .unwrap()
            .to_bytes()
            .ends_with(b" `ti\xfftle`")
    );

    // SAFETY: both strings were allocated above and are no longer borrowed.
    unsafe {
        ffi::HiddenString_Free(fields[0].fieldName, true);
        ffi::HiddenString_Free(fields[0].fieldPath, true);
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn empty_text_is_accepted_for_indexempty_and_non_text_masks() {
    let _guard = validation_lock();
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    for mut fields in [
        [text_field(0x100)],
        [{
            let mut field = text_field(0);
            field.set_types(2);
            field
        }],
    ] {
        let spec = spec_with_fields(&mut fields, &mut rule);
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
        assert_eq!(status.code(), QueryErrorCode::Ok);
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn null_field_tag_sets_context_and_still_validates_children() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
    let child = MockQueryNode::with_token(TokenNodeType::Token, b"");
    let mut tag = MockQueryNode::new(QueryNodeType::Tag);
    tag.set_tag_field(ptr::null());
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
    assert_eq!(opts.flags & QueryNodeFlags::IndexesEmpty as u32, 0);
    assert_eq!(status.code(), QueryErrorCode::Syntax);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn null_and_missing_nodes_do_not_recurse() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);

    for type_ in [QueryNodeType::Null, QueryNodeType::Missing] {
        let mut child = MockQueryNode::new(QueryNodeType::Token);
        child.opts_mut().explicit_weight = true;
        let mut parent = MockQueryNode::new(type_);
        parent.set_children(&[child.as_ptr()]);
        // SAFETY: all-zero is a valid search-options baseline.
        let mut opts = unsafe { std::mem::zeroed() };
        let mut status = QueryError::default();

        assert!(validate(
            &parent,
            &spec,
            &mut opts,
            &mut status,
            QASTValidationFlags::NoWeight.into()
        ));
        assert_eq!(status.code(), QueryErrorCode::Ok);
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (SearchDisk_IsEnabledForValidation)")]
fn traversal_stops_at_first_invalid_child() {
    let _guard = validation_lock();
    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
    let mut weighted = MockQueryNode::new(QueryNodeType::Token);
    weighted.opts_mut().explicit_weight = true;
    let mut filter = NumericFilter {
        field_spec: ptr::from_ref(&fields[0]),
        min: 2.0,
        max: 1.0,
        ..NumericFilter::default()
    };
    let mut numeric = MockQueryNode::new(QueryNodeType::Numeric);
    numeric.set_numeric_filter(&mut filter);
    let mut root = MockQueryNode::new(QueryNodeType::Phrase);
    root.set_children(&[weighted.as_ptr(), numeric.as_ptr()]);
    // SAFETY: all-zero is a valid search-options baseline.
    let mut opts = unsafe { std::mem::zeroed() };
    let mut status = QueryError::default();

    assert!(!validate(
        &root,
        &spec,
        &mut opts,
        &mut status,
        QASTValidationFlags::NoWeight.into()
    ));
    assert_eq!(status.code(), QueryErrorCode::WeightNotAllowed);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI and process-wide Flex configuration")]
fn flex_rejects_tag_prefix_and_wildcard_children() {
    let _guard = validation_lock();
    let _flex_mode = FlexModeGuard::enable();

    let mut fields = [text_field(0)];
    // SAFETY: all-zero selects the HASH document type and null optional fields.
    let mut rule = unsafe { std::mem::zeroed() };
    let spec = spec_with_fields(&mut fields, &mut rule);
    let mut prefix = MockQueryNode::with_token(TokenNodeType::Prefix, b"pre");
    prefix.set_prefix_mode(true, false);
    let wildcard = MockQueryNode::with_token(TokenNodeType::WildcardQuery, b"w*ld");
    for (child, expected) in [
        (
            prefix,
            b"TAG prefix/suffix/infix queries are not supported on Flex indexes".as_slice(),
        ),
        (
            wildcard,
            b"TAG wildcard queries are not supported on Flex indexes".as_slice(),
        ),
    ] {
        let mut tag = MockQueryNode::new(QueryNodeType::Tag);
        tag.set_children(&[child.as_ptr()]);
        // SAFETY: all-zero is a valid search-options baseline.
        let mut opts = unsafe { std::mem::zeroed() };
        let mut status = QueryError::default();

        assert!(!validate(
            &tag,
            &spec,
            &mut opts,
            &mut status,
            QASTValidationFlagsSet::empty()
        ));
        assert_eq!(status.code(), QueryErrorCode::FlexUnsupportedQuery);
        assert_eq!(status.public_message().unwrap().to_bytes(), expected);
    }
}
