/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Validation of parsed query ASTs before iterator construction.

use field_spec::FieldSpec;
use index_spec::IndexSpec;
use query::{QueryNode, QueryNodeRef};
use query_error::{QueryError, QueryErrorCode};
use query_types::{QASTValidationFlags, QASTValidationFlagsSet, QueryNodeFlags};
use rqe_core::RS_FIELDMASK_ALL;
use rs_token::RSTokenRef;
use std::io::Write;

/// Validate a parsed query AST rooted at `root` against `spec` and `opts`.
///
/// TAG traversal deliberately accumulates [`QueryNodeFlags::IsTag`] and
/// [`QueryNodeFlags::IndexesEmpty`] in the caller-owned search options.
/// An invalid empty token sets `status` while retaining a successful return,
/// preserving the historical validation contract.
pub fn check_is_valid(
    root: QueryNodeRef,
    spec: &IndexSpec,
    opts: &mut ffi::RSSearchOptions,
    status: &mut QueryError,
    validation_flags: QASTValidationFlagsSet,
) -> bool {
    if !search_disk::is_enabled_for_validation()
        && !spec.has_non_empty_fields()
        && (!spec.is_json() || !spec.has_undefined_order())
    {
        return true;
    }

    check_node(root, spec, opts, status, validation_flags)
}

fn check_node(
    node: QueryNodeRef,
    spec: &IndexSpec,
    opts: &mut ffi::RSSearchOptions,
    status: &mut QueryError,
    validation_flags: QASTValidationFlagsSet,
) -> bool {
    let mut effective_flags = validation_flags;
    if matches!(node.as_enum(), QueryNode::Vector { .. })
        && node
            .opts()
            .flags
            .contains(QueryNodeFlags::HybridVectorSubqueryNode)
    {
        effective_flags.remove(QASTValidationFlags::NoWeight);
        effective_flags.remove(QASTValidationFlags::NoVector);
    }

    if effective_flags.contains(QASTValidationFlags::NoWeight) && node.opts().explicit_weight {
        status.set_code(QueryErrorCode::WeightNotAllowed);
        return false;
    }

    let recurse = match node.as_enum() {
        QueryNode::Phrase { .. } => {
            if spec.is_json() && spec.has_undefined_order() {
                let at_top_level =
                    opts.slop >= 0 || opts.flags & ffi::RSSearchFlags_Search_InOrder != 0;
                if !check_allow_slop_and_inorder(&node, spec, at_top_level, status) {
                    return false;
                }
            }
            true
        }
        QueryNode::Null | QueryNode::Missing { .. } => false,
        QueryNode::Tag { fs } => {
            opts.flags |= QueryNodeFlags::IsTag as u32;
            if fs.is_some_and(|fs| {
                // SAFETY: `QueryNodeRef` guarantees a present TAG field pointer
                // is valid for the node borrow.
                let fs = unsafe { FieldSpec::from_raw(fs) };
                fs.indexes_empty()
            }) {
                opts.flags |= QueryNodeFlags::IndexesEmpty as u32;
            }
            for child in node.children() {
                let valid = match child.as_enum() {
                    QueryNode::Prefix { .. } => {
                        validate_query_not_disk("TAG prefix/suffix/infix", status)
                    }
                    QueryNode::WildcardQuery { .. } => {
                        validate_query_not_disk("TAG wildcard", status)
                    }
                    _ => true,
                };
                if !valid {
                    return false;
                }
            }
            true
        }
        QueryNode::Token { tok } => {
            if spec.has_non_empty_fields() {
                let _ = validate_token(tok, &node, spec, opts, status);
            }
            true
        }
        QueryNode::Numeric { nf } if nf.min > nf.max => {
            // SAFETY: a well-formed numeric node has a valid, non-null field
            // spec that lives with the AST's schema.
            let field = unsafe { FieldSpec::from_raw(nf.field_spec) };
            let mut user_data = Vec::new();
            write!(user_data, ": @").expect("writing to a Vec cannot fail");
            user_data.extend_from_slice(field.field_name().secret_value().to_bytes());
            write!(user_data, ":[{:.6} {:.6}]", nf.min, nf.max)
                .expect("writing to a Vec cannot fail");
            status.set_with_user_data(
                QueryErrorCode::Syntax,
                "Invalid numeric range (min > max)",
                user_data,
            );
            return false;
        }
        QueryNode::Vector { .. } if effective_flags.contains(QASTValidationFlags::NoVector) => {
            status.set_code(QueryErrorCode::VectorNotAllowed);
            return false;
        }
        _ => true,
    };

    if recurse {
        for child in node.children() {
            if !check_node(child, spec, opts, status, validation_flags) {
                return false;
            }
        }
    }
    true
}

fn check_allow_slop_and_inorder(
    node: &QueryNodeRef,
    spec: &IndexSpec,
    at_top_level: bool,
    status: &mut QueryError,
) -> bool {
    if !at_top_level
        && node.opts().max_slop < 0
        && !node
            .opts()
            .flags
            .contains(QueryNodeFlags::OverriddenInOrder)
    {
        return true;
    }

    let mask = node.opts().field_mask;
    if let Some(field) = spec.field_specs().iter().find(|field| {
        field.is_indexable_text() && mask & field.field_mask() != 0 && field.has_undefined_order()
    }) {
        let mut user_data = b" `".to_vec();
        user_data.extend_from_slice(field.field_name().secret_value().to_bytes());
        user_data.push(b'`');
        status.set_with_user_data(
            QueryErrorCode::BadOrderOption,
            "slop/inorder are not supported for field with undefined ordering",
            user_data,
        );
        return false;
    }
    true
}

fn does_index_empty(node: &QueryNodeRef, spec: &IndexSpec, opts: &ffi::RSSearchOptions) -> bool {
    if opts.flags & QueryNodeFlags::IsTag as u32 != 0 {
        return opts.flags & QueryNodeFlags::IndexesEmpty as u32 != 0;
    }

    let mask = node.opts().field_mask;
    if mask == RS_FIELDMASK_ALL {
        return true;
    }
    let mut fields = spec
        .field_specs()
        .iter()
        .filter(|field| field.is_indexable_text() && mask & field.field_mask() != 0)
        .peekable();
    fields.peek().is_none() || fields.any(FieldSpec::indexes_empty)
}

fn validate_token(
    token: RSTokenRef<'_>,
    node: &QueryNodeRef,
    spec: &IndexSpec,
    opts: &ffi::RSSearchOptions,
    status: &mut QueryError,
) -> bool {
    if token.as_bytes() == Some(&[]) && !does_index_empty(node, spec, opts) {
        status.set_error(
            QueryErrorCode::Syntax,
            "Use `INDEXEMPTY` in field creation in order to index and query for empty strings",
        );
        return false;
    }
    true
}

fn validate_query_not_disk(query_type: &str, status: &mut QueryError) -> bool {
    if search_disk::is_enabled_for_validation() {
        status.set_error(
            QueryErrorCode::FlexUnsupportedQuery,
            &format!("{query_type} queries are not supported on Flex indexes"),
        );
        return false;
    }
    true
}
