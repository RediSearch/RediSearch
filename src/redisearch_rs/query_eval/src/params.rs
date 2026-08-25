/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Query-AST parameter resolution.

use query::QueryNodeMut;
use query_types::QueryNodeType;

/// Parameter evaluation failed after the retained C resolver populated the
/// supplied [`ffi::QueryError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParamEvaluationError;

/// Resolve parameters on `node` and then recursively resolve its children.
///
/// Earlier mutations are retained if a later parameter or child fails.
///
/// # Safety
///
/// `params` must be a valid parameter dictionary for every unresolved parameter
/// reachable from `node`; `status` must be valid for writes by the retained C
/// resolvers; and all parameter targets and vector payload pointers reachable
/// from `node` must remain valid and writable for the call. The subtree, its
/// parameter arrays, and every resolver target or vector payload allocation that
/// may be written must be exclusively borrowed.
pub unsafe fn eval_params(
    params: *mut ffi::dict,
    mut node: QueryNodeMut<'_>,
    dialect_version: u32,
    status: *mut ffi::QueryError,
) -> Result<(), ParamEvaluationError> {
    match node.node_type() {
        QueryNodeType::Vector => {
            // SAFETY: the caller guarantees the dictionary, status, whole node,
            // parameter targets, and vector payload satisfy the C resolver's
            // requirements, and no references into the node are live here.
            let result = unsafe {
                ffi::VectorQuery_EvalParams(
                    params,
                    node.as_non_null().as_ptr(),
                    dialect_version,
                    status,
                )
            };
            if result != redis_module::REDISMODULE_OK as i32 {
                return Err(ParamEvaluationError);
            }
        }
        QueryNodeType::Geo
        | QueryNodeType::Token
        | QueryNodeType::Numeric
        | QueryNodeType::Tag
        | QueryNodeType::Phrase
        | QueryNodeType::Not
        | QueryNodeType::Prefix
        | QueryNodeType::Fuzzy
        | QueryNodeType::Optional
        | QueryNodeType::Ids
        | QueryNodeType::Wildcard
        | QueryNodeType::WildcardQuery
        | QueryNodeType::Geometry => {
            // SAFETY: the caller carries the dictionary, status, target, and
            // exclusivity requirements through this local resolution step.
            unsafe { eval_params_common(params, &mut node, dialect_version, status) }?;
        }
        QueryNodeType::Union => debug_assert!(node.params_mut().is_empty()),
        QueryNodeType::Null | QueryNodeType::Missing => return Ok(()),
        QueryNodeType::Max => unreachable!("Max is a sentinel, not a real node type"),
    }

    for index in 0..node.num_children() {
        let child = node.child_mut(index);
        // SAFETY: `child` is an exclusive reborrow of the caller-provided
        // subtree, and all remaining resolver preconditions are unchanged.
        unsafe { eval_params(params, child, dialect_version, status) }?;
    }
    Ok(())
}

/// Resolve the parameters attached directly to `node`.
///
/// A term parameter resolving to a numeric value marks the node verbatim.
///
/// # Safety
///
/// `params` must be a valid parameter dictionary for every unresolved parameter
/// on `node`; `status` must be valid for writes by [`ffi::QueryParam_Resolve`];
/// and every target referenced by the node's [`ffi::Param`] array must remain
/// valid and writable for the call. `node`, its parameter array, and every
/// resolver target that may be written must be exclusively borrowed.
pub unsafe fn eval_params_common(
    params: *mut ffi::dict,
    node: &mut QueryNodeMut<'_>,
    dialect_version: u32,
    status: *mut ffi::QueryError,
) -> Result<(), ParamEvaluationError> {
    let count = node.params_mut().len();
    for index in 0..count {
        let result = {
            let param = &raw mut node.params_mut()[index];
            // SAFETY: the caller guarantees that `param`, its target, `params`,
            // and `status` satisfy the retained resolver's requirements.
            unsafe { ffi::QueryParam_Resolve(param, params, dialect_version, status) }
        };
        match result {
            result if result < 0 => return Err(ParamEvaluationError),
            2 => node.set_verbatim(),
            _ => {}
        }
    }
    Ok(())
}
