/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for a query's [`MetricRequests`], which C holds only as the
//! opaque `QueryAST.metricRequests` pointer.

use std::ffi::{CStr, CString, c_int};

use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use rlookup::{MetricKeyError, MetricRequests, OpaqueRLookup, RLookup};

/// Free a query's metric-request list, together with the key handles it owns.
///
/// # Safety
///
/// 1. `requests` must be null — the query reserved no metric — or a list that
///    query evaluation leaked with [`Box::into_raw`], not freed since.
/// 2. Freeing the list frees its key handles, so precondition (2) of
///    [`MetricRequests::bind`] must hold for each of them.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MetricRequests_Free(requests: *mut MetricRequests<'_>) {
    if !requests.is_null() {
        // SAFETY: ensured by caller (1.)
        drop(unsafe { Box::from_raw(requests) });
    }
}

/// Register the metrics of `requests` as keys of `lookup`, as
/// [`MetricRequests::register_keys`] does, checking each name against the
/// fields of `spec`.
///
/// Returns `REDISMODULE_OK`, or `REDISMODULE_ERR` with `status` set if a metric
/// is named after a schema field or after a key `lookup` already has.
///
/// # Safety
///
/// 1. `requests` must be a [valid], non-null pointer to a list that query
///    evaluation built, not mutated for the duration of the call. The metric
///    names it borrows, owned by the query AST, must still be live.
/// 2. `spec` must be a [valid], non-null pointer to an [`IndexSpec`](ffi::IndexSpec).
/// 3. `lookup` must be a [valid], non-null pointer to an [`RLookup`] that does
///    not outlive the metric names: its new keys borrow them from the query
///    AST rather than copying them.
/// 4. `status` must be a [valid], non-null pointer to a [`QueryError`].
/// 5. The preconditions of [`MetricRequests::bind`] must still hold for every
///    key handle of `requests`, with `lookup` among the lookups its
///    precondition (3) names.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MetricRequests_RegisterKeys(
    requests: *const MetricRequests<'_>,
    spec: *const ffi::IndexSpec,
    lookup: *mut OpaqueRLookup,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let requests = unsafe { requests.as_ref() }.expect("`requests` must not be null");
    assert!(!spec.is_null(), "`spec` must not be null");
    // SAFETY: ensured by caller (3.)
    let lookup =
        unsafe { RLookup::from_opaque_mut_ptr(lookup) }.expect("`lookup` must not be null");
    #[cfg(debug_assertions)]
    lookup.assert_valid("MetricRequests_RegisterKeys");
    // SAFETY: ensured by caller (4.)
    let status =
        unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("`status` must not be null");

    let is_schema_field = |name: &CStr| {
        // SAFETY: ensured by caller (2.), and `name` is readable for its
        // `count_bytes()` bytes.
        let field =
            unsafe { ffi::IndexSpec_GetFieldWithLength(spec, name.as_ptr(), name.count_bytes()) };
        !field.is_null()
    };
    let (code, name, detail) = match requests.register_keys(lookup, is_schema_field) {
        Ok(()) => return redis_module::REDISMODULE_OK as c_int,
        Err(MetricKeyError::InSchema(name)) => (
            QueryErrorCode::IndexExists,
            name,
            "` already exists in schema",
        ),
        Err(MetricKeyError::Duplicate(name)) => {
            (QueryErrorCode::DupField, name, "` specified more than once")
        }
    };
    set_property_error(status, code, name, detail);
    redis_module::REDISMODULE_ERR as c_int
}

/// Report a metric `name` refused for `detail`, the way
/// `QueryError_SetWithUserDataFmt` would: the name is user data, so it appears
/// only in the private message.
///
/// The private message is assembled from bytes rather than formatted, so a name
/// that is not UTF-8 still reaches the client unaltered.
fn set_property_error(status: &mut QueryError, code: QueryErrorCode, name: &CStr, detail: &str) {
    const PUBLIC: &CStr = c"Property";

    let mut private = code.prefix_c_str().to_bytes().to_vec();
    private.extend_from_slice(PUBLIC.to_bytes());
    private.extend_from_slice(b" `");
    private.extend_from_slice(name.to_bytes());
    private.extend_from_slice(detail.as_bytes());

    status.set_code_and_messages(
        code,
        Some(PUBLIC.to_owned()),
        Some(CString::new(private).expect("no interior NUL: every part is a C string body")),
    );
}
