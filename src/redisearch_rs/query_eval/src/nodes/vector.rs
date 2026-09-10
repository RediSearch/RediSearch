/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_VECTOR` query nodes.
//!
//! Field and distance-field names are binary-safe, so everything here handles
//! them as bytes. Decoding one as UTF-8 would have to be lossy, and the loss is
//! observable: two names differing only outside UTF-8 would compare equal once
//! both had decoded to the same replacement characters.

use std::{
    ffi::{CStr, CString},
    ptr::NonNull,
};

use hidden_string::HiddenString;
use query::MetricRequestId;
use query_error::QueryErrorCode;
use query_types::QueryNodeFlags;
use rqe_iterators::{IteratorType, c2rust::CRQEIterator, metric};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut, eval_node};

/// `QN_VECTOR` — a KNN or range vector-similarity search, optionally filtered
/// by a child subquery (a *hybrid* query).
///
/// The similarity search itself stays in C: this resolves the field the
/// distance is yielded under, reserves the metric request that binds it to a
/// lookup key, evaluates the optional child, and asks
/// [`NewVectorIterator`](ffi::NewVectorIterator) for the iterator.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
    vq: *mut ffi::VectorQuery,
    config: Config,
) -> Option<Evaluated<'index>> {
    debug_assert!(!vq.is_null(), "a vector node must carry a vector query");
    // SAFETY: a well-formed vector node carries a valid, non-null
    // `VectorQuery`, exclusively owned for the duration of this evaluation
    // because `node` is held exclusively.
    let vq = unsafe { &mut *vq };

    resolve_score_field(ctx, &mut node, vq).ok()?;

    // Reserved before the child is evaluated, so that a nested vector node
    // lands *after* this one in the array. Paths out of here that never build
    // an iterator simply leave it unbound — see `add_metric_request`.
    let request_id = (!vq.scoreField.is_null()).then(|| {
        let is_internal = node
            .opts()
            .flags
            .contains(QueryNodeFlags::HideVectorDistanceField);
        // SAFETY: `scoreField` is non-null, per the guard above, and is the
        // NUL-terminated string the parser or the distance-field move left on
        // the query. The request borrows it rather than owning it, and its
        // owner is the vector query — hence the AST, which outlives the
        // metric-request array.
        unsafe { ctx.add_metric_request(vq.scoreField, is_internal) }
    });

    debug_assert!(
        node.num_children() <= 1,
        "a vector node has at most one (filter) child"
    );
    let child = if node.num_children() == 0 {
        std::ptr::null_mut()
    } else {
        // A child that yields nothing leaves the hybrid query with nothing to
        // filter, so the whole node yields nothing.
        eval_node(ctx, node.child_mut(0), config)?
            .into_c_iterator()
            .as_ptr()
    };

    // SAFETY: `ctx` wraps a valid, exclusively-held `QueryEvalCtx`, `vq` is a
    // valid `VectorQuery`, and `child` is either null or an owning iterator that
    // `NewVectorIterator` takes over on success.
    let it = unsafe { ffi::NewVectorIterator(ctx.as_non_null().as_ptr(), vq, child) };

    let Some(it) = NonNull::new(it) else {
        // `NewVectorIterator` does not take ownership of the child on its
        // failure paths, so the child is ours to release.
        if let Some(child) = NonNull::new(child) {
            // SAFETY: `child` is an owning `QueryIterator` handle with all its
            // callbacks populated, and nothing else holds it.
            drop(unsafe { CRQEIterator::new(child) });
        }
        return None;
    };

    if let Some(id) = request_id {
        bind_metric_request(ctx, id, it);
    }
    Some(Evaluated::C(it))
}

/// The query named the distance field twice.
///
/// Carries nothing: the error is reported into the query status where it is
/// detected, since that is where both names are in hand.
struct DuplicateDistanceField;

/// Reconcile the two syntaxes that can name the distance field, leaving the
/// winner on the vector query's `scoreField`.
///
/// KNN can name the field as `…=>[KNN … AS <f>]`, which the parser stores on
/// the vector query (defaulting to [`default_score_field`] when the user names
/// none), or as `…=>{$YIELD_DISTANCE_AS:<f>}`, which it stores on the node's
/// options. When both are set and the stored one is not the default, the user
/// really did name the field twice and the query is rejected. Otherwise the
/// node's name wins and is *moved* onto the query.
///
/// On `Err` the AST is left exactly as it was — both names still owned where
/// they were — and the error is already reported into `ctx`'s status.
fn resolve_score_field(
    ctx: &mut QueryEvalContext,
    node: &mut QueryNodeMut<'_>,
    vq: &mut ffi::VectorQuery,
) -> Result<(), DuplicateDistanceField> {
    if node.opts().dist_field.is_null() {
        return Ok(());
    }

    if !vq.scoreField.is_null() {
        debug_assert!(!vq.field.is_null(), "a vector query must have a field spec");
        // SAFETY: a well-formed vector query points at a valid `FieldSpec`
        // whose `fieldName` is a valid `HiddenString`.
        let field = unsafe { &*vq.field };
        let default = default_score_field(field);
        // SAFETY: the parser only ever stores a live, NUL-terminated string here.
        let score_field = unsafe { CStr::from_ptr(vq.scoreField) };

        if !score_field
            .to_bytes()
            .eq_ignore_ascii_case(default.to_bytes())
        {
            // SAFETY: as above, for the node's own name.
            let dist_field = unsafe { CStr::from_ptr(node.opts().dist_field) };
            report_duplicate(ctx, score_field, dist_field);
            return Err(DuplicateDistanceField);
        }

        // The stored name is the one the parser generated, so the user named
        // the field exactly once: the explicit name replaces it.
        //
        // SAFETY: this Redis API function pointer is set once during module
        // load and never mutated afterwards, so reading it during query
        // evaluation cannot race.
        let free = unsafe { redis_module::RedisModule_Free }.expect("RedisModule_Free unset");
        // SAFETY: `scoreField` is a live string owned by the vector query,
        // allocated with the module allocator and freed exactly once here; the
        // move below overwrites the dangling pointer before anything reads it.
        unsafe { free(vq.scoreField.cast()) };
    }

    vq.scoreField = node.take_dist_field();
    Ok(())
}

/// Report the duplicate-distance-field error into the query status.
fn report_duplicate(ctx: &mut QueryEvalContext, score_field: &CStr, dist_field: &CStr) {
    // Carries no user data, so it is safe to show even under obfuscation.
    let public = c"Distance field was specified twice for vector query";

    // Unlike `set_with_user_data`, `set_code_and_messages` prepends no error
    // code prefix of its own, so the private message is assembled here around
    // `prefix_c_str` — the single source of that prefix.
    let mut private = QueryErrorCode::DupField.prefix_c_str().to_bytes().to_vec();
    private.extend_from_slice(public.to_bytes());
    private.extend_from_slice(b": ");
    private.extend_from_slice(score_field.to_bytes());
    private.extend_from_slice(b" and ");
    private.extend_from_slice(dist_field.to_bytes());

    ctx.status().set_code_and_messages(
        QueryErrorCode::DupField,
        Some(public.to_owned()),
        Some(CString::new(private).expect("no interior NUL: every part is a CStr body")),
    );
}

/// The name a vector query on `field` yields its distance under when the user
/// names none: `__<field>_score`.
///
/// The name is read with [`HiddenString::secret_value`], so an interior NUL
/// truncates it — matching the `%s` formatting this reproduces rather than the
/// length-driven `VectorQuery_GetDefaultScoreFieldName`, which keeps it.
fn default_score_field(field: &ffi::FieldSpec) -> CString {
    debug_assert!(
        !field.fieldName.is_null(),
        "a field spec must carry a field name"
    );
    // SAFETY: a well-formed field spec's `fieldName` is a valid, non-null
    // `HiddenString` that is not mutated for the duration of the borrow.
    let name = unsafe { HiddenString::from_raw(field.fieldName) };

    let name = name.secret_value().to_bytes();
    let mut default = Vec::with_capacity(name.len() + b"___score".len());
    default.extend_from_slice(b"__");
    default.extend_from_slice(name);
    default.extend_from_slice(b"_score");

    CString::new(default).expect("no interior NUL: the name is read up to its first one")
}

/// Point the metric request `id` reserved at the freshly built iterator's
/// own-key slot, and give the iterator the back-reference it uses to invalidate
/// the handle when it is freed.
///
/// A no-op for iterator types that yield no metric — the range branch can
/// return a plain empty iterator, for instance — which drops `id` and leaves
/// the request unbound.
fn bind_metric_request(
    ctx: &mut QueryEvalContext,
    id: MetricRequestId,
    it: NonNull<ffi::QueryIterator>,
) {
    // SAFETY: `it` is a valid iterator freshly returned by `NewVectorIterator`.
    let iterator_type = unsafe { it.as_ref() }.type_;

    // Neither accessor can infer the key's borrow — one comes from C, the other
    // from a type-erased header — and `bind_metric_request_key` leaves it free
    // on both sides too, so nothing constrains the lifetime inferred here. It is
    // discharged by discarding it: the key leaves this function only as a raw
    // pointer, handed straight back to the iterator that owns the slot, so no
    // `RLookupKey` reference — nor any borrow its safe accessors hand out — is
    // ever formed at that lifetime.
    match iterator_type {
        IteratorType::Hybrid => {
            // SAFETY: the discriminant says this is a hybrid iterator, and we
            // hold it exclusively.
            //
            // The cast is C's view of the key — its header — widened back to
            // the whole key the header is the first field of.
            let own_key = unsafe { ffi::HybridIterator_GetOwnKeyRef(it.as_ptr()) }.cast();
            // SAFETY: `id` was reserved by `add_metric_request` on this same
            // context; `own_key` points into the iterator, which outlives the
            // handle (it clears the handle's validity flag when freed).
            let handle = unsafe { ctx.bind_metric_request_key(id, own_key) };
            // SAFETY: as above, and `handle` is a valid handle that lives as
            // long as the AST. The cast is the same handle as C declares it.
            unsafe { ffi::HybridIterator_SetKeyHandle(it.as_ptr(), handle.cast()) };
        }
        IteratorType::MetricSortedById
        | IteratorType::MetricSortedByScore
        | IteratorType::MetricLazySortedById
        | IteratorType::MetricLazySortedByScore => {
            // SAFETY: the discriminant says this is one of the metric
            // iterators, built by a metric constructor, and we hold it
            // exclusively; the borrow it is typed with is discarded rather
            // than relied upon, as above.
            let own_key = unsafe { metric::own_key_ref(it) };
            // SAFETY: as above.
            let handle = unsafe { ctx.bind_metric_request_key(id, own_key.as_ptr()) };
            // SAFETY: as above.
            unsafe { metric::set_key_handle(it, NonNull::new(handle)) };
        }
        // Every other type yields no metric to bind.
        _ => {}
    }
}
