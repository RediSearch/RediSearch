/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Expiration checking strategies for inverted index iterators.
//!
//! This module provides different strategies for checking if documents have expired.

use std::ptr::NonNull;

use ffi::{IndexFlags_Index_WideSchema, RedisSearchCtx};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
use rqe_core::RS_INVALID_FIELD_INDEX;

/// Trait for checking if a document has expired.
///
/// This trait allows different expiration checking strategies to be used
/// with the inverted index iterator.
pub trait ExpirationChecker {
    /// Returns `true` if expiration checking is enabled for this checker.
    ///
    /// This is used to determine whether to use the fast path (no expiration checks)
    /// or the slow path (with expiration checks) in the iterator.
    fn has_expiration(&self) -> bool;

    /// Returns `true` if the document in the result is expired.
    fn is_expired(&self, result: &RSIndexResult) -> bool;
}

/// A no-op expiration checker that never considers documents expired.
///
/// This is a zero-sized type that can be used when expiration checking is not needed.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpChecker;

impl ExpirationChecker for NoOpChecker {
    #[inline(always)]
    fn has_expiration(&self) -> bool {
        false
    }

    #[inline(always)]
    fn is_expired(&self, _result: &RSIndexResult) -> bool {
        false
    }
}

/// Field-level expiration checker using TTL table.
///
/// This checker uses the in-memory TTL table to determine if a document's field has expired.
pub struct FieldExpirationChecker {
    /// The search context used to check for expiration.
    sctx: NonNull<RedisSearchCtx>,
    /// The context for the field/s filter, used to determine if the field/s is/are expired.
    filter_ctx: FieldFilterContext,
    /// Whether the inverted index uses wide schema (more than 32 fields).
    /// Derived from the reader flags at construction time.
    is_wide_schema: bool,
}

impl FieldExpirationChecker {
    /// Creates a new field-level expiration checker.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// 1. `sctx` is a valid pointer to a [`RedisSearchCtx`].
    /// 2. `sctx.spec` is a valid pointer to an [`IndexSpec`](ffi::IndexSpec).
    /// 3. Both pointers remain valid for the lifetime of this checker.
    pub const unsafe fn new(
        sctx: NonNull<RedisSearchCtx>,
        filter_ctx: FieldFilterContext,
        reader_flags: ffi::IndexFlags,
    ) -> Self {
        let is_wide_schema = (reader_flags & IndexFlags_Index_WideSchema) != 0;
        Self {
            sctx,
            filter_ctx,
            is_wide_schema,
        }
    }
}

impl ExpirationChecker for FieldExpirationChecker {
    fn has_expiration(&self) -> bool {
        // SAFETY: Guaranteed by the safety contract of `new`.
        let sctx = unsafe { self.sctx.as_ref() };
        // SAFETY: Guaranteed by the safety contract of `new`.
        let spec = unsafe { *(sctx.spec) };

        // The TTL table holds field-level (HEXPIRE) entries only and is
        // destroyed once the last one leaves the index, so a NULL `ttl`
        // pointer is a sufficient and tight gate by itself: it means no doc
        // in this spec currently has a field-level expiration.
        if spec.docs.ttl.is_null() {
            return false;
        }

        // Check if the specific field/fieldMask has expiration
        // For masks, expiration is always enabled
        // For indices, expiration is only enabled if the index is valid
        match self.filter_ctx.field {
            FieldMaskOrIndex::Mask(_) => true,
            FieldMaskOrIndex::Index(index) => index != RS_INVALID_FIELD_INDEX,
        }
    }

    fn is_expired(&self, result: &RSIndexResult) -> bool {
        // SAFETY: Guaranteed by the safety contract of `new`.
        let sctx = unsafe { self.sctx.as_ref() };
        // SAFETY: Guaranteed by the safety contract of `new`.
        let spec = unsafe { *(sctx.spec) };

        // The TTL table may transition from non-NULL to NULL mid-query when the
        // last HFE doc is removed via `DocTable_Pop` (with the spec lock briefly
        // released around an iterator yield). The InvIndIterator caches its
        // `read_impl`/`skip_to_impl` function pointers at construction based on
        // `has_expiration()` and does not refresh them in `revalidate()`, so we
        // can be entered here with a now-NULL `ttl`. Mirror the C-side guard in
        // `DocTable_CheckFieldExpirationPredicate` and treat that as "not
        // expired" — passing NULL into the FFI verifier would deref it.
        if spec.docs.ttl.is_null() {
            return false;
        }

        // The reader set this bit from the block's per-entry expiration bitset: it is
        // set iff one of the fields this entry belongs to has a field-level expiration
        // for this document.
        //
        // We only short-circuit on it for the `Default` predicate. There, a clear bit
        // means none of the entry's fields can be expired, and a query's matched
        // fields are always a subset of the entry's fields, so `Verify*` would report
        // the document as valid (not expired) — we skip the TTL-table lookup entirely.
        // This removes the per-record probe for entries whose fields never expire (the
        // common case once any document in the index uses HFE).
        //
        // The `Missing` predicate (`ismissing`) is excluded: it reads a per-field
        // missing-docs index whose postings do not carry this bit, and its `Verify`
        // semantics differ for documents that *do* have a TTL entry (a clear bit would
        // not imply "valid"), so it must always run the full check.
        if self.filter_ctx.predicate == FieldExpirationPredicate::Default
            && !result.has_field_expiration
        {
            return false;
        }

        let current_time = &sctx.time.current as *const _;
        let doc_id = result.doc_id;

        match self.filter_ctx.field {
            // SAFETY:
            // - The safety contract of `new` guarantees that the ttl pointer is valid.
            // - We just allocated `current_time` on the stack so its pointer is valid.
            FieldMaskOrIndex::Index(index) => unsafe {
                !ffi::TimeToLiveTable_VerifyDocAndField(
                    spec.docs.ttl,
                    doc_id,
                    index,
                    self.filter_ctx.predicate.as_u32(),
                    current_time,
                )
            },
            FieldMaskOrIndex::Mask(mask) if !self.is_wide_schema => {
                // SAFETY:
                // - The safety contract of `new` guarantees that the ttl pointer is valid.
                // - We just allocated `current_time` on the stack so its pointer is valid.
                unsafe {
                    !ffi::TimeToLiveTable_VerifyDocAndFieldMask(
                        spec.docs.ttl,
                        doc_id,
                        (result.field_mask & mask) as u32,
                        self.filter_ctx.predicate.as_u32(),
                        current_time,
                        spec.fieldIdToIndex,
                    )
                }
            }
            FieldMaskOrIndex::Mask(mask) => {
                // wide mask
                // SAFETY:
                // - The safety contract of `new` guarantees that the ttl pointer is valid.
                // - We just allocated `current_time` on the stack so its pointer is valid.
                unsafe {
                    !ffi::TimeToLiveTable_VerifyDocAndWideFieldMask(
                        spec.docs.ttl,
                        doc_id,
                        result.field_mask & mask,
                        self.filter_ctx.predicate.as_u32(),
                        current_time,
                        spec.fieldIdToIndex,
                    )
                }
            }
        }
    }
}
