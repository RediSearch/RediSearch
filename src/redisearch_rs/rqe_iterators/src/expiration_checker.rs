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

use ffi::{IndexFlags_Index_WideSchema, RS_INVALID_FIELD_INDEX, RedisSearchCtx, t_docId};
use field::{FieldFilterContext, FieldMaskOrIndex};
use inverted_index::RSIndexResult;

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

    /// Returns `true` if the document with the given ID and result is expired.
    fn is_expired(&self, doc_id: t_docId, result: &RSIndexResult) -> bool;
}

/// A no-op expiration checker that never considers documents expired.
///
/// This is a zero-sized type that can be used when expiration checking is not needed.
#[derive(Debug, Clone, Copy)]
pub struct NoOpChecker;

impl ExpirationChecker for NoOpChecker {
    #[inline(always)]
    fn has_expiration(&self) -> bool {
        false
    }

    #[inline(always)]
    fn is_expired(&self, _doc_id: t_docId, _result: &RSIndexResult) -> bool {
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
    /// 1. `sctx` is a valid pointer to a `RedisSearchCtx`.
    /// 2. `sctx.spec` is a valid pointer to an `IndexSpec`.
    /// 3. 1 and 2 must stay valid during the checker's lifetime.
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

        // Check if TTL is configured and field expiration monitoring is enabled
        if spec.docs.ttl.is_null() || !spec.monitorFieldExpiration {
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

    fn is_expired(&self, doc_id: t_docId, result: &RSIndexResult) -> bool {
        // `has_expiration()` should have been checked before calling this method.
        // If TTL is not configured, the iterator should use the fast path without expiration checks.
        debug_assert!(
            self.has_expiration(),
            "is_expired() should not be called when has_expiration() returns false"
        );

        // SAFETY: Guaranteed by the safety contract of `new`.
        let sctx = unsafe { self.sctx.as_ref() };
        // SAFETY: Guaranteed by the safety contract of `new`.
        let spec = unsafe { *(sctx.spec) };

        let current_time = &sctx.time.current as *const _;

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
