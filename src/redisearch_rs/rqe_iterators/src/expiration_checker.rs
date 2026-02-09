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
//! This module provides different strategies for checking if documents have expired,
//! allowing the inverted index iterator to work with both memory-based and disk-based
//! storage systems.

use std::ptr::NonNull;

use ffi::{IndexFlags_Index_WideSchema, RedisSearchCtx, t_docId};
use field::{FieldFilterContext, FieldMaskOrIndex};
use inverted_index::RSIndexResult;

/// Trait for checking if a document has expired.
///
/// This trait allows different expiration checking strategies to be used
/// with the inverted index iterator, such as memory-based or disk-based expiration.
pub trait ExpirationChecker {
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
    /// Whether the index uses wide schema (more than 32 fields).
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
    pub unsafe fn new(sctx: NonNull<RedisSearchCtx>, filter_ctx: FieldFilterContext) -> Self {
        // SAFETY: Guaranteed by the caller's safety contract.
        let spec = unsafe { *sctx.as_ref().spec };
        let is_wide_schema = (spec.flags & IndexFlags_Index_WideSchema) != 0;
        Self {
            sctx,
            filter_ctx,
            is_wide_schema,
        }
    }
}

impl ExpirationChecker for FieldExpirationChecker {
    fn is_expired(&self, doc_id: t_docId, result: &RSIndexResult) -> bool {
        // SAFETY: Guaranteed by the safety contract of `new`.
        let sctx = unsafe { self.sctx.as_ref() };
        // SAFETY: Guaranteed by the safety contract of `new`.
        let spec = unsafe { *(sctx.spec) };

        // If TTL is not configured or field expiration monitoring is disabled, no documents are expired
        if spec.docs.ttl.is_null() || !spec.monitorFieldExpiration {
            return false;
        }

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

