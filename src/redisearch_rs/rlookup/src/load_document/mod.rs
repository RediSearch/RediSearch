/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(dead_code, reason = "used by later PRs")]

use std::ffi::CStr;
use std::ops::Deref;
use std::ptr;
use std::ptr::NonNull;

use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use document_metadata::DocumentMetadata;
use query_error::QueryErrorCode;
use redis_module::{RedisString, key::KeyFlags};
use sorting_vector::RSSortingVector;

const UNDERSCORE_KEY: &CStr = c"__key";

/// Equivalent to `DOCUMENT_OPEN_KEY_QUERY_FLAGS` from `document.h`.
///
/// FIXME: `ACCESS_TRIMMED` (bit 22) is hardcoded because the `redis-module` crate
/// does not expose it yet. This MUST be fixed by upstreaming the flag to
/// `redismodule-rs` (`KeyFlags::ACCESS_TRIMMED`) and then replacing the raw bit
/// here.
const DOCUMENT_OPEN_KEY_QUERY_FLAGS: KeyFlags = KeyFlags::from_bits_retain(
    KeyFlags::NOEFFECTS.bits()
        | KeyFlags::NOEXPIRE.bits()
        | KeyFlags::ACCESS_EXPIRED.bits()
        | (1 << 22), // ACCESS_TRIMMED
);

#[derive(Debug, thiserror::Error)]
pub enum LoadFieldError {
    /// `RedisModule_OpenKey` / `japi->openKeyWithFlags` returned NULL.
    #[error("document key does not exist")]
    KeyNotFound,

    /// Key exists but is not a hash.
    #[error("document key has the wrong type")]
    WrongHashKeyType,
}

impl LoadFieldError {
    pub const fn to_query_error_code(&self) -> QueryErrorCode {
        match self {
            Self::KeyNotFound => QueryErrorCode::NoDoc,
            Self::WrongHashKeyType => QueryErrorCode::RedisKeyType,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadAllError {
    /// `RedisModule_OpenKey` / `japi->openKeyWithFlags` returned NULL.
    #[error("document key is missing or has the wrong type")]
    KeyNotFound,
}

pub struct DocumentLoader<'env, 'a, F: DocumentFormat> {
    rlookup: &'env mut RLookup<'a>,
    dst_row: &'env mut RLookupRow<'a>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    force_load: bool,
    dmd: &'a DocumentMetadata,
    format: F,
}

/// Format-specific document-loading logic.
///
/// This abstracts away the details of hash vs json documents.
pub trait DocumentFormat {
    /// A handle to a single opened document.
    type Document<'key>: OpenDocument
    where
        Self: 'key;

    /// Open a specific document for per-field loading.
    fn open<'key>(
        &'key self,
        key_name: &'key RedisString,
    ) -> Result<Self::Document<'key>, LoadFieldError>;

    /// Bulk-load all fields at once (HGETALL / JSON root scan).
    ///
    /// This is an alternative to `open` + per-field `load_field`, used when
    /// the caller needs every field (e.g. `FT.SEARCH` with no `RETURN`).
    /// Needs `&mut RLookup` because it may create new keys on the fly.
    fn load_all(
        &self,
        rlookup: &mut RLookup,
        dst_row: &mut RLookupRow,
        key_name: &RedisString,
    ) -> Result<(), LoadAllError>;
}

/// Handle to an opened document. Load individual fields from it.
pub trait OpenDocument {
    fn load_field(&self, kk: &RLookupKey, dst_row: &mut RLookupRow) -> Result<(), LoadFieldError>;
}

impl<'env, 'a, F: DocumentFormat> DocumentLoader<'env, 'a, F> {
    pub fn new(
        rlookup: &'env mut RLookup<'a>,
        dst_row: &'env mut RLookupRow<'a>,
        ctx: NonNull<ffi::RedisModuleCtx>,
        dmd: &'a DocumentMetadata,
        format: F,
    ) -> Self {
        // SAFETY: `ffi::RSSortingVector` and `RSSortingVector` share a layout.
        let sorting_vector = unsafe {
            ptr::from_ref(&dmd.sortVector)
                .cast::<RSSortingVector>()
                .as_ref()
                .unwrap()
        };
        dst_row.set_sorting_vector(Some(sorting_vector));

        Self {
            rlookup,
            dst_row,
            ctx,
            force_load: false,
            dmd,
            format,
        }
    }

    pub const fn force_load(mut self, force_load: bool) -> Self {
        self.force_load = force_load;
        self
    }

    /// Load the given `keys` from the document into `dst_row`.
    ///
    /// By default keys that are already cached are skipped, this can be overridden by setting `Self::force_load`.
    pub fn load_specific<'k, I>(self, keys: I) -> Result<(), LoadFieldError>
    where
        I: IntoIterator,
        I::Item: Deref<Target = RLookupKey<'k>>,
    {
        load_specific_keys(
            &self.format,
            self.dst_row,
            &self.dmd.key_name(Some(self.ctx)),
            keys,
            self.force_load,
        )
    }

    /// Load all keys from the document into `dst_row`.
    ///
    /// This is an optimized version equivalent to `Self::load_specific(rlookup.iter())` and should
    /// be preferred if you need to load all keys in an rlookup.
    ///
    /// `Self::force_load` has **no effect** on this method, all keys are always loaded anyways.
    pub fn load_all(self) -> Result<(), LoadAllError> {
        self.format.load_all(
            self.rlookup,
            self.dst_row,
            &self.dmd.key_name(Some(self.ctx)),
        )
    }
}

/// Load specific fields from a document, lazily opening the key handle on first
/// field that actually needs loading.
///
/// Mirrors the C `loadIndividualKeys` pattern: open the key handle once
/// (lazily, on first field that actually needs loading) and reuse it
/// across all field loads. The key is closed when dropped at function end.
pub(crate) fn load_specific_keys<'a, F, I>(
    format: &F,
    dst_row: &mut RLookupRow,
    key_name: &RedisString,
    keys_to_load: I,
    force_load: bool,
) -> Result<(), LoadFieldError>
where
    F: DocumentFormat,
    I: IntoIterator,
    I::Item: std::ops::Deref<Target = RLookupKey<'a>>,
{
    let mut doc = None;
    for key_to_load in keys_to_load {
        let key_to_load = key_to_load.deref();

        if !force_load && should_skip_load(key_to_load, dst_row) {
            continue;
        }
        let d = match &doc {
            Some(d) => d,
            None => {
                doc = Some(format.open(key_name)?);
                doc.as_ref().unwrap()
            }
        };
        d.load_field(key_to_load, dst_row)?;
    }
    Ok(())
}

/// Returns `true` if we should skip loading the value because it is already available
///
/// e.g. we can use the sorting vector as a cache
fn should_skip_load(kk: &RLookupKey, dst_row: &RLookupRow) -> bool {
    // No need to "write" this key. It's always implicitly loaded!
    kk.flags.contains(RLookupKeyFlag::ValAvailable)
        ||
        // The key is marked as being in the sorting vector, but the sorting vector doesn't have it.
        // Skip, because loading from the document won't actually help
        (kk.flags.contains(RLookupKeyFlag::SvSrc) && dst_row.get(kk).is_none())
}
