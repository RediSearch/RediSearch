/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(dead_code, reason = "used by later PRs")]

mod hash;
mod json;

use std::ffi::CStr;
use std::fmt::Display;
use std::ptr::NonNull;
use std::{error::Error, ptr};

use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use document_metadata::DocumentMetadata;
use itertools::Either;
use query_error::QueryError;
use redis_module::RedisString;
use sorting_vector::RSSortingVector;

const UNDERSCORE_KEY: &CStr = c"__key";

#[derive(Debug)]
pub struct LoadDocumentError {}

impl Display for LoadDocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to load document")
    }
}

impl Error for LoadDocumentError {}

pub struct DocumentLoader<'env, 'a, F: DocumentFormat> {
    rlookup: &'env mut RLookup<'a>,
    dst_row: &'env mut RLookupRow<'a>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    force_load: bool,
    cached_only: bool,
    dmd: &'env DocumentMetadata,
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
        status: &mut QueryError,
    ) -> Result<Self::Document<'key>, LoadDocumentError>;

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
        status: &mut QueryError,
    ) -> Result<(), LoadDocumentError>;
}

/// Handle to an opened document. Load individual fields from it.
pub trait OpenDocument {
    fn load_field(
        &self,
        kk: &RLookupKey,
        dst_row: &mut RLookupRow,
    ) -> Result<(), LoadDocumentError>;
}

impl<'env, 'a, F: DocumentFormat> DocumentLoader<'env, 'a, F> {
    pub const fn new(
        rlookup: &'env mut RLookup<'a>,
        dst_row: &'env mut RLookupRow<'a>,
        ctx: NonNull<ffi::RedisModuleCtx>,
        dmd: &'env DocumentMetadata,
        format: F,
    ) -> Self {
        Self {
            rlookup,
            dst_row,
            ctx,
            force_load: false,
            cached_only: false,
            dmd,
            format,
        }
    }

    pub const fn force_load(mut self, force_load: bool) -> Self {
        self.force_load = force_load;
        self
    }

    pub const fn cached_only(mut self, cached_only: bool) -> Self {
        self.cached_only = cached_only;
        self
    }

    pub fn load_all(self, status: &mut QueryError) -> Result<(), LoadDocumentError> {
        // Safety: we know that the ffi::RSSortingVector is the same type as RSSortingVector
        let sorting_vector = unsafe {
            ptr::from_ref(&self.dmd.sortVector)
                .cast::<RSSortingVector>()
                .as_ref()
                .unwrap()
        };
        self.dst_row.set_sorting_vector(Some(sorting_vector));

        self.format.load_all(
            self.rlookup,
            self.dst_row,
            &self.dmd.key_name(Some(self.ctx)),
            status,
        )
    }

    // If we called load to perform IF operation with FT.ADD command
    pub fn load_specific(self, status: &mut QueryError) -> Result<(), LoadDocumentError> {
        // Safety: we know that the ffi::RSSortingVector is the same type as RSSortingVector
        let sorting_vector = unsafe {
            ptr::from_ref(&self.dmd.sortVector)
                .cast::<RSSortingVector>()
                .as_ref()
                .unwrap()
        };
        self.dst_row.set_sorting_vector(Some(sorting_vector));

        let keys_to_load = self
            .rlookup
            .iter()
            // include only keys that are part of the document schema. We cannot load other keys anyway.
            .filter(|key| key.flags.contains(RLookupKeyFlag::SchemaSrc));

        let keys_to_load = if self.cached_only && !self.force_load {
            // if we're told to load only sortable keys, we'll filter out all non-sortable ones
            // UNLESS force_load is given which overrides this filtering
            Either::Left(keys_to_load.filter(|key| key.flags.contains(RLookupKeyFlag::SvSrc)))
        } else {
            // otherwise, just keep the list of keys as-is.
            Either::Right(keys_to_load)
        };

        load_specific_keys(
            &self.format,
            self.dst_row,
            &self.dmd.key_name(Some(self.ctx)),
            keys_to_load,
            self.force_load,
            status,
        )
    }
}

/// Load specific fields from a document, lazily opening the key handle on first
/// field that actually needs loading.
///
/// Mirrors the C `loadIndividualKeys` pattern: open the key handle once
/// (lazily, on first field that actually needs loading) and reuse it
/// across all field loads. The key is closed when dropped at function end.
pub(crate) fn load_specific_keys<'a, 'k, F, I>(
    format: &F,
    dst_row: &mut RLookupRow,
    key_name: &RedisString,
    keys_to_load: I,
    force_load: bool,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError>
where
    F: DocumentFormat,
    I: IntoIterator<Item = &'k RLookupKey<'a>>,
    'a: 'k,
{
    let mut doc = None;
    for key_to_load in keys_to_load {
        if is_value_available(key_to_load, dst_row, force_load) {
            continue;
        }
        let d = match &doc {
            Some(d) => d,
            None => {
                doc = Some(format.open(key_name, status)?);
                doc.as_ref().unwrap()
            }
        };
        d.load_field(key_to_load, dst_row)?;
    }
    Ok(())
}

// returns true if the value of the key is already available
// avoids the need to call to redis api to get the value
// i.e. we can use the sorting vector as a cache
fn is_value_available(kk: &RLookupKey, dst_row: &RLookupRow, force_load: bool) -> bool {
    !force_load
        && (
            // No need to "write" this key. It's always implicitly loaded!
            kk.flags.contains(RLookupKeyFlag::ValAvailable)
        ||
        // There is no value in the sorting vector, and we don't need to load it from the document.
        (kk.flags.contains(RLookupKeyFlag::SvSrc) && dst_row.get(kk).is_none())
        )
}
