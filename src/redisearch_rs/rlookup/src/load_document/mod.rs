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
use document::DocumentType;
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

pub struct DocumentLoader<'env, 'a> {
    rlookup: &'env mut RLookup<'a>,
    dst_row: &'env mut RLookupRow<'a>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    force_load: bool,
    force_string: bool,
    cached_only: bool, // corresponds to RLOOKUP_LOAD_SVKEYS
    api_version: u32,
}

impl<'env, 'a> DocumentLoader<'env, 'a> {
    pub const fn new(
        rlookup: &'env mut RLookup<'a>,
        dst_row: &'env mut RLookupRow<'a>,
        ctx: NonNull<ffi::RedisModuleCtx>,
    ) -> Self {
        Self {
            rlookup,
            dst_row,
            ctx,
            force_load: false,
            force_string: false,
            cached_only: false,
            api_version: 0,
        }
    }

    pub const fn force_load(mut self, force_load: bool) -> Self {
        self.force_load = force_load;
        self
    }

    pub const fn force_string(mut self, force_string: bool) -> Self {
        self.force_string = force_string;
        self
    }

    pub const fn cached_only(mut self, cached_only: bool) -> Self {
        self.cached_only = cached_only;
        self
    }

    pub const fn api_version(mut self, api_version: u32) -> Self {
        self.api_version = api_version;
        self
    }

    pub fn load_all(
        self,
        dmd: &DocumentMetadata,
        status: &mut QueryError,
    ) -> Result<(), LoadDocumentError> {
        // Safety: we know that the ffi::RSSortingVector is the same type as RSSortingVector
        let sorting_vector = unsafe {
            ptr::from_ref(&dmd.sortVector)
                .cast::<RSSortingVector>()
                .as_ref()
                .unwrap()
        };

        self.dst_row.set_sorting_vector(Some(sorting_vector));

        match dmd.type_() {
            DocumentType::Hash => hash::load_all_keys(
                self.rlookup,
                self.dst_row,
                self.ctx,
                &dmd.key_name(Some(self.ctx)),
                self.force_string,
                self.api_version,
                status,
            ),
            DocumentType::Json => json::load_all_keys(
                self.rlookup,
                self.dst_row,
                self.ctx,
                &dmd.key_name(Some(self.ctx)),
                self.api_version,
                status,
            ),
            DocumentType::Unsupported => unimplemented!("unsupported document type"),
        }
    }

    // If we called load to perform IF operation with FT.ADD command
    pub fn load_specific(
        self,
        dmd: &DocumentMetadata,
        status: &mut QueryError,
    ) -> Result<(), LoadDocumentError> {
        // Safety: we know that the ffi::RSSortingVector is the same type as RSSortingVector
        let sorting_vector = unsafe {
            ptr::from_ref(&dmd.sortVector)
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

        load_specific_keys_internal(
            self.dst_row,
            self.ctx,
            dmd.type_(),
            &dmd.key_name(Some(self.ctx)),
            keys_to_load,
            self.force_load,
            self.api_version,
            status,
        )
    }
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn load_specific_keys_internal<'a, 'k, I>(
    dst_row: &mut RLookupRow,
    ctx: NonNull<ffi::RedisModuleCtx>,
    doc_ty: DocumentType,
    key_name: &RedisString,
    keys_to_load: I,
    force_load: bool,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError>
where
    I: IntoIterator<Item = &'k RLookupKey<'a>>,
    'a: 'k,
{
    // Mirrors the C `loadIndividualKeys` pattern: open the key handle once
    // (lazily, on first field that actually needs loading) and reuse it
    // across all field loads. The key is closed when dropped at function end.
    //
    // On error we silently skip the rest.
    // On success we continue (success includes "no value found").
    match doc_ty {
        DocumentType::Hash => {
            let mut key_handle = None;
            for key_to_load in keys_to_load {
                if is_value_available(key_to_load, dst_row, force_load) {
                    continue;
                }
                let key = match &key_handle {
                    Some(k) => k,
                    None => {
                        key_handle = Some(hash::open_key(ctx, key_name, status)?);
                        key_handle.as_ref().unwrap()
                    }
                };
                hash::load_field(key_to_load, dst_row, key, key_name, api_version)?;
            }
        }
        DocumentType::Json => {
            let mut key_handle = None;
            for key_to_load in keys_to_load {
                if is_value_available(key_to_load, dst_row, force_load) {
                    continue;
                }
                let key = match &key_handle {
                    Some(k) => k,
                    None => {
                        key_handle = Some(json::open_key(ctx, key_name, status)?);
                        key_handle.as_ref().unwrap()
                    }
                };
                json::load_field(key_to_load, dst_row, key, key_name, api_version)?;
            }
        }
        DocumentType::Unsupported => unimplemented!("unsupported document type"),
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
