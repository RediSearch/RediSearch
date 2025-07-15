/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bindings and wrapper types for as-of-yet unported types and modules. This makes the actual RLookup code cleaner and safer
//! isolating much of the unsafe FFI code.

use enumflags2::{BitFlags, bitflags};

type QueryErrorPtr = *mut ffi::QueryError;
type RedisSearchCtxPtr = *mut ffi::RedisSearchCtx;
type RSDocumentMetadataPtr = *const ffi::RSDocumentMetadata;

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecOption {
    Sortable = 0x01,
    NoStemming = 0x02,
    NotIndexable = 0x04,
    Phonetics = 0x08,
    Dynamic = 0x10,
    Unf = 0x20,
    WithSuffixTrie = 0x40,
    UndefinedOrder = 0x80,
    IndexEmpty = 0x100,   // Index empty values (i.e., empty strings)
    IndexMissing = 0x200, // Index missing values (non-existing field)
}
pub type FieldSpecOptions = BitFlags<FieldSpecOption>;

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecType {
    Fulltext = 1,
    Numeric = 2,
    Geo = 4,
    Tag = 8,
    Vector = 16,
    Geometry = 32,
}
pub type FieldSpecTypes = BitFlags<FieldSpecType>;

/// Three Loading modes for RLookup
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RLookupLoadMode {
    /// Use keylist to load a number of [RLookupLoadOptions::n_keys] from [RLookupLoadOptions::keys]
    KeyList = 0,

    /// Load only cached keys from the [sorting_vector::RSSortingVector] and do not load from [crate::row::RLookupRow]
    SortingVectorKeys = 1,

    /// Load all keys from both the [sorting_vector::RSSortingVector] and from the [crate::row::RLookupRow]
    AllKeys = 2,
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RLookupCoerceType {
    Str = 0,
    #[expect(unused, reason = "Don't used in RLookup but listed for completeness")]
    Int = 1,
    Dbl = 2,
    #[expect(unused, reason = "Used by Follow Up PRs")]
    Bool = 3,
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DocumentType {
    Hash = 0,
    Json = 1,
    Unsupported = 2,
}

/// Comment
/// cbindgen:field-names=[sctx, dmd, keyPtr, type, keys, nkeys, mode, forceLoad, forceString, status]
#[repr(C)]
pub struct RLookupLoadOptions {
    pub sctx: RedisSearchCtxPtr,

    /** Needed for the key name, and perhaps the sortable */
    pub dmd: RSDocumentMetadataPtr,

    /// Needed for rule filter where dmd does not exist
    pub key_ptr: *const std::ffi::c_char,

    /// Type of document to load, either Hash or JSON.
    pub doc_type: DocumentType,

    /// Keys to load. If present, then loadNonCached and loadAllFields is ignored
    pub keys: *const *const ffi::RLookupKey,

    /// Number of keys in keys array
    pub n_keys: libc::size_t,

    /// The following mode controls the loading behavior of fields
    pub mode: RLookupLoadMode,

    /// Don't use sortables when loading documents. This will enforce the loader to load
    /// the fields from the document itself, even if they are sortables and un-normalized.
    pub force_load: bool,

    /// Force string return; don't coerce to native type    
    pub force_string: bool,

    pub status: QueryErrorPtr,
}
