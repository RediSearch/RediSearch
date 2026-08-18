/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod key;
mod key_list;

use crate::HashDocumentFormat;
use crate::JsonDocumentFormat;
use crate::LoadFieldError;
use crate::{
    IndexSpec, RLookupRow,
    bindings::{FieldSpec, FieldSpecOption, FieldSpecOptions, IndexSpecCache},
    load_document,
};
use document::DocumentType;
use enumflags2::{BitFlags, bitflags};
use key_list::KeyList;
use redis_json_api::RedisJsonApi;
use redis_module::RedisString;
use std::{borrow::Cow, ffi::CStr, pin::Pin, ptr::NonNull};

pub use key::{GET_KEY_FLAGS, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, TRANSIENT_FLAGS};
pub use key_list::{Cursor, CursorMut, Iter, IterMut};

#[cheadergen::config(export, rename = "RLookup_Opt")]
#[bitflags]
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RLookupOption {
    /// If the key cannot be found, do not mark it as an error, but create it and
    /// mark it as F_UNRESOLVED
    #[cheadergen(rename = "RLOOKUP_OPT_ALLOWUNRESOLVED")]
    AllowUnresolved = 0x01,

    /// If a loader was added to load the entire document, this flag will allow
    /// later calls to GetKey in read mode to create a key (from the schema) even if it is not sortable
    #[cheadergen(rename = "RLOOKUP_OPT_ALLLOADED")]
    AllLoaded = 0x02,
}

/// Helper type to represent a set of [`RLookupOption`]s.
#[cheadergen::config(skip)]
pub type RLookupOptions = BitFlags<RLookupOption>;

/// An append-only list of [`RLookupKey`]s.
///
/// This type maintains a list of [`RLookupKey`]s addressable by string name.
///
/// # Sealing
///
/// At the end of pipeline construction the lookup is [sealed](Self::seal):
/// from that point on it is *append-only*. Creating new keys stays legal —
/// document loaders and the coordinator append keys during execution — but
/// every operation that changes an existing key panics. Each mutating method
/// documents on which side of that line it falls. The invariant exists so
/// that state derived from the key set at finalization (cached
/// [`RLookupKey`] pointers, compiled reply plans) stays valid for the rest of
/// the request without re-validation.
#[derive(Debug)]
pub struct RLookup<'a> {
    keys: KeyList<'a>,

    // Flags/options
    options: RLookupOptions,

    // If present, then GetKey will consult this list if the value is not found in
    // the existing list of keys.
    index_spec_cache: Option<IndexSpecCache>,
}

// ===== impl RLookup =====

impl Default for RLookup<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> RLookup<'a> {
    pub fn new() -> Self {
        Self {
            keys: KeyList::new(),
            options: RLookupOptions::empty(),
            index_spec_cache: None,
        }
    }

    /// Asserts as many of the lookup's invariants as possible.
    #[track_caller]
    #[cfg(any(debug_assertions, test))]
    pub fn assert_valid(&self, ctx: &str) {
        self.keys.assert_valid(ctx);
    }

    /// Asserts invariants without dereferencing data borrowed by lookup keys.
    #[track_caller]
    #[cfg(any(debug_assertions, test))]
    pub fn assert_structure_valid(&self, ctx: &str) {
        self.keys.assert_structure_valid(ctx);
    }

    /// Seal this lookup: from now on it is **append-only** (see the
    /// [type-level docs](Self#sealing)). Idempotent.
    ///
    /// The C pipeline-construction code calls this (through `RLookup_Seal`)
    /// wherever a request's plan becomes final; the call sites are the
    /// canonical record of where that is.
    pub const fn seal(&mut self) {
        self.keys.seal();
    }

    /// Build the optional key-name index used by wide coordinator rows.
    ///
    /// This is idempotent and may be called after [`Self::seal`]: it derives an index from the
    /// existing keys without changing their identity, order, or flags, and indexes later appends.
    pub fn enable_name_index(&mut self) {
        self.keys.enable_name_index();
    }

    /// Whether [`Self::seal`] has been called.
    pub const fn is_sealed(&self) -> bool {
        self.keys.is_sealed()
    }

    /// Set the [`IndexSpecCache`] associated with this [`RLookup`].
    ///
    /// Sealing: **forbidden** after [`Self::seal`] — the cache determines how
    /// names resolve, so swapping it mutates the lookup's observable key set.
    ///
    /// # Panics
    ///
    /// Panics if this lookup already has an index spec cache, or is sealed.
    pub fn set_cache(&mut self, spcache: Option<IndexSpecCache>) {
        debug_assert!(
            self.index_spec_cache.is_none(),
            "cannot replace an existing index_spec_cache"
        );
        assert!(
            !self.is_sealed(),
            "cannot set the index spec cache of a sealed RLookup (sealed lookups are append-only)"
        );

        self.index_spec_cache = spcache;

        // Keys created before the cache was attached could not be checked
        // against the rule's special fields — mark them now.
        if let Some(cache) = &self.index_spec_cache {
            for key in self.keys.iter_mut() {
                if cache.is_rule_special_field(key.name().as_ref()) {
                    key.project().header.flags |= RLookupKeyFlag::Hidden;
                }
            }
        }
    }

    /// [`RLookupKeyFlag::Hidden`] if `name` is one of the schema rule's
    /// special document fields (language / score / payload) recorded on the
    /// attached spec cache, empty otherwise. These are control fields: they
    /// are loaded into rows but never replied. Applied at key creation — by
    /// reply time no keys are created and the schema rule may already be
    /// freed, so the reply path must be able to filter by flag alone.
    fn hidden_if_schema_special(&self, name: &CStr) -> RLookupKeyFlags {
        if self
            .index_spec_cache
            .as_ref()
            .is_some_and(|cache| cache.is_rule_special_field(name))
        {
            RLookupKeyFlag::Hidden.into()
        } else {
            RLookupKeyFlags::empty()
        }
    }

    /// Sealing: allowed after [`Self::seal`] — options only gate how *future*
    /// keys are created (appends stay legal on sealed lookups); existing keys
    /// are unaffected.
    pub fn disable_options(&mut self, options: RLookupOptions) {
        self.options &= !options;
    }

    /// Sealing: allowed after [`Self::seal`]; see [`Self::disable_options`].
    pub fn enable_options(&mut self, options: RLookupOptions) {
        self.options |= options;
    }

    pub const fn has_index_spec_cache(&self) -> bool {
        self.index_spec_cache.is_some()
    }

    pub fn find_field_in_spec_cache(&self, name: &CStr) -> Option<&ffi::FieldSpec> {
        self.index_spec_cache
            .as_ref()
            .and_then(|c| c.find_field(name))
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a [`Cursor`] pointing to the key if found.
    // FIXME [MOD-10315] replace with more efficient search
    pub fn find_key_by_name(&self, name: &CStr) -> Option<Cursor<'_, 'a>> {
        self.keys.find_by_name(name)
    }

    /// Add all non-overridden keys from `src` to `self`.
    ///
    /// For each key in `src`, check if it already exists *by name*.
    /// - If it does, the `flag` argument controls the behaviour (skip with `RLookupKeyFlags::empty()`, override with `RLookupKeyFlag::Override`).
    /// - If it doesn't, a new key will be created.
    ///
    /// Flag handling:
    /// - Preserves persistent source key properties (F_SVSRC, F_HIDDEN, F_EXPLICITRETURN, etc.)
    /// - Filters out transient flags from source keys (F_OVERRIDE, F_FORCE_LOAD)
    /// - Respects caller's control flags for behavior (F_OVERRIDE, F_FORCE_LOAD, etc.)
    /// - Target flags = caller_flags | (source_flags & ~RLOOKUP_TRANSIENT_FLAGS)
    ///
    /// Sealing: follows [`Self::get_key_write`] per key — appends are allowed
    /// on a sealed `self`; overriding an existing key panics.
    pub fn add_keys_from(&mut self, src: &RLookup<'a>, flags: RLookupKeyFlags) {
        debug_assert!(
            !flags.contains(RLookupKeyFlag::NameAlloc),
            "The NameAlloc flag should have been handled in the FFI function. This is a bug."
        );

        for src_key in src.iter() {
            // Combine caller's control flags with source key's persistent properties
            // Only preserve non-transient flags from source (F_SVSRC, F_HIDDEN, etc.)
            // while respecting caller's control flags (F_OVERRIDE, F_FORCE_LOAD, etc.)
            let combined_flags = flags | src_key.flags & !TRANSIENT_FLAGS;

            // NB: get_key_write returns none if the key already exists and `flags` don't contain `Override`.
            // In this case, we just want to move on to the next key
            let _ = self.get_key_write(src_key.name().clone(), combined_flags);
        }
    }

    /// Returns a [`Cursor`] starting at the first key.
    #[inline(always)]
    pub fn cursor(&self) -> Cursor<'_, 'a> {
        self.keys.cursor_front()
    }

    /// Returns an iterator over immutable references to keys.
    #[inline(always)]
    pub fn iter(&self) -> Iter<'_, 'a> {
        self.keys.iter()
    }

    /// Returns the current keys as a contiguous array of pointers in row-slot order.
    ///
    /// The array itself is invalidated by any subsequent mutation of this lookup. The pointed-to
    /// keys retain stable addresses until the lookup is dropped, including after an override.
    pub fn raw_key_ptrs(&self) -> (*const *const RLookupKey<'a>, usize) {
        self.keys.raw_parts()
    }

    /// Returns an iterator over pinned mutable references to keys.
    ///
    /// Sealing: **forbidden** after [`Self::seal`] — the returned references
    /// allow mutating existing keys behind the seal's back.
    ///
    /// # Panics
    ///
    /// Panics if this lookup is sealed.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> IterMut<'_, 'a> {
        assert!(
            !self.is_sealed(),
            "cannot mutably iterate a sealed RLookup (sealed lookups are append-only)"
        );

        self.keys.iter_mut()
    }

    // ===== Get key for reading (create only if in schema and sortable) =====

    /// Gets a key by its name from the lookup table, if not found it uses the schema as a fallback to search the key.
    ///
    /// If the flag `RLookupKeyFlag::AllowUnresolved` is set, it will create a new key if it does not exist in the lookup table
    /// nor in the schema.
    ///
    /// Sealing: allowed after [`Self::seal`] — this either finds an existing
    /// key or appends a new one; it never changes an existing key.
    pub fn get_key_read(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        let slot = self.get_key_read_slot(name, flags)?;
        self.keys.get(slot)
    }

    /// FFI-facing variant of [`Self::get_key_read`] that preserves the allocation's raw-pointer
    /// provenance.
    #[doc(hidden)]
    pub fn get_key_read_ptr(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        flags: RLookupKeyFlags,
    ) -> Option<NonNull<RLookupKey<'a>>> {
        let slot = self.get_key_read_slot(name, flags)?;
        self.keys.get_ptr(slot)
    }

    fn get_key_read_slot(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        mut flags: RLookupKeyFlags,
    ) -> Option<u16> {
        flags &= GET_KEY_FLAGS;

        let name = name.into();

        if let Some(slot) = self.keys.find_slot(&name) {
            return Some(slot);
        }

        // If we didn't find the key at the lookup table, check if it exists in
        // the schema as SORTABLE, and create only if so.
        let name = match self.gen_key_from_spec(name, flags) {
            Ok(key) => {
                return Some(self.keys.push_slot(key));
            }
            Err(name) => name,
        };

        // If we didn't find the key in the schema (there is no schema) and unresolved is OK, create an unresolved key.
        if self.options.contains(RLookupOption::AllowUnresolved) {
            let mut key = RLookupKey::new(name, flags);
            key.flags |= RLookupKeyFlag::Unresolved;
            let special = self.hidden_if_schema_special(key.name().as_ref());
            key.flags |= special;

            return Some(self.keys.push_slot(key));
        }

        None
    }

    // Gets a key from the schema if the field is sortable (so its data is available), unless an RP upstream
    // has promised to load the entire document.
    //
    // # Errors
    //
    // If the key cannot be created, either because there is no IndexSpecCache associated with this RLookup OR,
    // because the field is not sortable the name will be returned in the `Err` variant.
    fn gen_key_from_spec(
        &mut self,
        name: Cow<'a, CStr>,
        flags: RLookupKeyFlags,
    ) -> Result<RLookupKey<'a>, Cow<'a, CStr>> {
        let Some(fs) = self
            .index_spec_cache
            .as_ref()
            .and_then(|spcache| spcache.find_field(&name))
        else {
            return Err(name);
        };
        let fs_options = FieldSpecOptions::from_bits(fs.options()).unwrap();

        // FIXME: (from C code) LOAD ALL loads the key properties by their name, and we won't find their value by the field name
        //        if the field has a different name (alias) than its path.
        if !fs_options.contains(FieldSpecOption::Sortable)
            && !self.options.contains(RLookupOption::AllLoaded)
        {
            return Err(name);
        }

        let mut key = RLookupKey::new(name, flags);
        key.update_from_field_spec(fs);
        let special = self.hidden_if_schema_special(key.name().as_ref());
        key.flags |= special;
        Ok(key)
    }

    /// Writes a key to the lookup table. If the key already exists
    /// - it is overwritten and returned if flags are set to `RLookupKeyFlag::Override`
    /// - `None` is returned if the key is in exclusive mode (the opposite of Override)
    ///
    /// This will never get a key from the cache, it will either create a new key, override an existing key or return `None` if the key
    /// is in exclusive mode.
    ///
    /// Sealing: the append and exclusive-mode arms are allowed after
    /// [`Self::seal`]; overriding an existing key (the
    /// [`RLookupKeyFlag::Override`] arm) panics on a sealed lookup.
    pub fn get_key_write(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        let slot = self.get_key_write_slot(name, flags)?;
        self.keys.get(slot)
    }

    /// FFI-facing variant of [`Self::get_key_write`] that preserves the allocation's raw-pointer
    /// provenance.
    #[doc(hidden)]
    pub fn get_key_write_ptr(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        flags: RLookupKeyFlags,
    ) -> Option<NonNull<RLookupKey<'a>>> {
        let slot = self.get_key_write_slot(name, flags)?;
        self.keys.get_ptr(slot)
    }

    fn get_key_write_slot(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        mut flags: RLookupKeyFlags,
    ) -> Option<u16> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        let name = name.into();
        let flags = flags | self.hidden_if_schema_special(&name);

        let key = if let Some(slot) = self.keys.find_slot(&name) {
            // A. we found the key in the lookup table:
            if flags.contains(RLookupKeyFlag::Override) {
                // We are in create mode, overwrite the key (remove schema related data, mark with new flags).
                self.keys
                    .cursor_at_mut(slot)
                    .override_current(flags | RLookupKeyFlag::QuerySrc)
                    .unwrap();
                slot
            } else {
                // We are in exclusive mode, return None
                return None;
            }
        } else {
            // B. we didn't find the key in the lookup table:
            // create a new key with the name and flags.
            self.keys
                .push_slot(RLookupKey::new(name, flags | RLookupKeyFlag::QuerySrc))
        };

        Some(key)
    }

    // ===== Load key from redis keyspace (include known information on the key, fail if already loaded) =====

    /// Sealing: the append arm is allowed after [`Self::seal`]; the arms that
    /// change an existing key (override, or marking a found key as explicit
    /// return) panic on a sealed lookup.
    pub fn get_key_load(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        field_name: &'a CStr,
        flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        let slot = self.get_key_load_slot(name, field_name, flags)?;
        self.keys.get(slot)
    }

    /// FFI-facing variant of [`Self::get_key_load`] that preserves the allocation's raw-pointer
    /// provenance.
    #[doc(hidden)]
    pub fn get_key_load_ptr(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        field_name: &'a CStr,
        flags: RLookupKeyFlags,
    ) -> Option<NonNull<RLookupKey<'a>>> {
        let slot = self.get_key_load_slot(name, field_name, flags)?;
        self.keys.get_ptr(slot)
    }

    fn get_key_load_slot(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        field_name: &'a CStr,
        mut flags: RLookupKeyFlags,
    ) -> Option<u16> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        let name = name.into();
        let flags = flags | self.hidden_if_schema_special(&name);
        let sealed = self.is_sealed();

        // 1. if the key is already loaded, or it has created by earlier RP for writing, return NULL (unless override was requested)
        // 2. create a new key with the name of the field, and mark it as doc-source.
        // 3. if the key is in the schema, mark it as schema-source and apply all the relevant flags according to the field spec.
        // 4. if the key is "loaded" at this point (in schema, sortable and un-normalized), create the key but return NULL
        //    (no need to load it from the document).

        // Ensure the key is available, if it is check for flags and return None or override the key depending on flags, if key not available insert it.
        let slot = if let Some(slot) = self.keys.find_slot(&name) {
            let mut c = self.keys.cursor_at_mut(slot);
            // Scoped borrow: must end before `override_current` consumes the cursor.
            {
                let key = c.current().unwrap();

                if (key.flags.contains(RLookupKeyFlag::ValAvailable)
                    && !key.flags.contains(RLookupKeyFlag::IsLoaded))
                    && !key
                        .flags
                        .intersects(RLookupKeyFlag::Override | RLookupKeyFlag::ForceLoad)
                    || (key.flags.contains(RLookupKeyFlag::IsLoaded)
                        && !flags.contains(RLookupKeyFlag::Override))
                    || (key.flags.contains(RLookupKeyFlag::QuerySrc)
                        && !flags.contains(RLookupKeyFlag::Override))
                {
                    // We found a key with the same name. We return NULL if:
                    // 1. The key has the origin data available (from the sorting vector, UNF) and the caller didn't
                    //    request to override or forced loading.
                    // 2. The key is already loaded (from the document) and the caller didn't request to override.
                    // 3. The key was created by the query (upstream) and the caller didn't request to override.

                    // If the caller wanted to mark this key as explicit return, mark it as such even if we don't return it.
                    // Only touch the key when that actually changes it: execution-time callers
                    // (the loaders' `load_all` paths) reach this arm with no flags to add, and a
                    // no-op must not trip the sealing check.
                    let add = flags & RLookupKeyFlag::ExplicitReturn;
                    if !key.flags.contains(add) {
                        assert!(
                            !sealed,
                            "cannot mutate key flags in a sealed RLookup (sealed lookups are append-only)"
                        );
                        let key = key.project();
                        key.header.flags |= add;
                    }

                    return None;
                }
            }

            c.override_current(flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded)
                .unwrap();
            slot
        } else {
            self.keys.push_slot(RLookupKey::new(
                name.clone(),
                flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded,
            ))
        };

        let key = self.keys.cursor_at_mut(slot).into_current().unwrap();

        if let Some(fs) = self
            .index_spec_cache
            .as_ref()
            .and_then(|spcache| spcache.find_field(field_name))
        {
            key.update_from_field_spec(fs);

            if key.flags.contains(RLookupKeyFlag::ValAvailable)
                && !flags.contains(RLookupKeyFlag::ForceLoad)
            {
                // If the key is marked as "value available", it means that it is sortable and un-normalized.
                // so we can use the sorting vector as the source, and we don't need to load it from the document.
                return None;
            }
        } else {
            // Field not found in the schema.
            let is_borrowed = matches!(key.name(), Cow::Borrowed(_));

            // Safety: We treat the pointer as pinned internally and never move out of the key.
            let key = unsafe { Pin::new_unchecked(&mut *key) };

            // We assume `field_name` is the path to load from in the document.
            if is_borrowed {
                key.set_path(Cow::Borrowed(field_name));
            } else if name.as_ref() != field_name {
                let field_name: Cow<'_, CStr> = Cow::Owned(field_name.to_owned());
                key.set_path(field_name);
            } // else
            // If the caller requested to allocate the name, and the name is the same as the path,
            // it was already set to the same allocation for the name, so we don't need to do anything.
        }

        Some(slot)
    }

    /// The row len of the [`RLookup`] is the number of keys in its key list not counting the overridden keys.
    pub fn get_row_len(&self) -> u32 {
        self.keys.row_len()
    }

    /// Returns the schema-source keys eligible for individual document loading.
    ///
    /// Mirrors the C `loadIndividualKeys` selection for the "load every loadable
    /// key" case (`nkeys == 0`): only keys flagged [`RLookupKeyFlag::SchemaSrc`]
    /// are considered, and when `cached_only` is set without `force_load` the set
    /// is further restricted to keys backed by the sorting vector
    /// ([`RLookupKeyFlag::SvSrc`]).
    pub fn schema_src_keys(
        &self,
        cached_only: bool,
        force_load: bool,
    ) -> impl Iterator<Item = &RLookupKey<'a>> {
        self.iter()
            .filter(|k| k.flags.contains(RLookupKeyFlag::SchemaSrc))
            .filter(move |k| !cached_only || force_load || k.flags.contains(RLookupKeyFlag::SvSrc))
    }

    /// `open_key`, when `Some`, is an already-open handle for `key_name` that the loader
    /// reuses instead of opening the document by name; it is borrowed, not closed here.
    ///
    /// Sealing: **forbidden** after [`Self::seal`] — every call blindly appends
    /// one key per rule field, so repeated calls on the same lookup only make
    /// sense while it is still being built (in practice: the indexing path's
    /// transient lookups, which are never sealed).
    pub fn load_rule_fields(
        &mut self,
        search_ctx: &mut ffi::RedisSearchCtx,
        dst_row: &mut RLookupRow<'a>,
        index_spec: &'a IndexSpec,
        key_name: &CStr,
        open_key: Option<&redis_module::RedisModuleKey>,
    ) -> Result<(), LoadFieldError> {
        assert!(
            !self.is_sealed(),
            "cannot load rule fields into a sealed RLookup (sealed lookups are append-only)"
        );

        let first_new_slot = self.keys.row_len() as usize;
        create_keys_from_spec(index_spec).for_each(|mut key| {
            let special = self.hidden_if_schema_special(key.name().as_ref());
            key.flags |= special;
            self.keys.push(key);
        });
        let keys_to_load = self.keys.iter().skip(first_new_slot);

        let key_name =
            RedisString::create_from_slice(search_ctx.redisCtx.cast(), key_name.to_bytes());

        match index_spec.rule().type_() {
            DocumentType::Hash => {
                let format =
                    HashDocumentFormat::new(NonNull::new(search_ctx.redisCtx).unwrap(), false);

                load_document::load_specific_keys(
                    &format,
                    dst_row,
                    &key_name,
                    keys_to_load,
                    true,
                    open_key,
                    None,
                )
            }
            DocumentType::Json => {
                // Safety: this function will be called long after module initialization
                let japi = unsafe { RedisJsonApi::get().ok_or(LoadFieldError::JsonUnsupported) }?;

                let format = JsonDocumentFormat::new(
                    NonNull::new(search_ctx.redisCtx).unwrap(),
                    &japi,
                    search_ctx.apiVersion,
                );

                load_document::load_specific_keys(
                    &format,
                    dst_row,
                    &key_name,
                    keys_to_load,
                    true,
                    open_key,
                    None,
                )
            }
            DocumentType::Unsupported => unimplemented!("unsupported document type"),
        }
    }
}

fn create_keys_from_spec<'a>(
    index_spec: &'a IndexSpec,
) -> impl ExactSizeIterator<Item = RLookupKey<'a>> {
    let rule = index_spec.rule();
    let field_specs = index_spec.field_specs();
    rule.filter_fields_index()
        .iter()
        .zip(rule.filter_fields())
        .map(|(&index, filter_field)| create_key_from_data(index, filter_field, field_specs))
}

fn create_key_from_data<'a>(
    index: i32,
    filter_field: &'a CStr,
    field_specs: &'a [FieldSpec],
) -> RLookupKey<'a> {
    const NO_MATCH: i32 = -1;
    if NO_MATCH == index {
        RLookupKey::new_with_path(filter_field, filter_field, RLookupKeyFlags::empty())
    } else {
        let index = usize::try_from(index).expect("index must be positive and fit into usize");
        let field_spec = &field_specs[index];
        let field_name = field_spec.field_name().secret_value();
        let path = field_spec.field_path().secret_value();

        RLookupKey::new_with_path(field_name, path, RLookupKeyFlags::empty())
    }
}

pub mod opaque {
    use super::RLookup;
    use c_ffi_utils::opaque::Size;
    /// An opaque lookup which can be passed by value to C.
    ///
    /// The size and alignment of this struct must match the Rust `RLookup`
    /// structure exactly.
    #[cheadergen::config(rename = "RLookup")]
    #[repr(C, align(8))]
    pub struct OpaqueRLookup(Size<32>);

    c_ffi_utils::opaque!(RLookup<'_>, OpaqueRLookup);
}

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod tests {
    use super::*;
    #[cfg_attr(miri, allow(unused))]
    use crate::bindings::FieldSpecBuilder;
    use enumflags2::make_bitflags;
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::ptr;

    #[cfg(not(miri))]
    use proptest::prelude::*;

    // Assert that RLookup::iter and iter_mut yield the keys written via get_key_write,
    // and that mutations through iter_mut are observable on a subsequent iter pass.
    #[test]
    fn rlookup_iter_round_trip() {
        let mut rlookup = RLookup::new();

        for name in [c"a", c"b", c"c"] {
            rlookup
                .get_key_write(name, RLookupKeyFlags::empty())
                .unwrap();
        }

        let names: Vec<_> = rlookup
            .iter()
            .map(|k| k.name().as_ref().to_owned())
            .collect();
        assert_eq!(
            names,
            vec![c"a".to_owned(), c"b".to_owned(), c"c".to_owned()]
        );

        for key in rlookup.iter_mut() {
            key.project().header.flags |= RLookupKeyFlag::ExplicitReturn;
        }

        for key in rlookup.iter() {
            assert!(key.flags.contains(RLookupKeyFlag::ExplicitReturn));
        }
    }

    // Assert that we can successfully write keys to the rlookup
    #[test]
    fn rlookup_write_new_key() {
        let name = CString::new("new_key").unwrap();
        let flags = RLookupKeyFlags::empty();
        let mut rlookup = RLookup::new();

        // Assert that we can write a new key
        let key = rlookup.get_key_write(name.as_c_str(), flags).unwrap();
        assert_eq!(key.name().as_ref(), name.as_c_str());
        assert_eq!(key.name, name.as_ptr());
        assert!(key.flags.contains(RLookupKeyFlag::QuerySrc));
    }

    // Assert that we fail to write a key if the key already exists and no overwrite is allowed
    #[test]
    fn rlookup_write_key_multiple_times_fails() {
        let name = CString::new("new_key").unwrap();
        let flags = RLookupKeyFlags::empty();
        let mut rlookup = RLookup::new();

        // Assert that we can write a new key
        let key = rlookup.get_key_write(name.as_c_str(), flags).unwrap();
        assert_eq!(key.name().as_ref(), name.as_c_str());
        assert_eq!(key.name, name.as_ptr());
        assert!(key.flags.contains(RLookupKeyFlag::QuerySrc));

        // Assert that we cannot write the same key again without allowing overwrites
        let not_key = rlookup.get_key_write(name.as_c_str(), flags);
        assert!(not_key.is_none());
    }

    // Assert that we can override an existing key
    #[test]
    fn rlookup_write_key_override() {
        let name = CString::new("new_key").unwrap();
        let flags = RLookupKeyFlags::empty();
        let mut rlookup = RLookup::new();

        let key = rlookup.get_key_write(name.as_c_str(), flags).unwrap();
        assert_eq!(key.name().as_ref(), name.as_c_str());
        assert_eq!(key.name, name.as_ptr());
        assert!(key.flags.contains(RLookupKeyFlag::QuerySrc));

        let new_flags = make_bitflags!(RLookupKeyFlag::{ExplicitReturn | Override});

        let new_key = rlookup.get_key_write(name.as_c_str(), new_flags).unwrap();
        assert_eq!(new_key.name().as_ref(), name.as_c_str());
        assert_eq!(new_key.name, name.as_ptr());
        assert!(new_key.flags.contains(RLookupKeyFlag::QuerySrc));
        assert!(new_key.flags.contains(RLookupKeyFlag::ExplicitReturn));
    }

    #[test]
    fn ffi_pointer_remains_valid_after_override() {
        let mut rlookup = RLookup::new();
        let old = rlookup
            .get_key_load_ptr(c"foo", c"$.foo", RLookupKeyFlags::empty())
            .unwrap();

        let replacement = rlookup
            .get_key_write_ptr(c"foo", make_bitflags!(RLookupKeyFlag::Override))
            .unwrap();

        assert_ne!(old, replacement);
        // SAFETY: overridden keys remain owned by the lookup until it is dropped.
        let old = unsafe { old.as_ref() };
        assert!(old.is_tombstone());
        // SAFETY: tombstones retain their original path allocation for existing C consumers.
        assert_eq!(unsafe { CStr::from_ptr(old.path) }, c"$.foo");
    }

    // Assert that a key can be loaded from the RLookup even if we have no associated index spec cache
    #[test]
    fn rlookup_get_key_load_override_no_spcache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        let mut rlookup = RLookup::new();

        let key = RLookupKey::new(key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(key_name, field_name, RLookupKeyFlags::empty())
            .expect("expected to find key by name");

        assert_eq!(retrieved_key.name().as_ref(), key_name);
        assert_eq!(retrieved_key.path().as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    // Assert that a key can be retrieved by its name and is been overridden with the `DocSrc` and `IsLoaded` flags.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn rlookup_get_key_load_override_no_field_in_cache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        let spcache = IndexSpecCache::from_fields([]);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let key = RLookupKey::new(key_name, RLookupKeyFlags::empty());
        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key.name().as_ref(), key_name);
        assert_eq!(retrieved_key.path().as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    // Assert that a key can be retrieved by its name and is been overridden with the `DocSrc` and `IsLoaded` flags.
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let spcache = IndexSpecCache::from_fields([FieldSpecBuilder::new(cache_field_name)
            .with_sort_idx(12)
            .with_options(make_bitflags!(FieldSpecOption::{
                Sortable
            }))
            .finish()]);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let key = RLookupKey::new(key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                cache_field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key.name().as_ref(), key_name);
        assert_eq!(
            retrieved_key.path().as_ref().unwrap().as_ref(),
            cache_field_name
        );
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache_but_value_availabe() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let spcache = IndexSpecCache::from_fields([FieldSpecBuilder::new(cache_field_name)
            .with_sort_idx(12)
            .with_options(make_bitflags!(FieldSpecOption::{
                Sortable | Unf
            }))
            .finish()]);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let key = RLookupKey::new(key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup.get_key_load(
            key_name,
            cache_field_name,
            make_bitflags!(RLookupKeyFlag::Override),
        );

        // we should access the sorting vector instead
        assert!(retrieved_key.is_none());
    }

    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache_but_value_availabe_however_force_load() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let spcache = IndexSpecCache::from_fields([FieldSpecBuilder::new(cache_field_name)
            .with_sort_idx(12)
            .with_options(make_bitflags!(FieldSpecOption::{
                Sortable | Unf
            }))
            .finish()]);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let key = RLookupKey::new(key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                cache_field_name,
                make_bitflags!(RLookupKeyFlag::{Override | ForceLoad}),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key.name().as_ref(), key_name);
        assert_eq!(
            retrieved_key.path().as_ref().unwrap().as_ref(),
            cache_field_name
        );
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    // Assert the the cases in which None is returned also the key could be found
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn rlookup_get_key_load_returns_none_although_key_is_available() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";
        let key_flags = [
            RLookupKeyFlag::ValAvailable,
            RLookupKeyFlag::IsLoaded,
            RLookupKeyFlag::QuerySrc,
        ];

        for flag in key_flags {
            let spcache = IndexSpecCache::from_fields([]);

            let mut rlookup = RLookup::new();
            rlookup.set_cache(Some(spcache));

            let key = RLookupKey::new(key_name, flag.into());

            rlookup.keys.push(key);

            let retrieved_key =
                rlookup.get_key_load(key_name, field_name, RLookupKeyFlags::empty());
            assert!(retrieved_key.is_none());
            if let Some(key) = rlookup.get_key_read(key_name, RLookupKeyFlags::empty()) {
                assert!(!key.flags.contains(RLookupKeyFlag::ExplicitReturn));
            } else {
                panic!("expected to find key by name");
            }

            // let's use the load to tag explicit return
            let opt =
                rlookup.get_key_load(key_name, field_name, RLookupKeyFlag::ExplicitReturn.into());
            assert!(opt.is_none(), "expected None, got {opt:?}");

            if let Some(key) = rlookup.get_key_read(key_name, RLookupKeyFlags::empty()) {
                assert!(key.flags.contains(RLookupKeyFlag::ExplicitReturn));
            } else {
                panic!("expected to find key by name");
            }
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn rlookup_get_load_key_on_empty_rlookup_and_cache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        let spcache = IndexSpecCache::from_fields([]);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key.name().as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key.path().as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn rlookup_get_load_key_name_equals_field_name() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"key_no_cache";

        let spcache = IndexSpecCache::from_fields([]);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key.name().as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key.path().as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    #[test]
    fn rlookup_add_keys_from_basic() {
        let mut src = RLookup::new();
        src.get_key_write(c"foo", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"bar", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"baz", RLookupKeyFlags::empty()).unwrap();

        let mut dst = RLookup::new();
        dst.add_keys_from(&src, RLookupKeyFlags::empty());

        assert!(dst.keys.find_by_name(c"foo").is_some());
        assert!(dst.keys.find_by_name(c"bar").is_some());
        assert!(dst.keys.find_by_name(c"baz").is_some());
    }

    #[test]
    fn rlookup_add_keys_from_empty_source() {
        let src = RLookup::new();

        let mut dst = RLookup::new();
        dst.get_key_write(c"existing", RLookupKeyFlags::empty())
            .unwrap();

        assert_eq!(dst.get_row_len(), 1);
        dst.add_keys_from(&src, RLookupKeyFlags::empty());
        assert_eq!(dst.get_row_len(), 1);

        assert!(dst.keys.find_by_name(c"existing").is_some());
    }

    #[test]
    fn rlookup_add_keys_from_multiple_sources() {
        // Initialize lookups
        let mut src1 = RLookup::new();
        let mut src2 = RLookup::new();
        let mut src3 = RLookup::new();
        let mut dest = RLookup::new();

        // Create overlapping keys in different sources
        // src1: field1, field2, field3
        let _src1_key1 = src1.get_key_write(c"field1", RLookupKeyFlags::empty());
        let _src1_key2 = src1.get_key_write(c"field2", RLookupKeyFlags::empty());
        let _src1_key3 = src1.get_key_write(c"field3", RLookupKeyFlags::empty());

        // src2: field2, field3, field4 (field2, field3 overlap with src1)
        let _src2_key2 = src2.get_key_write(c"field2", RLookupKeyFlags::empty());
        let _src2_key3 = src2.get_key_write(c"field3", RLookupKeyFlags::empty());
        let _src2_key4 = src2.get_key_write(c"field4", RLookupKeyFlags::empty());

        // src3: field3, field4, field5 (field3, field4 overlap)
        let _src3_key3 = src3.get_key_write(c"field3", RLookupKeyFlags::empty());
        let _src3_key4 = src3.get_key_write(c"field4", RLookupKeyFlags::empty());
        let _src3_key5 = src3.get_key_write(c"field5", RLookupKeyFlags::empty());

        // Add sources sequentially (first wins for conflicts)
        dest.add_keys_from(&src1, RLookupKeyFlags::empty()); // field1, field2, field3
        dest.add_keys_from(&src2, RLookupKeyFlags::empty()); // field4 (field2, field3 already exist)
        dest.add_keys_from(&src3, RLookupKeyFlags::empty()); // field5 (field3, field4 already exist)

        // Verify final result: all unique keys present (first wins for conflicts)
        assert_eq!(5, dest.get_row_len()); // field1, field2, field3, field4, field5

        let d_key1 = dest.get_key_read(c"field1", RLookupKeyFlags::empty());
        assert!(d_key1.is_some());

        let d_key2 = dest.get_key_read(c"field2", RLookupKeyFlags::empty());
        assert!(d_key2.is_some());

        let d_key3 = dest.get_key_read(c"field3", RLookupKeyFlags::empty());
        assert!(d_key3.is_some());

        let d_key4 = dest.get_key_read(c"field4", RLookupKeyFlags::empty());
        assert!(d_key4.is_some());

        let d_key5 = dest.get_key_read(c"field5", RLookupKeyFlags::empty());
        assert!(d_key5.is_some());
    }

    /// Asserts that if a key already exists in `dst` AND the `Override` flag is set, it will override that key.
    /// This is an explicit override behavior, and thus the flag must be given as parameter to add_keys_from.
    #[test]
    fn rlookup_add_keys_from_override_existing() {
        let mut src = RLookup::new();
        src.get_key_write(c"foo", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"bar", RLookupKeyFlags::empty()).unwrap();
        let src_baz = &raw const *src
            .get_key_write(c"baz", make_bitflags!(RLookupKeyFlag::ExplicitReturn))
            .unwrap();

        let mut dst = RLookup::new();
        let old_dst_baz = &raw const *dst.get_key_write(c"baz", RLookupKeyFlags::empty()).unwrap();

        assert_eq!(dst.get_row_len(), 1);
        dst.add_keys_from(&src, make_bitflags!(RLookupKeyFlag::Override));
        assert_eq!(dst.get_row_len(), 3);

        assert!(dst.keys.find_by_name(c"foo").is_some());
        assert!(dst.keys.find_by_name(c"bar").is_some());
        assert!(dst.keys.find_by_name(c"baz").is_some());
        let dst_baz = dst
            .keys
            .find_by_name(c"baz")
            .unwrap()
            .into_current()
            .unwrap();

        // the new key should have a different address than both src and old dst keys
        assert!(!ptr::addr_eq(src_baz, &raw const *dst_baz));
        assert!(!ptr::addr_eq(old_dst_baz, &raw const *dst_baz));

        // BUT the new key should contain the `src` flags
        assert!(dst_baz.flags == make_bitflags!(RLookupKeyFlag::{ExplicitReturn | QuerySrc}));
    }

    /// Asserts that if a key already exists in `dst` AND the `Override` flag is NOT set, it will skip copying that key.
    /// That is default override behavior: the existing key is kept.
    #[test]
    fn rlookup_add_keys_from_skip_existing() {
        let mut src = RLookup::new();
        src.get_key_write(c"foo", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"bar", RLookupKeyFlags::empty()).unwrap();
        let src_baz = &raw const *src.get_key_write(c"baz", RLookupKeyFlags::empty()).unwrap();

        let mut dst = RLookup::new();
        let old_dst_baz = &raw const *dst
            .get_key_write(c"baz", make_bitflags!(RLookupKeyFlag::ExplicitReturn))
            .unwrap();

        assert_eq!(dst.get_row_len(), 1);
        dst.add_keys_from(&src, RLookupKeyFlags::empty());
        assert_eq!(dst.get_row_len(), 3);

        assert!(dst.keys.find_by_name(c"foo").is_some());
        assert!(dst.keys.find_by_name(c"bar").is_some());
        assert!(dst.keys.find_by_name(c"baz").is_some());
        let dst_baz = dst
            .keys
            .find_by_name(c"baz")
            .unwrap()
            .into_current()
            .unwrap();

        // the new key should have a different address than the src key
        assert!(!ptr::addr_eq(src_baz, &raw const *dst_baz));
        // but the same address as the old baz
        assert!(ptr::addr_eq(old_dst_baz, &raw const *dst_baz));
        // and should still contain all the old flags
        assert!(dst_baz.flags == make_bitflags!(RLookupKeyFlag::{ExplicitReturn | QuerySrc}));
    }

    /// Keys named after the schema rule's special fields (score, lang, payload)
    /// are marked [`RLookupKeyFlag::Hidden`] — retroactively when the spec cache
    /// is attached, and at creation for keys made afterwards — so the reply path
    /// can filter them by flag alone, without reaching for the schema rule.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn rule_special_fields_hidden_at_creation_and_retroactively() {
        let mut rlookup = RLookup::new();
        rlookup
            .get_key_write(c"a", RLookupKeyFlags::empty())
            .unwrap();
        rlookup
            .get_key_write(c"score", RLookupKeyFlags::empty())
            .unwrap();

        // Without a spec cache recording special fields, nothing is hidden.
        for name in [c"a", c"score"] {
            let key = rlookup
                .keys
                .find_by_name(name)
                .unwrap()
                .into_current()
                .unwrap();
            assert!(!key.flags.contains(RLookupKeyFlag::Hidden));
        }

        // Attached after the keys exist: retro-marks `score` as hidden.
        let spcache = crate::IndexSpecCache::from_fields_and_rule(
            [],
            Some(c"lang"),
            Some(c"score"),
            Some(c"payload"),
        );
        rlookup.set_cache(Some(spcache));

        let score = rlookup
            .keys
            .find_by_name(c"score")
            .unwrap()
            .into_current()
            .unwrap();
        assert!(score.flags.contains(RLookupKeyFlag::Hidden));
        let a = rlookup
            .keys
            .find_by_name(c"a")
            .unwrap()
            .into_current()
            .unwrap();
        assert!(!a.flags.contains(RLookupKeyFlag::Hidden));

        // Keys created while the cache is attached are hidden at creation.
        rlookup
            .get_key_write(c"lang", RLookupKeyFlags::empty())
            .unwrap();
        rlookup
            .get_key_write(c"b", RLookupKeyFlags::empty())
            .unwrap();
        rlookup
            .get_key_write(c"payload", RLookupKeyFlags::empty())
            .unwrap();

        for (name, hidden) in [(c"lang", true), (c"b", false), (c"payload", true)] {
            let key = rlookup
                .keys
                .find_by_name(name)
                .unwrap()
                .into_current()
                .unwrap();
            assert_eq!(
                key.flags.contains(RLookupKeyFlag::Hidden),
                hidden,
                "key {name:?}"
            );
        }
    }

    /// Test that the Hidden flag is properly handled when adding keys from one lookup to another.
    /// Verifies that:
    /// 1. The Hidden flag is preserved when copying keys
    /// 2. The Override flag allows overriding an existing hidden key with a non-hidden key
    #[test]
    fn rlookup_add_keys_from_hidden_flag_handling() {
        // Create source and destination lookups
        let mut src1 = RLookup::new();
        let mut src2 = RLookup::new();
        let mut dest = RLookup::new();

        // Create key in src1 with Hidden flag
        let src1_key = src1
            .get_key_write(c"test_field", make_bitflags!(RLookupKeyFlag::Hidden))
            .expect("writing test_field to src1 failed");
        assert!(src1_key.flags.contains(RLookupKeyFlag::Hidden));

        // Add src1 keys first - test flag preservation
        dest.add_keys_from(&src1, RLookupKeyFlags::empty());
        assert_eq!(dest.get_row_len(), 1);

        let dest_key_after_src1 = dest
            .get_key_read(c"test_field", RLookupKeyFlags::empty())
            .expect("test_field cannot be read from dst");
        assert!(dest_key_after_src1.flags.contains(RLookupKeyFlag::Hidden));

        // Create same key name in src2 WITHOUT Hidden flag
        let src2_key = src2
            .get_key_write(c"test_field", RLookupKeyFlags::empty())
            .expect("writing test_field to src2 failed");
        assert!(!src2_key.flags.contains(RLookupKeyFlag::Hidden));

        // Store pointer to original dest key to check override behavior, without getting
        // borrow checker involved, this gives a false positive in Miri.
        #[cfg(not(miri))]
        let original_dest_key_ptr = std::ptr::from_ref(dest_key_after_src1);
        // Add src2 keys with Override flag - test flag override behavior
        dest.add_keys_from(&src2, make_bitflags!(RLookupKeyFlag::Override));
        assert_eq!(dest.get_row_len(), 1);

        // Verify the key was overridden
        let dest_key_after_src2 = dest
            .get_key_read(c"test_field", RLookupKeyFlags::empty())
            .expect("test_field cannot be read from dst after src2 add");

        #[cfg(not(miri))]
        {
            // Verify override happened (should point to new key object after override)
            assert!(!ptr::addr_eq(
                original_dest_key_ptr,
                &raw const *dest_key_after_src2
            ));
            assert_eq!(
                unsafe {
                    (original_dest_key_ptr.as_ref())
                        .expect("pointer is null")
                        .name
                },
                std::ptr::null_mut()
            );
        }

        // Verify Hidden flag is now gone (src2 overwrote src1's hidden status)
        assert!(!dest_key_after_src2.flags.contains(RLookupKeyFlag::Hidden));
    }

    // A sealed lookup keeps accepting new keys through every get_key_* entry
    // point (the execution-time paths are append-only), but returns existing
    // keys untouched.
    #[test]
    fn sealed_rlookup_allows_appends() {
        let mut rlookup = RLookup::new();
        rlookup
            .get_key_write(c"existing", RLookupKeyFlags::empty())
            .unwrap();
        rlookup.seal();
        assert!(rlookup.is_sealed());

        // get_key_write: append arm.
        assert!(
            rlookup
                .get_key_write(c"written", RLookupKeyFlags::empty())
                .is_some()
        );
        // get_key_write: exclusive-mode arm (existing key, no Override) — no
        // mutation, no panic.
        assert!(
            rlookup
                .get_key_write(c"existing", RLookupKeyFlags::empty())
                .is_none()
        );
        // get_key_load: append arm (hash `load_all` on a first document).
        assert!(
            rlookup
                .get_key_load(c"loaded", c"loaded_path", RLookupKeyFlags::empty())
                .is_some()
        );
        // get_key_read: find arm.
        assert!(
            rlookup
                .get_key_read(c"existing", RLookupKeyFlags::empty())
                .is_some()
        );
    }

    #[test]
    #[should_panic(expected = "sealed")]
    fn sealed_rlookup_forbids_get_key_write_override() {
        let mut rlookup = RLookup::new();
        rlookup
            .get_key_write(c"foo", RLookupKeyFlags::empty())
            .unwrap();
        rlookup.seal();

        rlookup.get_key_write(c"foo", make_bitflags!(RLookupKeyFlag::Override));
    }

    #[test]
    #[should_panic(expected = "sealed")]
    fn sealed_rlookup_forbids_set_cache() {
        let mut rlookup = RLookup::new();
        rlookup.seal();

        rlookup.set_cache(None);
    }

    // Marking a found key as explicit-return mutates it in place, which a
    // sealed lookup forbids ...
    #[test]
    #[should_panic(expected = "sealed")]
    fn sealed_rlookup_forbids_explicit_return_marking() {
        let mut rlookup = RLookup::new();
        rlookup
            .keys
            .push(RLookupKey::new(c"foo", RLookupKeyFlag::IsLoaded.into()));
        rlookup.seal();

        rlookup.get_key_load(c"foo", c"foo", RLookupKeyFlag::ExplicitReturn.into());
    }

    // ... but reaching the same arm with nothing to add must stay a no-op:
    // the JSON `load_all` path calls get_key_load with empty flags for every
    // document after the first, on a sealed lookup.
    #[test]
    fn sealed_rlookup_allows_noop_load_of_loaded_key() {
        let mut rlookup = RLookup::new();
        rlookup
            .keys
            .push(RLookupKey::new(c"foo", RLookupKeyFlag::IsLoaded.into()));
        rlookup.seal();

        assert!(
            rlookup
                .get_key_load(c"foo", c"foo", RLookupKeyFlags::empty())
                .is_none()
        );

        // Same when the flag to add is already present on the key.
        let mut rlookup = RLookup::new();
        rlookup.keys.push(RLookupKey::new(
            c"bar",
            make_bitflags!(RLookupKeyFlag::{IsLoaded | ExplicitReturn}),
        ));
        rlookup.seal();

        assert!(
            rlookup
                .get_key_load(c"bar", c"bar", RLookupKeyFlag::ExplicitReturn.into())
                .is_none()
        );
    }

    #[cfg(not(miri))]
    proptest! {
         // assert that a key can in the keylist can be retrieved by its name
         #[test]
         fn rlookup_get_key_read_found(name in "\\PC+") {
             let name = CString::new(name).unwrap();

             let mut rlookup = RLookup::new();

             let key = RLookupKey::new(&name, RLookupKeyFlags::empty());

             rlookup.keys.push(key);

             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty())
                 .unwrap();
             assert_eq!(key.name().as_ref(), name.as_ref());
             assert!(key.path().is_none());
         }

         // Assert that a key cannot be retrieved by any other string
         #[test]
         fn rlookup_get_key_read_not_found(name in "\\PC+", wrong_name in "\\PC+") {
            let name = CString::new(name).unwrap();
            let wrong_name = CString::new(wrong_name).unwrap();

            if wrong_name == name {
                // skip this test if the wrong name is the same as the name
                return Ok(());
            }

             let mut rlookup = RLookup::new();

             let key = RLookupKey::new(&name, RLookupKeyFlags::empty());
             rlookup.keys.push(key);

             let not_key = rlookup
                 .get_key_read(&wrong_name, RLookupKeyFlags::empty());
             prop_assert!(not_key.is_none());
         }

         // Assert that - if the key cannot be found in the rlookups keylist - it will be loaded from the index spec cache
         // and inserted into the list
         #[test]
         fn rlookup_get_key_read_not_found_spcache_hit(name in "\\PC+", path in "\\PC+", sort_idx in 0i16..i16::MAX) {
             let name = CString::new(name).unwrap();
             let path = CString::new(path).unwrap();

             let mut rlookup = RLookup::new();

             let spcache = IndexSpecCache::from_fields([
                 FieldSpecBuilder::new(&path)
                 .with_field_name(&name)
                 .with_sort_idx(sort_idx)
                 .with_options(make_bitflags!(FieldSpecOption::{
                     Sortable | Unf
                 }))
                 .finish()
             ]);

             rlookup.set_cache(Some(spcache));

             // the first call will load from the index spec cache
             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty()).unwrap();

             assert_eq!(key.name().as_ref(), name.as_c_str());
             assert_eq!(key.path().as_ref().unwrap().as_ref(), path.as_c_str());

             // the second call will load from the keylist
             // to ensure this we zero out the cache
             // NB: we need to keep the spec cache alive here for the scope of this test
             // otherwise the underlying hidden strings that the keys borrow their names from are freed
             // and we use-after-free. In production code this cannot happen as - once set - the spec cache
             // will never be removed from the rlookup.
             let _spec_cache = rlookup.index_spec_cache.take();

             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty())
                 .unwrap();
             assert_eq!(key.name().as_ref(), name.as_c_str());
             assert_eq!(key.path().as_ref().unwrap().as_ref(), path.as_c_str());
         }

        // Assert that, even though there is a key in the list AND a a field space in the cache, we won't load the key
        // if it is a wrong name, i.e. a name that's neither part of the list nor the cache.
         #[test]
         fn rlookup_get_key_read_not_found_no_spcache_hit(name1 in "\\PC+", name2 in "\\PC+", wrong_name in "\\PC+") {
             let name1 = CString::new(name1).unwrap();
             let name2 = CString::new(name2).unwrap();
             let wrong_name = CString::new(wrong_name).unwrap();

            if name1 == wrong_name || name2 == wrong_name {
                // skip this test if the wrong name is the same as one of the other random names
                return Ok(());
            }

             let mut rlookup = RLookup::new();

             // push a key to the keylist
             let key = RLookupKey::new(&name1, RLookupKeyFlags::empty());
             rlookup.keys.push(key);

             // push a field spec to the cache
             let spcache = IndexSpecCache::from_fields([
                 FieldSpecBuilder::new(&name2).finish()
             ]);

             // set the cache as the rlookup cache
             rlookup.set_cache(Some(spcache));

             let not_key = rlookup.get_key_read(&wrong_name, RLookupKeyFlags::empty());
             prop_assert!(not_key.is_none());
         }

        // Assert that, even though there is a key in the list AND a a field space in the cache, we won't load the key
        // if it is a wrong name, however if the flag `AllowUnresolved` is set, we will create an unresolved key instead.
         #[test]
         fn rlookup_get_key_read_not_found_no_spcache_hit_allow_unresolved(name1 in "\\PC+", name2 in "\\PC+", wrong_name in "\\PC+") {
             let name1 = CString::new(name1).unwrap();
             let name2 = CString::new(name2).unwrap();
             let wrong_name = CString::new(wrong_name).unwrap();

            if name1 == wrong_name || name2 == wrong_name {
                // skip this test if the wrong name is the same as one of the other random names
                return Ok(());
            }

             let mut rlookup = RLookup::new();

             let key = RLookupKey::new(&name1, RLookupKeyFlags::empty());

             rlookup.keys.push(key);

             // push a field spec to the cache
             let spcache = IndexSpecCache::from_fields([
                 FieldSpecBuilder::new(&name2).finish()
             ]);

             // set the cache as the rlookup cache
             rlookup.set_cache(Some(spcache));

             // set the AllowUnresolved option to allow unresolved keys in this rlookup
             rlookup.options.set(RLookupOption::AllowUnresolved, true);

             let key = rlookup.get_key_read(&wrong_name, RLookupKeyFlags::empty()).unwrap();
             prop_assert!(key.flags.contains(RLookupKeyFlag::Unresolved));
             prop_assert_eq!(key.name, wrong_name.as_ptr());
             prop_assert_eq!(key.name().as_ref(), wrong_name.as_c_str());
             prop_assert_eq!(key.path, wrong_name.as_ptr());
             prop_assert!(key.path().is_none());
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn create_keys_from_spec() {
        // Arrange
        let mut index_spec = unsafe { MaybeUninit::<ffi::IndexSpec>::zeroed().assume_init() };

        let mut schema_rule = unsafe { MaybeUninit::<ffi::SchemaRule>::zeroed().assume_init() };
        let mut filter_fields_index = [-1, 0, 1];
        schema_rule.filter_fields_index = filter_fields_index.as_mut_ptr();
        schema_rule.filter_fields =
            rs_array([c"ff0", c"ff1", c"ff2"].map(|str| str.as_ptr().cast_mut()));

        index_spec.rule = ptr::from_mut(&mut schema_rule);

        let mut field_specs = [
            field_spec(c"fn0", c"fp0"),
            field_spec(c"fn1", c"fp1"),
            field_spec(c"fn2", c"fp2"),
        ];
        index_spec.fields = field_specs.as_mut_ptr();
        index_spec.numFields = field_specs.len().try_into().unwrap();

        let index_spec = unsafe { IndexSpec::from_raw(&raw const index_spec) };

        // Act
        let mut actual = super::create_keys_from_spec(index_spec);

        // Assert
        assert_eq!(actual.len(), 3);

        let key = actual.next().unwrap();
        assert_eq!(key.name(), c"ff0");
        assert_eq!(key.path(), &Some(c"ff0".into()));

        let key = actual.next().unwrap();
        assert_eq!(key.name(), c"fn0");
        assert_eq!(key.path(), &Some(c"fp0".into()));

        let key = actual.next().unwrap();
        assert_eq!(key.name(), c"fn1");
        assert_eq!(key.path(), &Some(c"fp1".into()));

        // Clean up
        unsafe { ffi::array_free(schema_rule.filter_fields.cast()) };
        for fs in field_specs {
            unsafe {
                ffi::HiddenString_Free(fs.fieldName, false);
            }
            unsafe {
                ffi::HiddenString_Free(fs.fieldPath, false);
            }
        }
    }

    /// Create a C array from a fixed-size Rust array using the C `array_new_sz` function.
    fn rs_array<const N: usize, T: Copy>(fields: [T; N]) -> *mut T {
        let arr = unsafe {
            let size_t_u16 = const { size_of::<T>() as u16 };
            let len_u32 = const { N as u32 };

            ffi::array_new_sz(size_t_u16, 0, len_u32).cast::<T>()
        };

        unsafe {
            let elements = std::slice::from_raw_parts_mut(arr, fields.len());
            elements.copy_from_slice(&fields);
        }

        arr
    }

    fn field_spec(field_name: &CStr, field_path: &CStr) -> ffi::FieldSpec {
        let mut res = unsafe { MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init() };
        res.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        res.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        res
    }

    /// Build an [`RLookup`] whose key list mirrors the selection cases exercised by
    /// `schema_src_keys`: a non-schema key (must never be selected), a schema key
    /// without a sorting-vector source, and a schema key with one.
    fn rlookup_with_selection_keys<'a>() -> RLookup<'a> {
        let mut rlookup = RLookup::new();
        rlookup.keys.push(RLookupKey::new(
            c"query_only",
            make_bitflags!(RLookupKeyFlag::QuerySrc),
        ));
        rlookup.keys.push(RLookupKey::new(
            c"schema_no_sv",
            make_bitflags!(RLookupKeyFlag::SchemaSrc),
        ));
        rlookup.keys.push(RLookupKey::new(
            c"schema_sv",
            make_bitflags!(RLookupKeyFlag::{SchemaSrc | SvSrc}),
        ));
        rlookup
    }

    fn selected_names<'a>(
        rlookup: &'a RLookup<'a>,
        cached_only: bool,
        force_load: bool,
    ) -> Vec<CString> {
        rlookup
            .schema_src_keys(cached_only, force_load)
            .map(|k| k.name().as_ref().to_owned())
            .collect()
    }

    // Non-schema keys (e.g. QuerySrc) are never selected for individual loading.
    #[test]
    fn schema_src_keys_excludes_non_schema_keys() {
        let rlookup = rlookup_with_selection_keys();
        let names = selected_names(&rlookup, false, false);
        assert_eq!(
            names,
            vec![c"schema_no_sv".to_owned(), c"schema_sv".to_owned()]
        );
    }

    // `cached_only && !force_load` restricts the selection to sorting-vector-backed keys.
    #[test]
    fn schema_src_keys_cached_only_restricts_to_sv_src() {
        let rlookup = rlookup_with_selection_keys();
        let names = selected_names(&rlookup, true, false);
        assert_eq!(names, vec![c"schema_sv".to_owned()]);
    }

    // `force_load` overrides `cached_only`: all schema keys are selected again.
    #[test]
    fn schema_src_keys_force_load_overrides_cached_only() {
        let rlookup = rlookup_with_selection_keys();
        let names = selected_names(&rlookup, true, true);
        assert_eq!(
            names,
            vec![c"schema_no_sv".to_owned(), c"schema_sv".to_owned()]
        );
    }

    // Without `cached_only`, the SvSrc flag has no effect on the selection.
    #[test]
    fn schema_src_keys_not_cached_only_ignores_sv_src() {
        let rlookup = rlookup_with_selection_keys();
        assert_eq!(
            selected_names(&rlookup, false, true),
            vec![c"schema_no_sv".to_owned(), c"schema_sv".to_owned()]
        );
        assert_eq!(
            selected_names(&rlookup, false, false),
            vec![c"schema_no_sv".to_owned(), c"schema_sv".to_owned()]
        );
    }
}
