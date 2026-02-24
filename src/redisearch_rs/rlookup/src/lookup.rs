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

use crate::{
    IndexSpec, OpaqueRLookupRow,
    bindings::{FieldSpecOption, FieldSpecOptions, IndexSpecCache},
    field_spec::FieldSpec,
};
use enumflags2::{BitFlags, bitflags};
use key_list::KeyList;
use std::{borrow::Cow, ffi::CStr, pin::Pin, ptr};

pub use key::{GET_KEY_FLAGS, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, TRANSIENT_FLAGS};
pub use key_list::{Cursor, CursorMut};

#[bitflags]
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RLookupOption {
    /// If the key cannot be found, do not mark it as an error, but create it and
    /// mark it as F_UNRESOLVED
    AllowUnresolved = 0x01,

    /// If a loader was added to load the entire document, this flag will allow
    /// later calls to GetKey in read mode to create a key (from the schema) even if it is not sortable
    AllLoaded = 0x02,
}

/// Helper type to represent a set of [`RLookupOption`]s.
/// cbindgen:ignore
pub type RLookupOptions = BitFlags<RLookupOption>;

/// An append-only list of [`RLookupKey`]s.
///
/// This type maintains a mapping from string names to [`RLookupKey`]s.
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

    pub fn set_cache(&mut self, spcache: Option<IndexSpecCache>) {
        self.index_spec_cache = spcache;
    }

    pub fn disable_options(&mut self, options: RLookupOptions) {
        self.options &= !options;
    }

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
    pub fn add_keys_from(&mut self, src: &RLookup<'a>, flags: RLookupKeyFlags) {
        debug_assert!(
            !flags.contains(RLookupKeyFlag::NameAlloc),
            "The NameAlloc flag should have been handled in the FFI function. This is a bug."
        );

        // Manually iterate through all keys including hidden ones
        let mut c = src.cursor();
        while let Some(src_key) = c.current() {
            if !src_key.is_tombstone() {
                // Combine caller's control flags with source key's persistent properties
                // Only preserve non-transient flags from source (F_SVSRC, F_HIDDEN, etc.)
                // while respecting caller's control flags (F_OVERRIDE, F_FORCE_LOAD, etc.)
                let combined_flags = flags | src_key.flags & !TRANSIENT_FLAGS;

                // NB: get_key_write returns none if the key already exists and `flags` don't contain `Override`.
                // In this case, we just want to move on to the next key
                let _ = self.get_key_write(src_key.name().clone(), combined_flags);
            }

            c.move_next();
        }
    }

    /// Returns a [`Cursor`] starting at the first key.
    ///
    /// The [`Cursor`] type can be used as Iterator over the keys in this lookup.
    #[inline(always)]
    pub fn cursor(&self) -> Cursor<'_, 'a> {
        self.keys.cursor_front()
    }

    /// Returns a [`Cursor`] starting at the first key.
    ///
    /// The [`Cursor`] type can be used as Iterator over the keys in this lookup.
    #[inline(always)]
    pub fn cursor_mut(&mut self) -> CursorMut<'_, 'a> {
        self.keys.cursor_front_mut()
    }

    // ===== Get key for reading (create only if in schema and sortable) =====

    /// Gets a key by its name from the lookup table, if not found it uses the schema as a fallback to search the key.
    ///
    /// If the flag `RLookupKeyFlag::AllowUnresolved` is set, it will create a new key if it does not exist in the lookup table
    /// nor in the schema.
    pub fn get_key_read(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        flags &= GET_KEY_FLAGS;

        let name = name.into();

        let available = self.keys.find_by_name(&name).is_some();
        if available {
            // FIXME: We cannot use let-some above because of a borrow-checker false positive.
            // This duplication might have performance implications.
            // See <https://github.com/rust-lang/rust/issues/54663>
            return self.keys.find_by_name(&name).unwrap().into_current();
        }

        // If we didn't find the key at the lookup table, check if it exists in
        // the schema as SORTABLE, and create only if so.
        let name = match self.gen_key_from_spec(name, flags) {
            Ok(key) => {
                let key = self.keys.push(key);

                // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned immutable reference.
                return Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) });
            }
            Err(name) => name,
        };

        // If we didn't find the key in the schema (there is no schema) and unresolved is OK, create an unresolved key.
        if self.options.contains(RLookupOption::AllowUnresolved) {
            let mut key = RLookupKey::new(name, flags);
            key.flags |= RLookupKeyFlag::Unresolved;

            let key = self.keys.push(key);

            // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned immutable reference.
            return Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) });
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
        Ok(key)
    }

    /// Writes a key to the lookup table. If the key already exists
    /// - it is overwritten and returned if flags are set to `RLookupKeyFlag::Override`
    /// - `None` is returned if the key is in exclusive mode (the opposite of Override)
    ///
    /// This will never get a key from the cache, it will either create a new key, override an existing key or return `None` if the key
    /// is in exclusive mode.
    pub fn get_key_write(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        let name = name.into();

        if let Some(c) = self.keys.find_by_name_mut(&name) {
            // A. we found the key in the lookup table:
            if flags.contains(RLookupKeyFlag::Override) {
                // We are in create mode, overwrite the key (remove schema related data, mark with new flags)
                c.override_current(flags | RLookupKeyFlag::QuerySrc)
                    .unwrap();
            } else {
                // We are in exclusive mode, return None
                return None;
            }
        } else {
            // B. we didn't find the key in the lookup table:
            // create a new key with the name and flags
            let key = RLookupKey::new(name.clone(), flags | RLookupKeyFlag::QuerySrc);
            self.keys.push(key);
        };

        // FIXME: Duplication because of borrow-checker false positive. Duplication means performance implications.
        // See <https://github.com/rust-lang/rust/issues/54663>
        let cursor = self
            .keys
            .find_by_name(&name)
            .expect("key should have been created above");
        Some(cursor.into_current().unwrap())
    }

    // ===== Load key from redis keyspace (include known information on the key, fail if already loaded) =====

    pub fn get_key_load(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        field_name: &'a CStr,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        let name = name.into();

        // 1. if the key is already loaded, or it has created by earlier RP for writing, return NULL (unless override was requested)
        // 2. create a new key with the name of the field, and mark it as doc-source.
        // 3. if the key is in the schema, mark it as schema-source and apply all the relevant flags according to the field spec.
        // 4. if the key is "loaded" at this point (in schema, sortable and un-normalized), create the key but return NULL
        //    (no need to load it from the document).

        // Ensure the key is available, if it is check for flags and return None or override the key depending on flags, if key not available insert it.
        if let Some(mut c) = self.keys.find_by_name_mut(&name) {
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

                let key = key.project();

                // If the caller wanted to mark this key as explicit return, mark it as such even if we don't return it.
                key.header.flags |= flags & RLookupKeyFlag::ExplicitReturn;

                return None;
            } else {
                c.override_current(flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded)
                    .unwrap();
            }
        } else {
            let key = RLookupKey::new(
                name.clone(),
                flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded,
            );
            self.keys.push(key);
        };

        // FIXME: Duplication because of borrow-checker false positive. Duplication means performance implications.
        // See <https://github.com/rust-lang/rust/issues/54663>
        let mut cursor = self
            .keys
            .find_by_name_mut(&name)
            .expect("key should have been created above");
        let key = if let Some(fs) = self
            .index_spec_cache
            .as_ref()
            .and_then(|spcache| spcache.find_field(&name))
        {
            let key = cursor.into_current().unwrap();
            key.update_from_field_spec(fs);

            if key.flags.contains(RLookupKeyFlag::ValAvailable)
                && !flags.contains(RLookupKeyFlag::ForceLoad)
            {
                // If the key is marked as "value available", it means that it is sortable and un-normalized.
                // so we can use the sorting vector as the source, and we don't need to load it from the document.
                return None;
            }
            key
        } else {
            // Field not found in the schema.
            let key = cursor.current().unwrap();
            let is_borrowed = matches!(key.name(), Cow::Borrowed(_));

            // We assume `field_name` is the path to load from in the document.
            if is_borrowed {
                key.set_path(Cow::Borrowed(field_name));
            } else if name.as_ref() != field_name {
                let field_name: Cow<'_, CStr> = Cow::Owned(field_name.to_owned());
                key.set_path(field_name);
            } // else
            // If the caller requested to allocate the name, and the name is the same as the path,
            // it was already set to the same allocation for the name, so we don't need to do anything.

            cursor.into_current().unwrap()
        };

        Some(key)
    }

    /// The row len of the [`RLookup`] is the number of keys in its key list not counting the overridden keys.
    pub const fn get_row_len(&self) -> u32 {
        self.keys.rowlen
    }

    pub fn load_rule_fields(
        &mut self,
        search_ctx: &mut ffi::RedisSearchCtx,
        dst_row: &mut OpaqueRLookupRow,
        index_spec: &'a IndexSpec,
        key: &CStr,
        status: &mut ffi::QueryError,
    ) -> i32 {
        let keys = create_keys_from_spec(index_spec);
        let pushed_keys = keys.into_iter().map(|k| self.keys.push(k)).collect();
        load_specific_keys(
            self,
            search_ctx,
            dst_row,
            index_spec,
            key,
            pushed_keys,
            status,
        )
    }
}

fn create_keys_from_spec<'a>(index_spec: &'a IndexSpec) -> Vec<RLookupKey<'a>> {
    // TODO: Consider returning `impl Iterator` in order to avoid the `collect()` allocation below, refer to Jira ticket MOD-13907.
    let rule = index_spec.rule();
    let field_specs = index_spec.field_specs();
    rule.filter_fields_index()
        .iter()
        .zip(rule.filter_fields())
        .map(|(&index, filter_field)| create_key_from_data(index, filter_field, field_specs))
        .collect::<Vec<_>>()
}

fn create_key_from_data<'a>(
    index: i32,
    filter_field: &'a CStr,
    field_specs: &'a [FieldSpec],
) -> RLookupKey<'a> {
    const NO_MATCH: i32 = -1;
    if NO_MATCH == index {
        RLookupKey::new(filter_field, RLookupKeyFlags::empty())
    } else {
        let index = usize::try_from(index).expect("index must be positive and fit into usize");
        let field_spec = &field_specs[index];
        let field_name = field_spec.field_name().into_secret_value();
        let path = field_spec.field_path().into_secret_value();

        RLookupKey::new_with_path(field_name, path, RLookupKeyFlags::empty())
    }
}

fn load_specific_keys<'a>(
    lookup: &mut RLookup<'a>,
    search_ctx: &mut ffi::RedisSearchCtx,
    dst_row: &mut OpaqueRLookupRow,
    index_spec: &IndexSpec,
    key: &CStr,
    keys: Vec<Pin<&mut RLookupKey>>,
    status: &mut ffi::QueryError,
) -> i32 {
    let lookup = lookup.as_opaque_mut_ptr().cast::<ffi::RLookup>();
    let dst_row = ptr::from_mut(dst_row).cast::<ffi::RLookupRow>();

    let mut keys = keys
        .into_iter()
        .map(|k| {
            // Safety: `ffi::RLookupLoadOptions` requires a mutable pointer to the key array. We have full control over these keys as we handle them in the keylist. The following statements are not optimal, but will have to do for now.
            let k = unsafe { Pin::into_inner_unchecked(k.into_ref()) };
            ptr::from_ref(k).cast::<ffi::RLookupKey>()
        })
        .collect::<Vec<_>>();

    let mut options = ffi::RLookupLoadOptions {
        keys: keys.as_mut_ptr(),
        nkeys: keys.len(),
        sctx: ptr::from_mut(search_ctx),
        keyPtr: key.as_ptr(),
        type_: index_spec.rule().type_(),
        status: ptr::from_mut(status),
        forceLoad: true,
        mode: ffi::RLookupLoadFlags_RLOOKUP_LOAD_KEYLIST,
        dmd: ptr::null(),
        forceString: false,
    };

    // Safety: All pointers passed to this function are non-null and properly aligned since we created them above in this function.
    unsafe { ffi::loadIndividualKeys(lookup, dst_row, &mut options) }
}

pub mod opaque {
    use super::RLookup;
    use c_ffi_utils::opaque::Size;
    /// An opaque lookup which can be passed by value to C.
    ///
    /// The size and alignment of this struct must match the Rust `RLookup`
    /// structure exactly.
    #[repr(C, align(8))]
    pub struct OpaqueRLookup(Size<40>);

    c_ffi_utils::opaque!(RLookup<'_>, OpaqueRLookup);
}

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod tests {
    use super::*;

    use crate::mock::{array_free, array_new};

    use std::{ffi::CString, mem::MaybeUninit};
    use std::{ffi::c_char, ptr};

    use enumflags2::make_bitflags;
    use pretty_assertions::assert_eq;
    #[cfg(not(miri))]
    use proptest::prelude::*;

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
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key.path().as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    // Assert that a key can be retrieved by its name and is been overridden with the `DocSrc` and `IsLoaded` flags.
    #[test]
    fn rlookup_get_key_load_override_no_field_in_cache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        // we don't use the cache
        let empty_field_array = [];
        let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

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
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key.path().as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    // Assert that a key can be retrieved by its name and is been overridden with the `DocSrc` and `IsLoaded` flags.
    #[cfg(not(miri))] // uses strncmp under the hood for HiddenString
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let mut arr = unsafe { [MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init()] };
        let field_name = key_name;
        arr[0].fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = cache_field_name;
        arr[0].fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        arr[0].set_options(ffi::FieldSpecOptions_FieldSpec_Sortable);
        arr[0].sortIdx = 12;
        let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

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
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, cache_field_name.as_ptr());
        assert_eq!(
            retrieved_key.path().as_ref().unwrap().as_ref(),
            cache_field_name
        );
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));

        // cleanup
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldPath, false);
        }
    }

    #[cfg(not(miri))] // uses strncmp under the hood for HiddenString
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache_but_value_availabe() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let mut arr = unsafe { [MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init()] };
        let field_name = key_name;
        arr[0].fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = cache_field_name;
        arr[0].fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        arr[0].set_options(
            ffi::FieldSpecOptions_FieldSpec_Sortable | ffi::FieldSpecOptions_FieldSpec_UNF,
        );
        arr[0].sortIdx = 12;
        let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let key = RLookupKey::new(key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup.get_key_load(
            key_name,
            field_name,
            make_bitflags!(RLookupKeyFlag::Override),
        );

        // we should access the sorting vector instead
        assert!(retrieved_key.is_none());

        // cleanup
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldPath, false);
        }
    }

    #[cfg(not(miri))] // uses strncmp under the hood for HiddenString
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache_but_value_availabe_however_force_load() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let mut arr = unsafe { [MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init()] };
        let field_name = key_name;
        arr[0].fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = cache_field_name;
        arr[0].fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        arr[0].set_options(
            ffi::FieldSpecOptions_FieldSpec_Sortable | ffi::FieldSpecOptions_FieldSpec_UNF,
        );
        arr[0].sortIdx = 12;
        let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(spcache));

        let key = RLookupKey::new(key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::{Override | ForceLoad}),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key.name().as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, cache_field_name.as_ptr());
        assert_eq!(
            retrieved_key.path().as_ref().unwrap().as_ref(),
            cache_field_name
        );
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));

        // cleanup
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldPath, false);
        }
    }

    // Assert the the cases in which None is returned also the key could be found
    #[test]
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
            // we don't use the cache
            let empty_field_array = [];
            let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

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
    fn rlookup_get_load_key_on_empty_rlookup_and_cache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        // we don't use the cache
        let empty_field_array = [];
        let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

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
    fn rlookup_get_load_key_name_equals_field_name() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"key_no_cache";

        // we don't use the cache
        let empty_field_array = [];
        let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

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

             let mut arr = unsafe {
                 [
                     MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init(),
                 ]
             };

             let field_name = name.as_c_str();
             arr[0].fieldName =
                 unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
             let field_path = path.as_c_str();
             arr[0].fieldPath =
                 unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
             arr[0].set_options(
                 ffi::FieldSpecOptions_FieldSpec_Sortable | ffi::FieldSpecOptions_FieldSpec_UNF,
             );
             arr[0].sortIdx = sort_idx;

             let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

             rlookup.set_cache(Some(spcache));

             // the first call will load from the index spec cache
             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty()).unwrap();

             prop_assert_eq!(key.name, name.as_ptr());
             prop_assert_eq!(key.name().as_ref(), name.as_c_str());
             prop_assert_eq!(key.path, path.as_ptr());
             prop_assert_eq!(key.path().as_ref().unwrap().as_ref(), path.as_c_str());

             // the second call will load from the keylist
             // to ensure this we zero out the cache
             rlookup.index_spec_cache = None;

             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty())
                 .unwrap();
             prop_assert_eq!(key.name, name.as_ptr());
             prop_assert_eq!(key.name().as_ref(), name.as_c_str());
             prop_assert_eq!(key.path, path.as_ptr());
             prop_assert_eq!(key.path().as_ref().unwrap().as_ref(), path.as_c_str());

             // cleanup
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldName, false);
             }
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldPath, false);
             }
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
             let mut arr = unsafe {
                 [
                     MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init(),
                 ]
             };

             let field_name = name2.as_c_str();
             arr[0].fieldName =
                 unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };

             let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

             // set the cache as the rlookup cache
             rlookup.set_cache(Some(spcache));

             let not_key = rlookup.get_key_read(&wrong_name, RLookupKeyFlags::empty());
             prop_assert!(not_key.is_none());

             // cleanup
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldName, false);
             }
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
             let mut arr = unsafe {
                 [
                     MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init(),
                 ]
             };

             let field_name = name2.as_c_str();
             arr[0].fieldName =
                 unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };

             let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

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

             // cleanup
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldName, false);
             }
        }
    }

    #[test]
    fn create_keys_from_spec() {
        // Arrange
        let mut index_spec = unsafe { MaybeUninit::<ffi::IndexSpec>::zeroed().assume_init() };

        let mut schema_rule = unsafe { MaybeUninit::<ffi::SchemaRule>::zeroed().assume_init() };
        let mut filter_fields_index = [-1, 0, 1];
        schema_rule.filter_fields_index = filter_fields_index.as_mut_ptr();
        schema_rule.filter_fields = filter_fields_array(&[c"ff0", c"ff1", c"ff2"]);

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
        let actual = super::create_keys_from_spec(index_spec);

        // Assert
        assert_eq!(actual.len(), 3);

        assert_eq!(actual[0].name(), c"ff0");
        assert_eq!(actual[0].path(), &None);

        assert_eq!(actual[1].name(), c"fn0");
        assert_eq!(actual[1].path(), &Some(c"fp0".into()));

        assert_eq!(actual[2].name(), c"fn1");
        assert_eq!(actual[2].path(), &Some(c"fp1".into()));

        // Clean up
        unsafe { array_free(schema_rule.filter_fields) };
        for fs in field_specs {
            unsafe {
                ffi::HiddenString_Free(fs.fieldName, false);
            }
            unsafe {
                ffi::HiddenString_Free(fs.fieldPath, false);
            }
        }
    }

    fn filter_fields_array(filter_fields: &[&CStr]) -> *mut *mut c_char {
        let temp = filter_fields
            .iter()
            .map(|ff| ff.as_ptr().cast_mut())
            .collect::<Vec<_>>();

        array_new(&temp)
    }

    fn field_spec(field_name: &CStr, field_path: &CStr) -> ffi::FieldSpec {
        let mut res = unsafe { MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init() };
        res.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        res.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        res
    }
}
