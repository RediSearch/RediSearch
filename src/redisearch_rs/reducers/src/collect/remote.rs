/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Remote COLLECT reducer.
//!
//! Projects configured fields on each remote node and serializes collected rows
//! for the local merge step.

use rlookup::{RLookupKey, RLookupRow};
use value::{Array, Map, SharedValue, Value};

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Remote COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct RemoteCollectReducer<'a> {
    common: CollectCommon,
    pub(crate) field_keys: Box<[&'a RLookupKey<'a>]>,
    has_wildcard: bool,
    /// Raw sort-key references, including keys not present in `FIELDS`.
    pub(crate) sort_keys: Box<[&'a RLookupKey<'a>]>,
    /// Internal shard replies include sort-key values for coordinator merge.
    is_internal: bool,
}

// `CollectCommon` must live at offset 0 so the C layer can downcast to
// `ffi::Reducer`. Guard against accidental reordering of the struct fields.
const _: () = assert!(core::mem::offset_of!(RemoteCollectReducer<'_>, common) == 0);

/// Per-group instance of [`RemoteCollectReducer`].
///
/// Because `RemoteCollectCtx` is arena-allocated ([`Bump`][bumpalo::Bump] does
/// not run destructors), `ptr::drop_in_place` must be called to run
/// destructors for the inner `Vec`s and decrement `SharedValue` refcounts.
pub struct RemoteCollectCtx {
    field_values: Vec<Vec<SharedValue>>,
    /// Kept row-aligned with `field_values`.
    sort_values: Vec<Vec<SharedValue>>,
}

impl<'a> RemoteCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration.
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        has_wildcard: bool,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
        is_internal: bool,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            field_keys,
            has_wildcard,
            sort_keys,
            is_internal,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    pub fn alloc_instance(&self) -> &mut RemoteCollectCtx {
        self.common.arena.alloc(RemoteCollectCtx::new(self))
    }

    // Temporary C++ parser-test accessors, exposed through `reducers_ffi`.

    pub const fn field_keys_len(&self) -> usize {
        self.field_keys.len()
    }

    pub const fn has_wildcard(&self) -> bool {
        self.has_wildcard
    }

    pub const fn sort_keys_len(&self) -> usize {
        self.sort_keys.len()
    }

    pub const fn sort_asc_map(&self) -> u64 {
        self.common.sort_asc_map
    }

    pub const fn has_limit(&self) -> bool {
        self.common.limit.is_some()
    }

    pub const fn limit_offset(&self) -> u64 {
        match self.common.limit {
            Some((offset, _)) => offset,
            None => 0,
        }
    }

    pub const fn limit_count(&self) -> u64 {
        match self.common.limit {
            Some((_, count)) => count,
            None => 0,
        }
    }
}

impl RemoteCollectCtx {
    pub const fn new(_r: &RemoteCollectReducer) -> Self {
        Self {
            field_values: Vec::new(),
            sort_values: Vec::new(),
        }
    }

    /// Store projected field and sort values, filling missing values with nulls.
    pub fn add(&mut self, r: &RemoteCollectReducer, row: &RLookupRow) {
        let fv = r
            .field_keys
            .iter()
            .map(|key| {
                row.get(key)
                    .cloned()
                    .unwrap_or_else(SharedValue::null_static)
            })
            .collect();
        self.field_values.push(fv);

        let sv = r
            .sort_keys
            .iter()
            .map(|key| {
                row.get(key)
                    .cloned()
                    .unwrap_or_else(SharedValue::null_static)
            })
            .collect();
        self.sort_values.push(sv);
    }

    /// Serialize rows as maps; internal shard replies also include sort keys.
    pub fn finalize(&mut self, r: &RemoteCollectReducer) -> SharedValue {
        let row_maps: Vec<SharedValue> = self
            .field_values
            .drain(..)
            .zip(self.sort_values.drain(..))
            .map(|(fv, sv)| {
                let entries: Box<[_]> = if r.is_internal {
                    // Include sort-key values for coordinator ordering.
                    fv.into_iter()
                        .zip(r.field_keys.iter())
                        .chain(sv.into_iter().zip(r.sort_keys.iter()))
                        .map(|(val, key)| {
                            let name = SharedValue::new_string(key.name().to_bytes().to_vec());
                            (name, val)
                        })
                        .collect()
                } else {
                    fv.into_iter()
                        .zip(r.field_keys.iter())
                        .map(|(val, key)| {
                            let name = SharedValue::new_string(key.name().to_bytes().to_vec());
                            (name, val)
                        })
                        .collect()
                };
                SharedValue::new(Value::Map(Map::new(entries)))
            })
            .collect();
        SharedValue::new(Value::Array(Array::new(row_maps.into_boxed_slice())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rlookup::RLookupKeyFlags;

    #[test]
    fn new_with_no_fields_exposes_empty_configuration() {
        let r = RemoteCollectReducer::new(Box::new([]), false, Box::new([]), 0, None, false);
        assert_eq!(r.field_keys_len(), 0);
        assert!(!r.has_wildcard());
        assert_eq!(r.sort_keys_len(), 0);
        assert_eq!(r.sort_asc_map(), 0);
        assert!(!r.has_limit());
        assert_eq!(r.limit_offset(), 0);
        assert_eq!(r.limit_count(), 0);
        assert!(!r.is_internal);
    }

    #[test]
    fn new_with_limit_and_sort_exposes_configuration() {
        let r =
            RemoteCollectReducer::new(Box::new([]), true, Box::new([]), 0b101, Some((5, 10)), true);
        assert!(r.has_wildcard());
        assert_eq!(r.sort_asc_map(), 0b101);
        assert!(r.has_limit());
        assert_eq!(r.limit_offset(), 5);
        assert_eq!(r.limit_count(), 10);
        assert!(r.is_internal);
    }

    #[test]
    fn external_mode_stores_fields_and_sort_keys_independently() {
        let f1 = RLookupKey::new(c"name", RLookupKeyFlags::empty());
        let f2 = RLookupKey::new(c"color", RLookupKeyFlags::empty());
        let s1 = RLookupKey::new(c"price", RLookupKeyFlags::empty());
        let s2 = RLookupKey::new(c"weight", RLookupKeyFlags::empty());

        let r = RemoteCollectReducer::new(
            Box::new([&f1, &f2]),
            false,
            Box::new([&s1, &s2]),
            0,
            None,
            false,
        );

        assert_eq!(r.field_keys_len(), 2);
        assert_eq!(r.sort_keys_len(), 2);
        assert!(!r.is_internal);
    }

    #[test]
    fn internal_mode_stores_fields_and_sort_keys_independently() {
        let f1 = RLookupKey::new(c"name", RLookupKeyFlags::empty());
        let s1 = RLookupKey::new(c"price", RLookupKeyFlags::empty());

        let r = RemoteCollectReducer::new(Box::new([&f1]), false, Box::new([&s1]), 0, None, true);

        assert_eq!(r.field_keys_len(), 1);
        assert_eq!(r.sort_keys_len(), 1);
        assert!(r.is_internal);
    }

    #[test]
    fn internal_mode_without_sortby_has_empty_sort_keys() {
        let f1 = RLookupKey::new(c"name", RLookupKeyFlags::empty());

        let r = RemoteCollectReducer::new(Box::new([&f1]), false, Box::new([]), 0, None, true);

        assert_eq!(r.field_keys_len(), 1);
        assert_eq!(r.sort_keys_len(), 0);
        assert!(r.is_internal);
    }

    #[test]
    fn sort_key_overlapping_with_field_appears_in_both_slices() {
        let k = RLookupKey::new(c"price", RLookupKeyFlags::empty());

        let r = RemoteCollectReducer::new(Box::new([&k]), false, Box::new([&k]), 0, None, true);

        assert_eq!(r.field_keys_len(), 1);
        assert_eq!(r.sort_keys_len(), 1);
        assert!(std::ptr::eq(r.field_keys[0], r.sort_keys[0]));
    }
}
