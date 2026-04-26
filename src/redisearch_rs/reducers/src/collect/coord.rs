/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Coordinator-side COLLECT reducer.
//!
//! Consumes the per-shard payloads stored under `__SOURCE__` and rebuilds the
//! client-facing rows.
//!
//! ## Serialization contract
//!
//! Shards emit each row as `Map` (RESP3) or flat `[k, v, ...]` `Array`
//! (RESP2). The coordinator projects only requested field names; extra
//! internal sort-key values are ignored, and missing fields become nulls.

use rlookup::{RLookupKey, RLookupRow};
use value::{Array, Map, SharedValue, Value};

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Coordinator-side COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct CoordCollectReducer<'a> {
    common: CollectCommon,
    /// Lookup key for the per-shard `__SOURCE__` payload.
    source_key: &'a RLookupKey<'a>,
    field_names: Box<[Box<[u8]>]>,
    sort_key_names: Box<[Box<[u8]>]>,
}

// `CollectCommon` must live at offset 0 so the C layer can downcast to
// `ffi::Reducer`. Guard against accidental reordering of the struct fields.
const _: () = assert!(core::mem::offset_of!(CoordCollectReducer<'_>, common) == 0);

/// Per-group instance of [`CoordCollectReducer`].
///
/// Because `CoordCollectCtx` is arena-allocated ([`Bump`][bumpalo::Bump] does
/// not run destructors), `ptr::drop_in_place` must be called to run
/// destructors for the inner `Vec` and decrement `SharedValue` refcounts.
pub struct CoordCollectCtx {
    maps: Vec<SharedValue>,
}

impl<'a> CoordCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration. Names are owned byte
    /// copies because the coordinator cannot borrow shard `RLookupKey`s.
    pub fn new(
        source_key: &'a RLookupKey<'a>,
        field_names: Box<[Box<[u8]>]>,
        sort_key_names: Box<[Box<[u8]>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            source_key,
            field_names,
            sort_key_names,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    pub fn alloc_instance(&self) -> &mut CoordCollectCtx {
        self.common.arena.alloc(CoordCollectCtx::new(self))
    }
}

impl CoordCollectCtx {
    pub const fn new(_r: &CoordCollectReducer) -> Self {
        Self { maps: Vec::new() }
    }

    /// Append maps from the shard payload; missing or malformed payloads are skipped.
    pub fn add(&mut self, r: &CoordCollectReducer, row: &RLookupRow) {
        let Some(payload) = row.get(r.source_key) else {
            return;
        };
        if let Value::Array(array) = &**payload {
            self.maps.reserve(array.len());
            for entry in array.iter() {
                self.maps.push(entry.clone());
            }
        }
    }

    /// Rebuild shard rows as client-facing maps, accepting RESP3 maps and
    /// RESP2 flat arrays while ignoring internal-only extra keys.
    pub fn finalize(&mut self, r: &CoordCollectReducer) -> SharedValue {
        let rebuilt: Vec<SharedValue> = std::mem::take(&mut self.maps)
            .into_iter()
            .filter_map(|entry| {
                // Malformed shard payloads are skipped defensively.
                let is_valid = matches!(&*entry, Value::Map(_) | Value::Array(_));
                if !is_valid {
                    return None;
                }

                let row_entries: Box<[_]> = r
                    .field_names
                    .iter()
                    .map(|name| {
                        let val = match &*entry {
                            Value::Map(m) => m.get(name).cloned(),
                            Value::Array(a) => a.map_get(name).cloned(),
                            // Checked above; this arm is unreachable.
                            _ => unreachable!(),
                        }
                        .unwrap_or_else(SharedValue::null_static);
                        (SharedValue::new_string(name.to_vec()), val)
                    })
                    .collect();
                Some(SharedValue::new(Value::Map(Map::new(row_entries))))
            })
            .collect();
        SharedValue::new(Value::Array(Array::new(rebuilt.into_boxed_slice())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rlookup::{RLookupKey, RLookupKeyFlags};

    #[test]
    fn new_with_no_fields_uses_common_defaults() {
        let source_key = RLookupKey::new(c"__SOURCE__", RLookupKeyFlags::empty());
        let r = CoordCollectReducer::new(&source_key, Box::new([]), Box::new([]), 0, None);
        assert_eq!(r.common.sort_asc_map, 0);
        assert!(r.common.limit.is_none());
        assert!(std::ptr::eq(r.source_key, &source_key));
    }

    #[test]
    fn new_with_limit_and_sort_stores_configuration() {
        let source_key = RLookupKey::new(c"__SOURCE__", RLookupKeyFlags::empty());
        let r = CoordCollectReducer::new(
            &source_key,
            Box::new([b"a".to_vec().into_boxed_slice()]),
            Box::new([b"b".to_vec().into_boxed_slice()]),
            0b11,
            Some((2, 7)),
        );
        assert_eq!(r.common.sort_asc_map, 0b11);
        assert_eq!(r.common.limit, Some((2, 7)));
    }
}
