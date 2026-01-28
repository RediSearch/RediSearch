use crate::util::expect_value;
use std::mem::MaybeUninit;
use value::{RsValue, shared::SharedRsValue};

/// Opaque map structure used during map construction.
/// Holds uninitialized entries that are populated via `RSValueMap_SetEntry`
/// before being finalized into an `RsValue::Map` via `RSValue_NewMap`.
pub struct RSValueMap {
    entries: Vec<MaybeUninit<(*mut RsValue, *mut RsValue)>>,
}

/// Allocates a new, uninitialized [`RSValueMap`] with space for `len` entries.
///
/// The map entries are uninitialized and must be set using [`RSValueMap_SetEntry`]
/// before being finalized into an [`RsValue`] via [`RSValue_NewMap`].
///
/// # SAFETY
///
/// 1. All entries must be initialized via [`RSValueMap_SetEntry`] before
///    passing the map to [`RSValue_NewMap`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_AllocUninit(len: u32) -> *mut RSValueMap {
    let entries = vec![MaybeUninit::uninit(); len as usize];

    Box::into_raw(Box::new(RSValueMap { entries }))
}

/// Sets a key-value pair at a specific index in the map.
///
/// Takes ownership of both the `key` and `value` [`RsValue`] pointers.
///
/// # SAFETY
///
/// 1. `map` must be a valid pointer to an [`RSValueMap`] created by
///    [`RSValueMap_AllocUninit`].
/// 2. `index` must be less than the map length.
/// 3. `key` and `value` must be valid pointers to [`RsValue`]
///    obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_SetEntry(
    map: *mut RSValueMap,
    index: u32,
    key: *mut RsValue,
    value: *mut RsValue,
) {
    // Safety: ensured by caller (1.)
    let map = unsafe { map.as_mut().expect("map should not be null") };

    // Compatibility: C does an RS_ASSERT on index out of bounds
    map.entries[index as usize] = MaybeUninit::new((key, value));
}

/// Creates a heap-allocated map [`RsValue`] from an [`RSValueMap`].
///
/// Takes ownership of the map structure and all its entries. The [`RSValueMap`]
/// pointer is consumed and must not be used after this call.
///
/// # SAFETY
///
/// 1. `map` must be a valid pointer to an [`RSValueMap`] created by
///    [`RSValueMap_AllocUninit`].
/// 2. All entries in the map must have been initialized via [`RSValueMap_SetEntry`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewMap(map: *mut RSValueMap) -> *mut RsValue {
    // Safety: ensured by caller (1.)
    let map = unsafe { Box::from_raw(map) };

    let map = map
        .entries
        .into_iter()
        .map(|entry| {
            // Safety: ensured by caller (2.)
            let (key, value) = unsafe { entry.assume_init() };
            // Safety: ensured by caller (2.)
            unsafe { (SharedRsValue::from_raw(key), SharedRsValue::from_raw(value)) }
        })
        .collect();

    let value = RsValue::Map(map);
    let shared = SharedRsValue::new(value);
    shared.into_raw() as *mut _
}

/// Returns the number of key-value pairs in a map [`RsValue`].
///
/// # SAFETY
///
/// 1. `map` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panics
///
/// Panics if `map` is not a map value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_Len(map: *const RsValue) -> u32 {
    // Safety: ensured by caller (1.)
    let map = unsafe { expect_value(map) };

    if let RsValue::Map(map) = map {
        map.len() as u32
    } else {
        // Compatibility: C does an RS_ASSERT on incorrect type
        panic!("Expected a map value")
    }
}

/// Retrieves a key-value pair from a map [`RsValue`] at a specific index.
///
/// The returned key and value pointers are borrowed from the map and must
/// not be freed by the caller.
///
/// # SAFETY
///
/// 1. `map` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `index` must be less than the map length.
/// 3. `key` and `value` must be valid, non-null pointers to writable
///    `*mut RsValue` locations.
///
/// # Panics
///
/// Panics if `map` is not a map value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_GetEntry(
    map: *const RsValue,
    index: u32,
    key: *mut *mut RsValue,
    value: *mut *mut RsValue,
) {
    // Safety: ensured by caller (1.)
    let map = unsafe { expect_value(map) };

    if let RsValue::Map(map) = map {
        // Compatibility: C does an RS_ASSERT on index out of bounds
        let (shared_key, shared_value) = &map[index as usize];
        // Safety: ensured by caller (3.)
        unsafe { key.write(shared_key.as_ptr() as *mut _) };
        // Safety: ensured by caller (3.)
        unsafe { value.write(shared_value.as_ptr() as *mut _) };
    } else {
        panic!("Expected a map value")
    }
}
