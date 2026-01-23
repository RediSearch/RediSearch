use crate::util::expect_value;
use std::mem::MaybeUninit;
use value::{RsValue, shared::SharedRsValue};

/// Opaque map structure used during map construction.
/// Holds uninitialized entries that are populated via `RSValueMap_SetEntry`
/// before being finalized into an `RsValue::Map` via `RSValue_NewMap`.
pub struct RSValueMap {
    entries: Vec<MaybeUninit<(*mut RsValue, *mut RsValue)>>,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_AllocUninit(len: u32) -> *mut RSValueMap {
    let entries = vec![MaybeUninit::uninit(); len as usize];

    Box::into_raw(Box::new(RSValueMap { entries }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_SetEntry(
    map: *mut RSValueMap,
    index: u32,
    key: *mut RsValue,
    value: *mut RsValue,
) {
    let map = unsafe { map.as_mut().expect("map should not be null") };

    map.entries[index as usize] = MaybeUninit::new((key, value));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewMap(map: *mut RSValueMap) -> *mut RsValue {
    let map = unsafe { Box::from_raw(map) };

    let map = map
        .entries
        .into_iter()
        .map(|entry| {
            let (key, value) = unsafe { entry.assume_init() };
            unsafe { (SharedRsValue::from_raw(key), SharedRsValue::from_raw(value)) }
        })
        .collect();

    let value = RsValue::Map(map);
    let shared = SharedRsValue::new(value);
    shared.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_Len(map: *const RsValue) -> u32 {
    let map = unsafe { expect_value(map) };

    if let RsValue::Map(map) = map {
        map.len() as u32
    } else {
        panic!("Expected a map value")
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_GetEntry(
    map: *const RsValue,
    index: u32,
    key: *mut *mut RsValue,
    value: *mut *mut RsValue,
) {
    let map = unsafe { expect_value(map) };

    if let RsValue::Map(map) = map {
        let (shared_key, shared_value) = &map[index as usize];
        unsafe { key.write(shared_key.as_ptr() as *mut _) };
        unsafe { value.write(shared_value.as_ptr() as *mut _) };
    } else {
        panic!("Expected a map value")
    }
}
