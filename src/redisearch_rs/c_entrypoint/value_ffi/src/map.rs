use std::{
    ffi::c_void,
    mem::{ManuallyDrop, MaybeUninit},
};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_AllocUninit(len: u32) -> *mut c_void {
    let vec: Vec<MaybeUninit<(*mut RsValue, *mut RsValue)>> =
        vec![MaybeUninit::uninit(); len as usize];

    Box::into_raw(vec.into_boxed_slice()) as *mut c_void
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_SetEntry(
    map: *mut c_void,
    index: u32,
    key: *mut RsValue,
    value: *mut RsValue,
) {
    let slice = unsafe {
        std::slice::from_raw_parts_mut(
            map as *mut MaybeUninit<(*mut RsValue, *mut RsValue)>,
            (index + 1) as usize,
        )
    };

    slice[index as usize] = MaybeUninit::new((key, value));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewMap(map: *mut c_void, len: u32) -> *mut RsValue {
    let map = unsafe {
        Vec::from_raw_parts(
            map as *mut MaybeUninit<(*mut RsValue, *mut RsValue)>,
            len as usize,
            len as usize,
        )
    };

    let map = map
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
    let shared_value = unsafe { SharedRsValue::from_raw(map) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    if let RsValue::Map(map) = value {
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
    let shared_value = unsafe { SharedRsValue::from_raw(map) };
    let shared_value = ManuallyDrop::new(shared_value);

    if let RsValue::Map(map) = shared_value.value() {
        let (shared_key, shared_value) = &map[index as usize];
        unsafe { key.write(shared_key.as_ptr() as *mut _) };
        unsafe { value.write(shared_value.as_ptr() as *mut _) };
    } else {
        panic!("Expected a map value")
    }
}
