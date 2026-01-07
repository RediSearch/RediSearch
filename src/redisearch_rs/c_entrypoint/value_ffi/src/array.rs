use std::mem::ManuallyDrop;
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_AllocateArray(len: u32) -> *mut *mut RsValue {
    let array: Vec<*mut RsValue> = vec![std::ptr::null_mut(); len as usize];

    Box::into_raw(array.into_boxed_slice()) as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArray(values: *mut *mut RsValue, len: u32) -> *mut RsValue {
    let array = unsafe { Vec::from_raw_parts(values, len as usize, len as usize) };

    let array = array
        .into_iter()
        .map(|val| unsafe { SharedRsValue::from_raw(val) })
        .collect();

    let value = RsValue::Array(array);
    let shared = SharedRsValue::new(value);
    shared.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayLen(value: *const RsValue) -> u32 {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    if let RsValue::Array(array) = value {
        array.len() as u32
    } else {
        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayItem(value: *const RsValue, index: u32) -> *mut RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    if let RsValue::Array(array) = value {
        let shared = &array[index as usize];
        shared.as_ptr() as *mut _
    } else {
        std::ptr::null_mut()
    }
}
