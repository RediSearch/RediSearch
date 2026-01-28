use std::ffi::c_double;
use std::mem::ManuallyDrop;
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Number_Get(value: *const RsValue) -> c_double {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    if let RsValue::Number(number) = shared_value.value() {
        *number
    } else {
        panic!("not a number")
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetLeft(value: *const RsValue) -> *const RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    if let RsValue::Trio(trio) = shared_value.value() {
        trio.left().as_ptr()
    } else {
        panic!("Expected trio")
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetMiddle(value: *const RsValue) -> *const RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    if let RsValue::Trio(trio) = shared_value.value() {
        trio.middle().as_ptr()
    } else {
        panic!("Expected trio")
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetRight(value: *const RsValue) -> *const RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    if let RsValue::Trio(trio) = shared_value.value() {
        trio.right().as_ptr()
    } else {
        panic!("Expected trio")
    }
}
