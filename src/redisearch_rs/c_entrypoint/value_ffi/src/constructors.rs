use std::ffi::c_double;
use value::trio::RsValueTrio;
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewUndefined() -> *mut RsValue {
    SharedRsValue::new(RsValue::Undefined).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNull() -> *mut RsValue {
    SharedRsValue::new(RsValue::Null).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNumber(value: c_double) -> *mut RsValue {
    SharedRsValue::new(RsValue::Number(value)).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewTrio(
    left: *mut RsValue,
    middle: *mut RsValue,
    right: *mut RsValue,
) -> *mut RsValue {
    let shared_left = unsafe { SharedRsValue::from_raw(left) };
    let shared_middle = unsafe { SharedRsValue::from_raw(middle) };
    let shared_right = unsafe { SharedRsValue::from_raw(right) };

    SharedRsValue::new(RsValue::Trio(RsValueTrio::new(
        shared_left,
        shared_middle,
        shared_right,
    )))
    .into_raw() as *mut _
}
