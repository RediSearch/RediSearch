use std::ffi::c_double;
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
