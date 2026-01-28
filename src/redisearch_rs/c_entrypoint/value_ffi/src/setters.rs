use crate::util::expect_shared_value;
use std::ffi::c_double;
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(value: *mut RsValue, n: c_double) {
    let mut shared_value = unsafe { expect_shared_value(value) };

    let new_value = RsValue::Number(n);
    unsafe { shared_value.set_value(new_value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNull(value: *mut RsValue) {
    let mut shared_value = unsafe { expect_shared_value(value) };

    unsafe { shared_value.set_value(RsValue::Null) };
}
