use ffi::QueryError;
use std::ffi::c_int;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use value::{RsValue, Value, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Cmp(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    unimplemented!("RSValue_Cmp")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Equal(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    unimplemented!("RSValue_Equal")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_BoolTest(v: *const RsValue) -> c_int {
    unimplemented!("RSValue_BoolTest")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Hash(v: *const RsValue, hval: u64) -> u64 {
    unimplemented!("RSValue_Hash")
}
