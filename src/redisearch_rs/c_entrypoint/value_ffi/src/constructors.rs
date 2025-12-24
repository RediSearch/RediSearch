use ffi::RedisModuleString;
use std::ffi::{c_char, c_double};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::LazyLock;
use value::{RsValue, Value, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewUndefined() -> *mut RsValue {
    SharedRsValue::from_value(RsValue::undefined()).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNull() -> *mut RsValue {
    unimplemented!("RSValue_NewNull")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NullStatic() -> *mut RsValue {
    static RSVALUE_NULL: LazyLock<SharedRsValue> =
        LazyLock::new(|| SharedRsValue::from_value(RsValue::Null));
    RSVALUE_NULL.as_ptr() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNumber(value: c_double) -> *mut RsValue {
    SharedRsValue::from_value(RsValue::Number(value)).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewRedisString(s: *const RedisModuleString) -> *mut RsValue {
    unimplemented!("RSValue_NewRedisString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewCopiedString(value: *const c_char, len: u32) -> *mut RsValue {
    unimplemented!("RSValue_NewCopiedString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewReference(src: *const RsValue) -> *mut RsValue {
    let shared_src = unsafe { SharedRsValue::from_raw(src) };
    let shared_src = ManuallyDrop::new(shared_src);
    let ref_value = RsValue::Ref(shared_src.deref().clone());
    SharedRsValue::from_value(ref_value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewParsedNumber(value: *const c_char, len: u32) -> *mut RsValue {
    unimplemented!("RSValue_NewParsedNumber")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNumberFromInt64(ii: i64) -> *mut RsValue {
    unimplemented!("RSValue_NewNumberFromInt64")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewTrio(
    val: *mut RsValue,
    otherval: *mut RsValue,
    other2val: *mut RsValue,
) -> *mut RsValue {
    unimplemented!("RSValue_NewTrio")
}
