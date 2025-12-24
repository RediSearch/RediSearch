use ffi::{QueryError, RedisModuleString};
use libc::size_t;
use std::ffi::{c_char, c_double, c_int};
use std::mem::ManuallyDrop;
use value::{RsValue, Value, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Dereference(value: *const RsValue) -> *mut RsValue {
    unimplemented!("RSValue_Dereference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Clear(value: &RsValue) {
    unimplemented!("RSValue_Clear")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IncrRef(value: *const RsValue) -> *mut RsValue {
    unimplemented!("RSValue_IncrRef")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DecrRef(value: *const RsValue) {
    let _ = unsafe { SharedRsValue::from_raw(value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeReference(dst: *const RsValue, src: *const RsValue) {
    unimplemented!("RSValue_MakeReference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeOwnReference(dst: *const RsValue, src: *const RsValue) {
    unimplemented!("RSValue_MakeOwnReference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Replace(dstpp: *mut *mut RsValue, src: *const RsValue) {
    unimplemented!("RSValue_Replace")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Number_Get(value: *const RsValue) -> c_double {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    shared_value.get_number().unwrap()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Refcount(value: *const RsValue) -> usize {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    shared_value.refcount()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetLeft(value: *const RsValue) -> *const RsValue {
    unimplemented!("RSValue_Trio_GetLeft")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetMiddle(value: *const RsValue) -> *const RsValue {
    unimplemented!("RSValue_Trio_GetMiddle")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetRight(value: *const RsValue) -> *const RsValue {
    unimplemented!("RSValue_Trio_GetRight")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(value: *mut RsValue, n: c_double) {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let mut shared_value = ManuallyDrop::new(shared_value);
    let new_value = RsValue::Number(n);
    unsafe { shared_value.set_value(new_value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IntoNull(value: *mut RsValue) {
    unimplemented!("RSValue_IntoNull")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_RedisString_Get(
    value: *const RsValue,
) -> *const RedisModuleString {
    unimplemented!("RSValue_RedisString_Get")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToNumber(v: *const RsValue, d: *mut c_double) -> c_int {
    unimplemented!("RSValue_ToNumber")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IntoNumber(v: *mut RsValue, n: c_double) {
    unimplemented!("RSValue_IntoNumber")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ConvertStringPtrLen(
    v: *const RsValue,
    lenp: *mut size_t,
    buf: *mut c_char,
    buflen: size_t,
) -> *const c_char {
    unimplemented!("RSValue_ConvertStringPtrLen")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_StringPtrLen(
    value: *const RsValue,
    lenp: *mut size_t,
) -> *const c_char {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.fully_dereferenced_value();

    match value {
        RsValue::String2(str) => {
            if let Some(lenp) = unsafe { lenp.as_mut() } {
                *lenp = str.len();
            }
            str.as_ptr() as *const c_char
        }
        _ => std::ptr::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToString(dst: *const RsValue, value: *const RsValue) {
    let shared_dst = unsafe { SharedRsValue::from_raw(dst) };
    let mut shared_dst = ManuallyDrop::new(shared_dst);
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    let value = shared_value.fully_dereferenced_value();
    match value {
        RsValue::Number(number) => {
            let mut buf = [0u8; 128];
            let len = value::util::num_to_string_cstyle(*number, &mut buf);
            let str_val = std::str::from_utf8(&buf[..(len as usize)]).unwrap();
            let str_val = str_val.to_owned();
            let new_val = RsValue::String2(str_val);
            unsafe { shared_dst.set_value(new_val) };
        }
        _ => unimplemented!("RSValue_ToString for type 'unknown'"),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NumToString(
    v: *const RsValue,
    buf: *mut c_char,
    buflen: size_t,
) -> size_t {
    unimplemented!("RSValue_NumToString")
}
