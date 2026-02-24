use crate::util::{expect_shared_value, expect_value};
use libc::size_t;
use std::ffi::{CString, c_char, c_double, c_int};
use value::util::{num_to_str, str_to_float};
use value::{RsString, RsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToNumber(value: *const RsValue, d: *mut c_double) -> c_int {
    let Some(value) = (unsafe { value.as_ref() }) else {
        return 0;
    };

    let d = unsafe { d.as_mut().expect("d is null") };

    let value = value.fully_dereferenced_ref_and_trio();

    let num = match value {
        RsValue::Number(n) => Some(*n),
        RsValue::String(string) => str_to_float(string.as_bytes()),
        RsValue::RedisString(string) => str_to_float(string.as_bytes()),
        _ => return 0,
    };

    if let Some(num) = num {
        *d = num;
        return 1;
    } else {
        *d = 0.0;
        return 0;
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ConvertStringPtrLen(
    value: *const RsValue,
    len_ptr: *mut size_t,
    buf: *mut c_char,
    buflen: size_t,
) -> *const c_char {
    let value = unsafe { expect_value(value) };
    let len_ptr = unsafe { len_ptr.as_mut().expect("len_ptr is null") };

    let value = value.fully_dereferenced_ref();

    let (ptr, len): (*const c_char, size_t) = match value {
        RsValue::Number(num) => {
            let buffer = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, buflen as usize) };
            let n = num_to_str(*num, buffer);

            if n < buflen {
                (buf as *const _, n)
            } else {
                (b"\0".as_ptr() as *const _, 0)
            }
        }
        RsValue::String(string) => {
            let (ptr, len) = string.as_ptr_len_for_nul_terminated();
            (ptr, len as usize)
        }
        RsValue::RedisString(string) => string.as_ptr_len(),
        _ => (b"\0".as_ptr() as *const _, 0),
    };

    *len_ptr = len;
    ptr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToString(dst: *mut RsValue, value: *const RsValue) {
    let mut dst = unsafe { expect_shared_value(dst) };

    let value = unsafe { expect_value(value) };
    let value = value.fully_dereferenced_ref_and_trio();

    match value {
        RsValue::Number(number) => {
            let mut buf = [0u8; 32];
            let len = num_to_str(*number, &mut buf);
            let cstring = CString::new(&buf[..(len as usize)]).unwrap();
            let new_val = RsValue::String(RsString::cstring(cstring));
            dst.set_value(new_val);
        }
        _ => unimplemented!("RSValue_ToString for type 'unknown'"),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NumToString(
    value: *const RsValue,
    buf: *mut c_char,
    buflen: size_t,
) -> size_t {
    let value = unsafe { expect_value(value) };

    let buf = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, buflen as usize) };

    let RsValue::Number(num) = value else {
        panic!("Expected number")
    };

    num_to_str(*num, buf)
}
