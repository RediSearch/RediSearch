use std::{
    ffi::{CString, c_char, c_double, c_int},
    mem::ManuallyDrop,
};

use libc::{size_t, snprintf};
use value::{RsString, RsValue, SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToNumber(value: *const RsValue, d: *mut c_double) -> c_int {
    if value.is_null() {
        return 0;
    }

    let d = unsafe { d.as_mut().expect("d is null") };

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let mut value = shared_value.value().fully_dereferenced();

    let num = loop {
        let slice = match value {
            RsValue::Number(n) => break Some(*n),
            RsValue::String(string) => string.as_bytes_checked(),
            RsValue::RedisString(string) => string.as_bytes(),
            RsValue::Ref(ref_val) => {
                value = ref_val.value();
                continue;
            }
            RsValue::Trio(trio) => {
                value = trio.left().value();
                continue;
            }
            _ => break None,
        };

        break crate::util::rsvalue_str_to_float(slice);
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
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value().fully_dereferenced();

    let (ptr, len): (*const c_char, size_t) = match value {
        RsValue::Number(num) => {
            let n = unsafe { snprintf(buf, buflen, c"%.12g".as_ptr(), num) };
            if n >= (buflen as i32) {
                (b"\0".as_ptr() as *const _, 0)
            } else {
                (buf as *const _, n as usize)
            }
        }
        RsValue::String(string) => {
            let (ptr, len) = string.as_ptr_len_checked();
            (ptr, len as usize)
        }
        RsValue::RedisString(string) => {
            let (ptr, len) = string.as_ptr_len();
            (ptr, len as usize)
        }
        _ => (b"\0".as_ptr() as *const _, 0),
    };

    if let Some(len_ptr) = unsafe { len_ptr.as_mut() } {
        *len_ptr = len;
    }
    ptr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToString(dst: *const RsValue, value: *const RsValue) {
    let shared_dst = unsafe { SharedRsValue::from_raw(dst) };
    let mut shared_dst = ManuallyDrop::new(shared_dst);
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    let value = shared_value.value().fully_dereferenced();
    match value {
        RsValue::Number(number) => {
            let mut buf = [0u8; 128];
            let len = crate::util::num_to_string_cstyle(*number, &mut buf);
            let cstring = CString::new(&buf[..(len as usize)]).unwrap();
            // let str_val = std::str::from_utf8(&buf[..(len as usize)]).unwrap();
            // let str_val = str_val.to_owned();
            // let new_val = RsValue::String(Box::new(RsValueString::from_string(str_val)));
            let new_val = RsValue::String(RsString::cstring(cstring));
            shared_dst.set_value(new_val);
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
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let buf = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, buflen as usize) };

    let RsValue::Number(num) = shared_value.value() else {
        panic!("Expected number")
    };

    crate::util::num_to_string_cstyle(*num, buf) as size_t
}
