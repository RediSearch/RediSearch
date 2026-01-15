use ffi::RedisModuleString;
use libc::{size_t, snprintf};
use std::ffi::{c_char, c_double, c_int};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use value::strings::{ConstString, RmAllocString, RsValueString};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Dereference(value: *const RsValue) -> *mut RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let dereferenced_value = shared_value.fully_dereferenced();
    dereferenced_value.as_ptr() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Clear(value: *const RsValue) {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let mut shared_value = ManuallyDrop::new(shared_value);
    unsafe { shared_value.set_value(RsValue::Undefined) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IncrRef(value: *const RsValue) -> *mut RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    <SharedRsValue as Clone>::clone(&shared_value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DecrRef(value: *const RsValue) {
    let _ = unsafe { SharedRsValue::from_raw(value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeReference(dst: *const RsValue, src: *const RsValue) {
    let shared_dst = unsafe { SharedRsValue::from_raw(dst) };
    let mut shared_dst = ManuallyDrop::new(shared_dst);
    let shared_src = unsafe { SharedRsValue::from_raw(src) };
    let shared_src = ManuallyDrop::new(shared_src);

    let new_value = RsValue::Ref(shared_src.deref().clone());
    unsafe { shared_dst.set_value(new_value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeOwnReference(dst: *const RsValue, src: *const RsValue) {
    let shared_dst = unsafe { SharedRsValue::from_raw(dst) };
    let mut shared_dst = ManuallyDrop::new(shared_dst);
    let shared_src = unsafe { SharedRsValue::from_raw(src) };

    let new_value = RsValue::Ref(shared_src);
    unsafe { shared_dst.set_value(new_value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Replace(dstpp: *mut *mut RsValue, src: *const RsValue) {
    let shared_dst = unsafe { SharedRsValue::from_raw(*dstpp) };
    let shared_src = unsafe { SharedRsValue::from_raw(src) };
    let shared_src = ManuallyDrop::new(shared_src);
    let shared_src_clone = shared_src.deref().clone();
    unsafe {
        *dstpp = shared_src_clone.into_raw() as *mut _;
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Number_Get(value: *const RsValue) -> c_double {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let RsValue::Number(number) = shared_value.value() else {
        panic!("not a number")
    };
    *number
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Refcount(value: *const RsValue) -> usize {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    shared_value.refcount()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetLeft(value: *const RsValue) -> *const RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    let RsValue::Trio(trio) = shared_value.value() else {
        panic!("Expected trio")
    };

    trio.left().as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetMiddle(value: *const RsValue) -> *const RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    let RsValue::Trio(trio) = shared_value.value() else {
        panic!("Expected trio")
    };

    trio.middle().as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetRight(value: *const RsValue) -> *const RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    let RsValue::Trio(trio) = shared_value.value() else {
        panic!("Expected trio")
    };

    trio.right().as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(value: *mut RsValue, n: c_double) {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let mut shared_value = ManuallyDrop::new(shared_value);
    let new_value = RsValue::Number(n);
    unsafe { shared_value.set_value(new_value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNull(value: *mut RsValue) {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let mut shared_value = ManuallyDrop::new(shared_value);
    unsafe { shared_value.set_value(RsValue::Null) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_RedisString_Get(
    value: *const RsValue,
) -> *const RedisModuleString {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);

    let RsValue::RedisString(redis_string) = shared_value.value() else {
        panic!("Expected redis string")
    };

    redis_string.as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToNumber(value: *const RsValue, d: *mut c_double) -> c_int {
    if value.is_null() {
        return 0;
    }

    let d = unsafe { d.as_mut().expect("d is null") };

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.fully_dereferenced_value();

    fn to_number(value: &RsValue) -> Option<f64> {
        match value {
            RsValue::Number(n) => Some(*n),
            val if crate::util::rsvalue_any_str(val) => {
                let slice = crate::util::rsvalue_as_byte_slice(value).unwrap();
                crate::util::rsvalue_str_to_float(slice)
            }
            RsValue::Trio(trio) => to_number(trio.left().value()),
            _ => None,
        }
    }

    if let Some(num) = to_number(value) {
        *d = num;
        return 1;
    } else {
        return 0;
    }
}

// /* Convert a value to a number, either returning the actual numeric values or by parsing a string
// into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
// not. If possible, we put the actual value into the double pointer */
// int RSValue_ToNumber(const RSValue *v, double *d) {
//   if (RSValue_IsNull(v)) return 0;
//   v = RSValue_Dereference(v);

//   const char *p = NULL;
//   size_t l = 0;
//   switch (v->_t) {
//     // for numerics - just set the value and return
//     case RSValueType_Number:
//       *d = v->_numval;
//       return 1;

//     case RSValueType_String:
//       // C strings - take the ptr and len
//       p = v->_strval.str;
//       l = v->_strval.len;
//       break;
//     case RSValueType_RedisString:
//       // Redis strings - take the number and len
//       p = RedisModule_StringPtrLen(v->_rstrval, &l);
//       break;

//     case RSValueType_Trio:
//       return RSValue_ToNumber(RSValue_Trio_GetLeft(v), d);

//     case RSValueType_Null:
//     case RSValueType_Array:
//     case RSValueType_Map:
//     case RSValueType_Undef:
//     default:
//       return 0;
//   }
//   // If we have a string - try to parse it
//   if (p) {
//     char *e;
//     errno = 0;
//     *d = fast_float_strtod(p, &e);
//     if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
//         *e != '\0') {
//       return 0;
//     }

//     return 1;
//   }

//   return 0;
// }

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ConvertStringPtrLen(
    value: *const RsValue,
    len_ptr: *mut size_t,
    buf: *mut c_char,
    buflen: size_t,
) -> *const c_char {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.fully_dereferenced_value();

    let (ptr, len): (*const c_char, size_t) = if let RsValue::Number(num) = value {
        let n = unsafe { snprintf(buf, buflen, c"%.12g".as_ptr(), num) };
        if n >= (buflen as i32) {
            ("".as_ptr() as *const _, 0)
        } else {
            (buf as *const _, n as usize)
        }
    } else if let Some(str) = crate::util::rsvalue_as_byte_slice(value) {
        (str.as_ptr() as *const c_char, str.len())
    } else {
        ("".as_ptr() as *const _, 0)
    };

    if let Some(len_ptr) = unsafe { len_ptr.as_mut() } {
        *len_ptr = len;
    }
    ptr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_StringPtrLen(
    value: *const RsValue,
    len_ptr: *mut size_t,
) -> *const c_char {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.fully_dereferenced_value();

    fn string_ptr_len(value: &RsValue) -> (*const c_char, size_t) {
        if let Some(str) = crate::util::rsvalue_as_byte_slice(value) {
            (str.as_ptr() as *const c_char, str.len())
        } else if let RsValue::Trio(trio) = value {
            string_ptr_len(trio.left().value())
        } else {
            (std::ptr::null(), 0)
        }
    }

    let (ptr, len) = string_ptr_len(value);
    tracing::info!("StringPtrLen ret: {ptr:?}, {len}");

    // let slice: () = unsafe { std::slice::from_raw_parts(ptr, len) };
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

    let value = shared_value.fully_dereferenced_value();
    match value {
        RsValue::Number(number) => {
            let mut buf = [0u8; 128];
            let len = value::util::num_to_string_cstyle(*number, &mut buf);
            let str_val = std::str::from_utf8(&buf[..(len as usize)]).unwrap();
            let str_val = str_val.to_owned();
            let new_val = RsValue::String(Box::new(RsValueString::from_string(str_val)));
            unsafe { shared_dst.set_value(new_val) };
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

    value::util::num_to_string_cstyle(*num, buf) as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetString(value: *const RsValue, str: *mut c_char, len: size_t) {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let mut shared_value = ManuallyDrop::new(shared_value);
    let str = NonNull::new(str).expect("str is null");
    let owned_rm_alloc_string = unsafe { RmAllocString::take_unchecked(str, len as u32) };
    let value = RsValue::RmAllocString(owned_rm_alloc_string);
    unsafe { shared_value.set_value(value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetConstString(
    value: *const RsValue,
    str: *const c_char,
    len: size_t,
) {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let mut shared_value = ManuallyDrop::new(shared_value);
    let const_string = unsafe { ConstString::new(str, len as u32) };
    let value = RsValue::ConstString(const_string);
    unsafe { shared_value.set_value(value) };
}
