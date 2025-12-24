use ffi::{Obfuscate_Number, Obfuscate_Text};
use ffi::{sds, sdscat, sdscatfmt, sdscatlen, sdscatprintf};
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(value: *const RsValue, s: sds, obfuscate: bool) -> sds {
    match unsafe { value.as_ref() } {
        None => unsafe { sdscat(s, c"nil".as_ptr()) },
        Some(value) => dump_sds(value, s, obfuscate),
    }
}

fn dump_sds(value: &RsValue, mut s: sds, obfuscate: bool) -> sds {
    match value {
        RsValue::Undefined => unsafe { sdscat(s, c"<Undefined>".as_ptr()) },
        RsValue::Null => unsafe { sdscat(s, c"NULL".as_ptr()) },
        RsValue::Number(num) => {
            if obfuscate {
                // SAFETY: `Obfuscate_Number` returns a valid static C string.
                unsafe { sdscat(s, Obfuscate_Number(*num)) }
            } else {
                num_to_sds(s, *num)
            }
        }
        RsValue::String(str) => {
            let (ptr, len) = str.as_ptr_len();
            if obfuscate {
                unsafe {
                    // SAFETY: `Obfuscate_Text` returns a valid static C string.
                    let obfuscated = Obfuscate_Text(ptr);
                    s = sdscat(s, c"\"".as_ptr());
                    s = sdscat(s, obfuscated);
                    sdscat(s, c"\"".as_ptr())
                }
            } else {
                unsafe {
                    s = sdscat(s, c"\"".as_ptr());
                    s = sdscatlen(s, ptr.cast(), len as usize);
                    sdscat(s, c"\"".as_ptr())
                }
            }
        }
        RsValue::RedisString(str) => {
            let (ptr, len) = str.as_ptr_len();
            if obfuscate {
                unsafe {
                    // SAFETY: `Obfuscate_Text` returns a valid static C string.
                    let obfuscated = Obfuscate_Text(ptr);
                    s = sdscat(s, c"\"".as_ptr());
                    s = sdscat(s, obfuscated);
                    sdscat(s, c"\"".as_ptr())
                }
            } else {
                unsafe {
                    s = sdscat(s, c"\"".as_ptr());
                    s = sdscatlen(s, ptr.cast(), len);
                    sdscat(s, c"\"".as_ptr())
                }
            }
        }
        RsValue::Array(arr) => {
            s = unsafe { sdscat(s, c"[".as_ptr()) };
            for (i, elem) in arr.iter().enumerate() {
                if i > 0 {
                    s = unsafe { sdscat(s, c", ".as_ptr()) };
                }
                s = dump_sds(elem.value(), s, obfuscate);
            }
            unsafe { sdscat(s, c"]".as_ptr()) }
        }
        RsValue::Map(map) => {
            s = unsafe { sdscat(s, c"{".as_ptr()) };
            for (i, (key, val)) in map.iter().enumerate() {
                if i > 0 {
                    s = unsafe { sdscat(s, c", ".as_ptr()) };
                }
                s = dump_sds(key.value(), s, obfuscate);
                s = unsafe { sdscat(s, c": ".as_ptr()) };
                s = dump_sds(val.value(), s, obfuscate);
            }
            unsafe { sdscat(s, c"}".as_ptr()) }
        }
        RsValue::Ref(ref_value) => dump_sds(ref_value.value(), s, obfuscate),
        RsValue::Trio(trio) => dump_sds(trio.left().value(), s, obfuscate),
    }
}

fn num_to_sds(s: sds, num: f64) -> sds {
    let ll = num as i64;
    if ll as f64 == num {
        // SAFETY: `sdscatfmt` with `%I` expects a 64-bit signed integer.
        unsafe { sdscatfmt(s, c"%I".as_ptr(), ll) }
    } else {
        // SAFETY: `sdscatprintf` with `%.12g` expects a double.
        unsafe { sdscatprintf(s, c"%.12g".as_ptr(), num) }
    }
}
