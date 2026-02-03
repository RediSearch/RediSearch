use ffi::{sds, sdscat, sdscatlen};
use std::ffi::c_void;
use std::mem::ManuallyDrop;
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(
    value: *const RsValue,
    mut sds: sds,
    obfuscate: bool,
) -> sds {
    if value.is_null() {
        return unsafe { sdscat(sds, c"nil".as_ptr()) };
    }

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.fully_dereferenced_value();

    match value {
        RsValue::Null => return unsafe { sdscat(sds, c"NULL".as_ptr()) },
        RsValue::Number(num) => {
            if obfuscate {
                // TODO: Use `Obfuscate_Number` instead.
                return unsafe { sdscat(sds, c"Number".as_ptr()) };
            } else {
                let mut buf = [0u8; 128];
                let len = value::util::num_to_string_cstyle(*num, &mut buf);
                return unsafe { sdscatlen(sds, buf.as_ptr() as *const c_void, len as usize) };
            }
        }
        RsValue::String(str) => {
            if obfuscate {
                // TODO: Use `Obfuscate_Text` instead.
                return unsafe { sdscat(sds, c"\"Text\"".as_ptr()) };
            } else {
                let (ptr, len) = str.as_ptr_len();
                sds = unsafe { sdscat(sds, c"\"".as_ptr()) };
                sds = unsafe { sdscatlen(sds, ptr as *const c_void, len as usize) };
                sds = unsafe { sdscat(sds, c"\"".as_ptr()) };
                return sds;
            }
        }
        // TODO: Implement rest of RSValue_DumpSds
        value => unimplemented!("RSValue_DumpSds {:?}", value),
    }
}
