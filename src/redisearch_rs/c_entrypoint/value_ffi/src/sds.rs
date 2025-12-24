use ffi::sds;
use libc::size_t;
use std::ffi::c_char;
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(value: *const RsValue, sds: sds, obfuscate: bool) -> sds {
    unimplemented!("RSValue_DumpSds")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetString(
    v: *const RsValue,
    str: *mut c_char,
    len: size_t,
) -> sds {
    unimplemented!("RSValue_SetString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetConstString(
    v: *const RsValue,
    str: *const c_char,
    len: size_t,
) -> sds {
    unimplemented!("RSValue_SetConstString")
}
