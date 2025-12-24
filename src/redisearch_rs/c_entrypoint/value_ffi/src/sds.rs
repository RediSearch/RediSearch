use ffi::sds;
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(value: *const RsValue, sds: sds, obfuscate: bool) -> sds {
    unimplemented!("RSValue_DumpSds")
}
