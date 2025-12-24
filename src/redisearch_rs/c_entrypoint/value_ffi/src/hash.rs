use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Hash(v: *const RsValue, hval: u64) -> u64 {
    unimplemented!("RSValue_Hash")
}
