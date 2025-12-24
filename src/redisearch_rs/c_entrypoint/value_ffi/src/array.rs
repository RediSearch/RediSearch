use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_AllocateArray(len: usize) -> *mut *mut RsValue {
    unimplemented!("RSValue_AllocateArray")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArray(vals: *mut *mut RsValue, len: u32) -> *mut RsValue {
    unimplemented!("RSValue_NewArray")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayLen(value: *const RsValue) -> u32 {
    unimplemented!("RSValue_ArrayLen")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayItem(value: *const RsValue, index: u32) -> *mut RsValue {
    unimplemented!("RSValue_ArrayItem")
}
