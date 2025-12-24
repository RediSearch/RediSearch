use value::RsValue;
use value::collection::RsValueMap;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_AllocUninit(value: u32) -> RsValueMap {
    unimplemented!("RSValueMap_AllocUninit")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewMap(map: RsValueMap) -> *mut RsValue {
    unimplemented!("RSValue_NewMap")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_SetEntry(
    map: *mut RsValueMap,
    i: isize,
    key: *mut RsValue,
    value: *mut RsValue,
) {
    unimplemented!("RSValueMap_SetEntry")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_Len(map: *const RsValue) -> u32 {
    unimplemented!("RSValue_Map_Len")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_GetEntry(
    map: *const RsValue,
    i: u32,
    key: *mut *mut RsValue,
    value: *mut *mut RsValue,
) {
    unimplemented!("RSValue_Map_Len")
}
