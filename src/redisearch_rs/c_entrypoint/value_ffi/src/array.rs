use std::mem::{ManuallyDrop, MaybeUninit};
use value::{RsValue, Value, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_AllocateArray(len: usize) -> *mut *mut RsValue {
    // let rm_alloc = unsafe { RedisModule_Alloc.expect("Redis allocator not available") };
    // let buf = unsafe { rm_alloc(std::mem::size_of::<*mut SharedRsValue>() * len) };
    // return buf as *mut *mut RsValue;
    let array: Vec<MaybeUninit<*mut RsValue>> = vec![MaybeUninit::uninit(); len];
    let array = array.into_boxed_slice();
    Box::into_raw(array) as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArray(vals: *mut *mut RsValue, len: u32) -> *mut RsValue {
    // let slice = unsafe { slice::from_raw_parts(vals, len as usize) };
    // let iter = slice.into_iter().map(|val| {
    //     // SAFETY: stuff
    //     unsafe { SharedRsValue::from_raw(*val) }
    // });
    // // TODO: Should deallocate rm_alloc'd array
    // let array = RsValueCollection::collect_from_exact_size_iterator(iter);
    // let array = RsValueArray::from_collection(array);
    // SharedRsValue::array(array).into_raw() as *mut _
    let array = unsafe { Vec::from_raw_parts(vals, len as usize, len as usize) };
    let array = array
        .into_iter()
        .map(|val| unsafe { SharedRsValue::from_raw(val) })
        .collect::<Vec<_>>();
    let shared = SharedRsValue::array(array);
    shared.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayLen(value: *const RsValue) -> u32 {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();
    if let RsValue::Array(array) = value {
        array.len() as u32
    } else {
        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayItem(value: *const RsValue, index: u32) -> *mut RsValue {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();
    if let RsValue::Array(array) = value {
        let shared = &array[index as usize];
        // let shared_ptr = unsafe { array.inner().entry_at(index) };
        // let shared = unsafe { &mut *shared_ptr };
        shared.as_ptr() as *mut _
    } else {
        std::ptr::null_mut()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::*;

    #[unsafe(no_mangle)]
    pub static mut RSDummyContext: *mut ffi::RedisModuleCtx = std::ptr::null_mut();

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn RedisModule_Free() {
        unimplemented!("RedisModule_Free")
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn RedisModule_FreeString() {
        unimplemented!("RedisModule_FreeString")
    }

    // Converted from test_cpp_value.cpp testArray test
    #[test]
    fn test_array_item() {
        unsafe {
            let array = RSValue_AllocateArray(3);
            let foo = RSValue_NewConstString(c"foo".as_ptr(), c"foo".count_bytes() as u32);
            let bar = RSValue_NewConstString(c"bar".as_ptr(), c"bar".count_bytes() as u32);
            let baz = RSValue_NewConstString(c"baz".as_ptr(), c"baz".count_bytes() as u32);
            array.wrapping_add(0).write(foo);
            array.wrapping_add(1).write(bar);
            array.wrapping_add(2).write(baz);
            let arr = RSValue_NewArray(array, 3);

            assert_eq!(RSValue_ArrayLen(arr), 3);

            assert_eq!(
                RsValueType::String as u32,
                RSValue_Type(RSValue_ArrayItem(arr, 0)) as u32
            );

            let val = RSValue_ArrayItem(arr, 0);
            let str = RSValue_String_Get(val, std::ptr::null_mut());
            assert_eq!(c"foo", std::ffi::CStr::from_ptr(str));

            assert_eq!(
                RsValueType::String as u32,
                RSValue_Type(RSValue_ArrayItem(arr, 1)) as u32
            );

            let val = RSValue_ArrayItem(arr, 1);
            let str = RSValue_String_Get(val, std::ptr::null_mut());
            assert_eq!(c"bar", std::ffi::CStr::from_ptr(str));

            assert_eq!(
                RsValueType::String as u32,
                RSValue_Type(RSValue_ArrayItem(arr, 2)) as u32
            );

            let val = RSValue_ArrayItem(arr, 2);
            let str = RSValue_String_Get(val, std::ptr::null_mut());
            assert_eq!(c"baz", std::ffi::CStr::from_ptr(str));

            RSValue_DecrRef(arr);
        }
    }
}
