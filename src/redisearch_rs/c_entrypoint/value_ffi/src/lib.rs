#![allow(non_camel_case_types, non_snake_case)]

use std::{
    ffi::{c_char, c_double},
    ptr::NonNull,
};

use crate::value_type::{AsRsValueType, RsValueType};

pub mod value_type;

#[repr(C)]
pub enum RSValue {
    /// Reference-counted RsValue, allocated on the heap
    Heap(value::SharedRsValue),
    /// Non-reference-counted RsValue, allocated on the stack
    Stack(value::RsValue),
}

/// Creates a stack-allocated undefined RSValue.
/// The returned value is not allocated on the heap and should not be freed.
/// Returns a stack-allocated RSValue of type `RSValueType_Undef`
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Undefined() -> RSValue {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Number(n: c_double) -> RSValue {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_MallocString(str: Option<NonNull<c_char>>, len: u32) -> RSValue {
    todo!()
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewString(str: Option<NonNull<c_char>>, len: u32) -> RSValue {
    todo!()
}

// TODO add constructors for each type of string,
// corresponding to RSStringType in the C module

pub extern "C" fn RSValue_NullStatic() -> &'static RSValue {
    static RSVALUE_NULL: RSValue = RSValue::Stack(value::RsValue::null());
    &RSVALUE_NULL
}

// #[unsafe(no_mangle)]
// pub extern "C" fn RSValue_X() -> RSValue {
//     todo!()
// }

// #[unsafe(no_mangle)]
// pub extern "C" fn RSValue_X() -> RSValue {
//     todo!()
// }

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Type(v: &RSValue) -> RsValueType {
    v.as_value_type()
}
