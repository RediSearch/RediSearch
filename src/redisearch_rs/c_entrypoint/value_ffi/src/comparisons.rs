use ffi::QueryError;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::{cmp::Ordering, ffi::c_int};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Cmp(
    v1: *const RsValue,
    v2: *const RsValue,
    _status: *mut QueryError,
) -> c_int {
    let shared_v1 = unsafe { SharedRsValue::from_raw(v1) };
    let shared_v1 = ManuallyDrop::new(shared_v1);
    let shared_v2 = unsafe { SharedRsValue::from_raw(v2) };
    let shared_v2 = ManuallyDrop::new(shared_v2);

    let v1 = shared_v1.value().fully_dereferenced();
    let v2 = shared_v2.value().fully_dereferenced();

    match cmp(v1, v2) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Equal(
    v1: *const RsValue,
    v2: *const RsValue,
    _status: *mut QueryError,
) -> c_int {
    let shared_v1 = unsafe { SharedRsValue::from_raw(v1) };
    let shared_v1 = ManuallyDrop::new(shared_v1);
    let shared_v2 = unsafe { SharedRsValue::from_raw(v2) };
    let shared_v2 = ManuallyDrop::new(shared_v2);

    let v1 = shared_v1.value().fully_dereferenced();
    let v2 = shared_v2.value().fully_dereferenced();

    match cmp(v1, v2) {
        Ordering::Equal => 1,
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_BoolTest(value: *const RsValue) -> c_int {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value().fully_dereferenced();

    let result = match value {
        RsValue::Number(num) => *num != 0.0,
        RsValue::Array(arr) => arr.len() != 0,
        _ if crate::util::rsvalue_any_str(value) => {
            crate::util::rsvalue_as_byte_slice2(value).unwrap().len() != 0
        }
        _ => false,
    };

    result as c_int
}

fn cmp(v1: &RsValue, v2: &RsValue) -> Ordering {
    match (v1, v2) {
        (RsValue::Null, RsValue::Null) => Ordering::Equal,
        (RsValue::Null, _) => Ordering::Less,
        (_, RsValue::Null) => Ordering::Greater,
        (RsValue::Number(n1), RsValue::Number(n2)) => cmp_float(*n1, *n2),
        (RsValue::Number(n1), right) if crate::util::rsvalue_any_str(right) => {
            let slice = crate::util::rsvalue_as_byte_slice2(right).unwrap();
            if let Some(n2) = crate::util::rsvalue_str_to_float(slice) {
                cmp_float(*n1, n2)
            } else {
                crate::util::rsvalue_num_to_str(*n1).as_bytes().cmp(slice)
            }
        }
        (left, RsValue::Number(n2)) if crate::util::rsvalue_any_str(left) => {
            let slice = crate::util::rsvalue_as_byte_slice2(left).unwrap();
            if let Some(n1) = crate::util::rsvalue_str_to_float(slice) {
                cmp_float(n1, *n2)
            } else {
                slice.cmp(crate::util::rsvalue_num_to_str(*n2).as_bytes())
            }
        }
        (left, right)
            if crate::util::rsvalue_any_str(left) && crate::util::rsvalue_any_str(right) =>
        {
            let slice1 = crate::util::rsvalue_as_byte_slice2(left).unwrap();
            let slice2 = crate::util::rsvalue_as_byte_slice2(right).unwrap();
            slice1.cmp(slice2)
        }
        (RsValue::Trio(t1), RsValue::Trio(t2)) => cmp(t1.left().value(), t2.left().value()),
        (RsValue::Array(a1), RsValue::Array(a2)) => {
            for (i1, i2) in a1.iter().zip(a2.deref()) {
                let cmp = cmp(i1.value(), i2.value());
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            a1.len().cmp(&a2.len())
        }
        (RsValue::Map(_), RsValue::Map(_)) => Ordering::Equal, // can't compare maps ATM
        _ => unimplemented!("RSValue cmp (v1: {:?}, v2: {:?})", v1, v2),
    }
}

fn cmp_float(n1: f64, n2: f64) -> Ordering {
    // C version: v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
    // DISCUSS: In the C version any one side with a NaN is always 'equal' regardless of the other side.
    //          But NaN is never greater/less/equal to any other value, not even to another NaN.
    if n1 > n2 {
        Ordering::Greater
    } else if n1 < n2 {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}
