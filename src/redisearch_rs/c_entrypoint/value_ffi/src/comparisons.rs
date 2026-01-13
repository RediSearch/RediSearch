use query_error::{QueryError, QueryErrorCode};
use std::mem::ManuallyDrop;
use std::{cmp::Ordering, ffi::c_int};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Cmp(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    let shared_v1 = unsafe { SharedRsValue::from_raw(v1) };
    let shared_v1 = ManuallyDrop::new(shared_v1);
    let shared_v2 = unsafe { SharedRsValue::from_raw(v2) };
    let shared_v2 = ManuallyDrop::new(shared_v2);

    let v1 = shared_v1.fully_dereferenced_value();
    let v2 = shared_v2.fully_dereferenced_value();

    match compare(v1, v2, status.is_null()) {
        Ok(Ordering::Less) => -1,
        Ok(Ordering::Equal) => 0,
        Ok(Ordering::Greater) => 1,
        Err(CompareError::NaNNumber) => 0,
        Err(CompareError::MapComparison) => 0,
        Err(CompareError::IncompatibleTypes) => 0,
        Err(CompareError::NoNumberToStringFallback) => {
            // SAFETY: Only returned if status is not null.
            let query_error = unsafe { status.as_mut().unwrap() };
            // dbg!(query_error.is_ok());
            // if QueryError_HasError
            let message = c"Error converting string".to_owned();
            query_error.set_code_and_message(QueryErrorCode::NotNumeric, Some(message));
            0
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Equal(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    let shared_v1 = unsafe { SharedRsValue::from_raw(v1) };
    let shared_v1 = ManuallyDrop::new(shared_v1);
    let shared_v2 = unsafe { SharedRsValue::from_raw(v2) };
    let shared_v2 = ManuallyDrop::new(shared_v2);

    let v1 = shared_v1.fully_dereferenced_value();
    let v2 = shared_v2.fully_dereferenced_value();

    match compare(v1, v2, false) {
        Ok(Ordering::Less) => 0,
        Ok(Ordering::Equal) => 1,
        Ok(Ordering::Greater) => 0,
        Err(CompareError::NaNNumber) => 1,
        Err(CompareError::MapComparison) => 1,
        Err(CompareError::IncompatibleTypes) => 1,
        Err(CompareError::NoNumberToStringFallback) => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_BoolTest(value: *const RsValue) -> c_int {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.fully_dereferenced_value();

    let result = match value {
        RsValue::Number(num) => *num != 0.0,
        RsValue::Array(arr) => arr.len() != 0,
        _ if crate::util::rsvalue_any_str(value) => {
            crate::util::rsvalue_as_byte_slice(value).unwrap().len() != 0
        }
        _ => false,
    };

    result as c_int
}

#[derive(Debug)]
enum CompareError {
    NaNNumber,
    NoNumberToStringFallback,
    MapComparison,
    IncompatibleTypes,
}

// Compares two values.
// num_to_str_cmp_fallback is only enabled when called from RSValue_Cmp if a QueryError is provided.
// num_to_str_cmp_fallback is always disabled when called from RSValue_Equal, regardless of the provided QueryError.
fn compare(
    v1: &RsValue,
    v2: &RsValue,
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    match (v1, v2) {
        (RsValue::Null, RsValue::Null) => Ok(Ordering::Equal),
        (RsValue::Null, _) => Ok(Ordering::Less),
        (_, RsValue::Null) => Ok(Ordering::Greater),
        (RsValue::Number(n1), RsValue::Number(n2)) => {
            n1.partial_cmp(n2).ok_or(CompareError::NaNNumber)
        }
        (RsValue::Number(n1), right) if crate::util::rsvalue_any_str(right) => {
            compare_number_to_string(*n1, right, num_to_str_cmp_fallback)
        }
        (left, RsValue::Number(n2)) if crate::util::rsvalue_any_str(left) => {
            compare_number_to_string(*n2, left, num_to_str_cmp_fallback).map(Ordering::reverse)
        }
        (left, right)
            if crate::util::rsvalue_any_str(left) && crate::util::rsvalue_any_str(right) =>
        {
            let slice1 = crate::util::rsvalue_as_byte_slice(left).unwrap();
            let slice2 = crate::util::rsvalue_as_byte_slice(right).unwrap();
            Ok(slice1.cmp(slice2))
        }
        (RsValue::Trio(t1), RsValue::Trio(t2)) => compare(
            t1.left().value(),
            t2.left().value(),
            num_to_str_cmp_fallback,
        ),
        (RsValue::Array(a1), RsValue::Array(a2)) => {
            for (i1, i2) in a1.iter().zip(a2) {
                let cmp = compare(i1.value(), i2.value(), num_to_str_cmp_fallback);
                if matches!(cmp, Ok(Ordering::Equal)) {
                    return cmp;
                }
            }
            Ok(a1.len().cmp(&a2.len()))
        }
        (RsValue::Map(m1), RsValue::Map(m2)) => Err(CompareError::MapComparison),
        _ => Err(CompareError::IncompatibleTypes),
    }
}

fn compare_number_to_string(
    number: f64,
    string: &RsValue,
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    let slice = crate::util::rsvalue_as_byte_slice(string).unwrap();
    // first try to convert the string to a number for comparison
    if let Some(other_number) = crate::util::rsvalue_str_to_float(slice) {
        number
            .partial_cmp(&other_number)
            .ok_or(CompareError::NaNNumber)
    // else only if num_to_str_cmp_fallback is enabled, convert the number to a string for comparison
    } else if num_to_str_cmp_fallback {
        Ok(crate::util::rsvalue_num_to_str(number)
            .as_bytes()
            .cmp(slice))
    } else {
        Err(CompareError::NoNumberToStringFallback)
    }
}

// /* Compare 2 values for sorting */
// int RSValue_Cmp(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
//   RS_LOG_ASSERT(v1 && v2, "missing RSvalue");
//   if (v1->_t == v2->_t) {
//     return RSValue_CmpNC(v1, v2, qerr);
//   }

//   // if one of the values is null, the other wins
//   if (v1 == RSValue_NullStatic()) {
//     return -1;
//   } else if (v2 == RSValue_NullStatic()) {
//     return 1;
//   }

//   // if either of the arguments is a number, convert the other one to a number
//   // if, however, error handling is not available, fallback to string comparison
//   do {
//     if (v1->_t == RSValueType_Number) {
//       RSValue v2n;
//       if (!convert_to_number(v2, &v2n, qerr)) {
//         // if it is possible to indicate an error, return
//         if (qerr) return 0;
//         // otherwise, fallback to string comparison
//         break;
//       }
//       return cmp_numbers(v1, &v2n);
//     } else if (v2->_t == RSValueType_Number) {
//       RSValue v1n;
//       if (!convert_to_number(v1, &v1n, qerr)) {
//         // if it is possible to indicate an error, return
//         if (qerr) return 0;
//         // otherwise, fallback to string comparison
//         break;
//       }
//       // otherwise, fallback to string comparison
//       return cmp_numbers(&v1n, v2);
//     }
//   } while (0);

//   // cast to strings and compare as strings
//   char buf1[100], buf2[100];

//   size_t l1, l2;
//   const char *s1 = RSValue_ConvertStringPtrLen(v1, &l1, buf1, sizeof(buf1));
//   const char *s2 = RSValue_ConvertStringPtrLen(v2, &l2, buf2, sizeof(buf2));
//   return cmp_strings(s1, s2, l1, l2);
// }

// /* Return 1 if the two values are equal */
// int RSValue_Equal(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
//   RS_LOG_ASSERT(v1 && v2, "missing RSvalue");

//   if (v1->_t == v2->_t) {
//     return RSValue_CmpNC(v1, v2, qerr) == 0;
//   }

//   if (v1 == RSValue_NullStatic() || v2 == RSValue_NullStatic()) {
//     return 0;
//   }

//   // if either of the arguments is a number, convert the other one to a number
//   RSValue vn;
//   if (v1->_t == RSValueType_Number) {
//     if (!convert_to_number(v2, &vn, NULL)) return 0;
//     return cmp_numbers(v1, &vn) == 0;
//   } else if (v2->_t == RSValueType_Number) {
//     if (!convert_to_number(v1, &vn, NULL)) return 0;
//     return cmp_numbers(&vn, v2) == 0;
//   }
//
// // static inline int compare_arrays_first(const RSValue *arr1, const RSValue *arr2, QueryError *qerr) {
//   uint32_t len1 = arr1->_arrval.len;
//   uint32_t len2 = arr2->_arrval.len;

//   uint32_t len = MIN(len1, len2);
//   if (len) {
//     // Compare only the first entry
//     return RSValue_Cmp(arr1->_arrval.vals[0], arr2->_arrval.vals[0], qerr);
//   }
//   return len1 - len2;
// }

// // TODO: Use when SORTABLE is not looking only at the first array element
// static inline int compare_arrays(const RSValue *arr1, const RSValue *arr2, QueryError *qerr) {
//   uint32_t len1 = arr1->_arrval.len;
//   uint32_t len2 = arr2->_arrval.len;

//   uint32_t len = MIN(len1, len2);
//   for (uint32_t i = 0; i < len; i++) {
//     int cmp = RSValue_Cmp(arr1->_arrval.vals[i], arr2->_arrval.vals[i], qerr);
//     if (cmp != 0) {
//       return cmp;
//     }
//   }
//   return len1 - len2;
// }

// static int RSValue_CmpNC(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
//   switch (v1->_t) {
//     case RSValueType_Number:
//       return cmp_numbers(v1, v2);
//     case RSValueType_String:
//       return cmp_strings(v1->_strval.str, v2->_strval.str, v1->_strval.len, v2->_strval.len);
//     case RSValueType_RedisString: {
//       size_t l1, l2;
//       const char *s1 = RedisModule_StringPtrLen(v1->_rstrval, &l1);
//       const char *s2 = RedisModule_StringPtrLen(v2->_rstrval, &l2);
//       return cmp_strings(s1, s2, l1, l2);
//     }
//     case RSValueType_Trio:
//       return RSValue_Cmp(RSValue_Trio_GetLeft(v1), RSValue_Trio_GetLeft(v2), qerr);
//     case RSValueType_Null:
//       return 0;
//     case RSValueType_Array:
//       return compare_arrays_first(v1, v2, qerr);

//     case RSValueType_Map:   // can't compare maps ATM
//     default:
//       return 0;
//   }
// }

//   // cast to strings and compare as strings
//   char buf1[100], buf2[100];

//   size_t l1, l2;
//   const char *s1 = RSValue_ConvertStringPtrLen(v1, &l1, buf1, sizeof(buf1));
//   const char *s2 = RSValue_ConvertStringPtrLen(v2, &l2, buf2, sizeof(buf2));
//   return cmp_strings(s1, s2, l1, l2) == 0;
// }

// static inline int cmp_strings(const char *s1, const char *s2, size_t l1, size_t l2) {
//   // Use memcmp instead of strncmp to correctly handle binary data with embedded NULLs
//   int cmp = memcmp(s1, s2, MIN(l1, l2));
//   if (l1 == l2) {
//     // if the strings are the same length, just return the result of memcmp
//     return cmp;
//   } else {  // if the lengths aren't identical
//     // if the strings are identical but the lengths aren't, return the longer string
//     if (cmp == 0) {
//       return l1 > l2 ? 1 : -1;
//     } else {  // the strings are lexically different, just return that
//       return cmp;
//     }
//   }
// }

// static inline int cmp_numbers(const RSValue *v1, const RSValue *v2) {
//   return v1->_numval > v2->_numval ? 1 : (v1->_numval < v2->_numval ? -1 : 0);
// }

// int RSValue_BoolTest(const RSValue *v) {
//   if (RSValue_IsNull(v)) return 0;

//   v = RSValue_Dereference(v);
//   switch (v->_t) {
//     case RSValueType_Array:
//       return v->_arrval.len != 0;
//     case RSValueType_Number:
//       return v->_numval != 0;
//     case RSValueType_String:
//       return v->_strval.len != 0;
//     case RSValueType_RedisString: {
//       size_t l = 0;
//       const char *p = RedisModule_StringPtrLen(v->_rstrval, &l);
//       return l != 0;
//     }
//     default:
//       return 0;
//   }
// }

// static inline bool convert_to_number(const RSValue *v, RSValue *vn, QueryError *qerr) {
//   double d;
//   if (!RSValue_ToNumber(v, &d)) {
//     if (!qerr) return false;

//     const char *s = RSValue_StringPtrLen(v, NULL);
//     QueryError_SetWithUserDataFmt(qerr, QUERY_ERROR_CODE_NOT_NUMERIC, "Error converting string", " '%s' to number", s);
//     return false;
//   }

//   RSValue_SetNumber(vn, d);
//   return true;
// }

// /* Convert a value to a number, either returning the actual numeric values or by parsing a string
// into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
// not. If possible, we put the actual value into the double pointer */
// int RSValue_ToNumber(const RSValue *v, double *d) {
//   if (RSValue_IsNull(v)) return 0;
//   v = RSValue_Dereference(v);

//   const char *p = NULL;
//   size_t l = 0;
//   switch (v->_t) {
//     // for numerics - just set the value and return
//     case RSValueType_Number:
//       *d = v->_numval;
//       return 1;

//     case RSValueType_String:
//       // C strings - take the ptr and len
//       p = v->_strval.str;
//       l = v->_strval.len;
//       break;
//     case RSValueType_RedisString:
//       // Redis strings - take the number and len
//       p = RedisModule_StringPtrLen(v->_rstrval, &l);
//       break;

//     case RSValueType_Trio:
//       return RSValue_ToNumber(RSValue_Trio_GetLeft(v), d);

//     case RSValueType_Null:
//     case RSValueType_Array:
//     case RSValueType_Map:
//     case RSValueType_Undef:
//     default:
//       return 0;
//   }
//   // If we have a string - try to parse it
//   if (p) {
//     char *e;
//     errno = 0;
//     *d = fast_float_strtod(p, &e);
//     if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
//         *e != '\0') {
//       return 0;
//     }

//     return 1;
//   }

//   return 0;
// }
