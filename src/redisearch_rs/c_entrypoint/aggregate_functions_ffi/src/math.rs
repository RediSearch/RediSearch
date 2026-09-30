/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The single-argument math functions: `log`, `log2`, `exp`, `sqrt`, `abs`,
//! `floor` and `ceil`.
//!
//! Each converts its argument as [`Value::to_number`] does and yields NaN for an
//! argument with no numeric value. `log`, `log2` and `exp` call the platform math
//! library, as the C implementation did; the others are exact in IEEE 754.

use std::ffi::c_int;

use value::Value;
use value_ffi::RSValue;
use value_ffi::util::{as_shared_value, try_value};

/// Sets `result` to `op` of the numeric value of the first argument, or to NaN
/// if it has none.
///
/// # Safety
///
/// 1. `argv` must point to `argc` [valid] [`RSValue`] pointers, and `argc` must be
///    at least 1.
/// 2. `result` must be a [valid] pointer to an [`RSValue`] that nothing else
///    references.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe fn apply_unary(
    argv: *const *mut RSValue,
    argc: usize,
    result: *mut RSValue,
    op: fn(f64) -> f64,
) -> c_int {
    debug_assert!(argc >= 1, "math functions take one argument");
    debug_assert!(!argv.is_null() && !result.is_null());

    // SAFETY: `argv` holds at least one pointer (1.)
    let arg = unsafe { *argv };
    // SAFETY: that pointer is valid (1.)
    let arg = unsafe { try_value(arg) };
    let num = arg.and_then(Value::to_number).map_or(f64::NAN, op);

    // SAFETY: ensured by caller (2.)
    let mut result = unsafe { as_shared_value(result) };
    result.set_value(Value::Number(num));
    ffi::EXPR_EVAL_OK as c_int
}

/// Defines an exported `RSFunction` callback applying `$op` to its argument.
macro_rules! math_function {
    ($(#[$doc:meta])* $name:ident => $op:expr) => {
        $(#[$doc])*
        ///
        /// # Safety
        ///
        /// 1. `argv` must point to `argc` [valid] [`RSValue`] pointers, and `argc`
        ///    must be at least 1.
        /// 2. `result` must be a [valid] pointer to an [`RSValue`] that nothing
        ///    else references.
        ///
        /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(
            _ctx: *mut ffi::ExprEval,
            argv: *mut *mut RSValue,
            argc: usize,
            result: *mut RSValue,
        ) -> c_int {
            // SAFETY: ensured by caller (1., 2.)
            unsafe { apply_unary(argv, argc, result, $op) }
        }
    };
}

math_function! {
    /// `log(x)`: the natural logarithm.
    MathFunction_Log => f64::ln
}

math_function! {
    /// `log2(x)`: the base-2 logarithm.
    MathFunction_Log2 => f64::log2
}

math_function! {
    /// `exp(x)`: e raised to `x`.
    MathFunction_Exp => f64::exp
}

math_function! {
    /// `sqrt(x)`: the square root.
    MathFunction_Sqrt => f64::sqrt
}

math_function! {
    /// `abs(x)`: the absolute value.
    MathFunction_Abs => f64::abs
}

math_function! {
    /// `floor(x)`: the largest integer not greater than `x`.
    MathFunction_Floor => f64::floor
}

math_function! {
    /// `ceil(x)`: the smallest integer not less than `x`.
    MathFunction_Ceil => f64::ceil
}
