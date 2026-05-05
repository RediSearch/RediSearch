/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assertion macros that integrate with the [`tracing`] ecosystem.
//!
//! These macros bridge debug-time invariants and release-time observability:
//! a violation panics in debug builds and emits a `tracing::warn` event in
//! release builds.
//!
//! A single macro is provided:
//!
//! * [`debug_assert_warn!`] — guards a condition that *should* hold.
//!
//! The message tokens are forwarded verbatim and must be valid format-args
//! input (a literal format string followed by positional/named arguments).
//! Tracing-only structured field syntax (`field = value, "message"`) is not
//! supported because the same tokens are also fed to [`debug_assert!`] /
//! [`panic!`], which only accept format-args.

/// Fires a [`debug_assert!`] and emits a [`tracing::warn`] when `$cond` is `false`.
///
/// In debug builds the process panics immediately on violation; in release
/// builds the warning is emitted as a `tracing::warn` event without aborting.
///
/// The message tokens must be valid format-args input — see the
/// [crate-level docs](crate) for the constraint.
///
/// # Example
///
/// ```
/// # fn check(items: &[u8]) {
/// tracing_assert::debug_assert_warn!(items.len().is_multiple_of(2), "odd-length array");
/// # }
/// # check(&[0, 1]);
/// ```
#[macro_export]
macro_rules! debug_assert_warn {
    ($cond:expr, $($arg:tt)+) => {{
        let ok = $cond;
        ::core::debug_assert!(ok, $($arg)+);
        if !ok {
            ::tracing::warn!($($arg)+);
        }
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn debug_assert_warn_passes_when_condition_holds() {
        let items: &[u8] = &[0, 1];
        crate::debug_assert_warn!(items.len().is_multiple_of(2), "odd-length array");
    }

    #[test]
    fn debug_assert_warn_forwards_format_args() {
        let len = 4_usize;
        crate::debug_assert_warn!(len > 0, "len = {len}");
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn debug_assert_warn_false_only_warns_in_release() {
        // In release builds the macro must not panic; in debug builds the
        // companion `debug_assert_warn_false_panics_in_debug` test exercises the panic
        // path instead.
        crate::debug_assert_warn!(false, "unreachable branch hit");
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "unreachable branch hit")]
    fn debug_assert_warn_false_panics_in_debug() {
        crate::debug_assert_warn!(false, "unreachable branch hit");
    }
}
