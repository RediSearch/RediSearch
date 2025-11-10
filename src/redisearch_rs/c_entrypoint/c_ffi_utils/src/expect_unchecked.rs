/// Generalization of `Result` and `Option`,
/// providing access to their `expect`
/// and `unwrap_unchecked` methods in a generic
/// fashion. Used in [`expect_unchecked`](crate::expect_unchecked!).
///
///
/// This trait is [sealed](https://predr.ag/blog/definitive-guide-to-sealed-traits-in-rust/)
/// and only implemented on `Option<T>` and `Result<T, E: Debug>`.
pub trait Expectable<T>: sealed::Sealed {
    /// Forward call to `T::expect`
    fn expect(self, msg: &str) -> T;

    /// Forward call to `T::unwrap_unchecked`
    /// # Safety
    /// - See [`Option::unwrap_unchecked`] when invoking this macro on an `Option`;
    /// - See [`Result::unwrap_unchecked`] when invoking this macro on a `Result`.
    unsafe fn unwrap_unchecked(self) -> T;
}

impl<T> Expectable<T> for Option<T> {
    fn expect(self, msg: &str) -> T {
        self.expect(msg)
    }

    unsafe fn unwrap_unchecked(self) -> T {
        // Safety: the caller is required to uphold
        // the safety requirements of `Option::unwrap_unchecked`
        unsafe { self.unwrap_unchecked() }
    }
}

impl<T, E: std::fmt::Debug> Expectable<T> for Result<T, E> {
    fn expect(self, msg: &str) -> T {
        self.expect(msg)
    }

    unsafe fn unwrap_unchecked(self) -> T {
        // Safety: the caller is required to uphold
        // the safety requirements of `Result::unwrap_unchecked`
        unsafe { self.unwrap_unchecked() }
    }
}

mod sealed {
    use std::fmt::Debug;

    pub trait Sealed {}

    impl<T> Sealed for Option<T> {}

    impl<T, E: Debug> Sealed for Result<T, E> {}
}

/// A convenience macro that allows for `expect`-ing `Option<T>` and `Result<T, E: Debug>`
/// only in debug mode, and calls `unwrap_unchecked` in release mode.
///
/// That way, tests and debug builds panic with a helpful message, while
/// in release mode the check is skipped entirely, making things more performant.
///
/// The macro is set up in such a way that the invocation needs to be wrapped
/// in an unsafe block, even in debug mode.
///
/// When used with a single parameter, the macro expects that parameter to
/// be of type `Option<std::ptr::NonNull<T>>`, and it provides a default
/// error message tailored for that type.
///
/// If this macro is invocated on another type, an error message to
/// pass to `expect` is to be provided.
///
/// # Safety
/// - See [`Option::unwrap_unchecked`] when invoking this macro on an `Option`;
/// - See [`Result::unwrap_unchecked`] when invoking this macro on a `Result`.
#[macro_export]
macro_rules! expect_unchecked {
    ($opt:expr) => {{
        // Validate that $opt is an `Option<NonNull<_>>`.
        // If not, the debug message wouldn't make sense,
        // in which case a custom message should be provided.
        let opt: Option<std::ptr::NonNull<_>> = $opt;
        $crate::expect_unchecked!(opt, concat!(stringify!($opt), " must not be NULL"))
    }};
    ($opt:expr, $msg:expr) => {{
        // Have the macro expand to a call to an unsafe function,
        // forcing the user to put the macro invocation inside an
        // unsafe block, even in debug mode.
        #[inline(always)]
        unsafe fn do_expect<T, E: $crate::expect_unchecked::Expectable<T>>(opt: E, msg: &str) -> T {
            #[cfg(debug_assertions)]
            {
                opt.expect(msg)
            }
            #[cfg(not(debug_assertions))]
            unsafe {
                opt.unwrap_unchecked()
            }
        }
        do_expect($opt, $msg)
    }};
}
