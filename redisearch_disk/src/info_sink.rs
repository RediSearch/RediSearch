//! Trait and implementations for writing to Redis INFO output.

use std::ffi::CStr;

use ffi::RedisModuleInfoCtx;

/// Trait for writing fields to Redis INFO output.
///
/// This abstracts the INFO writing operations, allowing different implementations
/// for production (FFI calls) and testing (mock capture).
pub trait InfoSink {
    fn add_u64(&mut self, name: &CStr, value: u64);

    /// Write a section with the given name, calling `f` to populate its contents.
    fn with_section(&mut self, name: &CStr, f: impl FnOnce(&mut Self));

    /// Write a dictionary with the given name, calling `f` to populate its contents.
    fn with_dict(&mut self, name: &CStr, f: impl FnOnce(&mut Self));
}

impl InfoSink for &mut RedisModuleInfoCtx {
    fn add_u64(&mut self, name: &CStr, value: u64) {
        // SAFETY: Function pointer is initialized during module load
        let func = unsafe { ffi::RedisModule_InfoAddFieldULongLong.unwrap() };
        // SAFETY: ctx is valid, name is a valid C string
        unsafe { func(*self, name.as_ptr(), value) };
    }

    fn with_section(&mut self, name: &CStr, f: impl FnOnce(&mut Self)) {
        // SAFETY: Function pointer is initialized during module load
        let func = unsafe { ffi::RedisModule_InfoAddSection.unwrap() };
        // SAFETY: ctx is valid, name is a valid C string
        unsafe { func(*self, name.as_ptr()) };
        f(self);
    }

    fn with_dict(&mut self, name: &CStr, f: impl FnOnce(&mut Self)) {
        // SAFETY: Function pointer is initialized during module load
        let begin = unsafe { ffi::RedisModule_InfoBeginDictField.unwrap() };
        // SAFETY: ctx is valid, name is a valid C string
        unsafe { begin(*self, name.as_ptr()) };

        f(self);

        // SAFETY: Function pointer is initialized during module load
        let end = unsafe { ffi::RedisModule_InfoEndDictField.unwrap() };
        // SAFETY: ctx is valid
        unsafe { end(*self) };
    }
}
