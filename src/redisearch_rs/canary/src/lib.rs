#![no_std]

use core::ops::{Deref, DerefMut};

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Canary<T, const MAGIC: u64> {
    _canary: u64,
    inner: T,
}

impl<T, const CANARY: u64> Canary<T, CANARY> {
    pub const fn new(inner: T) -> Self {
        Self {
            _canary: CANARY,
            inner,
        }
    }

    pub fn assert_valid(me: &Self) {
        assert!(
            me._canary == CANARY,
            "canary mismatch! {} is not properly initialized (expected {CANARY} but found {})",
            core::any::type_name::<T>(),
            me._canary
        );
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T, const CANARY: u64> Deref for Canary<T, CANARY> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        #[cfg(debug_assertions)]
        Self::assert_valid(self);

        &self.inner
    }
}

impl<T, const CANARY: u64> DerefMut for Canary<T, CANARY> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        #[cfg(debug_assertions)]
        Self::assert_valid(self);

        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basically_works() {
        const CANARY: u64 = u64::from_le_bytes(*b"testmagc");

        let mut canary = Canary::<_, CANARY>::new(42);
        assert_eq!(*canary, 42);
        *canary += 1;
        assert_eq!(*canary, 43);
    }

    #[test]
    #[cfg_attr(debug_assertions, should_panic)]
    fn panics_with_incorrect_canary() {
        const CANARY: u64 = u64::from_le_bytes(*b"testmagc");

        let mut canary = Canary::<_, CANARY>::new(42);
        canary._canary = 10;
        assert_eq!(*canary, 42);
    }
}
