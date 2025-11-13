/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::{Deref, DerefMut};

/// [CanaryProtected] can be implemented to protect structs from wrong initialization or memory corruption in respect with FFI boundaries.
///
/// A canary value is stored in the first field of the struct and validated on every access in debug mode. Thus if that
/// 8 bytes constant is corrupted, an assertion will fire, indicating memory corruption or wrong initialization as early
/// as possible.
///
/// # Safety
///
/// Types that implement this trait must ensure under `debug_assertions` that the first field is a `u64` canary variable.
/// In release mode the canary accessor method is not available.
pub unsafe trait CanaryProtected: Sized {
    const CANARY: u64;

    /// Get the canary value from the first field
    #[cfg(debug_assertions)]
    fn canary(&self) -> u64 {
        // Safety: The implementer promised that the first field is a u64 canary variable.
        unsafe { *(self as *const Self as *const u64) }
    }

    /// Validate the canary
    #[cfg(debug_assertions)]
    fn assert_valid(&self) {
        assert_eq!(
            self.canary(),
            Self::CANARY,
            r###"canary mismatch in {}! Expected {:#x} but found {:#x} -> possible memory corruption or wrong initialization.
              1. Ensure Rust side uses 
            "###,
            core::any::type_name::<Self>(),
            Self::CANARY,
            self.canary()
        );
    }
}

/// A Guard that auto-validates the canary on every access in debug mode.
#[repr(transparent)]
pub struct CanaryGuarded<T: CanaryProtected>(T);

impl<T: CanaryProtected> CanaryGuarded<T> {
    #[cfg(not(debug_assertions))]
    pub const fn new(inner: T) -> Self {
        Self(inner)
    }

    #[cfg(debug_assertions)]
    pub fn new(inner: T) -> Self {
        let guard = Self(inner);
        #[cfg(debug_assertions)]
        guard.0.assert_valid();
        guard
    }

    #[inline(always)]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: CanaryProtected> Deref for CanaryGuarded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        #[cfg(debug_assertions)]
        self.0.assert_valid();
        &self.0
    }
}

impl<T: CanaryProtected> DerefMut for CanaryGuarded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        #[cfg(debug_assertions)]
        self.0.assert_valid();
        &mut self.0
    }
}
