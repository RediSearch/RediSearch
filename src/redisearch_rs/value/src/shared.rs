/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{mem::ManuallyDrop, sync::Arc};

use crate::{RsValue, Value};

pub struct SharedRsValue {
    ptr: *const RsValue,
}

static NULL_VALUE: RsValue = RsValue::Null;

unsafe impl Send for SharedRsValue {}
unsafe impl Sync for SharedRsValue {}

impl SharedRsValue {
    pub fn null_static() -> Self {
        Self {
            ptr: &NULL_VALUE as *const RsValue,
        }
    }

    pub fn new(value: RsValue) -> Self {
        Self {
            ptr: Arc::into_raw(Arc::new(value)),
        }
    }

    /// Convert a [`SharedRsValue`] into a raw `*const RsValue` pointer.
    pub fn into_raw(self) -> *const RsValue {
        let ptr = self.ptr;
        std::mem::forget(self); // Prevent Drop from running
        ptr
    }

    pub fn as_ptr(&self) -> *const RsValue {
        self.ptr
    }

    /// Convert a `*const RsValue` back into a [`SharedRsValue`].
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer obtained from [`SharedRsValue::into_raw`].
    pub unsafe fn from_raw(ptr: *const RsValue) -> Self {
        Self { ptr }
    }

    fn is_static(&self) -> bool {
        std::ptr::eq(self.ptr, &NULL_VALUE)
    }

    pub fn refcount(&self) -> usize {
        if self.is_static() {
            1
        } else {
            let v = ManuallyDrop::new(unsafe { Arc::from_raw(self.ptr) });
            Arc::strong_count(&v)
        }
    }

    pub fn fully_dereferenced(&self) -> &Self {
        if let RsValue::Ref(ref_value) = self.value() {
            ref_value.fully_dereferenced()
        } else {
            self
        }
    }

    pub fn fully_dereferenced_value(&self) -> &RsValue {
        self.fully_dereferenced().value()
    }

    pub unsafe fn set_value(&mut self, new_value: RsValue) {
        if self.is_static() {
            panic!("Cannot change the value of static NULL");
        }
        let mut v = ManuallyDrop::new(unsafe { Arc::from_raw(self.ptr) });
        let value = Arc::get_mut(&mut v).expect("Failed to get mutable reference to inner value");
        *value = new_value;
    }

    pub fn from_value(value: RsValue) -> Self {
        Self::new(value)
    }

    pub fn value(&self) -> &RsValue {
        if self.is_static() {
            &NULL_VALUE
        } else {
            unsafe { &*self.ptr }
        }
    }
}

impl Value for SharedRsValue {
    fn from_value(value: RsValue) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Debug for SharedRsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value().fmt(f)
    }
}

impl Clone for SharedRsValue {
    fn clone(&self) -> Self {
        if self.is_static() {
            Self { ptr: self.ptr }
        } else {
            // Increment refcount
            let arc: Arc<RsValue> = unsafe { Arc::from_raw(self.ptr) };
            let cloned = Arc::clone(&arc);
            std::mem::forget(arc);
            Self {
                ptr: Arc::into_raw(cloned),
            }
        }
    }
}

impl Drop for SharedRsValue {
    fn drop(&mut self) {
        if !self.is_static() {
            unsafe { drop(Arc::from_raw(self.ptr)) };
        }
    }
}
