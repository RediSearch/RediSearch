use std::{fmt::Debug, ops::Deref, ptr::NonNull, sync::Arc};

#[repr(C)]
pub struct FFISafeArc<T>
where
    T: ?Sized,
{
    /// `NonNull` for niche optimization
    ptr: NonNull<T>,
}

impl<T: ?Sized> FFISafeArc<T> {
    fn as_const_ptr(&self) -> *const T {
        self.ptr.as_ptr() as *const _
    }
}

impl<T: ?Sized> From<Arc<T>> for FFISafeArc<T> {
    fn from(value: Arc<T>) -> Self {
        let ptr = Arc::into_raw(value) as *mut _;
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        Self { ptr }
    }
}

impl<T: ?Sized> From<FFISafeArc<T>> for Arc<T> {
    fn from(value: FFISafeArc<T>) -> Self {
        // Safety: FFISafeArc can only be constructed
        // from an Arc using `Arc::into_raw`
        let ptr = value.as_const_ptr();
        std::mem::forget(value);
        unsafe { Arc::from_raw(ptr) }
    }
}

impl<T: ?Sized> AsRef<T> for FFISafeArc<T> {
    fn as_ref(&self) -> &T {
        unsafe { &*self.as_const_ptr() }
    }
}

impl<T: ?Sized> Deref for FFISafeArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.as_const_ptr() }
    }
}

impl<T: Debug + ?Sized> Debug for FFISafeArc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(unsafe { &*self.as_const_ptr() }, f)
    }
}

impl<T: ?Sized> Clone for FFISafeArc<T> {
    fn clone(&self) -> Self {
        unsafe { Arc::increment_strong_count(self.as_const_ptr()) };
        Self { ptr: self.ptr }
    }
}

impl<T: ?Sized> Drop for FFISafeArc<T> {
    fn drop(&mut self) {
        unsafe {
            Arc::decrement_strong_count(self.as_const_ptr());
        }
    }
}

unsafe impl<T: ?Sized> Send for FFISafeArc<T> {}
unsafe impl<T: ?Sized> Sync for FFISafeArc<T> {}
