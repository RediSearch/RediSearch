use std::ops::{Add, AddAssign, Deref, Sub, SubAssign};
use std::{
    alloc::{alloc, realloc, Layout},
    ops::DerefMut,
};

use std::cmp::Ord;

use std::ptr::{copy, read, write};

pub trait CapType:
    SubAssign<Self>
    + Sub<Self, Output = Self>
    + AddAssign<Self>
    + Add<Self, Output = Self>
    + Default
    + Copy
    + Into<usize>
    + From<u8>
    + Ord
{
}

#[derive(Debug)]
pub struct LowMemoryVec<S: CapType, T> {
    cap: S,
    size: S,
    ptr: *mut T,
}

impl<S: CapType, T: Clone> LowMemoryVec<S, T> {
    pub fn from_slice(s: &[T]) -> LowMemoryVec<S, T> {
        let mut res = Self::new();
        for t in s {
            res.push(t.clone());
        }
        res
    }

    pub fn append(&mut self, other: &[T]) {
        for v in other{
            self.push(v.clone());
        }
    }
}

impl<S: CapType, T> LowMemoryVec<S, T> {
    pub fn new() -> LowMemoryVec<S, T> {
        LowMemoryVec {
            cap: S::default(),
            size: S::default(),
            ptr: std::ptr::null_mut(),
        }
    }

    fn ensure_cap(&mut self, cap: S) {
        if self.cap < cap {
            let new_layout = Layout::array::<T>(cap.into()).unwrap();
            if self.ptr.is_null() {
                self.ptr = unsafe { std::mem::transmute(alloc(new_layout)) };
            } else {
                let old_layout = Layout::array::<T>(self.cap.into()).unwrap();
                self.ptr = unsafe {
                    std::mem::transmute(realloc(self.ptr as *mut u8, old_layout, new_layout.size()))
                };
            }
            self.cap = cap;
        }
    }

    pub fn take(&mut self) -> LowMemoryVec<S, T> {
        let res = LowMemoryVec{
            cap: self.cap,
            size: self.size,
            ptr: self.ptr,
        };
        self.cap = S::default();
        self.size = S::default();
        self.ptr = std::ptr::null_mut();
        res
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.size == S::default() {
            return None;
        }
        unsafe {
            self.size -= S::from(1);
            let ptr = self.ptr.add(self.size.into());
            Some(read(ptr))
        }
    }

    pub fn remove(&mut self, i: S) -> T {
        if i >= self.size {
            panic!("On vector remove, given index is greater than the vector size.");
        }
        if i == self.size - S::from(1) {
            return self.pop().unwrap();
        }
        unsafe {
            let ptr = self.ptr.add(i.into());
            let new_ptr = self.ptr.add((i + S::from(1)).into());
            let res = read(ptr);
            copy(new_ptr, ptr, (self.size - i - S::from(1)).into());
            self.size -= S::from(1);
            res
        }
    }

    pub fn push(&mut self, val: T) {
        self.ensure_cap(self.size + S::from(1));
        unsafe {
            let ptr = self.ptr.add(self.size.into());
            write(ptr, val);
        }
        self.size += S::from(1);
    }

    pub fn insert(&mut self, index: S, val: T) {
        if index > self.size {
            panic!("On vector insert, given index is greater than the vector size.");
        }
        self.ensure_cap(self.size + S::from(1));
        unsafe {
            let ptr = self.ptr.add(index.into());
            let new_ptr = self.ptr.add((index + S::from(1)).into());
            copy(ptr, new_ptr, (self.size - index).into());
            write(ptr, val);
        }
        self.size += S::from(1);
    }

    pub fn get(&self, i: S) -> Option<&T> {
        if i >= self.size || i < S::default() {
            return None;
        }

        Some(unsafe { &*self.ptr.add(i.into()) })
    }

    pub fn get_mut(&mut self, i: S) -> Option<&mut T> {
        if i >= self.size || i < S::default() {
            return None;
        }

        Some(unsafe { &mut *self.ptr.add(i.into()) })
    }

    pub fn truncate(&mut self, len: S) {
        if self.size <= len {
            return;
        }

        self.size = len;
        // todo: actually return the memory?
    }
}

impl<S: CapType, T> Deref for LowMemoryVec<S, T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size.into()) }
    }
}

impl<S: CapType, T> DerefMut for LowMemoryVec<S, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size.into()) }
    }
}

impl CapType for u8 {}
impl CapType for u16 {}
impl CapType for usize {}

#[cfg(test)]
mod trie_tests {
    use crate::low_memory_vec::LowMemoryVec;

    #[test]
    fn basic1() {
        let mut v: LowMemoryVec<u8, i64> = LowMemoryVec::new();
        v.push(1);
        v.push(2);
        v.push(3);
        let res: Vec<i64> = v.iter().map(|v| *v).collect();
        assert_eq!(res, vec![1, 2, 3]);
    }

    #[test]
    fn test_pop() {
        let mut v: LowMemoryVec<u8, (i64, i64)> = LowMemoryVec::new();
        v.push((1, 1));
        v.push((2, 2));
        v.push((3, 3));
        assert_eq!((3, 3), v.pop().unwrap());
        assert_eq!((2, 2), v.pop().unwrap());
        assert_eq!((1, 1), v.pop().unwrap());
    }

    #[test]
    fn test_insert() {
        let mut v: LowMemoryVec<u8, i64> = LowMemoryVec::new();
        v.push(1);
        v.push(3);
        v.push(4);
        v.insert(1, 2);
        let res: Vec<i64> = v.iter().map(|v| *v).collect();
        assert_eq!(res, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_del() {
        let mut v: LowMemoryVec<u8, i64> = LowMemoryVec::new();
        v.push(1);
        v.push(3);
        v.push(4);
        v.insert(1, 2);
        assert_eq!(2, v.remove(1));
        let res: Vec<i64> = v.iter().map(|v| *v).collect();
        assert_eq!(res, vec![1, 3, 4]);
    }

    #[test]
    fn test_binary_search() {
        let mut v: LowMemoryVec<u8, i64> = LowMemoryVec::new();
        if let Err(i) = v.binary_search(&3) {
            v.insert(i as u8, 3);
        }
        if let Err(i) = v.binary_search(&1) {
            v.insert(i as u8, 1);
        }
        if let Err(i) = v.binary_search(&4) {
            v.insert(i as u8, 4);
        }
        if let Err(i) = v.binary_search(&2) {
            v.insert(i as u8, 2);
        }
        let res: Vec<i64> = v.iter().map(|v| *v).collect();
        assert_eq!(res, vec![1, 2, 3, 4]);
    }
}
