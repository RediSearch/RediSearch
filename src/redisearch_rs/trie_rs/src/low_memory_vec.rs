use std::ops::{Deref};
use std::{
    alloc::{alloc, realloc, Layout},
    ops::DerefMut,
};

use std::ptr::{copy, read, write};

#[derive(Debug)]
pub struct LowMemoryVec<T> {
    cap: u32,
    size: u32,
    ptr: *mut T,
}

impl<T: Clone> LowMemoryVec<T> {
    pub fn from_slice(s: &[T]) -> LowMemoryVec<T> {
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

impl<T> LowMemoryVec<T> {
    pub fn new() -> LowMemoryVec<T> {
        LowMemoryVec {
            cap: 0,
            size: 0,
            ptr: std::ptr::null_mut(),
        }
    }

    fn ensure_cap_force(&mut self, cap: u32) {
        let new_layout = Layout::array::<T>(cap as usize).unwrap();
        if self.ptr.is_null() {
            self.ptr = unsafe { std::mem::transmute(alloc(new_layout)) };
        } else {
            let old_layout = Layout::array::<T>(self.cap as usize).unwrap();
            self.ptr = unsafe {
                std::mem::transmute(realloc(self.ptr as *mut u8, old_layout, new_layout.size()))
            };
        }
        self.cap = cap;
    }

    fn truncate_cap(&mut self) {
        if self.size <= self.cap / 2 {
            self.ensure_cap_force(self.size);
            if self.cap == 0 {
                self.ptr = std::ptr::null_mut();
            }
        }
    }

    fn ensure_cap(&mut self, cap: u32) {
        if self.cap < cap {
            let new_cap = self.cap * 2;
            let new_cap = if cap > new_cap {cap} else {new_cap};
            self.ensure_cap_force(new_cap);
        }
    }

    pub fn take(&mut self) -> LowMemoryVec<T> {
        let res = LowMemoryVec{
            cap: self.cap,
            size: self.size,
            ptr: self.ptr,
        };
        self.cap = 0;
        self.size = 0;
        self.ptr = std::ptr::null_mut();
        res
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.size == 0 {
            return None;
        }
        unsafe {
            self.size -= 1;
            let ptr = self.ptr.add(self.size as usize);
            let res = Some(read(ptr));
            self.truncate_cap();
            res
        }
    }

    pub fn remove(&mut self, i: u32) -> T {
        if i >= self.size {
            panic!("On vector remove, given index is greater than the vector size.");
        }
        if i == self.size - 1 {
            return self.pop().unwrap();
        }
        unsafe {
            let ptr = self.ptr.add(i as usize);
            let new_ptr = self.ptr.add((i + 1) as usize);
            let res = read(ptr);
            copy(new_ptr, ptr, (self.size - i - 1) as usize);
            self.size -= 1;
            self.truncate_cap();
            res
        }
    }

    pub fn push(&mut self, val: T) {
        self.ensure_cap(self.size + 1);
        unsafe {
            let ptr = self.ptr.add(self.size as usize);
            write(ptr, val);
        }
        self.size += 1;
    }

    pub fn insert(&mut self, index: u32, val: T) {
        if index > self.size {
            panic!("On vector insert, given index is greater than the vector size.");
        }
        self.ensure_cap(self.size + 1);
        unsafe {
            let ptr = self.ptr.add(index as usize);
            let new_ptr = self.ptr.add((index + 1) as usize);
            copy(ptr, new_ptr, (self.size - index) as usize);
            write(ptr, val);
        }
        self.size += 1;
    }

    pub fn get(&self, i: u32) -> Option<&T> {
        if i >= self.size {
            return None;
        }

        Some(unsafe { &*self.ptr.add(i as usize) })
    }

    pub fn get_mut(&mut self, i: u32) -> Option<&mut T> {
        if i >= self.size {
            return None;
        }

        Some(unsafe { &mut *self.ptr.add(i as usize) })
    }

    pub fn truncate(&mut self, len: u32) {
        if self.size <= len {
            return;
        }

        self.size = len;
        if self.size <= self.cap / 2 {
            self.ensure_cap_force(self.size);
        }
    }
}

impl<T> Deref for LowMemoryVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size as usize) }
    }
}

impl<T> DerefMut for LowMemoryVec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size as usize) }
    }
}

#[cfg(test)]
mod trie_tests {
    use crate::low_memory_vec::LowMemoryVec;

    #[test]
    fn basic1() {
        let mut v: LowMemoryVec<i64> = LowMemoryVec::new();
        v.push(1);
        v.push(2);
        v.push(3);
        let res: Vec<i64> = v.iter().map(|v| *v).collect();
        assert_eq!(res, vec![1, 2, 3]);
    }

    #[test]
    fn test_pop() {
        let mut v: LowMemoryVec<(i64, i64)> = LowMemoryVec::new();
        v.push((1, 1));
        v.push((2, 2));
        v.push((3, 3));
        assert_eq!((3, 3), v.pop().unwrap());
        assert_eq!((2, 2), v.pop().unwrap());
        assert_eq!((1, 1), v.pop().unwrap());
        assert_eq!(0, v.cap);
        assert_eq!(0, v.size);
        assert_eq!(std::ptr::null_mut(), v.ptr);
    }

    #[test]
    fn test_insert() {
        let mut v: LowMemoryVec<i64> = LowMemoryVec::new();
        v.push(1);
        v.push(3);
        v.push(4);
        v.insert(1, 2);
        let res: Vec<i64> = v.iter().map(|v| *v).collect();
        assert_eq!(res, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_del() {
        let mut v: LowMemoryVec<i64> = LowMemoryVec::new();
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
        let mut v: LowMemoryVec<i64> = LowMemoryVec::new();
        if let Err(i) = v.binary_search(&3) {
            v.insert(i as u32, 3);
        }
        if let Err(i) = v.binary_search(&1) {
            v.insert(i as u32, 1);
        }
        if let Err(i) = v.binary_search(&4) {
            v.insert(i as u32, 4);
        }
        if let Err(i) = v.binary_search(&2) {
            v.insert(i as u32, 2);
        }
        let res: Vec<i64> = v.iter().map(|v| *v).collect();
        assert_eq!(res, vec![1, 2, 3, 4]);
    }
}
