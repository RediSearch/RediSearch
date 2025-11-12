/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    alloc::{Layout, alloc, dealloc},
    fmt,
    ptr::NonNull,
};

use crate::SharedRsValue;

/// An low-memory immutable structure that holds and manages a set of
/// T items.
///
/// This collection's capacity is represented by an `u32` and the
/// collection itself is `#[repr(C, packed)]` so that it's size is just
/// 12 bytes.
///
/// # Invariants
/// - (1) Can hold at most [`Self::MAX_CAPACITY`] items, which on 32-bit systems
///   is less than `u32::MAX`. The reason for this is that when doing pointer
///   addition, we must ensure we don't overflow `isize::MAX`.
///   See [`NonNull::add`].
#[repr(C, packed)]
pub struct RsValueCollection<T> {
    /// Pointer to a heap-allocated array of `Self::cap` items.
    entries: NonNull<T>,
    /// The number of items this collection can hold
    cap: u32,
}

impl<T> RsValueCollection<T> {
    /// The maximum number of items this collection can
    /// hold on this platform. Calculated as the minimum of
    /// `u32::MAX` and `isize::MAX / size_of::<T>()`.
    pub const MAX_CAPACITY: usize = {
        let max = (isize::MAX as usize) / size_of::<T>();
        if max > (u32::MAX) as usize {
            u32::MAX as usize
        } else {
            max
        }
    };

    /// Allocate space for `cap` items,
    /// and create a [`RsValueCollection`] that wraps these items.
    ///
    /// # Safety
    /// - (1) The created `RsValueCollection`'s items must all be initialized using [`RsValueCollection::write_entry`]
    ///   before the map's data can be used or the map itself can be dropped.
    /// - (2) `cap` must not exceed [`Self::MAX_CAPACITY`], which is relevant on e.g. 32-bit systems.
    pub unsafe fn reserve_uninit(cap: u32) -> Self {
        if cap == 0 {
            // No allocation needed
            return Self {
                entries: NonNull::dangling(),
                cap,
            };
        }
        debug_assert!(
            (cap as usize) <= Self::MAX_CAPACITY,
            "Capacity exceeds maximum capacity of {} items",
            Self::MAX_CAPACITY
        );
        let layout = Self::entry_layout(cap);
        // Safety: the size of `layout` is always greater than 0
        // as we return early if `cap` equals 0.
        let ptr = unsafe { alloc(layout) };
        let entries = NonNull::new(ptr as *mut T).unwrap();
        Self { entries, cap }
    }

    /// Write an item into the map at position `i`.
    /// Does not read or drop the existing value. Use this function to
    /// initialize an [`RsValueCollection`] that was created using [`RsValueCollection::reserve_uninit`].
    ///
    /// Memory will leak in case this function is used to overwrite an existing entry.
    ///
    /// # Safety
    /// - (1) `i` must be less than the map's capacity, which is always less than
    ///   `isize::MAX / size_of::<RsValueMapEntry>()`.
    pub unsafe fn write_entry(&mut self, entry: T, i: u32) {
        debug_assert!(i < self.cap, "Index was out of bounds");
        // Safety:
        // - The caller must ensure (1).
        // - In `Self::reserve_uninit`, `self.entries` pointer was obtained from a call to `alloc`
        //   unless its capacity was set to 0, in which case the caller
        //   is violating (1).
        let ptr = unsafe { self.entries.add(i as usize) };
        // Safety:
        // - `ptr` was obtained from the previous line and is therefore
        //   valid for writes and is properly aligned.
        unsafe { ptr.write(entry) };
    }

    /// Collect an [`ExactSizeIterator<Item = T>`] into a newly
    /// created [`RsValueCollection<T>`].
    ///
    /// # Panics
    /// Panics if `iter.len()` exceeds `isize::MAX / size_of::<T>()`.
    pub fn collect_from_exact_size_iterator<I: ExactSizeIterator<Item = T>>(iter: I) -> Self {
        let len = iter.len();
        assert!(
            len <= Self::MAX_CAPACITY,
            "Iterator length exceeds max capacity of {} items",
            Self::MAX_CAPACITY
        );

        // Safety:
        // - We're using `Self::write_entry` fully initialize the map before it
        //   is returned.
        // - `len` is asserted not to exceed `Self::MAX_CAPACITY`
        let mut map = unsafe { Self::reserve_uninit(len as u32) };
        for (i, entry) in iter.enumerate() {
            // Safety:
            // - `i` is strictly smaller than `len`, which is asserted to
            //   not exceed `Self::MAX_CAPACITY`.
            unsafe { map.write_entry(entry, i as u32) };
        }
        map
    }

    /// Clone all items from an [`ExactSizeIterator<Item = &T>`]
    /// into a newly created [`RsValueCollection<T>`].
    ///
    /// # Panics
    /// Panics if `iter.len()` exceeds `isize::MAX / size_of::<T>()`.
    pub fn clone_from_exact_size_iterator<'m, I: ExactSizeIterator<Item = &'m T>>(iter: I) -> Self
    where
        T: Clone + 'm,
    {
        let len = iter.len();
        assert!(
            len <= Self::MAX_CAPACITY,
            "Iterator length exceeds max capacity of {} items",
            Self::MAX_CAPACITY
        );
        // Safety:
        // - We're using `Self::write_entry` fully initialize the map before it
        //   is returned.
        // - `len` is asserted not to exceed `Self::MAX_CAPACITY`
        let mut map = unsafe { Self::reserve_uninit(len as u32) };
        for (i, entry) in iter.cloned().enumerate() {
            // Safety:
            // - `i` is strictly smaller than `len`, which is asserted to
            //   not exceed `Self::MAX_CAPACITY`
            unsafe { map.write_entry(entry, i as u32) };
        }
        map
    }

    /// Create a non-consuming iterator over the collection's entries.
    pub const fn iter(&self) -> Iter<'_, T> {
        Iter { map: self, i: 0 }
    }

    /// Calculate the [`Layout`] that should be used to allocate space for
    /// `cap` items for use in this collection.
    /// # Panics
    /// Panics if `cap` exceeds `isize::MAX / size_of::<T>()`.
    const fn entry_layout(cap: u32) -> Layout {
        let Ok(layout) = Layout::array::<T>(cap as usize) else {
            panic!("Capacity too high. Can be at most `isize::MAX / size_of::<T>()`");
        };
        layout
    }
}

impl<T: Clone> Clone for RsValueCollection<T> {
    fn clone(&self) -> Self {
        Self::clone_from_exact_size_iterator(self.iter())
    }
}

impl<T> Drop for RsValueCollection<T> {
    fn drop(&mut self) {
        if self.cap == 0 {
            // No allocation associated with this map,
            // so there's nothing to do.
            return;
        }

        for i in 0..self.cap {
            // Safety:

            // - `self.cap` is greater than 0, and therefore `self.entries`
            //   points to a heap-allocated array `self.cap` [`RsValueMapEntries`]
            // - `self.cap` does not exceed [`Self::MAX_CAPACITY`], and
            //   `i` is strictly smaller than `self.cap`
            let entry_ptr = unsafe { self.entries.add(i as usize) };
            // Safety:
            // - `self.entries` pointer is obtained from calling `Self::reserve_uninit`,
            //   which ensures it is non-null properly aligned, and valid for reads and writes
            //   as long as `self.cap` is greater than 0, in which case we don't reach this line.
            //   `entry_ptr` is derived from `self.entries` on the previous line of code.
            // - `*entry_ptr` is initialized and therefore valid for dropping.
            // - We're not accessing `*entry_ptr` while the drop is taking place.
            unsafe { entry_ptr.drop_in_place() };
        }
        // Safety:
        // - `self.entries` was obtained from calling `Self::reserve_uninit`, which
        //   ensures it points to a valid memory blocked allocated by the global
        //   allocator in case `self.cap > 0`. If `self.cap` were 0, we wouldn't
        //   reach this line.
        // - The layout is calculated the same way as it has been upon allocation
        //   in `Self::reserve_uninit`.
        unsafe {
            dealloc(
                self.entries.as_ptr() as *mut _,
                Self::entry_layout(self.cap),
            )
        };
    }
}

/// Safety:
/// [`RsValueMap`] is safe to send to other threads.
unsafe impl<T: Send> Send for RsValueCollection<T> {}

/// Safety:
/// [`&RsValueMap`](RsValueMap) is safe to send to other threads.
unsafe impl<T: Sync> Sync for RsValueCollection<T> {}

pub struct Iter<'m, T> {
    map: &'m RsValueCollection<T>,
    i: usize,
}

impl<'m, T> Iterator for Iter<'m, T> {
    type Item = &'m T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.map.cap as usize {
            // Safety:
            // - `self.i` is strictly smaller than `self.map.cap` which is
            //   validated to be at most `RsValueMap::MAX_CAPACITY`.
            // - If `self.map.cap` were 0, we wouldn't reach this line.
            let entry_ptr = unsafe { self.map.entries.add(self.i) };
            // Safety:
            // - `entry_ptr` was obtained from the previous line of code, which
            //   ensures it is properly aligned, non-null.
            // - `entry_ptr` is pointing to an initialized `RsValueMapEntry`
            // - `entry` does not outlive the `RsValueMap` reference held
            //    by this iterator.
            let entry = unsafe { entry_ptr.as_ref() };

            self.i += 1;
            Some(entry)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let cap = self.map.cap as usize;
        (cap, Some(cap))
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {}

#[repr(transparent)]
#[derive(Clone)]
pub struct RsValueArray(RsValueCollection<SharedRsValue>);

impl RsValueArray {
    /// # Safety
    /// See [`RsValueCollection::reserve_uninit`].
    pub unsafe fn reserve_uninit(cap: u32) -> Self {
        // Safety: see [`RsValueCollection::reserve_uninit`]
        Self(unsafe { RsValueCollection::reserve_uninit(cap) })
    }

    pub const fn inner_mut(&mut self) -> &mut RsValueCollection<SharedRsValue> {
        &mut self.0
    }

    pub const fn inner(&self) -> &RsValueCollection<SharedRsValue> {
        &self.0
    }
}

impl fmt::Debug for RsValueArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.iter()).finish()
    }
}

#[repr(transparent)]
#[derive(Clone)]
pub struct RsValueMap(RsValueCollection<RsValueMapEntry>);

impl RsValueMap {
    /// # Safety
    /// See [`RsValueCollection::reserve_uninit`].
    pub unsafe fn reserve_uninit(cap: u32) -> Self {
        // Safety: see [`RsValueCollection::reserve_uninit`]
        Self(unsafe { RsValueCollection::reserve_uninit(cap) })
    }

    pub const fn inner_mut(&mut self) -> &mut RsValueCollection<RsValueMapEntry> {
        &mut self.0
    }

    pub const fn inner(&self) -> &RsValueCollection<RsValueMapEntry> {
        &self.0
    }
}

impl fmt::Debug for RsValueMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map_fmt = f.debug_map();
        for e in self.0.iter() {
            map_fmt.key(&e.key);
            map_fmt.value(&e.value);
        }
        map_fmt.finish()
    }
}

/// A single entry of a [`RsValueMap`].
#[repr(C)]
#[derive(Clone)]
pub struct RsValueMapEntry {
    key: SharedRsValue,
    value: SharedRsValue,
}

impl RsValueMapEntry {
    pub const fn new(key: SharedRsValue, value: SharedRsValue) -> Self {
        Self { key, value }
    }
}

impl fmt::Debug for RsValueMapEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entry(&self.key).entry(&self.value).finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Value,
        collection::{RsValueCollection, RsValueMapEntry},
        shared::SharedRsValue,
    };

    #[test]
    fn test_map_create_iter_destroy() {
        let items = std::iter::repeat_n((), 100).enumerate().map(|(i, _)| {
            let key = SharedRsValue::number(i as f64);
            let value = SharedRsValue::number((2 * i) as f64);
            RsValueMapEntry { key, value }
        });
        RsValueCollection::collect_from_exact_size_iterator(items);
    }

    #[test]
    fn test_empty_map_create_iter_destroy() {
        let items = std::iter::empty();
        let map = RsValueCollection::<RsValueMapEntry>::collect_from_exact_size_iterator(items);
        map.iter()
            .for_each(|_entry| panic!("Iterating over an empty map is impossible"));
    }

    /// Create an [`RsValueMap`] and send it as well as a reference to it
    /// to another thread, allowing Miri to validate that it is indeed
    /// sound for it to implement [`Send`] and [`Sync`]
    #[test]
    fn test_send_sync() {
        let items = std::iter::repeat_n((), 100).enumerate().map(|(i, _)| {
            let key = SharedRsValue::number(i as f64);
            let value = SharedRsValue::number((2 * i) as f64);
            RsValueMapEntry { key, value }
        });
        let map = RsValueCollection::collect_from_exact_size_iterator(items);

        let t1 = std::thread::spawn({
            let map_ref = &map;
            || {
                let _ = map_ref;
            }
        });

        let t2 = std::thread::spawn(move || drop(map));
        // Explicitly join to make Miri happy
        t1.join().unwrap();
        t2.join().unwrap();
    }
}
