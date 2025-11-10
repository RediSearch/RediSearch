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

/// An immutable structure that holds and manages a set of
/// heap-allocated key-value pairs, i.e. [`RsValueMapEntry`] items.
///
/// # Invariants
/// - Can hold at most [`Self::MAX_CAPACITY`] entries, which on 32-bit systems
///   is less than `u32::MAX`. The reason for this is that when doing pointer
///   addition, we must ensure we don't overflow `isize::MAX`.
///   See [`NonNull::add`].
#[repr(C)]
pub struct RsValueMap {
    /// Pointer to a heap-allocated array of `Self::cap` [`RsValueMapEntry`] items.
    entries: NonNull<RsValueMapEntry>,
    /// The number of [`RsValueMapEntry`] items this map can hold
    cap: u32,
}

impl RsValueMap {
    /// The maximum number of `RsValueMapEntry` items this map can
    /// hold on this platform. Calculated as the minimum of
    /// `u32::MAX` and `isize::MAX / RsValueMapEntry::SIZE`.
    pub const MAX_CAPACITY: usize = {
        let max = (isize::MAX as usize) / RsValueMapEntry::SIZE;
        if max > (u32::MAX) as usize {
            u32::MAX as usize
        } else {
            max
        }
    };

    /// Allocate space for `cap` [`RsValueMapEntry`] items,
    /// and create a [`RsValueMap`] that wraps these items.
    ///
    /// # Safety
    /// - The created `RsValueMap`'s items must all be initialized using [`RsValueMap::write_entry`]
    ///   before the map's data can be used or the map itself can be dropped.
    /// - `cap` must not exceed [`Self::MAX_CAPACITY`], which is relevant on e.g. 32-bit systems.
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
        let entries = NonNull::new(ptr as *mut RsValueMapEntry).unwrap();
        Self { entries, cap }
    }

    /// Write an [`RsValueMapEntry`] into the map at position `i`.
    /// Does not read or drop the existing value. Use this function to
    /// initialize an [`RsValueMap`] that was created using [`RsValueMap::reserve_uninit`].
    ///
    /// Memory will leak in case this function is used to overwrite an existing entry.
    ///
    /// # Safety
    /// 1.`i` must be less than the map's capacity.
    pub unsafe fn write_entry(&mut self, entry: RsValueMapEntry, i: u32) {
        debug_assert!(i < self.cap, "Index was out of bounds");
        // Safety:
        // - The caller must ensure that `i` is smaller the map's
        //   capacity, which is always less than
        //   `isize::MAX / size_of::<RsValueMapEntry>()`.
        // - The `self.entries` pointer was obtained from a call to `alloc`,
        //   unless it's capacity was set to 0, in which case the caller
        //   is violating the first safety rule of this function.
        let ptr = unsafe { self.entries.add(i as usize) };
        // Safety:
        // - `ptr` was obtained from the previous line and is therefore
        //   valid for writes and is properly aligned.
        unsafe { ptr.write(entry) };
    }

    /// Collect an [`ExactSizeIterator<Item = RsValueMapEntry>`] into a newly
    /// created [`RsValueMap`].
    ///
    /// # Panics
    /// Panics if `iter.len()` exceeds `isize::MAX / RsValueMapEntry::SIZE`.
    pub fn collect_from_exact_size_iterator<I: ExactSizeIterator<Item = RsValueMapEntry>>(
        iter: I,
    ) -> Self {
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

    /// Clone all items from an [`ExactSizeIterator<Item = &RsValueMapEntry>`]
    /// into a newly created [`RsValueMap`].
    ///
    /// # Panics
    /// Panics if `iter.len()` exceeds `isize::MAX / RsValueMapEntry::SIZE`.
    pub fn clone_from_exact_size_iterator<'m, I: ExactSizeIterator<Item = &'m RsValueMapEntry>>(
        iter: I,
    ) -> Self {
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

    /// Create a non-consuming iterator over the map's entries.
    pub const fn iter(&self) -> Iter<'_> {
        Iter { map: self, i: 0 }
    }

    /// Calculate the [`Layout`] that should be used to allocate space for
    /// `cap` [`RsValueEntries`] for use in this map.
    const fn entry_layout(cap: u32) -> Layout {
        let Ok(layout) = Layout::array::<RsValueMapEntry>(cap as usize) else {
            panic!("Capacity too high. Can be at most `isize::MAX / size_of::<RsValueMapEntry>()`");
        };
        layout
    }
}

impl fmt::Debug for RsValueMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map_fmt = f.debug_map();
        for e in self.iter() {
            map_fmt.key(&e.key);
            map_fmt.value(&e.value);
        }
        map_fmt.finish()
    }
}

impl Clone for RsValueMap {
    fn clone(&self) -> Self {
        Self::clone_from_exact_size_iterator(self.iter())
    }
}

impl Drop for RsValueMap {
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
unsafe impl Send for RsValueMap {}

/// Safety:
/// [`&RsValueMap`](RsValueMap) is safe to send to other threads.
unsafe impl Sync for RsValueMap {}

pub struct Iter<'m> {
    map: &'m RsValueMap,
    i: usize,
}

impl<'m> Iterator for Iter<'m> {
    type Item = &'m RsValueMapEntry;

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

impl ExactSizeIterator for Iter<'_> {}

/// A single entry of a [`RsValueMap`].
#[repr(C)]
#[derive(Clone)]
pub struct RsValueMapEntry {
    key: SharedRsValue,
    value: SharedRsValue,
}

impl RsValueMapEntry {
    const SIZE: usize = std::mem::size_of::<Self>();
}

impl fmt::Debug for RsValueMapEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RsValueMapEntry")
            .field("key", &self.key)
            .field("value", &self.value)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Value,
        map::{RsValueMap, RsValueMapEntry},
        shared::SharedRsValue,
    };

    #[test]
    fn test_map_create_iter_destroy() {
        let items = std::iter::repeat_n((), 100).enumerate().map(|(i, _)| {
            let key = SharedRsValue::number(i as f64);
            let value = SharedRsValue::number((2 * i) as f64);
            RsValueMapEntry { key, value }
        });
        let map = RsValueMap::collect_from_exact_size_iterator(items);
        map.iter().for_each(|_entry| {});
    }

    #[test]
    fn test_empty_map_create_iter_destroy() {
        let items = std::iter::empty();
        let map = RsValueMap::collect_from_exact_size_iterator(items);
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
        let map = RsValueMap::collect_from_exact_size_iterator(items);
        std::thread::spawn({
            let map_ref = &map;
            || {
                let _ = map_ref;
            }
        });

        let t = std::thread::spawn(move || drop(map));
        // Explicitly join to make Miri happy
        t.join().unwrap();
    }
}
