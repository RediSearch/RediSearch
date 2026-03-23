#![no_std]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings), allow(dead_code, unused_variables))
))]
//! Pre-allocated storage for a uniform data type, with generational indexing.
//!
//! `Slab` provides pre-allocated storage for a single data type. If many values
//! of a single type are being allocated, it can be more efficient to
//! pre-allocate the necessary storage. Since the size of the type is uniform,
//! memory fragmentation can be avoided. Storing, clearing, and lookup
//! operations become very cheap.
//!
//! While `Slab` may look like other Rust collections, it is not intended to be
//! used as a general purpose collection. The primary difference between `Slab`
//! and `Vec` is that `Slab` returns the key when storing the value.
//!
//! Keys include a generation counter, so stale keys (from removed entries) are
//! detected on lookup and return `None` instead of silently accessing a
//! different value that now occupies the same slot.
//!
//! # Performance notes
//!
//! Methods that remove values and return them, such as [`Slab::remove`] and
//! [`Slab::try_remove`], might copy the removed values to the stack even if
//! their return values are unused. For types that don't have drop glue, the
//! compiler can usually elide these copies.
//!
//! # Examples
//!
//! Basic storing and retrieval.
//!
//! ```
//! # use generational_slab::*;
//! let mut slab = Slab::new();
//!
//! let hello = slab.insert("hello");
//! let world = slab.insert("world");
//!
//! assert_eq!(slab[hello], "hello");
//! assert_eq!(slab[world], "world");
//!
//! slab[world] = "earth";
//! assert_eq!(slab[world], "earth");
//! ```
//!
//! Sometimes it is useful to be able to associate the key with the value being
//! inserted in the slab. This can be done with the `vacant_entry` API as such:
//!
//! ```
//! # use generational_slab::*;
//! let mut slab = Slab::new();
//!
//! let hello = {
//!     let entry = slab.vacant_entry();
//!     let key = entry.key();
//!
//!     entry.insert((key, "hello"));
//!     key
//! };
//!
//! assert_eq!(hello, slab[hello].0);
//! assert_eq!("hello", slab[hello].1);
//! ```
//!
//! It is generally a good idea to specify the desired capacity of a slab at
//! creation time. Note that `Slab` will grow the internal capacity when
//! attempting to insert a new value once the existing capacity has been reached.
//! To avoid this, add a check.
//!
//! ```
//! # use generational_slab::*;
//! let mut slab = Slab::with_capacity(1024);
//!
//! // ... use the slab
//!
//! if slab.len() == slab.capacity() {
//!     panic!("slab full");
//! }
//!
//! slab.insert("the slab is not at capacity yet");
//! ```
//!
//! # Capacity and reallocation
//!
//! The capacity of a slab is the amount of space allocated for any future
//! values that will be inserted in the slab. This is not to be confused with
//! the *length* of the slab, which specifies the number of actual values
//! currently being inserted. If a slab's length is equal to its capacity, the
//! next value inserted into the slab will require growing the slab by
//! reallocating.
//!
//! For example, a slab with capacity 10 and length 0 would be an empty slab
//! with space for 10 more stored values. Storing 10 or fewer elements into the
//! slab will not change its capacity or cause reallocation to occur. However,
//! if the slab length is increased to 11 (due to another `insert`), it will
//! have to reallocate, which can be slow. For this reason, it is recommended to
//! use [`Slab::with_capacity`] whenever possible to specify how many values the
//! slab is expected to store.
//!
//! # Implementation
//!
//! `Slab` is backed by a `Vec` of slots. Each slot is either occupied or
//! vacant. `Slab` maintains a stack of vacant slots using a linked list. To
//! find a vacant slot, the stack is popped. When a slot is released, it is
//! pushed onto the stack.
//!
//! If there are no more available slots in the stack, then `Vec::reserve(1)` is
//! called and a new slot is created.
//!
//! Each slot carries a generation counter. When a slot is vacated, its
//! generation is incremented. Keys store the generation at insertion time, so
//! lookups with a stale key (whose generation doesn't match the slot) safely
//! return `None`.
//!
//! ## License
//!
//! Portions of this codebase are **originally from [`slab`](https://github.com/tokio-rs/slab)**, which is
//! under [MIT License](./LICENSE-MIT).
//! We have kept the same license for this fork.
//!
//! [`Slab::with_capacity`]: struct.Slab.html#with_capacity

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std as alloc;

use alloc::vec::{self, Vec};
use core::iter::{self, FusedIterator};
use core::mem::MaybeUninit;
use core::{fmt, mem, ops, slice};

/// A key into a [`Slab`].
///
/// Keys are returned by [`Slab::insert`] and can be used to access the stored
/// value via [`Slab::get`], [`Slab::get_mut`], or indexing (`slab[key]`).
///
/// Each key carries a generation counter so that stale keys (from removed
/// entries) are detected on lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct Key {
    position: u32,
    generation: u32,
}

impl Key {
    /// Return the position (slot index) of this key.
    pub const fn position(self) -> u32 {
        self.position
    }

    /// Return the generation of this key.
    pub const fn generation(self) -> u32 {
        self.generation
    }

    /// Reconstruct a key from its raw position and generation.
    ///
    /// This is intended for FFI round-trips where a key was previously
    /// decomposed via [`Key::position`] and [`Key::generation`].
    pub const fn from_raw_parts(position: u32, generation: u32) -> Self {
        Self {
            position,
            generation,
        }
    }
}

/// Pre-allocated storage for a uniform data type
///
/// See the [module documentation] for more details.
///
/// [module documentation]: index.html
pub struct Slab<T> {
    // Chunk of memory
    entries: Vec<Entry<T>>,

    // Number of Filled elements currently in the slab
    len: usize,

    // Offset of the next available slot in the slab. Set to the slab's
    // capacity when the slab is full.
    next: u32,

    // Minimum generation assigned to brand-new slots (the push branch of
    // `insert_at`). Bumped by `clear()`, `drain()`, `compact()`, and
    // `shrink_to_fit()` before entries are lost, so that old keys pointing
    // to those positions can never alias the new entries.
    generation_watermark: u32,
}

impl<T> Clone for Slab<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            len: self.len,
            next: self.next,
            generation_watermark: self.generation_watermark,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.entries.clone_from(&source.entries);
        self.len = source.len;
        self.next = source.next;
        self.generation_watermark = source.generation_watermark;
    }
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Slab::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The error type returned by [`Slab::get_disjoint_mut`].
pub enum GetDisjointMutError {
    /// An index provided was not associated with a value.
    IndexVacant,

    /// An index provided was out-of-bounds for the slab.
    IndexOutOfBounds,

    /// Two indices provided were overlapping.
    OverlappingIndices,

    /// A key's generation did not match the slot's current generation.
    GenerationMismatch,
}

impl fmt::Display for GetDisjointMutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            GetDisjointMutError::IndexVacant => "an index is vacant",
            GetDisjointMutError::IndexOutOfBounds => "an index is out of bounds",
            GetDisjointMutError::OverlappingIndices => "there were overlapping indices",
            GetDisjointMutError::GenerationMismatch => "a key's generation does not match the slot",
        };
        fmt::Display::fmt(msg, f)
    }
}

impl core::error::Error for GetDisjointMutError {}

/// A handle to a vacant entry in a `Slab`.
///
/// `VacantEntry` allows constructing values with the key that they will be
/// assigned to.
///
/// # Examples
///
/// ```
/// # use generational_slab::*;
/// let mut slab = Slab::new();
///
/// let hello = {
///     let entry = slab.vacant_entry();
///     let key = entry.key();
///
///     entry.insert((key, "hello"));
///     key
/// };
///
/// assert_eq!(hello, slab[hello].0);
/// assert_eq!("hello", slab[hello].1);
/// ```
#[derive(Debug)]
pub struct VacantEntry<'a, T> {
    slab: &'a mut Slab<T>,
    key: Key,
}

/// A consuming iterator over the values stored in a `Slab`
pub struct IntoIter<T> {
    entries: iter::Enumerate<vec::IntoIter<Entry<T>>>,
    len: usize,
}

/// An iterator over the values stored in the `Slab`
pub struct Iter<'a, T> {
    entries: iter::Enumerate<slice::Iter<'a, Entry<T>>>,
    len: usize,
}

impl<T> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            len: self.len,
        }
    }
}

/// A mutable iterator over the values stored in the `Slab`
pub struct IterMut<'a, T> {
    entries: iter::Enumerate<slice::IterMut<'a, Entry<T>>>,
    len: usize,
}

/// A draining iterator for `Slab`
pub struct Drain<'a, T> {
    inner: vec::Drain<'a, Entry<T>>,
    len: usize,
}

#[derive(Clone)]
pub(crate) enum Entry<T> {
    Vacant { next: u32, generation: u32 },
    Occupied { value: T, generation: u32 },
}

impl<T> Entry<T> {
    pub(crate) const fn generation(&self) -> u32 {
        match *self {
            Entry::Vacant { generation, .. } | Entry::Occupied { generation, .. } => generation,
        }
    }
}

impl<T> Slab<T> {
    /// Construct a new, empty `Slab`.
    ///
    /// The function does not allocate and the returned slab will have no
    /// capacity until `insert` is called or capacity is explicitly reserved.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let slab: Slab<i32> = Slab::new();
    /// ```
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            next: 0,
            len: 0,
            generation_watermark: 0,
        }
    }

    /// Construct a new, empty `Slab` with the specified capacity.
    ///
    /// The returned slab will be able to store exactly `capacity` without
    /// reallocating. If `capacity` is 0, the slab will not allocate.
    ///
    /// It is important to note that this function does not specify the *length*
    /// of the returned slab, but only the capacity. For an explanation of the
    /// difference between length and capacity, see [Capacity and
    /// reallocation](index.html#capacity-and-reallocation).
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::with_capacity(10);
    ///
    /// // The slab contains no values, even though it has capacity for more
    /// assert_eq!(slab.len(), 0);
    ///
    /// // These are all done without reallocating...
    /// for i in 0..10 {
    ///     slab.insert(i);
    /// }
    ///
    /// // ...but this may make the slab reallocate
    /// slab.insert(11);
    /// ```
    pub fn with_capacity(capacity: usize) -> Slab<T> {
        Slab {
            entries: Vec::with_capacity(capacity),
            next: 0,
            len: 0,
            generation_watermark: 0,
        }
    }

    /// Return the number of values the slab can store without reallocating.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let slab: Slab<i32> = Slab::with_capacity(10);
    /// assert_eq!(slab.capacity(), 10);
    /// ```
    pub const fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Returns the memory used by the slab's backing allocation in bytes.
    ///
    /// This accounts for the full capacity of the internal `Vec`, not just the
    /// occupied entries. It does not include heap memory owned by stored values.
    pub const fn mem_usage(&self) -> usize {
        self.entries.capacity() * size_of::<Entry<T>>()
    }

    /// Reserve capacity for at least `additional` more values to be stored
    /// without allocating.
    ///
    /// `reserve` does nothing if the slab already has sufficient capacity for
    /// `additional` more values. If more capacity is required, a new segment of
    /// memory will be allocated and all existing values will be copied into it.
    /// As such, if the slab is already very large, a call to `reserve` can end
    /// up being expensive.
    ///
    /// The slab may reserve more than `additional` extra space in order to
    /// avoid frequent reallocations. Use `reserve_exact` instead to guarantee
    /// that only the requested space is allocated.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// slab.insert("hello");
    /// slab.reserve(10);
    /// assert!(slab.capacity() >= 11);
    /// ```
    pub fn reserve(&mut self, additional: usize) {
        if self.capacity() - self.len >= additional {
            return;
        }
        let need_add = additional - (self.entries.len() - self.len);
        self.entries.reserve(need_add);
    }

    /// Reserve the minimum capacity required to store exactly `additional`
    /// more values.
    ///
    /// `reserve_exact` does nothing if the slab already has sufficient capacity
    /// for `additional` more values. If more capacity is required, a new segment
    /// of memory will be allocated and all existing values will be copied into
    /// it.  As such, if the slab is already very large, a call to `reserve` can
    /// end up being expensive.
    ///
    /// Note that the allocator may give the slab more space than it requests.
    /// Therefore capacity can not be relied upon to be precisely minimal.
    /// Prefer `reserve` if future insertions are expected.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// slab.insert("hello");
    /// slab.reserve_exact(10);
    /// assert!(slab.capacity() >= 11);
    /// ```
    pub fn reserve_exact(&mut self, additional: usize) {
        if self.capacity() - self.len >= additional {
            return;
        }
        let need_add = additional - (self.entries.len() - self.len);
        self.entries.reserve_exact(need_add);
    }

    /// Shrink the capacity of the slab as much as possible without invalidating keys.
    ///
    /// Because values cannot be moved to a different index, the slab cannot
    /// shrink past any stored values.
    /// It will drop down as close as possible to the length but the allocator may
    /// still inform the underlying vector that there is space for a few more elements.
    ///
    /// This function can take O(n) time even when the capacity cannot be reduced
    /// or the allocation is shrunk in place. Repeated calls run in O(1) though.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::with_capacity(10);
    ///
    /// for i in 0..3 {
    ///     slab.insert(i);
    /// }
    ///
    /// slab.shrink_to_fit();
    /// assert!(slab.capacity() >= 3 && slab.capacity() < 10);
    /// ```
    ///
    /// The slab cannot shrink past the last present value even if previous
    /// values are removed:
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::with_capacity(10);
    ///
    /// let mut keys = Vec::new();
    /// for i in 0..4 {
    ///     keys.push(slab.insert(i));
    /// }
    ///
    /// slab.remove(keys[0]);
    /// slab.remove(keys[3]);
    ///
    /// slab.shrink_to_fit();
    /// assert!(slab.capacity() >= 3 && slab.capacity() < 10);
    /// ```
    pub fn shrink_to_fit(&mut self) {
        // Remove all vacant entries after the last occupied one, so that
        // the capacity can be reduced to what is actually needed.
        // If the slab is empty the vector can simply be cleared, but that
        // optimization would not affect time complexity when T: Drop.

        // Raise the watermark from trailing vacant entries that are about
        // to be popped, so old keys pointing at those positions cannot
        // alias future inserts.
        let trim_start = {
            let mut i = self.entries.len();
            while i > 0 && matches!(self.entries[i - 1], Entry::Vacant { .. }) {
                i -= 1;
            }
            i
        };
        self.generation_watermark =
            Self::watermark_for(self.generation_watermark, &self.entries[trim_start..]);

        let len_before = self.entries.len();
        while let Some(&Entry::Vacant { .. }) = self.entries.last() {
            self.entries.pop();
        }

        // Removing entries breaks the list of vacant entries,
        // so it must be repaired
        if self.entries.len() != len_before {
            // Some vacant entries were removed, so the list now likely¹
            // either contains references to the removed entries, or has an
            // invalid end marker. Fix this by recreating the list.
            self.recreate_vacant_list();
            // ¹: If the removed entries formed the tail of the list, with the
            // most recently popped entry being the head of them, (so that its
            // index is now the end marker) the list is still valid.
            // Checking for that unlikely scenario of this infrequently called
            // is not worth the code complexity.
        }

        self.entries.shrink_to_fit();
    }

    /// Iterate through all entries to recreate and repair the vacant list.
    /// self.len must be correct and is not modified.
    fn recreate_vacant_list(&mut self) {
        self.next = self.entries.len() as u32;
        // We can stop once we've found all vacant entries
        let mut remaining_vacant = self.entries.len() - self.len;
        if remaining_vacant == 0 {
            return;
        }

        // Iterate in reverse order so that lower keys are at the start of
        // the vacant list. This way future shrinks are more likely to be
        // able to remove vacant entries.
        for (i, entry) in self.entries.iter_mut().enumerate().rev() {
            if let Entry::Vacant { ref mut next, .. } = *entry {
                *next = self.next;
                self.next = i as u32;
                remaining_vacant -= 1;
                if remaining_vacant == 0 {
                    break;
                }
            }
        }
    }

    /// Reduce the capacity as much as possible, changing the key for elements when necessary.
    ///
    /// To allow updating references to the elements which must be moved to a new key,
    /// this function takes a closure which is called before moving each element.
    /// The second and third parameters to the closure are the current key and
    /// new key respectively.
    /// In case changing the key for one element turns out not to be possible,
    /// the move can be cancelled by returning `false` from the closure.
    /// In that case no further attempts at relocating elements is made.
    /// If the closure unwinds, the slab will be left in a consistent state,
    /// but the value that the closure panicked on might be removed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    ///
    /// let mut slab = Slab::with_capacity(10);
    /// let a = slab.insert('a');
    /// slab.insert('b');
    /// slab.insert('c');
    /// slab.remove(a);
    /// slab.compact(|&mut value, from, to| {
    ///     assert_eq!((value, from.position(), to.position()), ('c', 2, 0));
    ///     true
    /// });
    /// assert!(slab.capacity() >= 2 && slab.capacity() < 10);
    /// ```
    ///
    /// The value is not moved when the closure returns `Err`:
    ///
    /// ```
    /// # use generational_slab::*;
    ///
    /// let mut slab = Slab::with_capacity(100);
    /// let a = slab.insert('a');
    /// let b = slab.insert('b');
    /// slab.remove(a);
    /// slab.compact(|&mut value, from, to| false);
    /// assert_eq!(slab.iter().next(), Some((b, &'b')));
    /// ```
    pub fn compact<F>(&mut self, mut rekey: F)
    where
        F: FnMut(&mut T, Key, Key) -> bool,
    {
        // Raise the watermark before any entries are lost. Entries at
        // positions that get truncated must not be silently aliased by
        // future inserts at the same position.
        self.generation_watermark = Self::watermark_for(self.generation_watermark, &self.entries);

        // If the closure unwinds, we need to restore a valid list of vacant entries
        struct CleanupGuard<'a, T> {
            slab: &'a mut Slab<T>,
            decrement: bool,
        }
        impl<T> Drop for CleanupGuard<'_, T> {
            fn drop(&mut self) {
                if self.decrement {
                    // Value was popped and not pushed back on
                    self.slab.len -= 1;
                }
                self.slab.recreate_vacant_list();
            }
        }
        let mut guard = CleanupGuard {
            slab: self,
            decrement: true,
        };

        let mut occupied_until = 0usize;
        // While there are vacant entries
        while guard.slab.entries.len() > guard.slab.len {
            // Find a value that needs to be moved,
            // by popping entries until we find an occupied one.
            // (entries cannot be empty because 0 is not greater than anything)
            if let Some(Entry::Occupied {
                mut value,
                generation,
            }) = guard.slab.entries.pop()
            {
                // Found one, now find a vacant entry to move it to
                while let Some(&Entry::Occupied { .. }) = guard.slab.entries.get(occupied_until) {
                    occupied_until += 1;
                }
                let from_pos = guard.slab.entries.len() as u32;
                // The destination slot's generation (it's vacant, so read from it)
                let to_generation = guard.slab.entries[occupied_until].generation();
                let from = Key {
                    position: from_pos,
                    generation,
                };
                let to = Key {
                    position: occupied_until as u32,
                    generation: to_generation,
                };
                // Let the caller try to update references to the key
                if !rekey(&mut value, from, to) {
                    // Changing the key failed, so push the entry back on at its old index.
                    guard
                        .slab
                        .entries
                        .push(Entry::Occupied { value, generation });
                    guard.decrement = false;
                    guard.slab.entries.shrink_to_fit();
                    return;
                    // Guard drop handles cleanup
                }
                // Put the value in its new spot, keeping the destination's generation
                guard.slab.entries[occupied_until] = Entry::Occupied {
                    value,
                    generation: to_generation,
                };
                // ... and mark it as occupied (this is optional)
                occupied_until += 1;
            }
        }
        guard.slab.next = guard.slab.len as u32;
        guard.slab.entries.shrink_to_fit();
        // Normal cleanup is not necessary
        mem::forget(guard);
    }

    /// Compute the minimum safe watermark for a set of entries that are about
    /// to be lost.
    ///
    /// For occupied entries the next safe generation is `gen + 1`; for vacant
    /// entries it is `gen` (already bumped by `remove_at`).
    fn watermark_for(current: u32, lost_entries: &[Entry<T>]) -> u32 {
        let mut watermark = current;
        for entry in lost_entries {
            let floor = match *entry {
                Entry::Occupied { generation, .. } => generation.wrapping_add(1),
                Entry::Vacant { generation, .. } => generation,
            };
            watermark = watermark.max(floor);
        }
        watermark
    }

    /// Clear the slab of all values.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// for i in 0..3 {
    ///     slab.insert(i);
    /// }
    ///
    /// slab.clear();
    /// assert!(slab.is_empty());
    /// ```
    pub fn clear(&mut self) {
        self.generation_watermark = Self::watermark_for(self.generation_watermark, &self.entries);
        self.entries.clear();
        self.len = 0;
        self.next = 0;
    }

    /// Return the number of stored values.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// for i in 0..3 {
    ///     slab.insert(i);
    /// }
    ///
    /// assert_eq!(3, slab.len());
    /// ```
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Return `true` if there are no values stored in the slab.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// assert!(slab.is_empty());
    ///
    /// slab.insert(1);
    /// assert!(!slab.is_empty());
    /// ```
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return an iterator over the slab.
    ///
    /// This function should generally be **avoided** as it is not efficient.
    /// Iterators must iterate over every slot in the slab even if it is
    /// vacant. As such, a slab with a capacity of 1 million but only one
    /// stored value must still iterate the million slots.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// for i in 0..3 {
    ///     slab.insert(i);
    /// }
    ///
    /// let mut iterator = slab.iter();
    ///
    /// assert_eq!(iterator.next().map(|(k, v)| (k.position(), v)), Some((0, &0)));
    /// assert_eq!(iterator.next().map(|(k, v)| (k.position(), v)), Some((1, &1)));
    /// assert_eq!(iterator.next().map(|(k, v)| (k.position(), v)), Some((2, &2)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            entries: self.entries.iter().enumerate(),
            len: self.len,
        }
    }

    /// Return an iterator that allows modifying each value.
    ///
    /// This function should generally be **avoided** as it is not efficient.
    /// Iterators must iterate over every slot in the slab even if it is
    /// vacant. As such, a slab with a capacity of 1 million but only one
    /// stored value must still iterate the million slots.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let key1 = slab.insert(0);
    /// let key2 = slab.insert(1);
    ///
    /// for (key, val) in slab.iter_mut() {
    ///     if key == key1 {
    ///         *val += 2;
    ///     }
    /// }
    ///
    /// assert_eq!(slab[key1], 2);
    /// assert_eq!(slab[key2], 1);
    /// ```
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            entries: self.entries.iter_mut().enumerate(),
            len: self.len,
        }
    }

    /// Return a reference to the value associated with the given key.
    ///
    /// If the given key is not associated with a value, then `None` is
    /// returned. This includes the case where the key's generation does not
    /// match the slot's current generation (stale key).
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert("hello");
    ///
    /// assert_eq!(slab.get(key), Some(&"hello"));
    /// ```
    pub fn get(&self, key: Key) -> Option<&T> {
        match self.entries.get(key.position as usize) {
            Some(Entry::Occupied { value, generation }) if *generation == key.generation => {
                Some(value)
            }
            _ => None,
        }
    }

    /// Return a mutable reference to the value associated with the given key.
    ///
    /// If the given key is not associated with a value, then `None` is
    /// returned. This includes the case where the key's generation does not
    /// match the slot's current generation (stale key).
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert("hello");
    ///
    /// *slab.get_mut(key).unwrap() = "world";
    ///
    /// assert_eq!(slab[key], "world");
    /// ```
    pub fn get_mut(&mut self, key: Key) -> Option<&mut T> {
        match self.entries.get_mut(key.position as usize) {
            Some(&mut Entry::Occupied {
                ref mut value,
                generation,
            }) if generation == key.generation => Some(value),
            _ => None,
        }
    }

    /// Return two mutable references to the values associated with the two
    /// given keys simultaneously.
    ///
    /// If any one of the given keys is not associated with a value, then `None`
    /// is returned.
    ///
    /// This function can be used to get two mutable references out of one slab,
    /// so that you can manipulate both of them at the same time, eg. swap them.
    ///
    /// # Panics
    ///
    /// This function will panic if `key1` and `key2` point to the same position
    /// in the slab.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// use std::mem;
    ///
    /// let mut slab = Slab::new();
    /// let key1 = slab.insert(1);
    /// let key2 = slab.insert(2);
    /// let (value1, value2) = slab.get2_mut(key1, key2).unwrap();
    /// mem::swap(value1, value2);
    /// assert_eq!(slab[key1], 2);
    /// assert_eq!(slab[key2], 1);
    /// ```
    pub fn get2_mut(&mut self, key1: Key, key2: Key) -> Option<(&mut T, &mut T)> {
        let pos1 = key1.position as usize;
        let pos2 = key2.position as usize;
        assert_ne!(pos1, pos2);

        let (entry1, entry2);

        if pos1 > pos2 {
            let (slice1, slice2) = self.entries.split_at_mut(pos1);
            entry1 = slice2.get_mut(0);
            entry2 = slice1.get_mut(pos2);
        } else {
            let (slice1, slice2) = self.entries.split_at_mut(pos2);
            entry1 = slice1.get_mut(pos1);
            entry2 = slice2.get_mut(0);
        }

        match (entry1, entry2) {
            (
                Some(Entry::Occupied {
                    value: val1,
                    generation: gen1,
                }),
                Some(Entry::Occupied {
                    value: val2,
                    generation: gen2,
                }),
            ) if *gen1 == key1.generation && *gen2 == key2.generation => Some((val1, val2)),
            _ => None,
        }
    }

    /// Returns mutable references to many indices at once.
    ///
    /// Returns [`GetDisjointMutError`] if the indices are out of bounds,
    /// overlapping, vacant, or have a generation mismatch.
    pub fn get_disjoint_mut<const N: usize>(
        &mut self,
        keys: [Key; N],
    ) -> Result<[&mut T; N], GetDisjointMutError> {
        // NB: The optimizer should inline the loops into a sequence
        // of instructions without additional branching.
        for (i, &key) in keys.iter().enumerate() {
            for &prev_key in &keys[..i] {
                if key.position == prev_key.position {
                    return Err(GetDisjointMutError::OverlappingIndices);
                }
            }
        }

        let entries_ptr = self.entries.as_mut_ptr();
        let entries_len = self.entries.len();

        let mut res = MaybeUninit::<[&mut T; N]>::uninit();
        let res_ptr = res.as_mut_ptr() as *mut &mut T;

        for (i, &key) in keys.iter().enumerate() {
            let idx = key.position as usize;
            // `idx` won't be greater than `entries_len`.
            if idx >= entries_len {
                return Err(GetDisjointMutError::IndexOutOfBounds);
            }
            // SAFETY: we made sure above that this key is in bounds.
            let entry_ptr = unsafe { entries_ptr.add(idx) };
            // SAFETY: `entry_ptr` is a valid pointer within the entries slice.
            match unsafe { &mut *entry_ptr } {
                Entry::Vacant { .. } => return Err(GetDisjointMutError::IndexVacant),
                Entry::Occupied { value, generation } => {
                    if *generation != key.generation {
                        return Err(GetDisjointMutError::GenerationMismatch);
                    }
                    // SAFETY: `res` and `keys` both have N elements so `i` must be in bounds.
                    let slot = unsafe { res_ptr.add(i) };
                    // SAFETY: We checked above that all selected `entry`s are distinct.
                    unsafe { slot.write(value) };
                }
            }
        }
        // SAFETY: the loop above only terminates successfully if it initialized
        // all elements of this array.
        Ok(unsafe { res.assume_init() })
    }

    /// Return a reference to the value associated with the given key without
    /// performing bounds or generation checking.
    ///
    /// For a safe alternative see [`get`](Slab::get).
    ///
    /// This function should be used with care.
    ///
    /// # Safety
    ///
    /// The key must be within bounds and point to an occupied entry.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert(2);
    ///
    /// unsafe {
    ///     assert_eq!(slab.get_unchecked(key), &2);
    /// }
    /// ```
    pub unsafe fn get_unchecked(&self, key: Key) -> &T {
        // SAFETY: The caller guarantees `key` is within bounds.
        match *unsafe { self.entries.get_unchecked(key.position as usize) } {
            Entry::Occupied { ref value, .. } => value,
            _ => unreachable!(),
        }
    }

    /// Return a mutable reference to the value associated with the given key
    /// without performing bounds or generation checking.
    ///
    /// For a safe alternative see [`get_mut`](Slab::get_mut).
    ///
    /// This function should be used with care.
    ///
    /// # Safety
    ///
    /// The key must be within bounds and point to an occupied entry.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert(2);
    ///
    /// unsafe {
    ///     let val = slab.get_unchecked_mut(key);
    ///     *val = 13;
    /// }
    ///
    /// assert_eq!(slab[key], 13);
    /// ```
    pub unsafe fn get_unchecked_mut(&mut self, key: Key) -> &mut T {
        // SAFETY: The caller guarantees `key` is within bounds.
        match *unsafe { self.entries.get_unchecked_mut(key.position as usize) } {
            Entry::Occupied { ref mut value, .. } => value,
            _ => unreachable!(),
        }
    }

    /// Return two mutable references to the values associated with the two
    /// given keys simultaneously without performing bounds checking and safety
    /// condition checking.
    ///
    /// For a safe alternative see [`get2_mut`](Slab::get2_mut).
    ///
    /// This function should be used with care.
    ///
    /// # Safety
    ///
    /// - Both keys must be within bounds.
    /// - The condition `key1.position() != key2.position()` must hold.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// use std::mem;
    ///
    /// let mut slab = Slab::new();
    /// let key1 = slab.insert(1);
    /// let key2 = slab.insert(2);
    /// let (value1, value2) = unsafe { slab.get2_unchecked_mut(key1, key2) };
    /// mem::swap(value1, value2);
    /// assert_eq!(slab[key1], 2);
    /// assert_eq!(slab[key2], 1);
    /// ```
    pub unsafe fn get2_unchecked_mut(&mut self, key1: Key, key2: Key) -> (&mut T, &mut T) {
        debug_assert_ne!(key1.position, key2.position);
        let ptr = self.entries.as_mut_ptr();
        // SAFETY: The caller guarantees `key1` is within bounds.
        let ptr1 = unsafe { ptr.add(key1.position as usize) };
        // SAFETY: The caller guarantees `key2` is within bounds.
        let ptr2 = unsafe { ptr.add(key2.position as usize) };
        // SAFETY: The caller guarantees the positions differ and both are within
        // bounds, so these are non-overlapping mutable references.
        match (unsafe { &mut *ptr1 }, unsafe { &mut *ptr2 }) {
            (Entry::Occupied { value: val1, .. }, Entry::Occupied { value: val2, .. }) => {
                (val1, val2)
            }
            _ => unreachable!(),
        }
    }

    /// Get the key for an element in the slab.
    ///
    /// The reference must point to an element owned by the slab.
    /// Otherwise this function will panic.
    /// This is a constant-time operation because the key can be calculated
    /// from the reference with pointer arithmetic.
    ///
    /// # Panics
    ///
    /// This function will panic if the reference does not point to an element
    /// of the slab.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    ///
    /// let mut slab = Slab::new();
    /// let key = slab.insert(String::from("foo"));
    /// let value = &slab[key];
    /// assert_eq!(slab.key_of(value), key);
    /// ```
    ///
    /// Values are not compared, so passing a reference to a different location
    /// will result in a panic:
    ///
    /// ```should_panic
    /// # use generational_slab::*;
    ///
    /// let mut slab = Slab::new();
    /// let key = slab.insert(0);
    /// let bad = &0;
    /// slab.key_of(bad); // this will panic
    /// unreachable!();
    /// ```
    #[track_caller]
    pub fn key_of(&self, present_element: &T) -> Key {
        let element_ptr = present_element as *const T as usize;
        let base_ptr = self.entries.as_ptr() as usize;
        // Use wrapping subtraction in case the reference is bad
        let byte_offset = element_ptr.wrapping_sub(base_ptr);
        // The division rounds away any offset of T inside Entry
        // The size of Entry<T> is never zero even if T is due to Vacant { next: u32, generation: u32 }
        let pos = byte_offset / mem::size_of::<Entry<T>>();
        // Prevent returning unspecified (but out of bounds) values
        if pos >= self.entries.len() {
            panic!("The reference points to a value outside this slab");
        }
        // The reference cannot point to a vacant entry, because then it would not be valid
        let generation = self.entries[pos].generation();
        Key {
            position: pos as u32,
            generation,
        }
    }

    /// Insert a value in the slab, returning key assigned to the value.
    ///
    /// The returned key can later be used to retrieve or remove the value using indexed
    /// lookup and `remove`. Additional capacity is allocated if needed. See
    /// [Capacity and reallocation](index.html#capacity-and-reallocation).
    ///
    /// # Panics
    ///
    /// Panics if the number of entries would exceed `u32::MAX`, or if the new
    /// storage in the vector exceeds `isize::MAX` bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// let key = slab.insert("hello");
    /// assert_eq!(slab[key], "hello");
    /// ```
    pub fn insert(&mut self, val: T) -> Key {
        let pos = self.next;
        let generation = self.insert_at(pos, val);

        Key {
            position: pos,
            generation,
        }
    }

    /// Returns the key of the next vacant entry.
    ///
    /// This function returns the key of the vacant entry which will be used
    /// for the next insertion. This is equivalent to
    /// `slab.vacant_entry().key()`, but it doesn't require mutable access.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    /// assert_eq!(slab.vacant_key().position(), 0);
    ///
    /// slab.insert(0);
    /// assert_eq!(slab.vacant_key().position(), 1);
    /// ```
    pub fn vacant_key(&self) -> Key {
        let pos = self.next as usize;
        let generation = self
            .entries
            .get(pos)
            .map_or(self.generation_watermark, |entry| entry.generation());
        Key {
            position: self.next,
            generation,
        }
    }

    /// Return a handle to a vacant entry allowing for further manipulation.
    ///
    /// This function is useful when creating values that must contain their
    /// slab key. The returned `VacantEntry` reserves a slot in the slab and is
    /// able to query the associated key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let hello = {
    ///     let entry = slab.vacant_entry();
    ///     let key = entry.key();
    ///
    ///     entry.insert((key, "hello"));
    ///     key
    /// };
    ///
    /// assert_eq!(hello, slab[hello].0);
    /// assert_eq!("hello", slab[hello].1);
    /// ```
    pub fn vacant_entry(&mut self) -> VacantEntry<'_, T> {
        let key = self.vacant_key();
        VacantEntry { key, slab: self }
    }

    /// Insert a value at the given position. Returns the generation of the entry.
    fn insert_at(&mut self, pos: u32, val: T) -> u32 {
        self.len += 1;
        let pos_usize = pos as usize;

        if pos_usize == self.entries.len() {
            assert!(
                self.entries.len() < u32::MAX as usize,
                "slab exceeded maximum capacity of {} entries",
                u32::MAX
            );
            let generation = self.generation_watermark;
            self.entries.push(Entry::Occupied {
                value: val,
                generation,
            });
            self.next = pos + 1;
            generation
        } else {
            let entry = &self.entries[pos_usize];
            let generation = entry.generation();
            self.next = match *entry {
                Entry::Vacant { next, .. } => next,
                _ => unreachable!(),
            };
            self.entries[pos_usize] = Entry::Occupied {
                value: val,
                generation,
            };
            generation
        }
    }

    /// Tries to remove the value associated with the given key,
    /// returning the value if the key existed.
    ///
    /// The key is then released and may be associated with future stored
    /// values. Returns `None` if the key's generation does not match (stale key).
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let hello = slab.insert("hello");
    ///
    /// assert_eq!(slab.try_remove(hello), Some("hello"));
    /// assert!(!slab.contains(hello));
    /// ```
    pub fn try_remove(&mut self, key: Key) -> Option<T> {
        let pos = key.position as usize;
        if let Some(entry) = self.entries.get_mut(pos)
            && let Entry::Occupied { generation, .. } = entry
            && *generation == key.generation
        {
            let new_generation = generation.wrapping_add(1);
            let val = match core::mem::replace(
                entry,
                Entry::Vacant {
                    next: self.next,
                    generation: new_generation,
                },
            ) {
                Entry::Occupied { value, .. } => value,
                _ => unreachable!(),
            };

            self.len -= 1;
            self.next = key.position;
            return val.into();
        }
        None
    }

    /// Remove and return the value associated with the given key.
    ///
    /// The key is then released and may be associated with future stored
    /// values.
    ///
    /// # Panics
    ///
    /// Panics if `key` is not associated with a value, including if the key's
    /// generation does not match (stale key).
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let hello = slab.insert("hello");
    ///
    /// assert_eq!(slab.remove(hello), "hello");
    /// assert!(!slab.contains(hello));
    /// ```
    #[track_caller]
    pub fn remove(&mut self, key: Key) -> T {
        self.try_remove(key).expect("invalid key")
    }

    /// Remove the value at a raw position without generation checking.
    ///
    /// This is used internally by `retain` and `compact` which iterate by
    /// position rather than by key.
    fn remove_at(&mut self, pos: u32) -> T {
        let pos_usize = pos as usize;
        let entry = &mut self.entries[pos_usize];
        let new_generation = entry.generation().wrapping_add(1);

        let val = match core::mem::replace(
            entry,
            Entry::Vacant {
                next: self.next,
                generation: new_generation,
            },
        ) {
            Entry::Occupied { value, .. } => value,
            _ => unreachable!(),
        };

        self.len -= 1;
        self.next = pos;
        val
    }

    /// Return `true` if a value is associated with the given key.
    ///
    /// Returns `false` if the key's generation does not match (stale key).
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let hello = slab.insert("hello");
    /// assert!(slab.contains(hello));
    ///
    /// slab.remove(hello);
    ///
    /// assert!(!slab.contains(hello));
    /// ```
    pub fn contains(&self, key: Key) -> bool {
        matches!(
            self.entries.get(key.position as usize),
            Some(&Entry::Occupied { generation, .. }) if generation == key.generation
        )
    }

    /// Retain only the elements specified by the predicate.
    ///
    /// In other words, remove all elements `e` such that `f(key, &mut e)`
    /// returns false. This method operates in place and preserves the key
    /// associated with the retained values.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let k1 = slab.insert(0);
    /// let k2 = slab.insert(1);
    /// let k3 = slab.insert(2);
    ///
    /// slab.retain(|key, val| key == k1 || *val == 1);
    ///
    /// assert!(slab.contains(k1));
    /// assert!(slab.contains(k2));
    /// assert!(!slab.contains(k3));
    ///
    /// assert_eq!(2, slab.len());
    /// ```
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(Key, &mut T) -> bool,
    {
        for i in 0..self.entries.len() {
            let keep = match self.entries[i] {
                Entry::Occupied {
                    ref mut value,
                    generation,
                } => f(
                    Key {
                        position: i as u32,
                        generation,
                    },
                    value,
                ),
                _ => true,
            };

            if !keep {
                self.remove_at(i as u32);
            }
        }
    }

    /// Return a draining iterator that removes all elements from the slab and
    /// yields the removed items.
    ///
    /// Note: Elements are removed even if the iterator is only partially
    /// consumed or not consumed at all.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let _ = slab.insert(0);
    /// let _ = slab.insert(1);
    /// let _ = slab.insert(2);
    ///
    /// {
    ///     let mut drain = slab.drain();
    ///
    ///     assert_eq!(Some(0), drain.next());
    ///     assert_eq!(Some(1), drain.next());
    ///     assert_eq!(Some(2), drain.next());
    ///     assert_eq!(None, drain.next());
    /// }
    ///
    /// assert!(slab.is_empty());
    /// ```
    pub fn drain(&mut self) -> Drain<'_, T> {
        self.generation_watermark = Self::watermark_for(self.generation_watermark, &self.entries);
        let old_len = self.len;
        self.len = 0;
        self.next = 0;
        Drain {
            inner: self.entries.drain(..),
            len: old_len,
        }
    }
}

impl<T> ops::Index<Key> for Slab<T> {
    type Output = T;

    #[track_caller]
    fn index(&self, key: Key) -> &T {
        match self.entries.get(key.position as usize) {
            Some(Entry::Occupied { value, generation }) if *generation == key.generation => value,
            _ => panic!("invalid key"),
        }
    }
}

impl<T> ops::IndexMut<Key> for Slab<T> {
    #[track_caller]
    fn index_mut(&mut self, key: Key) -> &mut T {
        match self.entries.get_mut(key.position as usize) {
            Some(&mut Entry::Occupied {
                ref mut value,
                generation,
            }) if generation == key.generation => value,
            _ => panic!("invalid key"),
        }
    }
}

impl<T> IntoIterator for Slab<T> {
    type Item = (Key, T);
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            entries: self.entries.into_iter().enumerate(),
            len: self.len,
        }
    }
}

impl<'a, T> IntoIterator for &'a Slab<T> {
    type Item = (Key, &'a T);
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut Slab<T> {
    type Item = (Key, &'a mut T);
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T> fmt::Debug for Slab<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        if fmt.alternate() {
            fmt.debug_map().entries(self.iter()).finish()
        } else {
            fmt.debug_struct("Slab")
                .field("len", &self.len)
                .field("cap", &self.capacity())
                .finish()
        }
    }
}

impl<T> fmt::Debug for IntoIter<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("IntoIter")
            .field("remaining", &self.len)
            .finish()
    }
}

impl<T> fmt::Debug for Iter<'_, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Iter")
            .field("remaining", &self.len)
            .finish()
    }
}

impl<T> fmt::Debug for IterMut<'_, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("IterMut")
            .field("remaining", &self.len)
            .finish()
    }
}

impl<T> fmt::Debug for Drain<'_, T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Drain").finish()
    }
}

// ===== VacantEntry =====

impl<'a, T> VacantEntry<'a, T> {
    /// Insert a value in the entry, returning a mutable reference to the value.
    ///
    /// To get the key associated with the value, use `key` prior to calling
    /// `insert`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let hello = {
    ///     let entry = slab.vacant_entry();
    ///     let key = entry.key();
    ///
    ///     entry.insert((key, "hello"));
    ///     key
    /// };
    ///
    /// assert_eq!(hello, slab[hello].0);
    /// assert_eq!("hello", slab[hello].1);
    /// ```
    pub fn insert(self, val: T) -> &'a mut T {
        let pos = self.key.position;
        self.slab.insert_at(pos, val);

        match self.slab.entries.get_mut(pos as usize) {
            Some(&mut Entry::Occupied { ref mut value, .. }) => value,
            _ => unreachable!(),
        }
    }

    /// Return the key associated with this entry.
    ///
    /// A value stored in this entry will be associated with this key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use generational_slab::*;
    /// let mut slab = Slab::new();
    ///
    /// let hello = {
    ///     let entry = slab.vacant_entry();
    ///     let key = entry.key();
    ///
    ///     entry.insert((key, "hello"));
    ///     key
    /// };
    ///
    /// assert_eq!(hello, slab[hello].0);
    /// assert_eq!("hello", slab[hello].1);
    /// ```
    pub const fn key(&self) -> Key {
        self.key
    }
}

// ===== IntoIter =====

impl<T> Iterator for IntoIter<T> {
    type Item = (Key, T);

    fn next(&mut self) -> Option<Self::Item> {
        for (idx, entry) in &mut self.entries {
            if let Entry::Occupied {
                value, generation, ..
            } = entry
            {
                self.len -= 1;
                return Some((
                    Key {
                        position: idx as u32,
                        generation,
                    },
                    value,
                ));
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((idx, entry)) = self.entries.next_back() {
            if let Entry::Occupied {
                value, generation, ..
            } = entry
            {
                self.len -= 1;
                return Some((
                    Key {
                        position: idx as u32,
                        generation,
                    },
                    value,
                ));
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }
}

impl<T> ExactSizeIterator for IntoIter<T> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<T> FusedIterator for IntoIter<T> {}

// ===== Iter =====

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (Key, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        for (idx, entry) in &mut self.entries {
            if let Entry::Occupied {
                ref value,
                generation,
                ..
            } = *entry
            {
                self.len -= 1;
                return Some((
                    Key {
                        position: idx as u32,
                        generation,
                    },
                    value,
                ));
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<T> DoubleEndedIterator for Iter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((idx, entry)) = self.entries.next_back() {
            if let Entry::Occupied {
                ref value,
                generation,
                ..
            } = *entry
            {
                self.len -= 1;
                return Some((
                    Key {
                        position: idx as u32,
                        generation,
                    },
                    value,
                ));
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<T> FusedIterator for Iter<'_, T> {}

// ===== IterMut =====

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = (Key, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        for (idx, entry) in &mut self.entries {
            if let Entry::Occupied {
                ref mut value,
                generation,
                ..
            } = *entry
            {
                self.len -= 1;
                return Some((
                    Key {
                        position: idx as u32,
                        generation,
                    },
                    value,
                ));
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<T> DoubleEndedIterator for IterMut<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((idx, entry)) = self.entries.next_back() {
            if let Entry::Occupied {
                ref mut value,
                generation,
                ..
            } = *entry
            {
                self.len -= 1;
                return Some((
                    Key {
                        position: idx as u32,
                        generation,
                    },
                    value,
                ));
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }
}

impl<T> ExactSizeIterator for IterMut<'_, T> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<T> FusedIterator for IterMut<'_, T> {}

// ===== Drain =====

impl<T> Iterator for Drain<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        for entry in &mut self.inner {
            if let Entry::Occupied { value, .. } = entry {
                self.len -= 1;
                return Some(value);
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<T> DoubleEndedIterator for Drain<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.inner.next_back() {
            if let Entry::Occupied { value, .. } = entry {
                self.len -= 1;
                return Some(value);
            }
        }

        debug_assert_eq!(self.len, 0);
        None
    }
}

impl<T> ExactSizeIterator for Drain<'_, T> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<T> FusedIterator for Drain<'_, T> {}
