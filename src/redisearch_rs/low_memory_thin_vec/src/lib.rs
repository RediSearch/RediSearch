//! `LowMemoryThinVec` is exactly the same as `Vec`, except that it stores its `len` and `capacity` in the buffer
//! it allocates.
//!
//! # Memory layout
//!
//! The memory layout of a local variable of type `LowMemoryThinVec<u64>` looks as follows:
//!
//!```text
//!   Stack               |              Heap
//!   -----               |              ----
//!                       |
//!  +---------------+    |
//!  | ptr (8 bytes) | ------->  Header +----------------------+
//!  +---------------+    |             | 3 (len)     (2 bytes)|
//!                       |             | 4 (cap)     (2 bytes)|
//!                       |             | (padding)   (4 bytes)|
//!                       |             +----------------------+
//!                       |      Data   | 12          (8 bytes)|
//!                       |             | 151         (8 bytes)|
//!                       |             | 2           (8 bytes)|
//!                       |             | (unused)    (8 bytes)|
//!                       |             +----------------------+
//! ```
//!
//! It's pointer-sized on the stack, compared to `Vec<T>` which has 3 pointer-sized fields:
//!
//! ```text
//!    Stack              |      Heap
//!    -----              |      ----
//!                       |
//!   +---------------+   |      +--------------------+
//!   | ptr (8 bytes) | -------> | 12        (8 bytes)|
//!   | len (8 bytes) |   |      | 151       (8 bytes)|
//!   | cap (8 bytes) |   |      | 2         (8 bytes)|
//!   +---------------+   |      | (unused)  (8 bytes)|
//!                       |      +--------------------+
//! ```
//!
//! # Memory footprint
//!
//! The memory footprint of `LowMemoryThinVec<T>` is smaller than `Vec<T>` ; notably in cases where space is reserved for
//! the non-existence of `LowMemoryThinVec<T>`. So `Vec<LowMemoryThinVec<T>>` and `Option<LowMemoryThinVec<T>>::None` will waste less
//! space. Being pointer-sized also means it can be passed/stored in registers.
//!
//! Of course, any actually constructed `LowMemoryThinVec` will theoretically have a bigger allocation, but
//! the fuzzy nature of allocators means that might not actually be the case.
//!
//! Properties of `Vec` that are preserved:
//! * `LowMemoryThinVec::new()` doesn't allocate (it points to a statically allocated singleton)
//! * reallocation can be done in place
//! * `size_of::<LowMemoryThinVec<T>>()` == `size_of::<Option<LowMemoryThinVec<T>>>()`
//!
//! Properties of `Vec` that aren't preserved:
//! * `LowMemoryThinVec<T>` can't ever be zero-cost roundtripped to a `Box<[T]>`, `String`, or `*mut T`
//! * `from_raw_parts` doesn't exist
//! * `LowMemoryThinVec` currently doesn't bother to not-allocate for Zero Sized Types (e.g. `LowMemoryThinVec<()>`),
//!   but it could be done.
//!
//! ## Differences with the original `thin_vec`
//!
//! - All Gecko-specific code has been removed
//! - `ThinVec::drain`, `ThinVec::append` and `ThinVec::splice` have been removed, since they aren't
//!   needed for our purposes and they contribute a significant amount of unsafe-related complexity.
//! - Maximum capacity is limited to `u16::MAX` elements for non-zero-sized types.
//!
//! ## License
//!
//! Portions of this codebase are **originally from [`thin-vec`](https://github.com/Gankra/thin-vec)**, which is licensed under either:
//!
//! - [Apache License 2.0](./LICENSE-APACHE)
//! - [MIT License](./LICENSE-MIT)
//!
//! We have kept the same license(s) for this codebase.
use std::alloc::*;
use std::borrow::*;
use std::cmp::*;
use std::convert::TryFrom;
use std::hash::*;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::{boxed::Box, vec::Vec};
use std::{fmt, mem, ptr, slice};

use header::*;
use layout::*;

pub(crate) mod header;
pub(crate) mod layout;

/// Allocates a header (and array) for a `LowMemoryThinVec<T>` with the given capacity.
///
/// # Panics
///
/// Panics if the required size overflows `isize::MAX`.
fn allocate_for_capacity<T>(cap: usize) -> NonNull<Header> {
    debug_assert!(cap > 0);
    let layout = allocation_layout::<T>(cap);
    let header = {
        debug_assert!(layout.size() > 0);
        // SAFETY:
        // `layout.size()` is greater than zero, since `cap` is greater than zero
        // and we always allocate a header, even if `T` is a zero-sized type.
        unsafe { alloc(layout) as *mut Header }
    };

    let Some(header) = NonNull::new(header) else {
        // The allocation failed!
        handle_alloc_error(layout)
    };

    // If `T` is a zero-sized type, we won't ever need to increase the size of
    // the allocation. Its values take no space in memory!
    // Therefore it's safe to set the capacity to `MAX_CAP`.
    let capacity = if mem::size_of::<T>() == 0 {
        MAX_CAP
    } else {
        cap
    };

    // Initialize the allocated buffer with a valid header value.
    //
    // SAFETY:
    // - The destination and the value are properly aligned,
    //   since the allocation was performed against a type layout
    //   that begins with a header field.
    unsafe {
        header.write(Header::new(0, capacity));
    }
    header
}

/// See the crate's top level documentation for a description of this type.
#[repr(C)]
pub struct LowMemoryThinVec<T> {
    // # Invariants
    //
    // It is always safe to convert this pointer to a `&Header`,
    // according to the criteria listed in <https://doc.rust-lang.org/std/ptr/index.html#pointer-to-reference-conversion>.
    //
    // This is due to the fact that `ptr` is actually a:
    //
    // ```rust,ignore
    // enum HeaderRef {
    //     Singleton, // -> pointing at `EMPTY_HEADER`
    //     Allocated(&Header), // -> pointing at the beginning of the allocated buffer
    // }
    // ```
    //
    // We can't model it as an enum in code because it would increase the size of the field from 8 bytes
    // to 16 bytes due to the discriminant. The Rust compiler, unfortunately, doesn't let us
    // express the fact that we have a niche in the `Allocated` variant (i.e. `&EMPTY_HEADER`)
    // that could be used to discriminate between the two cases.
    ptr: NonNull<Header>,
    // This marker type has no consequences for variance, but is necessary
    // to make the compiler's drop logic behave as if we own a `T`.
    //
    // For details, see:
    // https://github.com/rust-lang/rfcs/blob/master/text/0769-sound-generic-drop.md#phantom-data
    _phantom: PhantomData<T>,
}

// SAFETY:
// `LowMemoryThinVec<T>` is, for all `Send` intents and purposes, equivalent to a `Vec<T>`.
// This `Send` implementation is therefore safe for the same reasons as the one
// provided by `Vec<T>` when `T` is `Send`.
unsafe impl<T: Send> Send for LowMemoryThinVec<T> {}

// SAFETY:
// `LowMemoryThinVec<T>` is, for all `Sync` intents and purposes, equivalent to a `Vec<T>`.
// This `Sync` implementation is therefore safe for the same reasons as the one
// provided by `Vec<T>` when `T` is `Sync`.
unsafe impl<T: Sync> Sync for LowMemoryThinVec<T> {}

/// Creates a `LowMemoryThinVec` containing the arguments.
///
/// ```rust
/// use low_memory_thin_vec::low_memory_thin_vec;
///
/// let v = low_memory_thin_vec![1, 2, 3];
/// assert_eq!(v.len(), 3);
/// assert_eq!(v[0], 1);
/// assert_eq!(v[1], 2);
/// assert_eq!(v[2], 3);
///
/// let v = low_memory_thin_vec![1; 3];
/// assert_eq!(v, [1, 1, 1]);
/// ```
#[macro_export]
macro_rules! low_memory_thin_vec {
    (@UNIT $($t:tt)*) => (());

    ($elem:expr; $n:expr) => ({
        let mut vec = $crate::LowMemoryThinVec::new();
        vec.resize($n, $elem);
        vec
    });
    () => {$crate::LowMemoryThinVec::new()};
    ($($x:expr),*) => ({
        let len = [$(low_memory_thin_vec!(@UNIT $x)),*].len();
        let mut vec = $crate::LowMemoryThinVec::with_capacity(len);
        $(vec.push($x);)*
        vec
    });
    ($($x:expr,)*) => (low_memory_thin_vec![$($x),*]);
}

impl<T> LowMemoryThinVec<T> {
    /// Creates a new empty LowMemoryThinVec.
    ///
    /// This will not allocate.
    pub const fn new() -> LowMemoryThinVec<T> {
        // SAFETY:
        // The pointer is not null since it comes from a (static) reference.
        let ptr = unsafe {
            // TODO: Use [NonNull::from_ref](https://doc.rust-lang.org/std/ptr/struct.NonNull.html#method.from_ref)
            //   when it stabilizes
            NonNull::new_unchecked(&EMPTY_HEADER as *const Header as *mut Header)
        };
        LowMemoryThinVec {
            ptr,
            _phantom: PhantomData,
        }
    }

    /// Constructs a new, empty `LowMemoryThinVec<T>` with at least the specified capacity.
    ///
    /// The vector will be able to hold at least `capacity` elements without
    /// reallocating. This method is allowed to allocate for more elements than
    /// `capacity`. If `capacity` is 0, the vector will not allocate.
    ///
    /// It is important to note that although the returned vector has the
    /// minimum *capacity* specified, the vector will have a zero *length*.
    ///
    /// If it is important to know the exact allocated capacity of a `LowMemoryThinVec`,
    /// always use the [`capacity`] method after construction.
    ///
    /// **NOTE**: unlike `Vec`, `LowMemoryThinVec` **MUST** allocate once to keep track of non-zero
    /// lengths. As such, we cannot provide the same guarantees about ThinVecs
    /// of ZSTs not allocating. However the allocation never needs to be resized
    /// to add more ZSTs, since the underlying array is still length 0.
    ///
    /// [Capacity and reallocation]: #capacity-and-reallocation
    /// [`capacity`]: Vec::capacity
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::LowMemoryThinVec;
    ///
    /// let mut vec = LowMemoryThinVec::with_capacity(10);
    ///
    /// // The vector contains no items, even though it has capacity for more
    /// assert_eq!(vec.len(), 0);
    /// assert!(vec.capacity() >= 10);
    ///
    /// // These are all done without reallocating...
    /// for i in 0..10 {
    ///     vec.push(i);
    /// }
    /// assert_eq!(vec.len(), 10);
    /// assert!(vec.capacity() >= 10);
    ///
    /// // ...but this may make the vector reallocate
    /// vec.push(11);
    /// assert_eq!(vec.len(), 11);
    /// assert!(vec.capacity() >= 11);
    ///
    /// // A vector of a zero-sized type will always over-allocate, since no
    /// // space is needed to store the actual elements.
    /// let vec_units = LowMemoryThinVec::<()>::with_capacity(10);
    /// ```
    pub fn with_capacity(cap: usize) -> LowMemoryThinVec<T> {
        if cap == 0 {
            LowMemoryThinVec::new()
        } else {
            let ptr = allocate_for_capacity::<T>(cap);
            LowMemoryThinVec {
                ptr,
                _phantom: PhantomData,
            }
        }
    }

    // Accessor conveniences

    /// Return a reference to the header.
    fn header_ref(&self) -> &Header {
        // SAFETY:
        // Guaranteed by the invariants on the `ptr` field.
        // Check out [`LowMemoryThinVec::ptr`] for more details.
        unsafe { self.ptr.as_ref() }
    }
    /// Return a pointer to the data array located after the header.
    fn data_raw(&self) -> *mut T {
        let header_field_padding = header_field_padding::<T>();

        // Although we ensure the data array is aligned when we allocate,
        // we can't do that with the empty singleton. So when it might not
        // be properly aligned, we substitute in the NonNull::dangling
        // which *is* aligned.
        //
        // To minimize dynamic branches on `cap` for all accesses
        // to the data, we include this guard which should only involve
        // compile-time constants. Ideally this should result in the branch
        // only be included for types with excessive alignment, since all
        // operations are `const`.
        let singleton_header_is_aligned =
            // If the Header is at
            // least as aligned as T *and* the padding would have
            // been 0, then one-past-the-end of the empty singleton
            // *is* a valid data pointer and we can remove the
            // `dangling` special case.
            mem::align_of::<Header>() >= mem::align_of::<T>() && header_field_padding == 0;

        if !singleton_header_is_aligned && self.header_ref().capacity() == 0 {
            NonNull::dangling().as_ptr()
        } else {
            // This could technically result in overflow, but padding
            // would have to be absurdly large for this to occur.
            let header_size = mem::size_of::<Header>();
            let header_ptr = self.ptr.as_ptr() as *mut u8;
            // SAFETY:
            // Due to all the reasoning above, we know that offsetted
            // pointer is valid and aligned in both the singleton and
            // the non-singleton cases.
            unsafe { header_ptr.add(header_size + header_field_padding) as *mut T }
        }
    }

    /// # Safety
    ///
    /// The header pointer must not point to [`EMPTY_HEADER`].
    unsafe fn header_mut(&mut self) -> &mut Header {
        // SAFETY:
        // We know that `self.ptr` can be safely converted to a `&Header`,
        // thanks to the its documented invariants (see [`Self::ptr`] docs).
        // We also know there's no other references to the header, as we require
        // the call to pass `&mut self` as input.
        // Therefore it's safe to produce a `&mut Header` from `self.ptr`.
        unsafe { self.ptr.as_mut() }
    }

    /// Returns the number of elements in the vector, also referred to
    /// as its 'length'.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let a = low_memory_thin_vec![1, 2, 3];
    /// assert_eq!(a.len(), 3);
    /// ```
    pub fn len(&self) -> usize {
        self.header_ref().len()
    }

    /// Returns `true` if the vector contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::LowMemoryThinVec;
    ///
    /// let mut v = LowMemoryThinVec::new();
    /// assert!(v.is_empty());
    ///
    /// v.push(1);
    /// assert!(!v.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of elements the vector can hold without
    /// reallocating.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::LowMemoryThinVec;
    ///
    /// let vec: LowMemoryThinVec<i32> = LowMemoryThinVec::with_capacity(10);
    /// assert_eq!(vec.capacity(), 10);
    /// ```
    pub fn capacity(&self) -> usize {
        self.header_ref().capacity()
    }

    /// Returns the memory usage of the vector on the heap in bytes,
    /// i.e. the size of the header and the elements, as well as any padding.
    ///
    /// Does not take into account any additional memory allocated by elements
    /// of the vector. Returns 0 if the vector has no capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::LowMemoryThinVec;
    ///
    /// let vec: LowMemoryThinVec<i32> = LowMemoryThinVec::with_capacity(5);
    /// assert_eq!(vec.mem_usage(), 24);
    ///
    /// assert_eq!(LowMemoryThinVec::<u64>::new().mem_usage(), 0);
    /// ```
    pub fn mem_usage(&self) -> usize {
        if !self.has_allocated() {
            return 0;
        }
        allocation_layout::<T>(self.capacity()).size()
    }

    /// Returns `true` if the vector has allocated any memory via the global
    /// allocator.
    #[inline]
    pub fn has_allocated(&self) -> bool {
        !self.is_singleton()
    }

    /// Forces the length of the vector to `new_len`.
    ///
    /// This is a low-level operation that maintains none of the normal
    /// invariants of the type. Normally changing the length of a vector
    /// is done using one of the safe operations instead, such as
    /// [`truncate`], [`resize`], [`extend`], or [`clear`].
    ///
    /// [`truncate`]: LowMemoryThinVec::truncate
    /// [`resize`]: LowMemoryThinVec::resize
    /// [`extend`]: LowMemoryThinVec::extend
    /// [`clear`]: LowMemoryThinVec::clear
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`capacity()`].
    /// - The first `new_len` elements must be initialized.
    ///   This is trivially true if `new_len` is less than or equal to the current length,
    ///   but it is not guaranteed to be true if `new_len` is greater than the current length.
    ///
    /// [`capacity()`]: LowMemoryThinVec::capacity
    ///
    /// # Examples
    ///
    /// This method can be useful for situations in which the vector
    /// is serving as a buffer for other code, particularly over FFI:
    ///
    /// ```no_run
    /// use low_memory_thin_vec::LowMemoryThinVec;
    ///
    /// # // This is just a minimal skeleton for the doc example;
    /// # // don't use this as a starting point for a real library.
    /// # pub struct StreamWrapper { strm: *mut std::ffi::c_void }
    /// # const Z_OK: i32 = 0;
    /// # unsafe extern "C" {
    /// #     fn deflateGetDictionary(
    /// #         strm: *mut std::ffi::c_void,
    /// #         dictionary: *mut u8,
    /// #         dictLength: *mut usize,
    /// #     ) -> i32;
    /// # }
    /// # impl StreamWrapper {
    /// pub fn get_dictionary(&self) -> Option<LowMemoryThinVec<u8>> {
    ///     // Per the FFI method's docs, "32768 bytes is always enough".
    ///     let mut dict = LowMemoryThinVec::with_capacity(32_768);
    ///     let mut dict_length = 0;
    ///     // SAFETY: When `deflateGetDictionary` returns `Z_OK`, it holds that:
    ///     // 1. `dict_length` elements were initialized.
    ///     // 2. `dict_length` <= the capacity (32_768)
    ///     // which makes `set_len` safe to call.
    ///     unsafe {
    ///         // Make the FFI call...
    ///         let r = deflateGetDictionary(self.strm, dict.as_mut_ptr(), &mut dict_length);
    ///         if r == Z_OK {
    ///             // ...and update the length to what was initialized.
    ///             dict.set_len(dict_length);
    ///             Some(dict)
    ///         } else {
    ///             None
    ///         }
    ///     }
    /// }
    /// # }
    /// ```
    ///
    /// While the following example is sound, there is a memory leak since
    /// the inner vectors were not freed prior to the `set_len` call:
    ///
    /// ```no_run
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![low_memory_thin_vec![1, 0, 0],
    ///                    low_memory_thin_vec![0, 1, 0],
    ///                    low_memory_thin_vec![0, 0, 1]];
    /// // SAFETY:
    /// // 1. `old_len..0` is empty so no elements need to be initialized.
    /// // 2. `0 <= capacity` always holds whatever `capacity` is.
    /// unsafe {
    ///     vec.set_len(0);
    /// }
    /// ```
    ///
    /// Normally, here, one would use [`clear`] instead to correctly drop
    /// the contents and thus not leak memory.
    pub unsafe fn set_len(&mut self, len: usize) {
        if self.is_singleton() {
            // A prerequisite of `Vec::set_len` is that `new_len` must be
            // less than or equal to capacity(). The same applies here.
            debug_assert!(
                len == 0,
                "invalid set_len({}) on empty LowMemoryThinVec",
                len
            );
        } else {
            // SAFETY:
            // - We're not in the singleton case.
            // - We're asking the caller to uphold the initialization invariant
            //   for the first `len` elements in the data array.
            unsafe { self.set_len_non_singleton(len) }
        }
    }

    // For internal use only.
    //
    // # Safety
    //
    // - It must be known that the header doesn't point to the [`EMPTY_HEADER`] singleton.
    // - It must be known that the first `len` elements in the data array are initialized.
    //
    // # Panics
    //
    // Panics if `len` is greater than the current capacity.
    unsafe fn set_len_non_singleton(&mut self, len: usize) {
        // SAFETY:
        // Safety requirements have been passed to the caller.
        unsafe { self.header_mut().set_len(len) }
    }

    /// Appends an element to the back of a collection.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1, 2];
    /// vec.push(3);
    /// assert_eq!(vec, [1, 2, 3]);
    /// ```
    pub fn push(&mut self, val: T) {
        let old_len = self.len();
        if old_len == self.capacity() {
            self.reserve(1);
        }

        // SAFETY:
        // - The pointer is within the bounds of the allocated memory, since we
        //   have non-zero capacity, thanks to the `reserve` call above.
        // - The offsetted pointer doesn't overflow since `reserve` guarantees that the
        //   size of the new allocation doesn't exceed `isize::MAX`.
        let new_element_ptr = unsafe { self.data_raw().add(old_len) };

        // SAFETY:
        // We know that the pointer is valid for writes since the current capacity is
        // at least `old_len + 1` and we're not in the singleton case.
        unsafe {
            ptr::write(new_element_ptr, val);
        }

        // It won't panic since `old_len + 1` is less than the current capacity thanks
        //   to the `reserve` call above.
        //
        // SAFETY:
        // - We know we're not in the singleton case since we have non-zero capacity,
        //   thanks to the `reserve` call above.
        // - We know that the first `old_len` were already initialized
        // - We know that the `old_len` + 1 element was just initialized thanks to the write above.
        unsafe {
            self.set_len_non_singleton(old_len + 1);
        }
    }

    /// Removes the last element from a vector and returns it, or [`None`] if it
    /// is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1, 2, 3];
    /// assert_eq!(vec.pop(), Some(3));
    /// assert_eq!(vec, [1, 2]);
    /// ```
    pub fn pop(&mut self) -> Option<T> {
        let old_len = self.len();
        if old_len == 0 {
            // The vector is empty, so there's nothing to pop.
            return None;
        }

        // It won't panic because we're reducing the length, so we
        // are guaranteed to stay below the current capacity.
        //
        // SAFETY:
        // - We know we're not in the singleton case since `old_len > 0`.
        // - We know that the first `old_len - 1` elements are initialized,
        //   since the current length is `old_len`.
        unsafe {
            self.set_len_non_singleton(old_len - 1);
        }

        // SAFETY:
        // - The pointer is within the bounds of the allocation backing the vector.
        // - The pointer is well aligned, since `self.data_raw()` is aligned to `align_of::<T>()`.
        let last_element_ptr = unsafe { self.data_raw().add(old_len - 1) };
        // SAFETY:
        // - We know that the value we're reading is initialized since the length at the
        //   beginning of the function was `old_len`.
        unsafe { Some(ptr::read(last_element_ptr)) }
        // ^ This read leaves the `old_len`th slot uninitialized,
        // but that's okay because we reduced the length by one already.
    }

    /// Inserts an element at position `index` within the vector, shifting all
    /// elements after it to the right.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1, 2, 3];
    /// vec.insert(1, 4);
    /// assert_eq!(vec, [1, 4, 2, 3]);
    /// vec.insert(4, 5);
    /// assert_eq!(vec, [1, 4, 2, 3, 5]);
    /// ```
    pub fn insert(&mut self, idx: usize, elem: T) {
        let old_len = self.len();

        assert!(idx <= old_len, "Index out of bounds");
        if idx == old_len {
            self.push(elem);
            return;
        }

        if old_len == self.capacity() {
            self.reserve(1);
        }
        let data_ptr = self.data_raw();
        // SAFETY:
        // - Pointer is within the allocation backing the vector,
        //   since `idx` is in bounds thanks to assertion above.
        let idx_ptr = unsafe { data_ptr.add(idx) };
        // SAFETY:
        // - Pointer is within the allocation backing the vector,
        //   since `idx` is strictly smaller than `old_len` at
        //   this point. We've already deferred to `push` the
        //   case where `idx == old_len`.
        let after_idx_ptr = unsafe { data_ptr.add(idx + 1) };
        // SAFETY:
        // We know that we have enough capacity to shift everything
        // to the right by 1 thanks to `reserve`.
        unsafe {
            ptr::copy(idx_ptr, after_idx_ptr, old_len - idx);
        }

        // At this point, the `idx`th element is uninitialized.
        // We need to fix it before returning, otherwise our vector will
        // be left in an inconsistent state.

        // SAFETY:
        // The pointer is in bounds since `idx` is smaller than or equal to `old_len`,
        // and our capacity is at least `old_len + 1` thanks to `reserve`.
        unsafe {
            ptr::write(idx_ptr, elem);
        }
        // SAFETY:
        // - We know that we're not in the singleton case thanks to `reserve`.
        // - We know that the first `old_len + 1` elements are correctly initialized
        //   thanks to the `copy` and `write` operations above.
        unsafe {
            self.set_len_non_singleton(old_len + 1);
        }
    }

    /// Removes and returns the element at position `index` within the vector,
    /// shifting all elements after it to the left.
    ///
    /// Note: Because this shifts over the remaining elements, it has a
    /// worst-case performance of *O*(*n*). If you don't need the order of elements
    /// to be preserved, use [`swap_remove`] instead. If you'd like to remove
    /// elements from the beginning of the `LowMemoryThinVec`, consider using `std::collections::VecDeque`.
    ///
    /// [`swap_remove`]: LowMemoryThinVec::swap_remove
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut v = low_memory_thin_vec![1, 2, 3];
    /// assert_eq!(v.remove(1), 2);
    /// assert_eq!(v, [1, 3]);
    /// ```
    pub fn remove(&mut self, idx: usize) -> T {
        let old_len = self.len();

        assert!(idx < old_len, "Index out of bounds");

        // SAFETY:
        // - We know we're not in the singleton case.
        //   If we were, `old_len` would be 0 and the assertion
        //   above would have panicked for any value of `idx`.
        // - We know that the first `old_len` are initialized,
        //   therefore the first `old_len - 1` are initialized.
        unsafe {
            self.set_len_non_singleton(old_len - 1);
        }

        let data_ptr = self.data_raw();
        // SAFETY:
        // - Pointer is within the allocation backing the vector,
        //   since `idx` is in bounds thanks to assertion above.
        let idx_ptr = unsafe { data_ptr.add(idx) };
        // SAFETY:
        // - Pointer is within the allocation backing the vector,
        //   since `idx` is strictly smaller than `old_len`.
        let after_idx_ptr = unsafe { data_ptr.add(idx + 1) };
        // SAFETY:
        // - We know that the first `old_len` elements are initialized,
        //   and `idx` is smaller than `old_len`, so `idx` is within bounds.
        // - We know that the pointer is aligned and valid to read from,
        //   since we're in a non-singleton case.
        let val = unsafe { ptr::read(idx_ptr) };

        // At this point, the `idx`th element is uninitialized.
        // We need to fix it before returning, otherwise our vector will
        // be left in an inconsistent state.

        // We shift everything past `idx` to the left by 1, to fill the
        // gap left by the removed element.
        //
        // SAFETY:
        // - We know that the range we're copying from is initialized.
        // - We know that the range we're copying to is within bounds.
        // - We know that all pointers are aligned.
        unsafe {
            ptr::copy(after_idx_ptr, idx_ptr, old_len - idx - 1);
        }
        val
    }

    /// Removes an element from the vector and returns it.
    ///
    /// The removed element is replaced by the last element of the vector.
    ///
    /// This does not preserve ordering, but is *O*(1).
    /// If you need to preserve the element order, use [`remove`] instead.
    ///
    /// [`remove`]: LowMemoryThinVec::remove
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut v = low_memory_thin_vec!["foo", "bar", "baz", "qux"];
    ///
    /// assert_eq!(v.swap_remove(1), "bar");
    /// assert_eq!(v, ["foo", "qux", "baz"]);
    ///
    /// assert_eq!(v.swap_remove(0), "foo");
    /// assert_eq!(v, ["baz", "qux"]);
    /// ```
    pub fn swap_remove(&mut self, idx: usize) -> T {
        let old_len = self.len();

        assert!(idx < old_len, "Index out of bounds");

        let data_ptr = self.data_raw();

        // SAFETY:
        // - Pointer is within the allocation backing the vector,
        //   since `idx` is in bounds thanks to assertion above.
        let idx_ptr = unsafe { data_ptr.add(idx) };
        // SAFETY:
        // - Pointer is within the allocation backing the vector.
        let last_element_ptr = unsafe { data_ptr.add(old_len - 1) };
        // SAFETY:
        // - Both pointers are valid, aligned and within bounds.
        unsafe {
            ptr::swap(idx_ptr, last_element_ptr);
        }

        // It won't panic since the new length is smaller than the old length,
        // therefore it won't exceed the current capacity.
        //
        // SAFETY:
        // - We're not in singleton case because we checked idx < old_len,
        //   therefore `old_len` can't be 0.
        unsafe {
            self.set_len_non_singleton(old_len - 1);
        }

        // SAFETY:
        // - It points to a correctly initialized value.
        unsafe { ptr::read(last_element_ptr) }
        // ^ After this read, the "old" last element is no longer initialized.
        // But that's okay because we've reduced the length of the vector by 1.
    }

    /// Shortens the vector, keeping the first `len` elements and dropping
    /// the rest.
    ///
    /// If `len` is greater than the vector's current length, this has no
    /// effect.
    ///
    /// The [`drain`] method can emulate `truncate`, but causes the excess
    /// elements to be returned instead of dropped.
    ///
    /// Note that this method has no effect on the allocated capacity
    /// of the vector.
    ///
    /// # Examples
    ///
    /// Truncating a five element vector to two elements:
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1, 2, 3, 4, 5];
    /// vec.truncate(2);
    /// assert_eq!(vec, [1, 2]);
    /// ```
    ///
    /// No truncation occurs when `len` is greater than the vector's current
    /// length:
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1, 2, 3];
    /// vec.truncate(8);
    /// assert_eq!(vec, [1, 2, 3]);
    /// ```
    ///
    /// Truncating when `len == 0` is equivalent to calling the [`clear`]
    /// method.
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1, 2, 3];
    /// vec.truncate(0);
    /// assert_eq!(vec, []);
    /// ```
    ///
    /// [`clear`]: LowMemoryThinVec::clear
    /// [`drain`]: LowMemoryThinVec::drain
    pub fn truncate(&mut self, len: usize) {
        // drop any extra elements
        while len < self.len() {
            // Decrement the length *before* calling drop_in_place(),
            // so that a panic on `Drop` doesn't try to re-drop the
            // value with just-failed to drop.
            let new_len = self.len() - 1;
            // SAFETY:
            // - We're not in the singleton case, since the `while`
            //   loop would never be entered if we were in the singleton case,
            //   since `self.len` would be 0.
            // - The first `new_len` elements are initialized since we're
            //   truncating to a smaller length.
            unsafe {
                self.set_len_non_singleton(new_len);
            }
            // SAFETY:
            // - The offsetted pointer is within bounds of the allocated memory,
            //   since our capacity hasn't changed in this loop iteration and was
            //   guaranteed to be at least `new_len + 1`.
            let element_ptr = unsafe { self.data_raw().add(new_len) };

            // We now must invoke `Drop` on the element, to free any resource
            // it may hold.
            // Failing to drop the element may lead to memory leaks.
            //
            // SAFETY:
            // - The pointer is valid and aligned.
            // - We have exclusive access to the element, since `truncate`
            //   takes a `&mut self`.
            unsafe {
                ptr::drop_in_place(element_ptr);
            }
        }
    }

    /// Clears the vector, removing all values.
    ///
    /// Note that this method has no effect on the allocated capacity
    /// of the vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut v = low_memory_thin_vec![1, 2, 3];
    /// v.clear();
    /// assert!(v.is_empty());
    /// ```
    pub fn clear(&mut self) {
        let elements: *mut [T] = self.as_mut_slice();

        // We first set the length to 0 to avoid dropping elements
        // twice if an element's `Drop` implementation panics.
        //
        // SAFETY:
        // 0 is always within capacity and there are no elements
        // to initialize.
        unsafe {
            self.set_len(0); // could be the singleton
        }

        // Drop the elements to ensure any resource they own is released.
        // Failing to do so could lead to memory leaks or other resource leaks.
        //
        // SAFETY:
        // - The pointer is valid and aligned, since it comes from a reference.
        // - We have exclusive access to the element, the pointer
        //   comes from `&mut self`.
        unsafe {
            ptr::drop_in_place(elements);
        }
    }

    /// Extracts a slice containing the entire vector.
    ///
    /// Equivalent to `&s[..]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    /// use std::io::{self, Write};
    /// let buffer = low_memory_thin_vec![1, 2, 3, 5, 8];
    /// io::sink().write(buffer.as_slice()).unwrap();
    /// ```
    pub fn as_slice(&self) -> &[T] {
        // SAFETY:
        // - The pointer is valid and aligned for a vector of `self.len()`
        //  `T` elements, as guaranteed by [`Self::data_raw`].
        //   They all belong to the same allocation.
        // - All elements are initialized, as guaranteed by the length-related
        //   invariant of the `LowMemoryThinVec` type.
        // - There are no mutable references to the elements, since
        //   `as_slice` takes a shared reference to `self`.
        unsafe { slice::from_raw_parts(self.data_raw(), self.len()) }
    }

    /// Extracts a mutable slice of the entire vector.
    ///
    /// Equivalent to `&mut s[..]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    /// use std::io::{self, Read};
    /// let mut buffer = vec![0; 3];
    /// io::repeat(0b101).read_exact(buffer.as_mut_slice()).unwrap();
    /// ```
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY:
        // - The pointer is valid and aligned for a vector of `self.len()`
        //  `T` elements, as guaranteed by [`Self::data_raw`].
        //   They all belong to the same allocation.
        // - All elements are initialized, as guaranteed by the length-related
        //   invariant of the `LowMemoryThinVec` type.
        // - There are no other shared or mutable references to the elements, since
        //   `as_mut_slice` takes a shared reference to `self`.
        unsafe { slice::from_raw_parts_mut(self.data_raw(), self.len()) }
    }

    /// Reserve capacity for at least `additional` more elements to be inserted.
    ///
    /// May reserve more space than requested, to avoid frequent reallocations.
    ///
    /// Panics if the new capacity overflows `usize`.
    ///
    /// Re-allocates only if `self.capacity() < self.len() + additional`.
    pub fn reserve(&mut self, additional: usize) {
        let len = self.len();
        let old_cap = self.capacity();
        let min_cap = len.checked_add(additional).expect("capacity overflow");
        if min_cap <= old_cap {
            return;
        }
        // Ensure the new capacity is at least double, to guarantee exponential growth.
        let double_cap = if old_cap == 0 {
            // skip to 4 because tiny vecs are dumb; but not if that would cause overflow
            if mem::size_of::<T>() > usize::MAX / 8 {
                1
            } else {
                4
            }
        } else {
            old_cap.saturating_mul(2)
        };
        let new_cap = max(min_cap, double_cap);
        // SAFETY:
        // `new_cap` is at least `min_cap`, which is at least `len + additional`,
        // so greater than `len`.
        unsafe {
            self.reallocate(new_cap);
        }
    }

    /// Reserves the minimum capacity for `additional` more elements to be inserted.
    ///
    /// Panics if the new capacity overflows `usize`.
    ///
    /// Re-allocates only if `self.capacity() < self.len() + additional`.
    pub fn reserve_exact(&mut self, additional: usize) {
        let new_cap = self
            .len()
            .checked_add(additional)
            .expect("capacity overflow");
        let old_cap = self.capacity();
        if new_cap > old_cap {
            // SAFETY:
            // `new_cap` is at least `len + additional`, which is at least `len`.
            unsafe {
                self.reallocate(new_cap);
            }
        }
    }

    /// Shrinks the capacity of the vector as much as possible.
    ///
    /// It will drop down as close as possible to the length but the allocator
    /// may still inform the vector that there is space for a few more elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::LowMemoryThinVec;
    ///
    /// let mut vec = LowMemoryThinVec::with_capacity(10);
    /// vec.extend([1, 2, 3]);
    /// assert_eq!(vec.capacity(), 10);
    /// vec.shrink_to_fit();
    /// assert!(vec.capacity() >= 3);
    /// ```
    pub fn shrink_to_fit(&mut self) {
        let old_cap = self.capacity();
        let new_cap = self.len();
        if new_cap < old_cap {
            if new_cap == 0 {
                // No need to allocate memory for an empty vector.
                *self = LowMemoryThinVec::new();
            } else {
                // SAFETY:
                // `new_cap` *is* `len`.
                unsafe {
                    self.reallocate(new_cap);
                }
            }
        }
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all elements `e` such that `f(&e)` returns `false`.
    /// This method operates in place and preserves the order of the retained
    /// elements.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[macro_use] extern crate low_memory_thin_vec;
    /// # fn main() {
    /// let mut vec = low_memory_thin_vec![1, 2, 3, 4];
    /// vec.retain(|&x| x%2 == 0);
    /// assert_eq!(vec, [2, 4]);
    /// # }
    /// ```
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.retain_mut(|x| f(&*x));
    }

    /// Retains only the elements specified by the predicate, passing a mutable reference to it.
    ///
    /// In other words, remove all elements `e` such that `f(&mut e)` returns `false`.
    /// This method operates in place and preserves the order of the retained
    /// elements.
    ///
    /// # Examples
    ///
    /// ``rust
    /// # #[macro_use] extern crate low_memory_thin_vec;
    /// # fn main() {
    /// let mut vec = low_memory_thin_vec![1, 2, 3, 4, 5];
    /// vec.retain_mut(|x| {
    ///     *x += 1;
    ///     (*x)%2 == 0
    /// });
    /// assert_eq!(vec, [2, 4, 6]);
    /// # }
    /// ```
    pub fn retain_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut T) -> bool,
    {
        let len = self.len();
        let mut del = 0;
        {
            let v = self.as_mut_slice();

            for i in 0..len {
                if !f(&mut v[i]) {
                    del += 1;
                } else if del > 0 {
                    v.swap(i - del, i);
                }
            }
        }
        if del > 0 {
            self.truncate(len - del);
        }
    }

    /// Splits the collection into two at the given index.
    ///
    /// Returns a newly allocated vector containing the elements in the range
    /// `[at, len)`. After the call, the original vector will be left containing
    /// the elements `[0, at)` with its previous capacity unchanged.
    ///
    /// # Panics
    ///
    /// Panics if `at > len`.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1, 2, 3];
    /// let vec2 = vec.split_off(1);
    /// assert_eq!(vec, [1]);
    /// assert_eq!(vec2, [2, 3]);
    /// ```
    pub fn split_off(&mut self, at: usize) -> LowMemoryThinVec<T> {
        let old_len = self.len();
        let remaining_vec_len = old_len - at;

        assert!(at <= old_len, "Index out of bounds");

        let mut remaining_vec = LowMemoryThinVec::with_capacity(remaining_vec_len);

        // SAFETY:
        // The pointer is within the allocated memory since `at` is in bounds.
        let at_ptr = unsafe { self.data_raw().add(at) };
        // SAFETY:
        // - The source buffer and the destination buffer don't overlap because
        //   they belong to distinct non-overlapping allocations.
        // - The destination buffer is valid for writes of `remaining_vec_len` elements
        //   since it was just allocated with capacity `remaining_vec_len`.
        // - The source buffer is valid for reads of `remaining_vec_len` elements
        //   since `at + remaining_vec_len` matches its length.
        unsafe {
            ptr::copy_nonoverlapping(at_ptr, remaining_vec.data_raw(), remaining_vec_len);
        }

        // SAFETY:
        // - The new length matches the capacity of the vector.
        // - The first `remaining_vec_len` elements have been initialized
        //   by the copy operation above.
        unsafe {
            remaining_vec.set_len(remaining_vec_len); // could be the singleton if `at` is `old_len`.
        }

        // SAFETY:
        // - The new length is smaller than the previous length, so it's within capacity.
        // - The first `at` elements were initialized when this function was called, since
        //   `at` is within bounds.
        unsafe {
            self.set_len(at); // could be the singleton if `at` is `0` and `len` is `0`.
        }

        remaining_vec
    }

    /// Resize the buffer and update its capacity, without changing the length.
    ///
    /// # Safety
    ///
    /// You must ensure that the new capacity is greater than the current length.
    unsafe fn reallocate(&mut self, new_cap: usize) {
        debug_assert!(new_cap > 0);
        debug_assert!(
            new_cap >= self.len(),
            "New capacity is smaller than the current length"
        );
        if self.has_allocated() {
            let old_cap = self.capacity();
            let new_layout = allocation_layout::<T>(new_cap);
            // SAFETY:
            // - `self.ptr` was allocated via the same global allocator.
            // - We're using the correct layout for the old capacity.
            // - The new size doesn't exceed `isize::MAX`, since
            //   `allocation_size` would panic in that case.
            // - The size is not zero since `new_cap` is greater than zero
            //   and we always allocate a header, even for zero-sized types.
            let ptr = unsafe {
                realloc(
                    self.ptr.as_ptr() as *mut u8,
                    allocation_layout::<T>(old_cap),
                    new_layout.size(),
                ) as *mut Header
            };

            let Some(ptr) = NonNull::new(ptr) else {
                handle_alloc_error(new_layout)
            };
            self.ptr = ptr;
            // SAFETY:
            // - We're not in the singleton case.
            // - The new capacity matches the size of the allocation
            //   that backs the new pointer.
            unsafe {
                self.header_mut().set_capacity(new_cap);
            }
        } else {
            let new_header = allocate_for_capacity::<T>(new_cap);

            self.ptr = new_header;
        }
    }

    #[inline]
    fn is_singleton(&self) -> bool {
        self.ptr.as_ptr() as *const Header == &EMPTY_HEADER
    }
}

impl<T: Clone> LowMemoryThinVec<T> {
    /// Resizes the `Vec` in-place so that `len()` is equal to `new_len`.
    ///
    /// If `new_len` is greater than `len()`, the `Vec` is extended by the
    /// difference, with each additional slot filled with `value`.
    /// If `new_len` is less than `len()`, the `Vec` is simply truncated.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[macro_use] extern crate low_memory_thin_vec;
    /// # fn main() {
    /// let mut vec = low_memory_thin_vec!["hello"];
    /// vec.resize(3, "world");
    /// assert_eq!(vec, ["hello", "world", "world"]);
    ///
    /// let mut vec = low_memory_thin_vec![1, 2, 3, 4];
    /// vec.resize(2, 0);
    /// assert_eq!(vec, [1, 2]);
    /// # }
    /// ```
    pub fn resize(&mut self, new_len: usize, value: T) {
        let old_len = self.len();

        match new_len.cmp(&old_len) {
            Ordering::Less => {
                self.truncate(new_len);
            }
            Ordering::Greater => {
                let additional = new_len - old_len;
                self.reserve(additional);
                for _ in 1..additional {
                    self.push(value.clone());
                }
                // We can write the last element directly without cloning needlessly
                if additional > 0 {
                    self.push(value);
                }
            }
            Ordering::Equal => {}
        }
    }

    /// Creates a `LowMemoryThinVec` from a slice, cloning the elements.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use low_memory_thin_vec::LowMemoryThinVec;
    ///
    /// let vec = LowMemoryThinVec::from_slice(&[1, 2, 3]);
    /// assert_eq!(vec, [1, 2, 3]);
    /// ```
    pub fn from_slice(slice: &[T]) -> Self {
        let mut vec = LowMemoryThinVec::with_capacity(slice.len());
        vec.extend_from_slice(slice);
        vec
    }

    /// Clones and appends all elements in a slice to the `LowMemoryThinVec`.
    ///
    /// Iterates over the slice `other`, clones each element, and then appends
    /// it to this `LowMemoryThinVec`. The `other` slice is traversed in-order.
    ///
    /// Note that this function is same as [`extend`] except that it is
    /// specialized to work with slices instead. If and when Rust gets
    /// specialization this function will likely be deprecated (but still
    /// available).
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let mut vec = low_memory_thin_vec![1];
    /// vec.extend_from_slice(&[2, 3, 4]);
    /// assert_eq!(vec, [1, 2, 3, 4]);
    /// ```
    ///
    /// [`extend`]: LowMemoryThinVec::extend
    pub fn extend_from_slice(&mut self, other: &[T]) {
        self.extend(other.iter().cloned())
    }
}

impl<T: Copy + std::fmt::Debug> LowMemoryThinVec<T> {
    /// Reserves capacity to fit the given `prefix` slice,
    /// moves the current elements back to make room for the prefix
    /// and copies the prefix to the front of the vector.
    pub fn prepend_with_slice(&mut self, prefix: &[T]) {
        let prefix_len = prefix.len();
        let self_len = self.len();
        let new_len = prefix_len + self_len;
        debug_assert!(new_len <= isize::MAX as usize);

        if prefix.is_empty() {
            return;
        }

        if self.is_empty() {
            self.extend_from_slice(prefix);
            return;
        }

        self.reserve(prefix_len);
        {
            // SAFETY:
            // We reserved enough space for an additional `prefix_len`
            // amount of elements.
            let dst = unsafe { self.data_raw().add(prefix_len) };
            // SAFETY:
            // We have reserved enough space for `prefix_len` elements,
            // so it is safe to copy the elements from the current vector to the
            // newly reserved space.
            unsafe { std::ptr::copy(self.data_raw(), dst, self_len) };
        }

        // SAFETY:
        // `prefix` cannot be a reference to a slice in `self`,
        // as that would violate borrowing rules. Furthermore, `T` is bound to `Copy`
        // and therefore can simply be copied byte-for-byte and does not implement `Drop`.
        // Therefore, we can safely memcpy the elements from `prefix` to start of the buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(prefix.as_ptr(), self.data_raw(), prefix_len);
        }
        // SAFETY:
        // We have reserved enough space for `prefix_len` elements,
        // and all elements have been initialized.
        unsafe { self.set_len(new_len) };
    }
}

impl<T> Drop for LowMemoryThinVec<T> {
    #[inline]
    fn drop(&mut self) {
        if !self.is_singleton() {
            // First deallocate all elements, since they may
            // need own other resources that must be freed.
            // If we don't do this first, we may leak memory.
            // SAFETY:
            // - The pointer is valid and unaliased, since it comes from a `&mut` reference.
            // - We're inside a `Drop` implementation.
            unsafe {
                ptr::drop_in_place(self.as_mut_slice());
            }

            // SAFETY:
            // - The pointer was allocated via the same global allocator.
            // - The layout we used to allocate the pointer matches the layout
            //   we're using to deallocate it.
            unsafe {
                dealloc(
                    self.ptr.as_ptr() as *mut u8,
                    allocation_layout::<T>(self.capacity()),
                )
            }
        }
    }
}

impl<T> Deref for LowMemoryThinVec<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> DerefMut for LowMemoryThinVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T> Borrow<[T]> for LowMemoryThinVec<T> {
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> BorrowMut<[T]> for LowMemoryThinVec<T> {
    fn borrow_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T> AsRef<[T]> for LowMemoryThinVec<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Extend<T> for LowMemoryThinVec<T> {
    #[inline]
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let hint = iter.size_hint().0;
        if hint > 0 {
            self.reserve(hint);
        }
        for x in iter {
            self.push(x);
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for LowMemoryThinVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_slice(), f)
    }
}

impl<T> Hash for LowMemoryThinVec<T>
where
    T: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_slice().hash(state);
    }
}

impl<T> PartialOrd for LowMemoryThinVec<T>
where
    T: PartialOrd,
{
    #[inline]
    fn partial_cmp(&self, other: &LowMemoryThinVec<T>) -> Option<Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}

impl<T> Ord for LowMemoryThinVec<T>
where
    T: Ord,
{
    #[inline]
    fn cmp(&self, other: &LowMemoryThinVec<T>) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<A, B> PartialEq<LowMemoryThinVec<B>> for LowMemoryThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &LowMemoryThinVec<B>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<A, B> PartialEq<Vec<B>> for LowMemoryThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &Vec<B>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<A, B> PartialEq<[B]> for LowMemoryThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &[B]) -> bool {
        self.as_slice() == other
    }
}

impl<'a, A, B> PartialEq<&'a [B]> for LowMemoryThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &&'a [B]) -> bool {
        &self.as_slice() == other
    }
}

impl<const N: usize, A, B> PartialEq<[B; N]> for LowMemoryThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &[B; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<'a, const N: usize, A, B> PartialEq<&'a [B; N]> for LowMemoryThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &&'a [B; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T> Eq for LowMemoryThinVec<T> where T: Eq {}

impl<T> IntoIterator for LowMemoryThinVec<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            vec: self,
            start: 0,
        }
    }
}

impl<'a, T> IntoIterator for &'a LowMemoryThinVec<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> slice::Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut LowMemoryThinVec<T> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;

    fn into_iter(self) -> slice::IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T> Clone for LowMemoryThinVec<T>
where
    T: Clone,
{
    #[inline]
    fn clone(&self) -> LowMemoryThinVec<T> {
        #[cold]
        #[inline(never)]
        fn clone_non_singleton<T: Clone>(this: &LowMemoryThinVec<T>) -> LowMemoryThinVec<T> {
            LowMemoryThinVec::<T>::from(this.as_slice())
        }

        if self.is_singleton() {
            LowMemoryThinVec::new()
        } else {
            clone_non_singleton(self)
        }
    }
}

impl<T> Default for LowMemoryThinVec<T> {
    fn default() -> LowMemoryThinVec<T> {
        LowMemoryThinVec::new()
    }
}

impl<T> FromIterator<T> for LowMemoryThinVec<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> LowMemoryThinVec<T> {
        let mut vec = LowMemoryThinVec::new();
        vec.extend(iter);
        vec
    }
}

impl<T: Clone> From<&[T]> for LowMemoryThinVec<T> {
    /// Allocate a `LowMemoryThinVec<T>` and fill it by cloning `s`'s items.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    ///
    /// assert_eq!(LowMemoryThinVec::from(&[1, 2, 3][..]), low_memory_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: &[T]) -> LowMemoryThinVec<T> {
        s.iter().cloned().collect()
    }
}

impl<T: Clone> From<&mut [T]> for LowMemoryThinVec<T> {
    /// Allocate a `LowMemoryThinVec<T>` and fill it by cloning `s`'s items.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    ///
    /// assert_eq!(LowMemoryThinVec::from(&mut [1, 2, 3][..]), low_memory_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: &mut [T]) -> LowMemoryThinVec<T> {
        s.iter().cloned().collect()
    }
}

impl<T, const N: usize> From<[T; N]> for LowMemoryThinVec<T> {
    /// Allocate a `LowMemoryThinVec<T>` and move `s`'s items into it.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    ///
    /// assert_eq!(LowMemoryThinVec::from([1, 2, 3]), low_memory_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: [T; N]) -> LowMemoryThinVec<T> {
        core::iter::IntoIterator::into_iter(s).collect()
    }
}

impl<T> From<Box<[T]>> for LowMemoryThinVec<T> {
    /// Convert a boxed slice into a vector by transferring ownership of
    /// the existing heap allocation.
    ///
    /// **NOTE:** unlike `std`, this must reallocate to change the layout!
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    ///
    /// let b: Box<[i32]> = low_memory_thin_vec![1, 2, 3].into_iter().collect();
    /// assert_eq!(LowMemoryThinVec::from(b), low_memory_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: Box<[T]>) -> Self {
        // Can just lean on the fact that `Box<[T]>` -> `Vec<T>` is Free.
        Vec::from(s).into_iter().collect()
    }
}

impl<T> From<Vec<T>> for LowMemoryThinVec<T> {
    /// Convert a `std::Vec` into a `LowMemoryThinVec`.
    ///
    /// **NOTE:** this must reallocate to change the layout!
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    ///
    /// let b: Vec<i32> = vec![1, 2, 3];
    /// assert_eq!(LowMemoryThinVec::from(b), low_memory_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: Vec<T>) -> Self {
        s.into_iter().collect()
    }
}

impl<T> From<LowMemoryThinVec<T>> for Vec<T> {
    /// Convert a `LowMemoryThinVec` into a `std::Vec`.
    ///
    /// **NOTE:** this must reallocate to change the layout!
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    ///
    /// let b: LowMemoryThinVec<i32> = low_memory_thin_vec![1, 2, 3];
    /// assert_eq!(Vec::from(b), vec![1, 2, 3]);
    /// ```
    fn from(s: LowMemoryThinVec<T>) -> Self {
        s.into_iter().collect()
    }
}

impl<T> From<LowMemoryThinVec<T>> for Box<[T]> {
    /// Convert a vector into a boxed slice.
    ///
    /// If `v` has excess capacity, its items will be moved into a
    /// newly-allocated buffer with exactly the right capacity.
    ///
    /// **NOTE:** unlike `std`, this must reallocate to change the layout!
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    /// assert_eq!(Box::from(low_memory_thin_vec![1, 2, 3]), low_memory_thin_vec![1, 2, 3].into_iter().collect());
    /// ```
    fn from(v: LowMemoryThinVec<T>) -> Self {
        v.into_iter().collect()
    }
}

impl From<&str> for LowMemoryThinVec<u8> {
    /// Allocate a `LowMemoryThinVec<u8>` and fill it with a UTF-8 string.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    ///
    /// assert_eq!(LowMemoryThinVec::from("123"), low_memory_thin_vec![b'1', b'2', b'3']);
    /// ```
    fn from(s: &str) -> LowMemoryThinVec<u8> {
        From::from(s.as_bytes())
    }
}

impl<T, const N: usize> TryFrom<LowMemoryThinVec<T>> for [T; N] {
    type Error = LowMemoryThinVec<T>;

    /// Gets the entire contents of the `LowMemoryThinVec<T>` as an array,
    /// if its size exactly matches that of the requested array.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    /// use std::convert::TryInto;
    ///
    /// assert_eq!(low_memory_thin_vec![1, 2, 3].try_into(), Ok([1, 2, 3]));
    /// assert_eq!(<LowMemoryThinVec<i32>>::new().try_into(), Ok([]));
    /// ```
    ///
    /// If the length doesn't match, the input comes back in `Err`:
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    /// use std::convert::TryInto;
    ///
    /// let r: Result<[i32; 4], _> = (0..10).collect::<LowMemoryThinVec<_>>().try_into();
    /// assert_eq!(r, Err(low_memory_thin_vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));
    /// ```
    ///
    /// If you're fine with just getting a prefix of the `LowMemoryThinVec<T>`,
    /// you can call [`.truncate(N)`](LowMemoryThinVec::truncate) first.
    /// ```
    /// use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
    /// use std::convert::TryInto;
    ///
    /// let mut v = LowMemoryThinVec::from("hello world");
    /// v.sort();
    /// v.truncate(2);
    /// let [a, b]: [_; 2] = v.try_into().unwrap();
    /// assert_eq!(a, b' ');
    /// assert_eq!(b, b'd');
    /// ```
    fn try_from(mut vec: LowMemoryThinVec<T>) -> Result<[T; N], LowMemoryThinVec<T>> {
        if vec.len() != N {
            return Err(vec);
        }

        // SAFETY: `.set_len(0)` is always sound.
        unsafe { vec.set_len(0) };

        // SAFETY: A `LowMemoryThinVec`'s pointer is always aligned properly, and
        // the alignment the array needs is the same as the items.
        // We checked earlier that we have sufficient items.
        // The items will not double-drop as the `set_len`
        // tells the `LowMemoryThinVec` not to also drop them.
        let array = unsafe { ptr::read(vec.data_raw() as *const [T; N]) };
        Ok(array)
    }
}

/// An iterator that moves out of a vector.
///
/// This `struct` is created by the [`LowMemoryThinVec::into_iter`][]
/// (provided by the [`IntoIterator`] trait).
///
/// # Example
///
/// ```
/// use low_memory_thin_vec::low_memory_thin_vec;
///
/// let v = low_memory_thin_vec![0, 1, 2];
/// let iter: low_memory_thin_vec::IntoIter<_> = v.into_iter();
/// ```
pub struct IntoIter<T> {
    vec: LowMemoryThinVec<T>,
    start: usize,
}

impl<T> IntoIter<T> {
    /// Returns the remaining items of this iterator as a slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let vec = low_memory_thin_vec!['a', 'b', 'c'];
    /// let mut into_iter = vec.into_iter();
    /// assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
    /// let _ = into_iter.next().unwrap();
    /// assert_eq!(into_iter.as_slice(), &['b', 'c']);
    /// ```
    pub fn as_slice(&self) -> &[T] {
        // SAFETY:
        // - The data pointer is always aligned and it's safe to offset by an
        //   index within the bounds of the vector, since it'll fall within the same
        //   allocation.
        //   `self.start` has been checked to be within bounds when the iterator was created/last advanced.
        let start_ptr = unsafe { self.vec.data_raw().add(self.start) };
        let n_remaining_elements = self.len();
        // SAFETY:
        // - The raw slice is valid for the lifetime of the iterator, since ownership of the
        //   vector has been transferred to the iterator.
        // - All the elements in the slice are initialized, since the range is within
        //   the bounds of the vector.
        // - There is no mutable aliasing of the slice, since this method takes a
        //   shared reference to `self`.
        unsafe { slice::from_raw_parts(start_ptr, n_remaining_elements) }
    }

    /// Returns the remaining items of this iterator as a mutable slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use low_memory_thin_vec::low_memory_thin_vec;
    ///
    /// let vec = low_memory_thin_vec!['a', 'b', 'c'];
    /// let mut into_iter = vec.into_iter();
    /// assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
    /// into_iter.as_mut_slice()[2] = 'z';
    /// assert_eq!(into_iter.next().unwrap(), 'a');
    /// assert_eq!(into_iter.next().unwrap(), 'b');
    /// assert_eq!(into_iter.next().unwrap(), 'z');
    /// ```
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY:
        // - The data pointer is always aligned and it's safe to offset by an
        //   index within the bounds of the vector, since it'll fall within the same
        //   allocation.
        //   `self.start` has been checked to be within bounds when the iterator was created/last advanced.
        let start_ptr = unsafe { self.vec.data_raw().add(self.start) };
        let n_remaining_elements = self.len();
        let raw_mut_slice: *mut [T] =
            ptr::slice_from_raw_parts_mut(start_ptr, n_remaining_elements);
        // SAFETY:
        // - The raw slice is valid for the lifetime of the iterator, since ownership of the
        //   vector has been transferred to the iterator.
        // - All the elements in the slice are initialized, since the range is within
        //   the bounds of the vector.
        // - We have exclusive access to the slice, since this method takes a
        //   mutable reference to `self`.
        unsafe { &mut *raw_mut_slice }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if self.start == self.vec.len() {
            None
        } else {
            let old_start = self.start;
            self.start += 1;
            // SAFETY:
            // - We're not in the singleton case, since the length is greater than one.
            // - The data pointer is always aligned and it's safe to offset by an
            //   index within the bounds of the vector, since it'll fall within the same
            //   allocation.
            //   `self.start` is guaranteed to be within bounds when the iterator is created.
            let ptr_next = unsafe { self.vec.data_raw().add(old_start) };
            // SAFETY:
            // - The pointer points to an initialized element.
            unsafe { Some(ptr::read(ptr_next)) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.vec.len() - self.start;
        (len, Some(len))
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        if self.start == self.vec.len() {
            None
        } else {
            self.vec.pop()
        }
    }
}

impl<T> ExactSizeIterator for IntoIter<T> {}

impl<T> core::iter::FusedIterator for IntoIter<T> {}

impl<T> Drop for IntoIter<T> {
    #[inline]
    fn drop(&mut self) {
        #[cold]
        #[inline(never)]
        fn drop_non_singleton<T>(this: &mut IntoIter<T>) {
            // We need to take ownership of the vector to avoid dropping its elements twice
            let mut vec = mem::take(&mut this.vec);
            // SAFETY:
            // - The pointer is valid because it was obtained from a valid slice.
            // - We're in the `Drop` implementation.
            unsafe {
                ptr::drop_in_place(&mut vec[this.start..]);
            }
            // We set the length to zero to avoid trying to drop the elements twice
            // when `vec` goes out of scope at the end of this function.
            //
            // SAFETY:
            // - We're not in the singleton case.
            // - It's always safe to set the length to zero.
            unsafe { vec.set_len_non_singleton(0) }
        }

        if !self.vec.is_singleton() {
            drop_non_singleton(self);
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for IntoIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IntoIter").field(&self.as_slice()).finish()
    }
}

impl<T> AsRef<[T]> for IntoIter<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: Clone> Clone for IntoIter<T> {
    #[allow(clippy::into_iter_on_ref)]
    fn clone(&self) -> Self {
        // Just create a new `LowMemoryThinVec` from the remaining elements and IntoIter it
        self.as_slice()
            .into_iter()
            .cloned()
            .collect::<LowMemoryThinVec<_>>()
            .into_iter()
    }
}

/// Write is implemented for `LowMemoryThinVec<u8>` by appending to the vector.
/// The vector will grow as needed.
/// This implementation is identical to the one for `Vec<u8>`.
impl std::io::Write for LowMemoryThinVec<u8> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Tests that rely on access to `LowMemoryThinVec`'s internals to
    //! perform their assertions.
    //!
    //! All other tests are located in the `tests` module, as they only
    //! access methods and fields that are exposed via the public API.
    use super::*;

    #[test]
    fn test_data_ptr_alignment() {
        let v = LowMemoryThinVec::<u16>::new();
        assert!(v.data_raw() as usize % 2 == 0);

        let v = LowMemoryThinVec::<u32>::new();
        assert!(v.data_raw() as usize % 4 == 0);

        let v = LowMemoryThinVec::<u64>::new();
        assert!(v.data_raw() as usize % 8 == 0);
    }

    #[test]
    fn test_header_data() {
        macro_rules! assert_aligned_head_ptr {
            ($typename:ty) => {{
                let v: LowMemoryThinVec<$typename> =
                    LowMemoryThinVec::with_capacity(1 /* ensure allocation */);
                let head_ptr: *mut $typename = v.data_raw();
                assert_eq!(
                    head_ptr as usize % core::mem::align_of::<$typename>(),
                    0,
                    "expected Header::data<{}> to be aligned",
                    stringify!($typename)
                );
            }};
        }

        const HEADER_SIZE: usize = core::mem::size_of::<Header>();
        assert_eq!(2 * core::mem::size_of::<u16>(), HEADER_SIZE);

        #[repr(C, align(128))]
        struct Funky<T>(T);
        assert_eq!(header_field_padding::<Funky<()>>(), 128 - HEADER_SIZE);
        assert_aligned_head_ptr!(Funky<()>);

        assert_eq!(header_field_padding::<Funky<u8>>(), 128 - HEADER_SIZE);
        assert_aligned_head_ptr!(Funky<u8>);

        assert_eq!(
            header_field_padding::<Funky<[(); 1024]>>(),
            128 - HEADER_SIZE
        );
        assert_aligned_head_ptr!(Funky<[(); 1024]>);

        assert_eq!(
            header_field_padding::<Funky<[*mut usize; 1024]>>(),
            128 - HEADER_SIZE
        );
        assert_aligned_head_ptr!(Funky<[*mut usize; 1024]>);
    }

    #[test]
    fn test_clone() {
        let v: LowMemoryThinVec<i32> = low_memory_thin_vec![];
        let w = low_memory_thin_vec![1, 2, 3];

        assert_eq!(v, v.clone());

        let z = w.clone();
        assert_eq!(w, z);
        // they should be disjoint in memory.
        assert!(w.as_ptr() != z.as_ptr())
    }

    #[test]
    fn test_overaligned_type() {
        #[repr(align(16))]
        struct Align16(#[allow(dead_code)] u8);

        let v = LowMemoryThinVec::<Align16>::new();
        assert!(v.data_raw() as usize % 16 == 0);
    }

    #[test]
    fn test_resize_same_length() {
        let mut v = low_memory_thin_vec![1, 2, 3];
        let old_ptr = v.as_ptr();
        v.resize(v.len(), 0);
        // No reallocation has taken place.
        assert_eq!(old_ptr, v.as_ptr());
    }

    #[test]
    fn test_prepend_with_slice() {
        let mut v = low_memory_thin_vec![4, 5, 6, 7];
        v.prepend_with_slice(&[1, 2, 3]);
        assert_eq!(v, [1, 2, 3, 4, 5, 6, 7]);

        v.prepend_with_slice(&[-1, 0]);
        assert_eq!(v, [-1, 0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
