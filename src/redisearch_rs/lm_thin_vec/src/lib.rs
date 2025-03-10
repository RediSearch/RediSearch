//! `LMThinVec` is exactly the same as `Vec`, except that it stores its `len` and `capacity` in the buffer
//! it allocates.
//!
//! # Memory layout
//!
//! The memory layout of a local variable of type `LMThinVec<u64>` looks as follows:
//!
//!```text
//!   Stack               |              Heap
//!   -----               |              ----
//!                       |
//!  +---------------+    |
//!  | ptr (8 bytes) | ------->  Header +------------+
//!  +---------------+    |             | 3 (len)    |
//!                       |             | 4 (cap)    |
//!                       |             | (padding)  |
//!                       |             +------------+
//!                       |      Data   | 12         |
//!                       |             | 151        |
//!                       |             | 2          |
//!                       |             | (unused)   |
//!                       |             +------------+
//! ```
//!
//! It's pointer-sized on the stack, compared to `Vec<T>` which has 3 pointer-sized fields:
//!
//! ```text
//!    Stack              |      Heap
//!    -----              |      ----
//!                       |
//!   +---------------+   |      +------------+
//!   | ptr (8 bytes) | -------> | 12         |
//!   | len (8 bytes) |   |      | 151        |
//!   | cap (8 bytes) |   |      | 2          |
//!   +---------------+   |      | (unused)   |
//!                       |      +------------+
//! ```
//!
//! # Memory footprint
//!
//! The memory footprint of LMThinVecs is lower than `Vec<T>` ; notably in cases where space is reserved for
//! a non-existence `LMThinVec<T>`. So `Vec<LMThinVec<T>>` and `Option<LMThinVec<T>>::None` will waste less
//! space. Being pointer-sized also means it can be passed/stored in registers.
//!
//! Of course, any actually constructed `LMThinVec` will theoretically have a bigger allocation, but
//! the fuzzy nature of allocators means that might not actually be the case.
//!
//! Properties of `Vec` that are preserved:
//! * `LMThinVec::new()` doesn't allocate (it points to a statically allocated singleton)
//! * reallocation can be done in place
//! * `size_of::<LMThinVec<T>>()` == `size_of::<Option<LMThinVec<T>>>()`
//!
//! Properties of `Vec` that aren't preserved:
//! * `LMThinVec<T>` can't ever be zero-cost roundtripped to a `Box<[T]>`, `String`, or `*mut T`
//! * `from_raw_parts` doesn't exist
//! * `LMThinVec` currently doesn't bother to not-allocate for Zero Sized Types (e.g. `LMThinVec<()>`),
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

pub type SizeType = u16;
pub const MAX_CAP: usize = u16::MAX as usize;

#[inline(always)]
pub const fn assert_size(x: usize) -> SizeType {
    if x > MAX_CAP {
        panic!("LMThinVec size may not exceed the capacity of a 16-bit sized int");
    }
    x as SizeType
}

/// The header of a LMThinVec.
#[repr(C)]
struct Header {
    len: SizeType,
    cap: SizeType,
}

impl Header {
    /// Creates a new header with the given length and capacity.
    ///
    /// # Panics
    ///
    /// Panics if the length is greater than the capacity.
    const fn new(len: usize, cap: usize) -> Self {
        assert!(len <= cap, "Length must be less than or equal to capacity");
        Self {
            len: assert_size(len),
            cap: assert_size(cap),
        }
    }

    #[inline]
    const fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    const fn cap(&self) -> usize {
        self.cap as usize
    }

    #[inline]
    const fn set_cap(&mut self, cap: usize) {
        assert!(
            cap >= self.len as usize,
            "Capacity must be greater than or equal to the current length"
        );
        self.cap = assert_size(cap);
    }

    #[inline]
    const fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.cap as usize,
            "New length must be less than or equal to current capacity"
        );
        self.len = assert_size(len);
    }
}

/// Singleton that all empty collections share.
/// Note: can't store non-zero ZSTs, we allocate in that case. We could
/// optimize everything to not do that (basically, make ptr == len and branch
/// on size == 0 in every method), but it's a bunch of work for something that
/// doesn't matter much.
static EMPTY_HEADER: Header = Header::new(0, 0);

// Utils for computing layouts of allocations

/// Gets the layout of the allocated memory for a `LMThinVec<T>` with the given capacity.
///
/// # Panics
///
/// This will panic if the layout size exceeds [`isize::MAX`] at any point.
const fn layout<T>(cap: usize) -> Layout {
    const fn _layout<T>(cap: usize) -> Result<Layout, LayoutError> {
        let mut vec = Layout::new::<Header>();
        let elements = match Layout::array::<T>(cap) {
            Ok(elements) => elements,
            Err(err) => return Err(err),
        };
        vec = match vec.extend(elements) {
            Ok((layout, _)) => layout,
            Err(err) => return Err(err),
        };
        vec = vec.pad_to_align();
        Ok(vec)
    }

    match _layout::<T>(cap) {
        Ok(layout) => layout,
        Err(_) => panic!("capacity overflow"),
    }
}

/// Gets the align necessary to allocate a `LMThinVec<T>`
const fn alloc_align<T>() -> usize {
    layout::<T>(1).align()
}

/// Gets the size necessary to allocate a `LMThinVec<T>` with the given capacity.
///
/// # Panics
///
/// This will panic if isize::MAX is overflowed at any point.
const fn alloc_size<T>(cap: usize) -> usize {
    layout::<T>(cap).size()
}

/// Gets the padding necessary for the array of a `LMThinVec<T>`
const fn padding<T>() -> usize {
    let alloc_align = alloc_align::<T>();
    let header_size = mem::size_of::<Header>();
    alloc_align.saturating_sub(header_size)
}

/// Allocates a header (and array) for a `LMThinVec<T>` with the given capacity.
///
/// # Panics
///
/// Panics if the required size overflows `isize::MAX`.
fn header_with_capacity<T>(cap: usize) -> NonNull<Header> {
    debug_assert!(cap > 0);
    let layout = layout::<T>(cap);
    let header = {
        debug_assert!(layout.size() > 0);
        // SAFETY:
        // `layout.size()` is greater than zero, since `cap` is greater than zero
        // and we always allocate a header, even if `T` is a zero-sized type.
        unsafe { alloc(layout) as *mut Header }
    };

    if header.is_null() {
        handle_alloc_error(layout)
    } else {
        // SAFETY:
        // Allocation succeeded, so `header` is not a null pointer.
        let header = unsafe { NonNull::new_unchecked(header) };

        // Infinite capacity for zero-sized types
        let capacity = if mem::size_of::<T>() == 0 {
            MAX_CAP
        } else {
            cap
        };

        // Initialize the allocated buffer with a valid header value.
        //
        // SAFETY:
        // - The destination and the value are properly aligned,
        //   since the allocation was performed against a layout
        //   that begins with a header field.
        unsafe {
            header.write(Header::new(0, capacity));
        }
        header
    }
}

/// See the crate's top level documentation for a description of this type.
#[repr(C)]
pub struct LMThinVec<T> {
    ptr: NonNull<Header>,
    _phantom: PhantomData<T>,
}

// SAFETY:
// No one besides us has access to the raw pointer, so we can safely transfer the
// `ThinVec` to another thread if T can be safely transferred.
unsafe impl<T: Send> Send for LMThinVec<T> {}

unsafe impl<T: Sync> Sync for LMThinVec<T> {}

/// Creates a `LMThinVec` containing the arguments.
///
/// ```rust
/// use lm_thin_vec::lm_thin_vec;
///
/// let v = lm_thin_vec![1, 2, 3];
/// assert_eq!(v.len(), 3);
/// assert_eq!(v[0], 1);
/// assert_eq!(v[1], 2);
/// assert_eq!(v[2], 3);
///
/// let v = lm_thin_vec![1; 3];
/// assert_eq!(v, [1, 1, 1]);
/// ```
#[macro_export]
macro_rules! lm_thin_vec {
    (@UNIT $($t:tt)*) => (());

    ($elem:expr; $n:expr) => ({
        let mut vec = $crate::LMThinVec::new();
        vec.resize($n, $elem);
        vec
    });
    () => {$crate::LMThinVec::new()};
    ($($x:expr),*) => ({
        let len = [$(lm_thin_vec!(@UNIT $x)),*].len();
        let mut vec = $crate::LMThinVec::with_capacity(len);
        $(vec.push($x);)*
        vec
    });
    ($($x:expr,)*) => (lm_thin_vec![$($x),*]);
}

impl<T> LMThinVec<T> {
    /// Creates a new empty LMThinVec.
    ///
    /// This will not allocate.
    pub fn new() -> LMThinVec<T> {
        LMThinVec::with_capacity(0)
    }

    /// Constructs a new, empty `LMThinVec<T>` with at least the specified capacity.
    ///
    /// The vector will be able to hold at least `capacity` elements without
    /// reallocating. This method is allowed to allocate for more elements than
    /// `capacity`. If `capacity` is 0, the vector will not allocate.
    ///
    /// It is important to note that although the returned vector has the
    /// minimum *capacity* specified, the vector will have a zero *length*.
    ///
    /// If it is important to know the exact allocated capacity of a `LMThinVec`,
    /// always use the [`capacity`] method after construction.
    ///
    /// **NOTE**: unlike `Vec`, `LMThinVec` **MUST** allocate once to keep track of non-zero
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
    /// use lm_thin_vec::LMThinVec;
    ///
    /// let mut vec = LMThinVec::with_capacity(10);
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
    /// let vec_units = LMThinVec::<()>::with_capacity(10);
    /// ```
    pub fn with_capacity(cap: usize) -> LMThinVec<T> {
        let ptr = if cap == 0 {
            // # Safety
            //
            // This is safe because `EMPTY_HEADER` is a static variable that is never modified,
            // and always initialized with a valid `Header`.
            unsafe { NonNull::new_unchecked(&EMPTY_HEADER as *const Header as *mut Header) }
        } else {
            header_with_capacity::<T>(cap)
        };
        LMThinVec {
            ptr,
            _phantom: PhantomData,
        }
    }

    // Accessor conveniences

    fn ptr(&self) -> *mut Header {
        self.ptr.as_ptr()
    }
    fn header(&self) -> &Header {
        // # Safety
        //
        // The header pointer is always valid because it is either a static reference to
        // [`EMPTY_HEADER`] or a pointer returned by `header_with_capacity`,
        // which takes care of initializing the allocated memory before returning.
        unsafe { self.ptr.as_ref() }
    }
    /// Return a pointer to the data array located after the header.
    fn data_raw(&self) -> *mut T {
        let padding = padding::<T>();

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
        let empty_header_is_aligned =
            // If the Header is at
            // least as aligned as T *and* the padding would have
            // been 0, then one-past-the-end of the empty singleton
            // *is* a valid data pointer and we can remove the
            // `dangling` special case.
            mem::align_of::<Header>() >= mem::align_of::<T>() && padding == 0;

        unsafe {
            if !empty_header_is_aligned && self.header().cap() == 0 {
                NonNull::dangling().as_ptr()
            } else {
                // This could technically result in overflow, but padding
                // would have to be absurdly large for this to occur.
                let header_size = mem::size_of::<Header>();
                let ptr = self.ptr.as_ptr() as *mut u8;
                ptr.add(header_size + padding) as *mut T
            }
        }
    }

    /// # Safety
    ///
    /// The header pointer must not point to [`EMPTY_HEADER`].
    unsafe fn header_mut(&mut self) -> &mut Header {
        // # Safety
        //
        // We meet all conditions laid out in: <https://doc.rust-lang.org/std/ptr/index.html#pointer-to-reference-conversion>
        // Pointer is not null, aligned and points to a fully initialized Header,
        // therefore it is safe to dereference.
        // We also know there's no other references to the header, as we require
        // the call to pass `&mut self` as input.
        unsafe { self.ptr.as_mut() }
    }

    /// Returns the number of elements in the vector, also referred to
    /// as its 'length'.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let a = lm_thin_vec![1, 2, 3];
    /// assert_eq!(a.len(), 3);
    /// ```
    pub fn len(&self) -> usize {
        self.header().len()
    }

    /// Returns `true` if the vector contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::LMThinVec;
    ///
    /// let mut v = LMThinVec::new();
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
    /// use lm_thin_vec::LMThinVec;
    ///
    /// let vec: LMThinVec<i32> = LMThinVec::with_capacity(10);
    /// assert_eq!(vec.capacity(), 10);
    /// ```
    pub fn capacity(&self) -> usize {
        self.header().cap()
    }

    /// Returns `true` if the vector has the capacity to hold any element.
    pub fn has_capacity(&self) -> bool {
        !self.is_singleton()
    }

    /// Forces the length of the vector to `new_len`.
    ///
    /// This is a low-level operation that maintains none of the normal
    /// invariants of the type. Normally changing the length of a vector
    /// is done using one of the safe operations instead, such as
    /// [`truncate`], [`resize`], [`extend`], or [`clear`].
    ///
    /// [`truncate`]: LMThinVec::truncate
    /// [`resize`]: LMThinVec::resize
    /// [`extend`]: LMThinVec::extend
    /// [`clear`]: LMThinVec::clear
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`capacity()`].
    /// - The elements at `old_len..new_len` must be initialized.
    ///
    /// [`capacity()`]: LMThinVec::capacity
    ///
    /// # Examples
    ///
    /// This method can be useful for situations in which the vector
    /// is serving as a buffer for other code, particularly over FFI:
    ///
    /// ```no_run
    /// use lm_thin_vec::LMThinVec;
    ///
    /// # // This is just a minimal skeleton for the doc example;
    /// # // don't use this as a starting point for a real library.
    /// # pub struct StreamWrapper { strm: *mut std::ffi::c_void }
    /// # const Z_OK: i32 = 0;
    /// # extern "C" {
    /// #     fn deflateGetDictionary(
    /// #         strm: *mut std::ffi::c_void,
    /// #         dictionary: *mut u8,
    /// #         dictLength: *mut usize,
    /// #     ) -> i32;
    /// # }
    /// # impl StreamWrapper {
    /// pub fn get_dictionary(&self) -> Option<LMThinVec<u8>> {
    ///     // Per the FFI method's docs, "32768 bytes is always enough".
    ///     let mut dict = LMThinVec::with_capacity(32_768);
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
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![lm_thin_vec![1, 0, 0],
    ///                    lm_thin_vec![0, 1, 0],
    ///                    lm_thin_vec![0, 0, 1]];
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
            debug_assert!(len == 0, "invalid set_len({}) on empty LMThinVec", len);
        } else {
            // # Safety
            //
            // We're not in the singleton case.
            unsafe { self.set_len_non_singleton(len) }
        }
    }

    // For internal use only.
    //
    // # Safety
    //
    // It must be known that the header doesn't point to the [`EMPTY_HEADER`] singleton.
    unsafe fn set_len_non_singleton(&mut self, len: usize) {
        // # Safety
        //
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
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1, 2];
    /// vec.push(3);
    /// assert_eq!(vec, [1, 2, 3]);
    /// ```
    pub fn push(&mut self, val: T) {
        let old_len = self.len();
        if old_len == self.capacity() {
            self.reserve(1);
        }
        // # Safety
        //
        // - We know we're not in the singleton case thanks to the `reserve` call above.
        // - We know that the pointer is valid for writes since the current capacity is
        //   at least `old_len + 1`.
        unsafe {
            ptr::write(self.data_raw().add(old_len), val);
        }

        // # Safety
        //
        // We know we're not in the singleton case
        // thanks to the `reserve` call above.
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
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1, 2, 3];
    /// assert_eq!(vec.pop(), Some(3));
    /// assert_eq!(vec, [1, 2]);
    /// ```
    pub fn pop(&mut self) -> Option<T> {
        let old_len = self.len();
        if old_len == 0 {
            return None;
        }

        // # Safety
        //
        // We know we're not in the singleton case
        // since `old_len > 0`.
        unsafe {
            self.set_len_non_singleton(old_len - 1);
        }
        // # Safety
        //
        // The value we're reading is initialized.
        unsafe { Some(ptr::read(self.data_raw().add(old_len - 1))) }
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
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1, 2, 3];
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
        let ptr = self.data_raw();
        // # Safety
        //
        // We know that we have enough capacity to shift everything
        // to the right by 1 thanks to `reserve`.
        unsafe {
            ptr::copy(ptr.add(idx), ptr.add(idx + 1), old_len - idx);
        }
        // # Safety
        //
        // The pointer is in bounds since `idx` is smaller than `old_len`.
        unsafe {
            ptr::write(ptr.add(idx), elem);
        }
        // # Safety
        //
        // We know that we're not in the singleton case thanks to `reserve`.
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
    /// elements from the beginning of the `LMThinVec`, consider using `std::collections::VecDeque`.
    ///
    /// [`swap_remove`]: LMThinVec::swap_remove
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut v = lm_thin_vec![1, 2, 3];
    /// assert_eq!(v.remove(1), 2);
    /// assert_eq!(v, [1, 3]);
    /// ```
    pub fn remove(&mut self, idx: usize) -> T {
        let old_len = self.len();

        assert!(idx < old_len, "Index out of bounds");

        unsafe {
            self.set_len_non_singleton(old_len - 1);
            let ptr = self.data_raw();
            let val = ptr::read(self.data_raw().add(idx));
            ptr::copy(ptr.add(idx + 1), ptr.add(idx), old_len - idx - 1);
            val
        }
    }

    /// Removes an element from the vector and returns it.
    ///
    /// The removed element is replaced by the last element of the vector.
    ///
    /// This does not preserve ordering, but is *O*(1).
    /// If you need to preserve the element order, use [`remove`] instead.
    ///
    /// [`remove`]: LMThinVec::remove
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut v = lm_thin_vec!["foo", "bar", "baz", "qux"];
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

        unsafe {
            let ptr = self.data_raw();
            ptr::swap(ptr.add(idx), ptr.add(old_len - 1));
            self.set_len_non_singleton(old_len - 1);
            ptr::read(ptr.add(old_len - 1))
        }
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
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1, 2, 3, 4, 5];
    /// vec.truncate(2);
    /// assert_eq!(vec, [1, 2]);
    /// ```
    ///
    /// No truncation occurs when `len` is greater than the vector's current
    /// length:
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1, 2, 3];
    /// vec.truncate(8);
    /// assert_eq!(vec, [1, 2, 3]);
    /// ```
    ///
    /// Truncating when `len == 0` is equivalent to calling the [`clear`]
    /// method.
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1, 2, 3];
    /// vec.truncate(0);
    /// assert_eq!(vec, []);
    /// ```
    ///
    /// [`clear`]: LMThinVec::clear
    /// [`drain`]: LMThinVec::drain
    pub fn truncate(&mut self, len: usize) {
        unsafe {
            // drop any extra elements
            while len < self.len() {
                // decrement len before the drop_in_place(), so a panic on Drop
                // doesn't re-drop the just-failed value.
                let new_len = self.len() - 1;
                self.set_len_non_singleton(new_len);
                ptr::drop_in_place(self.data_raw().add(new_len));
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
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut v = lm_thin_vec![1, 2, 3];
    /// v.clear();
    /// assert!(v.is_empty());
    /// ```
    pub fn clear(&mut self) {
        unsafe {
            ptr::drop_in_place(&mut self[..]);
            self.set_len(0); // could be the singleton
        }
    }

    /// Extracts a slice containing the entire vector.
    ///
    /// Equivalent to `&s[..]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    /// use std::io::{self, Write};
    /// let buffer = lm_thin_vec![1, 2, 3, 5, 8];
    /// io::sink().write(buffer.as_slice()).unwrap();
    /// ```
    pub fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.data_raw(), self.len()) }
    }

    /// Extracts a mutable slice of the entire vector.
    ///
    /// Equivalent to `&mut s[..]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    /// use std::io::{self, Read};
    /// let mut buffer = vec![0; 3];
    /// io::repeat(0b101).read_exact(buffer.as_mut_slice()).unwrap();
    /// ```
    pub fn as_mut_slice(&mut self) -> &mut [T] {
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
            // skip to 4 because tiny ThinVecs are dumb; but not if that would cause overflow
            if mem::size_of::<T>() > (!0) / 8 { 1 } else { 4 }
        } else {
            old_cap.saturating_mul(2)
        };
        let new_cap = max(min_cap, double_cap);
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
    /// use lm_thin_vec::LMThinVec;
    ///
    /// let mut vec = LMThinVec::with_capacity(10);
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
                *self = LMThinVec::new();
            } else {
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
    /// # #[macro_use] extern crate lm_thin_vec;
    /// # fn main() {
    /// let mut vec = lm_thin_vec![1, 2, 3, 4];
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
    /// # #[macro_use] extern crate lm_thin_vec;
    /// # fn main() {
    /// let mut vec = lm_thin_vec![1, 2, 3, 4, 5];
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
            let v = &mut self[..];

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
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1, 2, 3];
    /// let vec2 = vec.split_off(1);
    /// assert_eq!(vec, [1]);
    /// assert_eq!(vec2, [2, 3]);
    /// ```
    pub fn split_off(&mut self, at: usize) -> LMThinVec<T> {
        let old_len = self.len();
        let new_vec_len = old_len - at;

        assert!(at <= old_len, "Index out of bounds");

        unsafe {
            let mut new_vec = LMThinVec::with_capacity(new_vec_len);

            ptr::copy_nonoverlapping(self.data_raw().add(at), new_vec.data_raw(), new_vec_len);

            new_vec.set_len(new_vec_len); // could be the singleton
            self.set_len(at); // could be the singleton

            new_vec
        }
    }

    /// Resize the buffer and update its capacity, without changing the length.
    ///
    /// # Safety
    ///
    /// You must ensure that the new capacity is greater than the current length.
    unsafe fn reallocate(&mut self, new_cap: usize) {
        debug_assert!(new_cap > 0);
        if self.has_allocation() {
            let old_cap = self.capacity();
            unsafe {
                let ptr = realloc(
                    self.ptr() as *mut u8,
                    layout::<T>(old_cap),
                    alloc_size::<T>(new_cap),
                ) as *mut Header;

                if ptr.is_null() {
                    handle_alloc_error(layout::<T>(new_cap))
                }
                (*ptr).set_cap(new_cap);
                self.ptr = NonNull::new_unchecked(ptr);
            }
        } else {
            let new_header = header_with_capacity::<T>(new_cap);

            self.ptr = new_header;
        }
    }

    #[inline]
    fn is_singleton(&self) -> bool {
        self.ptr.as_ptr() as *const Header == &EMPTY_HEADER
    }

    #[inline]
    fn has_allocation(&self) -> bool {
        !self.is_singleton()
    }
}

impl<T: Clone> LMThinVec<T> {
    /// Resizes the `Vec` in-place so that `len()` is equal to `new_len`.
    ///
    /// If `new_len` is greater than `len()`, the `Vec` is extended by the
    /// difference, with each additional slot filled with `value`.
    /// If `new_len` is less than `len()`, the `Vec` is simply truncated.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[macro_use] extern crate lm_thin_vec;
    /// # fn main() {
    /// let mut vec = lm_thin_vec!["hello"];
    /// vec.resize(3, "world");
    /// assert_eq!(vec, ["hello", "world", "world"]);
    ///
    /// let mut vec = lm_thin_vec![1, 2, 3, 4];
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

    /// Clones and appends all elements in a slice to the `LMThinVec`.
    ///
    /// Iterates over the slice `other`, clones each element, and then appends
    /// it to this `LMThinVec`. The `other` slice is traversed in-order.
    ///
    /// Note that this function is same as [`extend`] except that it is
    /// specialized to work with slices instead. If and when Rust gets
    /// specialization this function will likely be deprecated (but still
    /// available).
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let mut vec = lm_thin_vec![1];
    /// vec.extend_from_slice(&[2, 3, 4]);
    /// assert_eq!(vec, [1, 2, 3, 4]);
    /// ```
    ///
    /// [`extend`]: LMThinVec::extend
    pub fn extend_from_slice(&mut self, other: &[T]) {
        self.extend(other.iter().cloned())
    }
}

impl<T> Drop for LMThinVec<T> {
    #[inline]
    fn drop(&mut self) {
        #[cold]
        #[inline(never)]
        fn drop_non_singleton<T>(this: &mut LMThinVec<T>) {
            unsafe {
                ptr::drop_in_place(&mut this[..]);

                dealloc(this.ptr() as *mut u8, layout::<T>(this.capacity()))
            }
        }

        if !self.is_singleton() {
            drop_non_singleton(self);
        }
    }
}

impl<T> Deref for LMThinVec<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> DerefMut for LMThinVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T> Borrow<[T]> for LMThinVec<T> {
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> BorrowMut<[T]> for LMThinVec<T> {
    fn borrow_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T> AsRef<[T]> for LMThinVec<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Extend<T> for LMThinVec<T> {
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

impl<T: fmt::Debug> fmt::Debug for LMThinVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Hash for LMThinVec<T>
where
    T: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self[..].hash(state);
    }
}

impl<T> PartialOrd for LMThinVec<T>
where
    T: PartialOrd,
{
    #[inline]
    fn partial_cmp(&self, other: &LMThinVec<T>) -> Option<Ordering> {
        self[..].partial_cmp(&other[..])
    }
}

impl<T> Ord for LMThinVec<T>
where
    T: Ord,
{
    #[inline]
    fn cmp(&self, other: &LMThinVec<T>) -> Ordering {
        self[..].cmp(&other[..])
    }
}

impl<A, B> PartialEq<LMThinVec<B>> for LMThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &LMThinVec<B>) -> bool {
        self[..] == other[..]
    }
}

impl<A, B> PartialEq<Vec<B>> for LMThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &Vec<B>) -> bool {
        self[..] == other[..]
    }
}

impl<A, B> PartialEq<[B]> for LMThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &[B]) -> bool {
        self[..] == other[..]
    }
}

impl<'a, A, B> PartialEq<&'a [B]> for LMThinVec<A>
where
    A: PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &&'a [B]) -> bool {
        self[..] == other[..]
    }
}

macro_rules! array_impls {
    ($($N:expr)*) => {$(
        impl<A, B> PartialEq<[B; $N]> for LMThinVec<A> where A: PartialEq<B> {
            #[inline]
            fn eq(&self, other: &[B; $N]) -> bool { self[..] == other[..] }
        }

        impl<'a, A, B> PartialEq<&'a [B; $N]> for LMThinVec<A> where A: PartialEq<B> {
            #[inline]
            fn eq(&self, other: &&'a [B; $N]) -> bool { self[..] == other[..] }
        }
    )*}
}

array_impls! {
    0  1  2  3  4  5  6  7  8  9
    10 11 12 13 14 15 16 17 18 19
    20 21 22 23 24 25 26 27 28 29
    30 31 32
}

impl<T> Eq for LMThinVec<T> where T: Eq {}

impl<T> IntoIterator for LMThinVec<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            vec: self,
            start: 0,
        }
    }
}

impl<'a, T> IntoIterator for &'a LMThinVec<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> slice::Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut LMThinVec<T> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;

    fn into_iter(self) -> slice::IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T> Clone for LMThinVec<T>
where
    T: Clone,
{
    #[inline]
    fn clone(&self) -> LMThinVec<T> {
        #[cold]
        #[inline(never)]
        fn clone_non_singleton<T: Clone>(this: &LMThinVec<T>) -> LMThinVec<T> {
            let len = this.len();
            let mut new_vec = LMThinVec::<T>::with_capacity(len);
            let mut data_raw = new_vec.data_raw();
            for x in this.iter() {
                unsafe {
                    ptr::write(data_raw, x.clone());
                    data_raw = data_raw.add(1);
                }
            }
            unsafe {
                // `this` is not the singleton, but `new_vec` will be if
                // `this` is empty.
                new_vec.set_len(len); // could be the singleton
            }
            new_vec
        }

        if self.is_singleton() {
            LMThinVec::new()
        } else {
            clone_non_singleton(self)
        }
    }
}

impl<T> Default for LMThinVec<T> {
    fn default() -> LMThinVec<T> {
        LMThinVec::new()
    }
}

impl<T> FromIterator<T> for LMThinVec<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> LMThinVec<T> {
        let mut vec = LMThinVec::new();
        vec.extend(iter);
        vec
    }
}

impl<T: Clone> From<&[T]> for LMThinVec<T> {
    /// Allocate a `LMThinVec<T>` and fill it by cloning `s`'s items.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    ///
    /// assert_eq!(LMThinVec::from(&[1, 2, 3][..]), lm_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: &[T]) -> LMThinVec<T> {
        s.iter().cloned().collect()
    }
}

impl<T: Clone> From<&mut [T]> for LMThinVec<T> {
    /// Allocate a `LMThinVec<T>` and fill it by cloning `s`'s items.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    ///
    /// assert_eq!(LMThinVec::from(&mut [1, 2, 3][..]), lm_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: &mut [T]) -> LMThinVec<T> {
        s.iter().cloned().collect()
    }
}

impl<T, const N: usize> From<[T; N]> for LMThinVec<T> {
    /// Allocate a `LMThinVec<T>` and move `s`'s items into it.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    ///
    /// assert_eq!(LMThinVec::from([1, 2, 3]), lm_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: [T; N]) -> LMThinVec<T> {
        core::iter::IntoIterator::into_iter(s).collect()
    }
}

impl<T> From<Box<[T]>> for LMThinVec<T> {
    /// Convert a boxed slice into a vector by transferring ownership of
    /// the existing heap allocation.
    ///
    /// **NOTE:** unlike `std`, this must reallocate to change the layout!
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    ///
    /// let b: Box<[i32]> = lm_thin_vec![1, 2, 3].into_iter().collect();
    /// assert_eq!(LMThinVec::from(b), lm_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: Box<[T]>) -> Self {
        // Can just lean on the fact that `Box<[T]>` -> `Vec<T>` is Free.
        Vec::from(s).into_iter().collect()
    }
}

impl<T> From<Vec<T>> for LMThinVec<T> {
    /// Convert a `std::Vec` into a `LMThinVec`.
    ///
    /// **NOTE:** this must reallocate to change the layout!
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    ///
    /// let b: Vec<i32> = vec![1, 2, 3];
    /// assert_eq!(LMThinVec::from(b), lm_thin_vec![1, 2, 3]);
    /// ```
    fn from(s: Vec<T>) -> Self {
        s.into_iter().collect()
    }
}

impl<T> From<LMThinVec<T>> for Vec<T> {
    /// Convert a `LMThinVec` into a `std::Vec`.
    ///
    /// **NOTE:** this must reallocate to change the layout!
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    ///
    /// let b: LMThinVec<i32> = lm_thin_vec![1, 2, 3];
    /// assert_eq!(Vec::from(b), vec![1, 2, 3]);
    /// ```
    fn from(s: LMThinVec<T>) -> Self {
        s.into_iter().collect()
    }
}

impl<T> From<LMThinVec<T>> for Box<[T]> {
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
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    /// assert_eq!(Box::from(lm_thin_vec![1, 2, 3]), lm_thin_vec![1, 2, 3].into_iter().collect());
    /// ```
    fn from(v: LMThinVec<T>) -> Self {
        v.into_iter().collect()
    }
}

impl From<&str> for LMThinVec<u8> {
    /// Allocate a `LMThinVec<u8>` and fill it with a UTF-8 string.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    ///
    /// assert_eq!(LMThinVec::from("123"), lm_thin_vec![b'1', b'2', b'3']);
    /// ```
    fn from(s: &str) -> LMThinVec<u8> {
        From::from(s.as_bytes())
    }
}

impl<T, const N: usize> TryFrom<LMThinVec<T>> for [T; N] {
    type Error = LMThinVec<T>;

    /// Gets the entire contents of the `LMThinVec<T>` as an array,
    /// if its size exactly matches that of the requested array.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    /// use std::convert::TryInto;
    ///
    /// assert_eq!(lm_thin_vec![1, 2, 3].try_into(), Ok([1, 2, 3]));
    /// assert_eq!(<LMThinVec<i32>>::new().try_into(), Ok([]));
    /// ```
    ///
    /// If the length doesn't match, the input comes back in `Err`:
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    /// use std::convert::TryInto;
    ///
    /// let r: Result<[i32; 4], _> = (0..10).collect::<LMThinVec<_>>().try_into();
    /// assert_eq!(r, Err(lm_thin_vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));
    /// ```
    ///
    /// If you're fine with just getting a prefix of the `LMThinVec<T>`,
    /// you can call [`.truncate(N)`](LMThinVec::truncate) first.
    /// ```
    /// use lm_thin_vec::{LMThinVec, lm_thin_vec};
    /// use std::convert::TryInto;
    ///
    /// let mut v = LMThinVec::from("hello world");
    /// v.sort();
    /// v.truncate(2);
    /// let [a, b]: [_; 2] = v.try_into().unwrap();
    /// assert_eq!(a, b' ');
    /// assert_eq!(b, b'd');
    /// ```
    fn try_from(mut vec: LMThinVec<T>) -> Result<[T; N], LMThinVec<T>> {
        if vec.len() != N {
            return Err(vec);
        }

        // SAFETY: `.set_len(0)` is always sound.
        unsafe { vec.set_len(0) };

        // SAFETY: A `LMThinVec`'s pointer is always aligned properly, and
        // the alignment the array needs is the same as the items.
        // We checked earlier that we have sufficient items.
        // The items will not double-drop as the `set_len`
        // tells the `LMThinVec` not to also drop them.
        let array = unsafe { ptr::read(vec.data_raw() as *const [T; N]) };
        Ok(array)
    }
}

/// An iterator that moves out of a vector.
///
/// This `struct` is created by the [`LMThinVec::into_iter`][]
/// (provided by the [`IntoIterator`] trait).
///
/// # Example
///
/// ```
/// use lm_thin_vec::lm_thin_vec;
///
/// let v = lm_thin_vec![0, 1, 2];
/// let iter: lm_thin_vec::IntoIter<_> = v.into_iter();
/// ```
pub struct IntoIter<T> {
    vec: LMThinVec<T>,
    start: usize,
}

impl<T> IntoIter<T> {
    /// Returns the remaining items of this iterator as a slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let vec = lm_thin_vec!['a', 'b', 'c'];
    /// let mut into_iter = vec.into_iter();
    /// assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
    /// let _ = into_iter.next().unwrap();
    /// assert_eq!(into_iter.as_slice(), &['b', 'c']);
    /// ```
    pub fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.vec.data_raw().add(self.start), self.len()) }
    }

    /// Returns the remaining items of this iterator as a mutable slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use lm_thin_vec::lm_thin_vec;
    ///
    /// let vec = lm_thin_vec!['a', 'b', 'c'];
    /// let mut into_iter = vec.into_iter();
    /// assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
    /// into_iter.as_mut_slice()[2] = 'z';
    /// assert_eq!(into_iter.next().unwrap(), 'a');
    /// assert_eq!(into_iter.next().unwrap(), 'b');
    /// assert_eq!(into_iter.next().unwrap(), 'z');
    /// ```
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { &mut *self.as_raw_mut_slice() }
    }

    fn as_raw_mut_slice(&mut self) -> *mut [T] {
        unsafe { ptr::slice_from_raw_parts_mut(self.vec.data_raw().add(self.start), self.len()) }
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
            let ptr_next = unsafe { self.vec.data_raw().add(old_start) };
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
            let mut vec = mem::replace(&mut this.vec, LMThinVec::new());
            unsafe {
                ptr::drop_in_place(&mut vec[this.start..]);
            }
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
        // Just create a new `LMThinVec` from the remaining elements and IntoIter it
        self.as_slice()
            .into_iter()
            .cloned()
            .collect::<LMThinVec<_>>()
            .into_iter()
    }
}

/// Write is implemented for `LMThinVec<u8>` by appending to the vector.
/// The vector will grow as needed.
/// This implementation is identical to the one for `Vec<u8>`.
impl std::io::Write for LMThinVec<u8> {
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

// TODO: a million Index impls

#[cfg(test)]
mod tests {
    use super::LMThinVec;
    use std::vec;

    #[test]
    fn test_size_of() {
        use core::mem::size_of;
        assert_eq!(size_of::<LMThinVec<u8>>(), size_of::<&u8>());

        assert_eq!(size_of::<Option<LMThinVec<u8>>>(), size_of::<&u8>());
    }

    #[test]
    fn test_drop_empty() {
        LMThinVec::<u8>::new();
    }

    #[test]
    fn test_data_ptr_alignment() {
        let v = LMThinVec::<u16>::new();
        assert!(v.data_raw() as usize % 2 == 0);

        let v = LMThinVec::<u32>::new();
        assert!(v.data_raw() as usize % 4 == 0);

        let v = LMThinVec::<u64>::new();
        assert!(v.data_raw() as usize % 8 == 0);
    }

    #[test]
    fn test_overaligned_type() {
        #[repr(align(16))]
        struct Align16(#[allow(dead_code)] u8);

        let v = LMThinVec::<Align16>::new();
        assert!(v.data_raw() as usize % 16 == 0);
    }

    #[test]
    fn test_partial_eq() {
        assert_eq!(lm_thin_vec![0], lm_thin_vec![0]);
        assert_ne!(lm_thin_vec![0], lm_thin_vec![1]);
        assert_eq!(lm_thin_vec![1, 2, 3], vec![1, 2, 3]);
    }

    #[test]
    fn test_alloc() {
        let mut v = LMThinVec::new();
        assert!(!v.has_allocation());
        v.push(1);
        assert!(v.has_allocation());
        v.pop();
        assert!(v.has_allocation());
        v.shrink_to_fit();
        assert!(!v.has_allocation());
        v.reserve(64);
        assert!(v.has_allocation());
        v = LMThinVec::with_capacity(64);
        assert!(v.has_allocation());
        v = LMThinVec::with_capacity(0);
        assert!(!v.has_allocation());
    }

    #[test]
    fn test_clear() {
        let mut v = LMThinVec::<i32>::new();
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);

        v.clear();
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);

        v.push(1);
        v.push(2);
        assert_eq!(v.len(), 2);
        assert!(v.capacity() >= 2);
        assert_eq!(&v[..], &[1, 2]);

        v.clear();
        assert_eq!(v.len(), 0);
        assert!(v.capacity() >= 2);
        assert_eq!(&v[..], &[]);

        v.push(3);
        v.push(4);
        assert_eq!(v.len(), 2);
        assert!(v.capacity() >= 2);
        assert_eq!(&v[..], &[3, 4]);

        v.clear();
        assert_eq!(v.len(), 0);
        assert!(v.capacity() >= 2);
        assert_eq!(&v[..], &[]);

        v.clear();
        assert_eq!(v.len(), 0);
        assert!(v.capacity() >= 2);
        assert_eq!(&v[..], &[]);
    }

    #[test]
    fn test_empty_singleton_torture() {
        {
            let mut v = LMThinVec::<i32>::new();
            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert!(v.is_empty());
            assert_eq!(&v[..], &[]);
            assert_eq!(&mut v[..], &mut []);

            assert_eq!(v.pop(), None);
            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let v = LMThinVec::<i32>::new();
            assert_eq!(v.into_iter().count(), 0);

            let v = LMThinVec::<i32>::new();
            #[allow(clippy::never_loop)]
            for _ in v.into_iter() {
                unreachable!();
            }
        }

        {
            let mut v = LMThinVec::<i32>::new();
            v.truncate(1);
            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);

            v.truncate(0);
            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let mut v = LMThinVec::<i32>::new();
            v.shrink_to_fit();
            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let mut v = LMThinVec::<i32>::new();
            let new = v.split_off(0);
            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);

            assert_eq!(new.len(), 0);
            assert_eq!(new.capacity(), 0);
            assert_eq!(&new[..], &[]);
        }

        {
            let mut v = LMThinVec::<i32>::new();
            v.reserve(0);

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let mut v = LMThinVec::<i32>::new();
            v.reserve_exact(0);

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let mut v = LMThinVec::<i32>::new();
            v.reserve(0);

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let v = LMThinVec::<i32>::with_capacity(0);

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let v = LMThinVec::<i32>::default();

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let mut v = LMThinVec::<i32>::new();
            v.retain(|_| unreachable!());

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let mut v = LMThinVec::<i32>::new();
            v.retain_mut(|_| unreachable!());

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }

        {
            let v = LMThinVec::<i32>::new();
            let v = v.clone();

            assert_eq!(v.len(), 0);
            assert_eq!(v.capacity(), 0);
            assert_eq!(&v[..], &[]);
        }
    }

    #[test]
    fn test_clone() {
        let mut v = LMThinVec::<i32>::new();
        assert!(v.is_singleton());
        v.push(0);
        v.pop();
        assert!(!v.is_singleton());

        let v2 = v.clone();
        assert!(v2.is_singleton());
    }
}

#[cfg(test)]
mod std_tests {
    #![allow(clippy::reversed_empty_ranges)]

    use super::*;
    use core::mem::size_of;
    use core::usize;
    use std::format;

    struct DropCounter<'a> {
        count: &'a mut u32,
    }

    impl<'a> Drop for DropCounter<'a> {
        fn drop(&mut self) {
            *self.count += 1;
        }
    }

    #[test]
    fn test_small_vec_struct() {
        assert!(size_of::<LMThinVec<u8>>() == size_of::<usize>());
    }

    #[test]
    fn test_double_drop() {
        struct TwoVec<T> {
            x: LMThinVec<T>,
            y: LMThinVec<T>,
        }

        let (mut count_x, mut count_y) = (0, 0);
        {
            let mut tv = TwoVec {
                x: LMThinVec::new(),
                y: LMThinVec::new(),
            };
            tv.x.push(DropCounter {
                count: &mut count_x,
            });
            tv.y.push(DropCounter {
                count: &mut count_y,
            });

            // If LMThinVec had a drop flag, here is where it would be zeroed.
            // Instead, it should rely on its internal state to prevent
            // doing anything significant when dropped multiple times.
            drop(tv.x);

            // Here tv goes out of scope, tv.y should be dropped, but not tv.x.
        }

        assert_eq!(count_x, 1);
        assert_eq!(count_y, 1);
    }

    #[test]
    fn test_reserve() {
        let mut v = LMThinVec::new();
        assert_eq!(v.capacity(), 0);

        v.reserve(2);
        assert!(v.capacity() >= 2);

        for i in 0..16 {
            v.push(i);
        }

        assert!(v.capacity() >= 16);
        v.reserve(16);
        assert!(v.capacity() >= 32);

        v.push(16);

        v.reserve(16);
        assert!(v.capacity() >= 33)
    }

    #[test]
    fn test_extend() {
        let mut v = LMThinVec::<usize>::new();
        let mut w = LMThinVec::new();
        v.extend(w.clone());
        assert_eq!(v, &[]);

        v.extend(0..3);
        for i in 0..3 {
            w.push(i)
        }

        assert_eq!(v, w);

        v.extend(3..10);
        for i in 3..10 {
            w.push(i)
        }

        assert_eq!(v, w);

        v.extend(w.clone()); // specializes to `append`
        assert!(v.iter().eq(w.iter().chain(w.iter())));

        // Zero sized types
        #[derive(PartialEq, Debug)]
        struct Foo;

        let mut a = LMThinVec::new();
        let b = lm_thin_vec![Foo, Foo];

        a.extend(b);
        assert_eq!(a, &[Foo, Foo]);

        // Double drop
        let mut count_x = 0;
        {
            let mut x = LMThinVec::new();
            let y = lm_thin_vec![DropCounter {
                count: &mut count_x
            }];
            x.extend(y);
        }

        assert_eq!(count_x, 1);
    }

    /* TODO: implement extend for Iter<&Copy>
        #[test]
        fn test_extend_ref() {
            let mut v = lm_thin_vec![1, 2];
            v.extend(&[3, 4, 5]);

            assert_eq!(v.len(), 5);
            assert_eq!(v, [1, 2, 3, 4, 5]);

            let w = lm_thin_vec![6, 7];
            v.extend(&w);

            assert_eq!(v.len(), 7);
            assert_eq!(v, [1, 2, 3, 4, 5, 6, 7]);
        }
    */

    #[test]
    fn test_slice_from_mut() {
        let mut values = lm_thin_vec![1, 2, 3, 4, 5];
        {
            let slice = &mut values[2..];
            assert!(slice == [3, 4, 5]);
            for p in slice {
                *p += 2;
            }
        }

        assert!(values == [1, 2, 5, 6, 7]);
    }

    #[test]
    fn test_slice_to_mut() {
        let mut values = lm_thin_vec![1, 2, 3, 4, 5];
        {
            let slice = &mut values[..2];
            assert!(slice == [1, 2]);
            for p in slice {
                *p += 1;
            }
        }

        assert!(values == [2, 3, 3, 4, 5]);
    }

    #[test]
    fn test_split_at_mut() {
        let mut values = lm_thin_vec![1, 2, 3, 4, 5];
        {
            let (left, right) = values.split_at_mut(2);
            {
                let left: &[_] = left;
                assert!(left[..left.len()] == [1, 2]);
            }
            for p in left {
                *p += 1;
            }

            {
                let right: &[_] = right;
                assert!(right[..right.len()] == [3, 4, 5]);
            }
            for p in right {
                *p += 2;
            }
        }

        assert_eq!(values, [2, 3, 5, 6, 7]);
    }

    #[test]
    fn test_clone() {
        let v: LMThinVec<i32> = lm_thin_vec![];
        let w = lm_thin_vec![1, 2, 3];

        assert_eq!(v, v.clone());

        let z = w.clone();
        assert_eq!(w, z);
        // they should be disjoint in memory.
        assert!(w.as_ptr() != z.as_ptr())
    }

    #[test]
    fn test_clone_from() {
        let mut v = lm_thin_vec![];
        let three: LMThinVec<Box<_>> = lm_thin_vec![Box::new(1), Box::new(2), Box::new(3)];
        let two: LMThinVec<Box<_>> = lm_thin_vec![Box::new(4), Box::new(5)];
        // zero, long
        v.clone_from(&three);
        assert_eq!(v, three);

        // equal
        v.clone_from(&three);
        assert_eq!(v, three);

        // long, short
        v.clone_from(&two);
        assert_eq!(v, two);

        // short, long
        v.clone_from(&three);
        assert_eq!(v, three)
    }

    #[test]
    fn test_retain() {
        let mut vec = lm_thin_vec![1, 2, 3, 4];
        vec.retain(|&x| x % 2 == 0);
        assert_eq!(vec, [2, 4]);
    }

    #[test]
    fn test_retain_mut() {
        let mut vec = lm_thin_vec![9, 9, 9, 9];
        let mut i = 0;
        vec.retain_mut(|x| {
            i += 1;
            *x = i;
            i != 4
        });
        assert_eq!(vec, [1, 2, 3]);
    }

    #[test]
    fn zero_sized_values() {
        let mut v = LMThinVec::new();
        assert_eq!(v.len(), 0);
        v.push(());
        assert_eq!(v.len(), 1);
        v.push(());
        assert_eq!(v.len(), 2);
        assert_eq!(v.pop(), Some(()));
        assert_eq!(v.pop(), Some(()));
        assert_eq!(v.pop(), None);

        assert_eq!(v.iter().count(), 0);
        v.push(());
        assert_eq!(v.iter().count(), 1);
        v.push(());
        assert_eq!(v.iter().count(), 2);

        for &() in &v {}

        assert_eq!(v.iter_mut().count(), 2);
        v.push(());
        assert_eq!(v.iter_mut().count(), 3);
        v.push(());
        assert_eq!(v.iter_mut().count(), 4);

        for &mut () in &mut v {}
        unsafe {
            v.set_len(0);
        }
        assert_eq!(v.iter_mut().count(), 0);
    }

    #[test]
    fn test_partition() {
        assert_eq!(
            lm_thin_vec![].into_iter().partition(|x: &i32| *x < 3),
            (lm_thin_vec![], lm_thin_vec![])
        );
        assert_eq!(
            lm_thin_vec![1, 2, 3].into_iter().partition(|x| *x < 4),
            (lm_thin_vec![1, 2, 3], lm_thin_vec![])
        );
        assert_eq!(
            lm_thin_vec![1, 2, 3].into_iter().partition(|x| *x < 2),
            (lm_thin_vec![1], lm_thin_vec![2, 3])
        );
        assert_eq!(
            lm_thin_vec![1, 2, 3].into_iter().partition(|x| *x < 0),
            (lm_thin_vec![], lm_thin_vec![1, 2, 3])
        );
    }

    #[test]
    fn test_zip_unzip() {
        let z1 = lm_thin_vec![(1, 4), (2, 5), (3, 6)];

        let (left, right): (LMThinVec<_>, LMThinVec<_>) = z1.iter().cloned().unzip();

        assert_eq!((1, 4), (left[0], right[0]));
        assert_eq!((2, 5), (left[1], right[1]));
        assert_eq!((3, 6), (left[2], right[2]));
    }

    #[test]
    fn test_vec_truncate_drop() {
        static mut DROPS: u32 = 0;
        struct Elem(#[allow(dead_code)] i32);
        impl Drop for Elem {
            fn drop(&mut self) {
                unsafe {
                    DROPS += 1;
                }
            }
        }

        let mut v = lm_thin_vec![Elem(1), Elem(2), Elem(3), Elem(4), Elem(5)];
        assert_eq!(unsafe { DROPS }, 0);
        v.truncate(3);
        assert_eq!(unsafe { DROPS }, 2);
        v.truncate(0);
        assert_eq!(unsafe { DROPS }, 5);
    }

    #[test]
    #[should_panic]
    fn test_vec_truncate_fail() {
        struct BadElem(i32);
        impl Drop for BadElem {
            fn drop(&mut self) {
                let BadElem(ref mut x) = *self;
                if *x == 0xbadbeef {
                    panic!("BadElem panic: 0xbadbeef")
                }
            }
        }

        let mut v = lm_thin_vec![BadElem(1), BadElem(2), BadElem(0xbadbeef), BadElem(4)];
        v.truncate(0);
    }

    #[test]
    fn test_index() {
        let vec = lm_thin_vec![1, 2, 3];
        assert!(vec[1] == 2);
    }

    #[test]
    #[should_panic]
    fn test_index_out_of_bounds() {
        let vec = lm_thin_vec![1, 2, 3];
        let _ = vec[3];
    }

    #[test]
    #[should_panic]
    fn test_slice_out_of_bounds_1() {
        let x = lm_thin_vec![1, 2, 3, 4, 5];
        let _ = &x[!0..];
    }

    #[test]
    #[should_panic]
    fn test_slice_out_of_bounds_2() {
        let x = lm_thin_vec![1, 2, 3, 4, 5];
        let _ = &x[..6];
    }

    #[test]
    #[should_panic]
    fn test_slice_out_of_bounds_3() {
        let x = lm_thin_vec![1, 2, 3, 4, 5];
        let _ = &x[!0..4];
    }

    #[test]
    #[should_panic]
    fn test_slice_out_of_bounds_4() {
        let x = lm_thin_vec![1, 2, 3, 4, 5];
        let _ = &x[1..6];
    }

    #[test]
    #[should_panic]
    fn test_slice_out_of_bounds_5() {
        let x = lm_thin_vec![1, 2, 3, 4, 5];
        let _ = &x[3..2];
    }

    #[test]
    #[should_panic]
    fn test_swap_remove_empty() {
        let mut vec = LMThinVec::<i32>::new();
        vec.swap_remove(0);
    }

    #[test]
    fn test_move_items() {
        let vec = lm_thin_vec![1, 2, 3];
        let mut vec2 = lm_thin_vec![];
        for i in vec {
            vec2.push(i);
        }
        assert_eq!(vec2, [1, 2, 3]);
    }

    #[test]
    fn test_move_items_reverse() {
        let vec = lm_thin_vec![1, 2, 3];
        let mut vec2 = lm_thin_vec![];
        for i in vec.into_iter().rev() {
            vec2.push(i);
        }
        assert_eq!(vec2, [3, 2, 1]);
    }

    #[test]
    fn test_move_items_zero_sized() {
        let vec = lm_thin_vec![(), (), ()];
        let mut vec2 = lm_thin_vec![];
        for i in vec {
            vec2.push(i);
        }
        assert_eq!(vec2, [(), (), ()]);
    }

    #[test]
    fn test_split_off() {
        let mut vec = lm_thin_vec![1, 2, 3, 4, 5, 6];
        let vec2 = vec.split_off(4);
        assert_eq!(vec, [1, 2, 3, 4]);
        assert_eq!(vec2, [5, 6]);
    }

    #[test]
    fn test_into_iter_as_slice() {
        let vec = lm_thin_vec!['a', 'b', 'c'];
        let mut into_iter = vec.into_iter();
        assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
        let _ = into_iter.next().unwrap();
        assert_eq!(into_iter.as_slice(), &['b', 'c']);
        let _ = into_iter.next().unwrap();
        let _ = into_iter.next().unwrap();
        assert_eq!(into_iter.as_slice(), &[]);
    }

    #[test]
    fn test_into_iter_as_mut_slice() {
        let vec = lm_thin_vec!['a', 'b', 'c'];
        let mut into_iter = vec.into_iter();
        assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
        into_iter.as_mut_slice()[0] = 'x';
        into_iter.as_mut_slice()[1] = 'y';
        assert_eq!(into_iter.next().unwrap(), 'x');
        assert_eq!(into_iter.as_slice(), &['y', 'c']);
    }

    #[test]
    fn test_into_iter_debug() {
        let vec = lm_thin_vec!['a', 'b', 'c'];
        let into_iter = vec.into_iter();
        let debug = format!("{:?}", into_iter);
        assert_eq!(debug, "IntoIter(['a', 'b', 'c'])");
    }

    #[test]
    fn test_into_iter_count() {
        assert_eq!(lm_thin_vec![1, 2, 3].into_iter().count(), 3);
    }

    #[test]
    fn test_into_iter_clone() {
        fn iter_equal<I: Iterator<Item = i32>>(it: I, slice: &[i32]) {
            let v: LMThinVec<i32> = it.collect();
            assert_eq!(&v[..], slice);
        }
        let mut it = lm_thin_vec![1, 2, 3].into_iter();
        iter_equal(it.clone(), &[1, 2, 3]);
        assert_eq!(it.next(), Some(1));
        let mut it = it.rev();
        iter_equal(it.clone(), &[3, 2]);
        assert_eq!(it.next(), Some(3));
        iter_equal(it.clone(), &[2]);
        assert_eq!(it.next(), Some(2));
        iter_equal(it.clone(), &[]);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn overaligned_allocations() {
        #[repr(align(256))]
        struct Foo(usize);
        let mut v = lm_thin_vec![Foo(273)];
        for i in 0..0x1000 {
            v.reserve_exact(i);
            assert!(v[0].0 == 273);
            assert!(v.as_ptr() as usize & 0xff == 0);
            v.shrink_to_fit();
            assert!(v[0].0 == 273);
            assert!(v.as_ptr() as usize & 0xff == 0);
        }
    }

    #[test]
    fn test_reserve_exact() {
        // This is all the same as test_reserve

        let mut v = LMThinVec::new();
        assert_eq!(v.capacity(), 0);

        v.reserve_exact(2);
        assert!(v.capacity() >= 2);

        for i in 0..16 {
            v.push(i);
        }

        assert!(v.capacity() >= 16);
        v.reserve_exact(16);
        assert!(v.capacity() >= 32);

        v.push(16);

        v.reserve_exact(16);
        assert!(v.capacity() >= 33)
    }

    #[test]
    fn test_header_data() {
        macro_rules! assert_aligned_head_ptr {
            ($typename:ty) => {{
                let v: LMThinVec<$typename> =
                    LMThinVec::with_capacity(1 /* ensure allocation */);
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
        assert_eq!(2 * core::mem::size_of::<u32>(), HEADER_SIZE);

        #[repr(C, align(128))]
        struct Funky<T>(T);
        assert_eq!(padding::<Funky<()>>(), 128 - HEADER_SIZE);
        assert_aligned_head_ptr!(Funky<()>);

        assert_eq!(padding::<Funky<u8>>(), 128 - HEADER_SIZE);
        assert_aligned_head_ptr!(Funky<u8>);

        assert_eq!(padding::<Funky<[(); 1024]>>(), 128 - HEADER_SIZE);
        assert_aligned_head_ptr!(Funky<[(); 1024]>);

        assert_eq!(padding::<Funky<[*mut usize; 1024]>>(), 128 - HEADER_SIZE);
        assert_aligned_head_ptr!(Funky<[*mut usize; 1024]>);
    }

    #[test]
    fn test_set_len() {
        let mut vec: LMThinVec<u32> = lm_thin_vec![];
        unsafe {
            vec.set_len(0); // at one point this caused a crash
        }
    }

    #[test]
    #[should_panic(expected = "invalid set_len(1) on empty LMThinVec")]
    fn test_set_len_invalid() {
        let mut vec: LMThinVec<u32> = lm_thin_vec![];
        unsafe {
            vec.set_len(1);
        }
    }

    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_capacity_overflow_header_too_big() {
        let vec: LMThinVec<u8> = LMThinVec::with_capacity(isize::MAX as usize - 2);
        assert!(vec.capacity() > 0);
    }
    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_capacity_overflow_cap_too_big() {
        let vec: LMThinVec<u8> = LMThinVec::with_capacity(isize::MAX as usize + 1);
        assert!(vec.capacity() > 0);
    }
    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_capacity_overflow_size_mul1() {
        let vec: LMThinVec<u16> = LMThinVec::with_capacity(isize::MAX as usize + 1);
        assert!(vec.capacity() > 0);
    }
    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_capacity_overflow_size_mul2() {
        let vec: LMThinVec<u16> = LMThinVec::with_capacity(isize::MAX as usize / 2 + 1);
        assert!(vec.capacity() > 0);
    }
    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_capacity_overflow_cap_really_isnt_isize() {
        let vec: LMThinVec<u8> = LMThinVec::with_capacity(isize::MAX as usize);
        assert!(vec.capacity() > 0);
    }
}
