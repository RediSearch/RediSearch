/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::bindings::{
    FieldSpecOption, FieldSpecOptions, FieldSpecType, FieldSpecTypes, IndexSpecCache,
};
use enumflags2::{BitFlags, bitflags, make_bitflags};
use pin_project::pin_project;
use std::{
    borrow::Cow,
    cell::UnsafeCell,
    ffi::{CStr, c_char},
    mem,
    ops::DerefMut,
    pin::Pin,
    ptr::{self, NonNull},
    slice,
};

#[bitflags]
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RLookupKeyFlag {
    /// This field is (or assumed to be) part of the document itself.
    /// This is a basic flag for a loaded key.
    DocSrc = 0x01,

    /// This field is part of the index schema.
    SchemaSrc = 0x02,

    /// Check the sorting table, if necessary, for the index of the key.
    SvSrc = 0x04,

    /// This key was created by the query itself (not in the document)
    QuerySrc = 0x08,

    /// Copy the key string via strdup. `name` may be freed
    NameAlloc = 0x10,

    /// If the key is already present, then overwrite it (relevant only for LOAD or WRITE modes)
    Override = 0x20,

    /// Request that the key is returned for loading even if it is already loaded.
    ForceLoad = 0x40,

    /// This key is unresolved. Its source needs to be derived from elsewhere
    Unresolved = 0x80,

    /// This field is hidden within the document and is only used as a transient
    /// field for another consumer. Don't output this field.
    Hidden = 0x100,

    /// The opposite of [`RLookupKeyFlag::Hidden`]. This field is specified as an explicit return in
    /// the RETURN list, so ensure that this gets emitted. Only set if
    /// explicitReturn is true in the aggregation request.
    ExplicitReturn = 0x200,

    /// This key's value is already available in the RLookup table,
    /// if it was opened for read but the field is sortable and not normalized,
    /// so the data should be exactly the same as in the doc.
    ValAvailable = 0x400,

    /// This key's value was loaded (by a loader) from the document itself.
    IsLoaded = 0x800,

    /// This key type is numeric
    Numeric = 0x1000,
}

pub type RLookupKeyFlags = BitFlags<RLookupKeyFlag>;

// Flags that are allowed to be passed to [`RLookup::get_key_read`], [`RLookup::get_key_write`], or [`RLookup::get_key_load`].
const GET_KEY_FLAGS: RLookupKeyFlags =
    make_bitflags!(RLookupKeyFlag::{Override | Hidden | ExplicitReturn | ForceLoad});

/// Flags do not persist to the key, they are just options to [`RLookup::get_key_read`], [`RLookup::get_key_write`], or [`RLookup::get_key_load`].
const TRANSIENT_FLAGS: RLookupKeyFlags =
    make_bitflags!(RLookupKeyFlag::{Override | ForceLoad | NameAlloc});

#[bitflags]
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RLookupOption {
    /// If the key cannot be found, do not mark it as an error, but create it and
    /// mark it as F_UNRESOLVED
    AllowUnresolved = 0x01,

    /// If a loader was added to load the entire document, this flag will allow
    /// later calls to GetKey in read mode to create a key (from the schema) even if it is not sortable
    AllLoaded = 0x02,
}
pub type RLookupOptions = BitFlags<RLookupOption>;

/// RLookup key
///
/// `RLookupKey`s are used to speed up accesses in an `RLookupRow`. Instead of having to do repeated
/// string comparisons to find the correct value by path/name, an `RLookupKey` is created using the
/// `RLookup` which then allows `O(1)` lookup within the `RLookupRow`.
///
///
/// The old C documentation for this type for posterity and later reference. Note that it is unclear
/// how much this reflects the actual state of the code.
///
/// ```text
/// RLookup Key
///
/// A lookup key is a structure which contains an array index at which the
/// data may be reliably located. This avoids needless string comparisons by
/// using quick objects rather than "dynamic" string comparison mechanisms.
///
/// The basic workflow is that users of a given key (i.e. "foo") are expected
/// to first create the key by use of RLookup_GetKey(). This will provide
/// the consumer with an opaque object that is the slot of "foo". Once the
/// key is provided, it may then be use to both read and write the key.
///
/// Using a pre-defined key also allows the query to maintain a central registry
/// of used names. If a user makes a typo in a query, this registry will easily
/// detect that the name was not used previously.
///
/// Note that the same name can be registered twice, in which case it will simply
/// increment the reference to the same key.
///
/// There are two arrays which are accessed to check for the key. Their use is
/// mutually exclusive per-key, though multiple keys may exist which can access
/// either one or the other array. The first array is the "sorting vector" for
/// a given document. The F_SVSRC flag is set on keys which are expected to be
/// found within the sorting vector.
///
/// The second array is a "dynamic" array within a given result's row data.
/// This is used for data generated on the fly, or for data not stored within
/// the sorting vector.
/// ```
#[pin_project(!Unpin)]
#[derive(Debug)]
#[repr(C)]
pub struct RLookupKey<'a> {
    /// Index into the dynamic values array within the associated `RLookupRow`.
    pub dstidx: u16,

    /// If the source for this key is a sorting vector, this is the index
    /// into the `RSSortingVector` within the associated `RLookupRow`.
    pub svidx: u16,

    /// Various flags dictating the behavior of looking up the value of this key.
    /// Most notably, `Flags::SVSRC` means the source is an `RSSortingVector` and
    /// `Self::svidx` should be used to look up the value.
    pub flags: RLookupKeyFlags,

    /// The path of this key.
    ///
    /// For fields *not* loaded from a [`FieldSpec`][ffi::FieldSpec], this points to the *same* string
    /// as `Self::path`.
    pub path: *const c_char,

    /// The name of this key.
    pub name: *const c_char,
    /// The length of this key in bytes, without the null-terminator.
    /// Should be used to avoid repeated `strlen` computations.
    pub name_len: usize,

    /// Pointer to next field in the list
    pub next: UnsafeCell<Option<NonNull<RLookupKey<'a>>>>,

    // Private Rust fields
    /// The actual "owning" strings, we need to hold onto these
    /// so the pointers above stay valid. Note that you
    /// MUST NEVER MOVE THESE BEFORE THE name AND path FIELDS UNLESS
    /// YOU WANT TO POTENTIALLY RISK UB
    #[pin]
    _name: Cow<'a, CStr>,
    #[pin]
    _path: Option<Cow<'a, CStr>>,
}

/// An append-only list of [`RLookupKey`]s.
///
/// This type maintains a mapping from string names to [`RLookupKey`]s.
#[derive(Debug)]
#[repr(C)]
pub struct RLookup<'a> {
    keys: KeyList<'a>,

    // Flags/options
    options: RLookupOptions,

    // If present, then GetKey will consult this list if the value is not found in
    // the existing list of keys.
    index_spec_cache: Option<IndexSpecCache>,
}

#[derive(Debug)]
#[repr(C)]
struct KeyList<'a> {
    // The head and tail nodes of this linked-list.
    // FIXME [MOD-10314] make this more type-safe when we no longer have direct field access from C
    head: Option<NonNull<RLookupKey<'a>>>,
    tail: Option<NonNull<RLookupKey<'a>>>,
    // Length of the data row. This is not necessarily the number
    // of lookup keys. Hidden keys created through [`CursorMut::override_current`] increase
    // the number of actually allocated keys without increasing the conceptual rowlen.
    rowlen: u32,
}

/// A cursor over a [`KeyList`].
pub struct Cursor<'list, 'a> {
    _rlookup: &'list KeyList<'a>,
    current: Option<NonNull<RLookupKey<'a>>>,
}

/// A cursor over a [`KeyList`] with editing operations.
pub struct CursorMut<'list, 'a> {
    _rlookup: &'list mut KeyList<'a>,
    current: Option<NonNull<RLookupKey<'a>>>,
}

// ===== impl RLookupKey =====

// SAFETY NOTICE
//
// This type contains self-referential fields (e.g. `name` points to memory owned by `_name`) and therefore
// must be pinned at all times. This means in practice, to only ever hand out one of two types of references to the
// string: either an immutable `&CStr` - safe Rust cannot move out of an immutable reference - or a pinned mutable
// reference `Pin<&mut CStr>` which safe Rust also cannot move out of.
// This means you may NEVER EVER hand out a `&mut CStr` EVER.
impl<'a> RLookupKey<'a> {
    /// Constructs a new `RLookupKey` using the provided `CStr` and flags.
    ///
    /// If the [`RLookupKeyFlag::NameAlloc`] is given, then the provided `CStr` will be cloned into
    /// a new allocation that is owned by this key. If the flag is *not* provided the key
    /// will simply borrow the provided string.
    pub fn new(name: &'a CStr, flags: RLookupKeyFlags) -> Self {
        let name = if flags.contains(RLookupKeyFlag::NameAlloc) {
            Cow::Owned(name.to_owned())
        } else {
            Cow::Borrowed(name)
        };

        Self {
            dstidx: 0,
            svidx: 0,
            flags: flags & !TRANSIENT_FLAGS,
            name: name.as_ptr(),
            path: name.as_ptr(),
            name_len: name.count_bytes(),
            _name: name,
            _path: None,
            next: UnsafeCell::new(None),
        }
    }

    /// Updates this `RLookupKey`'s fields according to the provided [`ffi::FieldSpec`].
    pub fn update_from_field_spec(&mut self, fs: &ffi::FieldSpec) {
        self.flags |= RLookupKeyFlag::DocSrc | RLookupKeyFlag::SchemaSrc;

        let path = {
            debug_assert!(!fs.fieldPath.is_null());

            let mut path_len = 0;
            // Safety: we received the pointer from the field spec and have to assume it is valid
            let path_ptr =
                unsafe { ffi::HiddenString_GetUnsafe(fs.fieldPath, ptr::from_mut(&mut path_len)) };

            debug_assert!(!path_ptr.is_null());
            // Safety: We assume the `path_ptr` and `length` information returned by the field spec
            // point to a valid null-terminated C string. Importantly `length` here is value as returned by
            // `strlen` so **does not** include the null terminator (that is why we do `path_len + 1` below)
            let bytes = unsafe { slice::from_raw_parts(path_ptr.cast::<u8>(), path_len + 1) };
            let path = CStr::from_bytes_with_nul(bytes)
                .expect("string returned by HiddenString_GetUnsafe is malformed");

            // When the name is owned, we also want the path to be owned
            if matches!(self._name, Cow::Owned(_)) {
                Cow::Owned(path.to_owned())
            } else {
                Cow::Borrowed(path)
            }
        };
        self._path = Some(path);
        self.path = self._path.as_ref().unwrap().as_ptr();

        let fs_options = FieldSpecOptions::from_bits(fs.options()).unwrap();

        if fs_options.contains(FieldSpecOption::Sortable) {
            self.flags |= RLookupKeyFlag::SvSrc;
            self.svidx = u16::try_from(fs.sortIdx).unwrap();

            if fs_options.contains(FieldSpecOption::Unf) {
                // If the field is sortable and not normalized (UNF), the available data in the
                // sorting vector is the same as the data in the document.
                self.flags |= RLookupKeyFlag::ValAvailable;
            }
        }

        let fs_types = FieldSpecTypes::from_bits(fs.types()).unwrap();

        if fs_types.contains(FieldSpecType::Numeric) {
            self.flags |= RLookupKeyFlag::Numeric;
        }
    }

    /// Construct an `RLookupKey` from its main parts. Prefer Self::new if you are unsure which to use.
    fn from_parts(
        name: Cow<'a, CStr>,
        path: Option<Cow<'a, CStr>>,
        dstidx: u16,
        flags: RLookupKeyFlags,
    ) -> Self {
        debug_assert_eq!(
            matches!(name, Cow::Owned(_)),
            flags.contains(RLookupKeyFlag::NameAlloc),
            "`RLookupKeyFlag::NameAlloc` was provided, but `name` was not `Cow::Owned`"
        );
        if let Some(path) = &path {
            debug_assert_eq!(
                matches!(path, Cow::Owned(_)),
                flags.contains(RLookupKeyFlag::NameAlloc),
                "`RLookupKeyFlag::NameAlloc` was provided, but `path` was not `Cow::Owned`"
            );
        }

        Self {
            dstidx,
            svidx: 0,
            flags: flags & !TRANSIENT_FLAGS,
            name: name.as_ptr(),
            // if a separate path was provided we should set the pointer accordingly
            // if not, we fall back to the name as usual
            path: path.as_ref().map_or(name.as_ptr(), |path| path.as_ptr()),
            name_len: name.count_bytes(),
            _name: name,
            _path: path,
            next: UnsafeCell::new(None),
        }
    }

    /// Converts a heap-allocated `RLookupKey` into a raw pointer.
    ///
    /// The caller is responsible for the memory previously managed by the `Box`, in particular
    /// the caller should properly destroy the `RLookupKey` and deallocate the memory by calling
    /// `Self::from_ptr`.
    ///
    /// # Safety
    ///
    /// The caller *must* continue to treat the pointer as pinned.
    #[inline]
    unsafe fn into_ptr(me: Pin<Box<Self>>) -> NonNull<Self> {
        // This function must be kept in sync with `Self::from_ptr` below.

        // Safety: The caller promised to continue to treat the returned pointer
        // as pinned and never move out of it.
        let ptr = Box::into_raw(unsafe { Pin::into_inner_unchecked(me) });

        // Safety: we know the ptr we get from Box::into_raw is never null
        unsafe { NonNull::new_unchecked(ptr) }
    }

    /// Constructs a `Pin<Box<RLookupKey>>` from a raw pointer.
    ///
    /// The returned `Box` will own the raw pointer, in particular dropping the `Box`
    /// will deallocate the `RLookupKey`. This function should only be used by [`RLookup::drop`].
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure the pointer was previously created through [`Self::into_ptr`].
    /// 2. The caller has to be careful to never call this method twice for the same pointer, otherwise a
    ///    double-free or other memory corruptions will occur.
    /// 3. The caller *must* also ensure that `ptr` continues to be treated as pinned.
    #[inline]
    unsafe fn from_ptr(ptr: NonNull<Self>) -> Pin<Box<Self>> {
        // This function must be kept in sync with `Self::into_ptr` above.

        // Safety:
        // 1 -> This function will only ever be called through `RLookup::drop` below.
        //      We therefore know - because push_key creates pointers through `into_ptr` - that the invariant is upheld.
        // 2 -> Has to be upheld by the caller
        let b = unsafe { Box::from_raw(ptr.as_ptr()) };
        // Safety: 3 -> Caller has to uphold the pin contract
        unsafe { Pin::new_unchecked(b) }
    }

    #[cfg(any(debug_assertions, test))]
    fn is_tombstone(&self) -> bool {
        self.name.is_null()
            && self.name_len == usize::MAX
            && self.path.is_null()
            && self.flags.contains(RLookupKeyFlag::Hidden)
    }

    /// Returns `true` if this node is currently linked to a [`List`].
    #[cfg(test)]
    fn has_next(&self) -> bool {
        self.next().is_some()
    }

    /// Return the next pointer in the linked list
    #[inline]
    fn next(&self) -> Option<NonNull<RLookupKey<'a>>> {
        // Safety: RLookupKeys are created through `KeyList::push` and owned by the `List`. We
        // can therefore assume this pointer is safe to dereference at this point.
        unsafe { *self.next.get() }
    }

    /// Update the pointer to the next node
    #[inline]
    fn set_next(
        self: Pin<&mut Self>,
        next: Option<NonNull<RLookupKey<'a>>>,
    ) -> Option<NonNull<RLookupKey<'a>>> {
        let me = self.project();
        mem::replace(me.next.get_mut(), next)
    }

    #[cfg(any(debug_assertions, test))]
    fn assert_valid(&self, tail: &Self, ctx: &str) {
        assert!(
            !self.flags.intersects(TRANSIENT_FLAGS),
            "{ctx}key flags must not contain transient ({TRANSIENT_FLAGS:?}) flags. Found {:?}.",
            self.flags
        );

        if !self.is_tombstone() {
            assert!(
                ptr::eq(self.name, self._name.as_ptr()),
                "{ctx}`key.name` did not match `key._name`. ({self:?})",
            );
            if let Some(path) = self._path.as_ref() {
                assert!(
                    ptr::eq(self.path, path.as_ptr()),
                    "{ctx}`key._path` is present, but `key.path` did not match `key._path`. ({self:?})"
                );
            } else {
                assert!(
                    ptr::eq(self.path, self._name.as_ptr()),
                    "{ctx}`key._path` is not present, but `key.path` did not match `key._name`. ({self:?})"
                );
            }
            assert_eq!(
                self.name_len,
                self._name.count_bytes(),
                "{ctx}`key.name_len` did not match `key._name` length"
            );
        }

        if ptr::eq(self, tail) {
            assert_eq!(
                self.next(),
                None,
                "{ctx}tail key must not have a next link; node={self:#?}",
            );
        }

        if let Some(next) = self.next() {
            assert_ne!(
                // Safety:
                NonNull::from(unsafe { &next.as_ref().next }),
                NonNull::from(&self.next),
                "{ctx}key's next link cannot be to itself; node={self:#?}",
            );
        }
    }
}

// ===== impl KeyList =====

impl<'a> KeyList<'a> {
    /// Construct a new, empty `KeyList`.
    pub const fn new() -> Self {
        Self {
            head: None,
            tail: None,
            rowlen: 0,
        }
    }

    /// Insert a `RLookupKey` into this `KeyList` and return a mutable reference to it.
    ///
    /// The key will be owned by the list and freed when dropping the list.
    fn push(&mut self, mut key: RLookupKey<'a>) -> Pin<&mut RLookupKey<'a>> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::push before");

        key.dstidx = u16::try_from(self.rowlen).expect("conversion from u32 RLookup::rowlen to u16 RLookupKey::dstidx overflowed. This is a bug!");

        // Safety: RLookup never hands out mutable references to the key (except `Pin<&mut T>` which is fine)
        // and never copies, or memmoves the memory internally.
        let mut ptr = unsafe { RLookupKey::into_ptr(Box::pin(key)) };

        if let Some(mut tail) = self.tail.take() {
            // if we have a tail we also must have a head
            debug_assert!(self.head.is_some());

            // Safety: We know we can borrow tail here, since we mutably borrow the KeyList
            // which owns all keys allocated within it. This ensures the KeyList and all keys outlive
            // this method call AND that we have exclusive access to mutate the key.
            // Safety: we need to continue to treat the key as pinned
            let tail = unsafe { tail.as_mut() };
            // Safety: we need to continue to treat the key as pinned
            let tail = unsafe { Pin::new_unchecked(tail) };

            tail.set_next(Some(ptr));
            self.tail = Some(ptr);
        } else {
            // if we have no tail we also must have no head
            debug_assert!(self.head.is_none());
            self.head = Some(ptr);
            self.tail = Some(ptr);
        }

        // Increase the table row length. (all rows have the same length).
        self.rowlen += 1;

        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::push after");

        // Safety: we have allocated the memory above, this pointer is safe to dereference.
        let key = unsafe { ptr.as_mut() };
        // Safety: We treat the pointer as pinned internally and never hand out references that could be moved out of (in safe Rust)
        // publicly.
        unsafe { Pin::new_unchecked(key) }
    }

    /// Returns a [`Cursor`] starting at the first element.
    ///
    /// The [`Cursor`] type can be used as Iterator over this list.
    pub fn cursor_front(&self) -> Cursor<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::cursor_front");

        Cursor {
            _rlookup: self,
            current: self.head,
        }
    }

    /// Returns a [`CursorMut`] starting at the first element.
    ///
    /// The [`CursorMut`] type can be used as Iterator over this list. In addition, it may be used to manipulate the list.
    pub fn cursor_front_mut(&mut self) -> CursorMut<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::cursor_front_mut");

        CursorMut {
            current: self.head,
            _rlookup: self,
        }
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a [`Cursor`] pointing to the key if found.
    // FIXME [MOD-10315] replace with more efficient search
    fn find_by_name(&self, name: &'a CStr) -> Option<Cursor<'_, 'a>> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::find_by_name");

        let mut c = self.cursor_front();
        while let Some(key) = c.current() {
            if key._name.as_ref() == name {
                return Some(c);
            }
            c.move_next();
        }
        None
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a [`CursorMut`] pointing to the key if found.
    // FIXME [MOD-10315] replace with more efficient search
    fn find_by_name_mut(&mut self, name: &'a CStr) -> Option<CursorMut<'_, 'a>> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::find_by_name_mut");

        let mut c = self.cursor_front_mut();
        while let Some(key) = c.current() {
            if key._name.as_ref() == name {
                return Some(c);
            }
            c.move_next();
        }
        None
    }

    /// Asserts as many of the linked list's invariants as possible.
    ///
    /// We use this method to absolutely make sure the linked list is internally consistent
    /// before reading from it and after writing to it.
    #[track_caller]
    #[cfg(any(debug_assertions, test))]
    fn assert_valid(&self, ctx: &str) {
        let Some(head) = self.head else {
            assert!(
                self.tail.is_none(),
                "{ctx}if the linked list's head is null, the tail must also be null"
            );
            assert_eq!(
                self.rowlen, 0,
                "{ctx}if a linked list's head is null, its length must be 0"
            );
            return;
        };

        assert_ne!(
            self.rowlen, 0,
            "{ctx}if a linked list's head is not null, its length must be greater than 0"
        );
        assert_ne!(
            self.tail, None,
            "{ctx}if the linked list has a head, it must also have a tail"
        );

        let tail = self.tail.unwrap();

        // Safety: RLookupKeys are created through `KeyList::push` and owned by the `List`. We
        // can therefore assume this pointer is safe to dereference at this point.
        let head = unsafe { head.as_ref() };
        // Safety: see abvove
        let tail = unsafe { tail.as_ref() };

        if ptr::addr_eq(head, tail) {
            assert_eq!(
                NonNull::from(&head.next),
                NonNull::from(&tail.next),
                "{ctx}if the head and tail nodes are the same, their links must be the same"
            );
            assert_eq!(
                head.next(),
                None,
                "{ctx}if the linked list has only one node, it must not be linked"
            );
            return;
        }

        let mut curr = Some(head);
        let mut actual_len = 0;
        while let Some(key) = curr {
            key.assert_valid(tail, ctx);
            curr = key.next().map(|key| {
                // Safety: see abvove
                unsafe { key.as_ref() }
            });
            actual_len += 1;
        }

        assert!(
            self.rowlen <= actual_len,
            "{ctx}linked list's rowlen was greater than its actual length"
        );
    }
}

impl Drop for KeyList<'_> {
    fn drop(&mut self) {
        // drop all keys in this list
        // note that we are very defensive here and continually keep the head ptr correct, so
        // that if we happen to panic during drop, we don't leave the list in a bad state.
        while let Some(mut head_ptr) = self.head.take() {
            // Safety: This ptr has been created through `push_key` and is owned by this list,
            // which means it is valid & safe to deref at this point.
            let head = unsafe { head_ptr.as_mut() };
            // Safety: we need to continue to treat the key as pinned
            let head = unsafe { Pin::new_unchecked(head) };

            self.head = head.next();

            if head.next().is_none() {
                self.tail = None;
            }

            // clear the pointer before dropping the key, just to be sure
            head.set_next(None);

            // Safety:
            // 1 -> all keys here are created through `push_key`, which correctly calls into_ptr.
            // 2 -> after this destructor runs, this RLookup is inaccessible making double frees impossible.
            // 3 -> RLookupKey is about to be freed, we don't need to worry about pinning anymore.
            drop(unsafe { RLookupKey::from_ptr(head_ptr) });
        }
    }
}

// ===== impl Cursor =====

impl<'list, 'a> Cursor<'list, 'a> {
    /// Move the cursor to the next [`RLookupKey`] in the [`KeyList`].
    ///
    /// Note that contrary to [`Self::next`] this **does not** skip over hidden keys.
    pub fn move_next(&mut self) {
        if let Some(curr) = self.current.take() {
            // Safety: It is safe for us to borrow `curr`, because the iteraror mutably borrows the `KeyList`,
            // ensuring it will not be dropped while the iterator exists AND we have exclusive access
            // to the keys it owns (and can therefore hand out mutable references).
            // The returned item will not outlive the iterator.
            let curr = unsafe { curr.as_ref() };

            self.current = curr.next();
        }
    }

    /// If the cursor currently points to a key, return an immutable reference to it.
    pub fn current(&self) -> Option<&RLookupKey<'a>> {
        // Safety: See Self::move_next.
        Some(unsafe { self.current?.as_ref() })
    }

    /// Consume this cursor returning an immutable reference to the current key, if any.
    pub fn into_current(self) -> Option<&'list RLookupKey<'a>> {
        // Safety: See Self::move_next.
        Some(unsafe { self.current?.as_ref() })
    }

    /// Skip a consecutive run of keys marked as "hidden". Used in the [`Iterator`] implementation.
    fn skip_hidden(&mut self) {
        while let Some(curr) = self.current()
            && curr.flags.contains(RLookupKeyFlag::Hidden)
        {
            self.move_next();
        }
    }
}

impl<'list, 'a> Iterator for Cursor<'list, 'a> {
    type Item = &'list RLookupKey<'a>;

    /// Advances the [`Cursor`] to the next [`RLookupKey`] in the [`KeyList`] and returns it.
    ///
    /// This will automatically skip over any keys with the [`RLookupKeyFlag::Hidden`] flag.
    fn next(&mut self) -> Option<Self::Item> {
        self.skip_hidden();

        // Safety: See Self::move_next.
        let curr = unsafe { self.current?.as_ref() };

        self.move_next();

        Some(curr)
    }
}

// ===== impl CursorMut =====

impl<'list, 'a> CursorMut<'list, 'a> {
    pub fn move_next(&mut self) {
        if let Some(curr) = self.current.take() {
            // Safety: It is safe for us to borrow `curr`, because the iteraror mutably borrows the `KeyList`,
            // ensuring it will not be dropped while the iterator exists AND we have exclusive access
            // to the keys it owns (and can therefore hand out mutable references).
            // The returned item will not outlive the iterator.
            let curr = unsafe { curr.as_ref() };
            self.current = curr.next();
        }
    }

    /// If the cursor currently points to a key, return a mutable reference to it.
    pub fn current(&mut self) -> Option<Pin<&mut RLookupKey<'a>>> {
        // Safety: See Self::move_next.
        let curr = unsafe { self.current?.as_mut() };

        // Safety: RLookup treats the keys are pinned always, we just need consumers of this
        // iterator to uphold the pinning invariant too
        Some(unsafe { Pin::new_unchecked(curr) })
    }

    /// Override the [`RLookupKey`] at this cursor position and extend it with the given flags.
    ///
    /// The new key will inherit the `name`, `path`, and `dstidx`, and the `flags` of the key at the current position, but
    /// receive a **new pointer identity**. The *new key* is returned.
    ///
    /// The old key remains as a hidden tombstone in the linked list.
    pub fn override_current(
        mut self,
        flags: RLookupKeyFlags,
    ) -> Option<Pin<&'list mut RLookupKey<'a>>> {
        let mut old = self.current()?;

        let new = {
            let mut old = old.as_mut().project();

            *old.name = ptr::null();
            *old.name_len = usize::MAX;
            let name = mem::take(old._name.deref_mut());

            *old.path = ptr::null();
            let path = mem::take(old._path.deref_mut());

            let new = RLookupKey::from_parts(name, path, *old.dstidx, *old.flags | flags);

            // Mark the old key as hidden, so it won't show up in iteration.
            *old.flags |= RLookupKeyFlag::Hidden;

            new
        };

        // Safety: we treat the pointer as pinned below and only hand out a pinned mutable reference.
        let mut new_ptr = unsafe { RLookupKey::into_ptr(Box::pin(new)) };

        // Safety: we have allocated the memory above, this pointer is safe to dereference.
        let new = unsafe { new_ptr.as_mut() };
        // Safety: We treat the pointer as pinned internally and never hand out references that could be moved out of (in safe Rust)
        // publicly.
        let mut new = unsafe { Pin::new_unchecked(new) };

        // link the new key into the linked-list. Since KeyList is singly-linked and we don't know yet
        // if C code is still holding on to pointers to nodes, we replicate the C behaviour here:
        //
        // 1. We copy the next pointer from old to new
        // 2. We mark the old as "Hidden" so it doesn't show up in iteration anymore
        // 3. We point old.next to the new key, so the chain isn't broken
        //
        // This in effect, replaces the old key but turning it into a "tombstone value" and forcing iteration
        // to follow this indirection.
        new.as_mut().set_next(old.next());
        old.set_next(Some(new_ptr));

        // If the old key was the tail, set the new key as the tail
        if self._rlookup.tail == self.current {
            self._rlookup.tail = Some(new_ptr);
        }

        Some(new)
    }

    /// Skip a consecutive run of keys marked as "hidden". Used in the [`Iterator`] implementation.
    fn skip_hidden(&mut self) {
        while let Some(curr) = self.current()
            && curr.flags.contains(RLookupKeyFlag::Hidden)
        {
            self.move_next();
        }
    }
}

impl<'list, 'a> Iterator for CursorMut<'list, 'a> {
    type Item = Pin<&'list mut RLookupKey<'a>>;

    /// Advances the [`CursorMut`] to the next [`RLookupKey`] in the [`KeyList`] and returns it.
    ///
    /// This will automatically skip over any keys with the [`RLookupKeyFlag::Hidden`] flag.
    fn next(&mut self) -> Option<Self::Item> {
        self.skip_hidden();

        // Safety: See Self::move_next.
        let curr = unsafe { self.current?.as_mut() };

        self.move_next();

        // Safety: RLookup treats the keys are pinned always, we just need consumers of this
        // iterator to uphold the pinning invariant too
        Some(unsafe { Pin::new_unchecked(curr) })
    }
}

// ===== impl RLookup =====

impl Default for RLookup<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> RLookup<'a> {
    pub fn new() -> Self {
        Self {
            keys: KeyList::new(),
            options: RLookupOptions::empty(),
            index_spec_cache: None,
        }
    }

    pub fn init(&mut self, spcache: IndexSpecCache) {
        // c version used memset to zero initialize, We behave the same way in release, but add a debug assert to catch misuses.
        if self.index_spec_cache.is_some() {
            debug_assert!(false, "RLookup already initialized with an IndexSpecCache");
            *self = Self::new();
        }
        self.index_spec_cache = Some(spcache);
    }

    // ===== Get key for reading (create only if in schema and sortable) =====

    /// Gets a key by its name from the lookup table, if not found it uses the schema as a fallback to search the key.
    ///
    /// If the flag `RLookupKeyFlag::AllowUnresolved` is set, it will create a new key if it does not exist in the lookup table
    /// nor in the schema.
    pub fn get_key_read(
        &mut self,
        name: &'a CStr,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        flags &= GET_KEY_FLAGS;

        // Safety: The non-lexical lifetime analysis of current Rust, incorrectly handles borrows in early
        // return statements, expanding the borrow of `self` to the scope of the entire method. This is
        // obviously not correct as `key` is either found and borrowed in which case we never reach the code after
        // the if let branch, which it's early return.
        // If the key is *not* found then key is *not* borrowed which means the code below is fine to
        // borrow it again. The current compiler is not smart enough to get this though, so we create a disjoint
        // borrow below.
        // TODO remove once <https://github.com/rust-lang/rust/issues/54663> is fixed.
        let me = unsafe { &*(self as *const Self) };
        if let Some(c) = me.keys.find_by_name(name) {
            // A cursor returned from `KeyList::find_by_name()` will always always points to a valid RLookupKey
            // therefore `into_current()` is not None here.
            return c.into_current();
        }

        // If we didn't find the key at the lookup table, check if it exists in
        // the schema as SORTABLE, and create only if so.
        if let Some(key) = self.gen_key_from_spec(name, flags) {
            let key = self.keys.push(key);

            // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned immutable reference.
            return Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) });
        }

        // If we didn't find the key in the schema (there is no schema) and unresolved is OK, create an unresolved key.
        if self.options.contains(RLookupOption::AllowUnresolved) {
            let mut key = RLookupKey::new(name, flags);
            key.flags |= RLookupKeyFlag::Unresolved;

            let key = self.keys.push(key);

            // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned immutable reference.
            return Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) });
        }

        None
    }

    // Gets a key from the schema if the field is sortable (so its data is available), unless an RP upstream
    // has promised to load the entire document.
    fn gen_key_from_spec(
        &mut self,
        name: &'a CStr,
        flags: RLookupKeyFlags,
    ) -> Option<RLookupKey<'a>> {
        let fs = self.index_spec_cache.as_ref()?.find_field(name)?;
        let fs_options = FieldSpecOptions::from_bits(fs.options()).unwrap();

        // FIXME: (from C code) LOAD ALL loads the key properties by their name, and we won't find their value by the field name
        //        if the field has a different name (alias) than its path.
        if !fs_options.contains(FieldSpecOption::Sortable)
            && !self.options.contains(RLookupOption::AllLoaded)
        {
            return None;
        }

        let mut key = RLookupKey::new(name, flags);
        key.update_from_field_spec(fs);
        Some(key)
    }

    /// Writes a key to the lookup table, if the key already exists, it is either overwritten if flags is set to `RLookupKeyFlag::Override`
    /// or returns `None` if the key is in exclusive mode.
    ///
    /// This will never get a key from the cache, it will either create a new key, override an existing key or return `None` if the key
    /// is in exclusive mode.
    pub fn get_key_write(
        &mut self,
        name: &'a CStr,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        // Safety: The non-lexical lifetime analysis of Rust, incorrectly handles borrows in early
        // return statements, expanding the borrow of `self` to the scope of the entire method. This is
        // not correct here as `key` is either found and borrowed in which case we never reach the code in
        // the outer else branch. If we reach the outer else branch then `self` is *not* found and *not*
        // borrowed which means the last line of code is fine to borrow self again.
        //
        // The current compiler is not smart enough to get this though, so we create a disjoint borrow below.
        // TODO remove once <https://github.com/rust-lang/rust/issues/54663> is fixed.
        let me = unsafe { &mut *(self as *mut Self) };

        let key = if let Some(c) = me.keys.find_by_name_mut(name) {
            // A. we found the key at the lookup table:
            if flags.contains(RLookupKeyFlag::Override) {
                // We are in create mode, overwrite the key (remove schema related data, mark with new flags)
                c.override_current(flags | RLookupKeyFlag::QuerySrc)
                    .unwrap()
            } else {
                // 1. if we are in exclusive mode, return None
                return None;
            }
        } else {
            // B. we didn't find the key at the lookup table:
            // create a new key with the name and flags
            self.keys
                .push(RLookupKey::new(name, flags | RLookupKeyFlag::QuerySrc))
        };

        // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned immutable reference.
        Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::CString;
    use std::mem::MaybeUninit;

    #[cfg(not(miri))]
    use proptest::prelude::*;

    // Make sure that the `into_ptr` and `from_ptr` functions are inverses of each other.
    #[test]
    fn into_ptr_from_ptr_roundtrip() {
        let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
        let key = Box::pin(key);

        let ptr = unsafe { RLookupKey::into_ptr(key) };
        let key = unsafe { RLookupKey::from_ptr(ptr) };

        assert_eq!(unsafe { CStr::from_ptr(key.name) }, c"test");
        assert_eq!(key.flags, RLookupKeyFlags::empty());
    }

    // Assert that creating a RLookupKey with the NameAlloc flag indeed allocates a new string
    #[test]
    fn rlookupkey_new_with_namealloc() {
        let name = c"test";

        let key = RLookupKey::new(name, make_bitflags!(RLookupKeyFlag::NameAlloc));
        assert_ne!(key.name, name.as_ptr());
        assert!(matches!(key._name, Cow::Owned(_)));
    }

    // Assert that creating a RLookupKey *without* the NameAlloc flag keeps the provided string
    #[test]
    fn rlookupkey_new_without_namealloc() {
        let name = c"test";

        let key = RLookupKey::new(name, RLookupKeyFlags::empty());
        assert_eq!(key.name, name.as_ptr());
        assert!(matches!(key._name, Cow::Borrowed(_)));
    }

    // Assert that creating a RLookupKey with the NameAlloc flag indeed allocates a new string
    #[test]
    fn rlookupkey_new_utf8_with_namealloc() {
        let name = c"🔍🔥🎶";

        let key = RLookupKey::new(name, make_bitflags!(RLookupKeyFlag::NameAlloc));
        assert_ne!(key.name, name.as_ptr());
        assert_eq!(key.name_len, 12); // 3 characters, 4 bytes each
        assert!(matches!(key._name, Cow::Owned(_)));
    }

    // Assert that creating a RLookupKey *without* the NameAlloc flag keeps the provided string
    #[test]
    fn rlookupkey_new_utf8_without_namealloc() {
        let name = c"🔍🔥🎶";

        let key = RLookupKey::new(name, RLookupKeyFlags::empty());
        assert_eq!(key.name, name.as_ptr());
        assert_eq!(key.name_len, 12); // 3 characters, 4 bytes each
        assert!(matches!(key._name, Cow::Borrowed(_)));
    }

    #[test]
    fn update_from_field_spec() {
        let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

        let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
        let field_name = c"this is the field name";
        fs.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = c"this is the field path";
        fs.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };

        key.update_from_field_spec(&fs);

        assert!(
            key.flags
                .contains(RLookupKeyFlag::DocSrc | RLookupKeyFlag::SchemaSrc)
        );
        assert_ne!(key.path, key.name);
        assert!(matches!(key._path.as_ref().unwrap(), Cow::Borrowed(_)));
        assert_eq!(
            unsafe { CStr::from_ptr(key.path) },
            c"this is the field path"
        );

        // cleanup
        unsafe {
            ffi::HiddenString_Free(fs.fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(fs.fieldPath, false);
        }
    }

    #[test]
    fn update_from_field_spec_sortable() {
        let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

        let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
        let field_name = c"this is the field name";
        fs.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = c"this is the field path";
        fs.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        fs.set_options(
            ffi::FieldSpecOptions_FieldSpec_Sortable | ffi::FieldSpecOptions_FieldSpec_UNF,
        );
        fs.sortIdx = 43;

        key.update_from_field_spec(&fs);

        assert!(key.flags.contains(
            RLookupKeyFlag::DocSrc
                | RLookupKeyFlag::SchemaSrc
                | RLookupKeyFlag::SvSrc
                | RLookupKeyFlag::ValAvailable
        ));
        assert_ne!(key.path, key.name);
        assert!(matches!(key._path.as_ref().unwrap(), Cow::Borrowed(_)));
        assert_eq!(
            unsafe { CStr::from_ptr(key.path) },
            c"this is the field path"
        );
        assert_eq!(key.svidx, 43);

        // cleanup
        unsafe {
            ffi::HiddenString_Free(fs.fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(fs.fieldPath, false);
        }
    }

    #[test]
    fn update_from_field_spec_numeric() {
        let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

        let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
        let field_name = c"this is the field name";
        fs.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = c"this is the field path";
        fs.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        fs.set_types(ffi::FieldType_INDEXFLD_T_NUMERIC);

        key.update_from_field_spec(&fs);

        assert!(key.flags.contains(
            RLookupKeyFlag::DocSrc | RLookupKeyFlag::SchemaSrc | RLookupKeyFlag::Numeric
        ));
        assert_ne!(key.path, key.name);
        assert!(matches!(key._path.as_ref().unwrap(), Cow::Borrowed(_)));
        assert_eq!(
            unsafe { CStr::from_ptr(key.path) },
            c"this is the field path"
        );

        // cleanup
        unsafe {
            ffi::HiddenString_Free(fs.fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(fs.fieldPath, false);
        }
    }

    // `update_from_field_spec` clones the name if key`RLookupKey.flags` contains `NameAlloc`
    #[test]
    fn update_from_field_spec_namealloc() {
        let mut key = RLookupKey::new(c"test", make_bitflags!(RLookupKeyFlag::NameAlloc));

        let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
        let field_name = c"this is the field name";
        fs.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = c"this is the field path";
        fs.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };

        key.update_from_field_spec(&fs);

        assert!(
            key.flags
                .contains(RLookupKeyFlag::DocSrc | RLookupKeyFlag::SchemaSrc)
        );
        assert_ne!(key.path, key.name);
        assert_eq!(
            unsafe { CStr::from_ptr(key.path) },
            c"this is the field path"
        );
        assert!(matches!(key._path.as_ref().unwrap(), Cow::Owned(_)));

        // cleanup
        unsafe {
            ffi::HiddenString_Free(fs.fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(fs.fieldPath, false);
        }
    }

    #[test]
    fn key_from_parts_only_name() {
        let name = Cow::Borrowed(c"foo");
        let key = RLookupKey::from_parts(name, None, 0, RLookupKeyFlags::empty());

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._name.as_ptr());
    }

    #[test]
    fn key_from_parts_name_and_path() {
        let name = Cow::Borrowed(c"foo");
        let path = Cow::Borrowed(c"bar");
        let key = RLookupKey::from_parts(name, Some(path), 0, RLookupKeyFlags::empty());

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._path.as_ref().unwrap().as_ptr());
    }

    // Assert that `RLookupKey::from_parts` catches the mismatch between owned name & missing namealloc flag
    #[test]
    #[allow(unreachable_code, unused)]
    #[cfg_attr(debug_assertions, should_panic)]
    fn key_from_parts_name_namealloc_fail() {
        let name = Cow::Owned(c"foo".to_owned());
        let key = RLookupKey::from_parts(name, None, 0, RLookupKeyFlags::empty());

        #[cfg(debug_assertions)]
        unreachable!();

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._name.as_ptr());
    }

    // Assert that `RLookupKey::from_parts` catches the mismatch between owned path & missing namealloc flag
    #[test]
    #[allow(unreachable_code, unused)]
    #[cfg_attr(debug_assertions, should_panic)]
    fn key_from_parts_name_nonamealloc_fail() {
        let name = Cow::Borrowed(c"foo");
        let key = RLookupKey::from_parts(name, None, 0, make_bitflags!(RLookupKeyFlag::NameAlloc));

        #[cfg(debug_assertions)]
        unreachable!();

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._name.as_ptr());
    }

    // Assert that `RLookupKey::from_parts` catches the mismatch between borrowed name & namealloc flag
    #[test]
    #[allow(unreachable_code, unused)]
    #[cfg_attr(debug_assertions, should_panic)]
    fn key_from_parts_path_namealloc_fail() {
        let name = Cow::Borrowed(c"foo");
        let path = Cow::Owned(c"bar".to_owned());
        let key = RLookupKey::from_parts(name, Some(path), 0, RLookupKeyFlags::empty());

        #[cfg(debug_assertions)]
        unreachable!();

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._path.as_ref().unwrap().as_ptr());
    }

    // Assert that `RLookupKey::from_parts` catches the mismatch between borrowed path & namealloc flag
    #[test]
    #[allow(unreachable_code, unused)]
    #[cfg_attr(debug_assertions, should_panic)]
    fn key_from_parts_path_nonamealloc_fail() {
        let name = Cow::Owned(c"foo".to_owned());
        let path = Cow::Borrowed(c"bar");
        let key = RLookupKey::from_parts(
            name,
            Some(path),
            0,
            make_bitflags!(RLookupKeyFlag::NameAlloc),
        );

        #[cfg(debug_assertions)]
        unreachable!();

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._path.as_ref().unwrap().as_ptr());
    }

    // assert that the linked list is produced and linked correctly
    #[test]
    fn keylist_push_consistency() {
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        let bar = unsafe { NonNull::from(Pin::into_inner_unchecked(bar)) };

        keylist.assert_valid("tests::keylist_push_consistency after insertions");

        assert_eq!(keylist.head.unwrap(), foo);
        assert_eq!(keylist.tail.unwrap(), bar);
        unsafe {
            assert!(foo.as_ref().has_next());
        }
        unsafe {
            assert!(!bar.as_ref().has_next());
        }
    }

    #[test]
    fn keylist_cursor() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"baz", RLookupKeyFlags::empty()));
        keylist.assert_valid("tests::keylist_iter after insertions");

        let mut c = keylist.cursor_front();
        assert_eq!(c.next().unwrap()._name.as_ref(), c"foo");
        assert_eq!(c.next().unwrap()._name.as_ref(), c"bar");
        assert_eq!(c.next().unwrap()._name.as_ref(), c"baz");
        assert!(c.next().is_none());
    }

    #[test]
    fn keylist_cursor_mut() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"baz", RLookupKeyFlags::empty()));
        keylist.assert_valid("tests::keylist_iter_mut after insertions");

        let mut c = keylist.cursor_front_mut();

        assert_eq!(c.next().unwrap()._name.as_ref(), c"foo");
        assert_eq!(c.next().unwrap()._name.as_ref(), c"bar");
        assert_eq!(c.next().unwrap()._name.as_ref(), c"baz");
        assert!(c.next().is_none());
    }

    // Assert the iterator immediately returns None if all keys are marked hidden
    #[test]
    fn keylist_cursor_all_hidden() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(
            c"bar",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(
            c"baz",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.assert_valid("tests::keylist_cursor_all_hidden after insertions");

        let mut c = keylist.cursor_front();
        assert!(c.next().is_none());
    }

    // Assert the iterator skips all keys marked hidden
    #[test]
    fn keylist_cursor_skip_hidden() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(
            c"baz",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.assert_valid("tests::keylist_cursor_skip_hidden after insertions");

        let mut c = keylist.cursor_front();
        assert_eq!(c.next().unwrap()._name.as_ref(), c"bar");
        assert!(c.next().is_none());
    }

    // Assert the iterator immediately returns None if all keys are marked hidden
    #[test]
    fn keylist_cursor_mut_all_hidden() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(
            c"bar",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(
            c"baz",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.assert_valid("tests::keylist_cursor_mut_all_hidden after insertions");

        let mut c = keylist.cursor_front_mut();
        assert!(c.next().is_none());
    }

    // Assert the iterator skips all keys marked hidden
    #[test]
    fn keylist_cursor_mut_skip_hidden() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(
            c"baz",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.assert_valid("tests::keylist_cursor_mut_skip_hidden after insertions");

        let mut c = keylist.cursor_front_mut();
        assert_eq!(c.next().unwrap()._name.as_ref(), c"bar");
        assert!(c.next().is_none());
    }

    // Assert the Cursor::move_next method DOES NOT skip keys marked hidden
    #[test]
    fn keylist_cursor_move_next() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(
            c"baz",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.assert_valid("tests::keylist_cursor_move_next after insertions");

        let mut c = keylist.cursor_front();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"foo");
        c.move_next();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"bar");
        c.move_next();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"baz");
        c.move_next();
        assert!(c.current().is_none());
    }

    // Assert the CursorMut::move_next method DOES NOT skip keys marked hidden
    #[test]
    fn keylist_cursor_mut_move_next() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(
            c"baz",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.assert_valid("tests::keylist_cursor_mut_move_next after insertions");

        let mut c = keylist.cursor_front_mut();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"foo");
        c.move_next();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"bar");
        c.move_next();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"baz");
        c.move_next();
        assert!(c.current().is_none());
    }

    #[test]
    fn keylist_find() {
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        let bar = unsafe { NonNull::from(Pin::into_inner_unchecked(bar)) };

        keylist.assert_valid("tests::keylist_find after insertions");

        let found = keylist.find_by_name(c"foo").unwrap();
        assert_eq!(NonNull::from(found.current().unwrap()), foo);

        let found = keylist.find_by_name(c"bar").unwrap();
        assert_eq!(NonNull::from(found.current().unwrap()), bar);

        assert!(keylist.find_by_name(c"baz").is_none());
    }

    #[test]
    fn keylist_find_mut() {
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        let bar = unsafe { NonNull::from(Pin::into_inner_unchecked(bar)) };

        keylist.assert_valid("tests::keylist_find_mut after insertions");

        let mut found = keylist.find_by_name_mut(c"foo").unwrap();
        assert_eq!(
            NonNull::from(unsafe { Pin::into_inner_unchecked(found.current().unwrap()) }),
            foo
        );

        let mut found = keylist.find_by_name_mut(c"bar").unwrap();
        assert_eq!(
            NonNull::from(unsafe { Pin::into_inner_unchecked(found.current().unwrap()) }),
            bar
        );

        assert!(keylist.find_by_name(c"baz").is_none());
    }

    #[test]
    fn keylist_override_key_find() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Unresolved),
        ));

        keylist
            .cursor_front_mut()
            .override_current(make_bitflags!(RLookupKeyFlag::Numeric));

        let found = keylist
            .find_by_name(c"foo")
            .expect("expected to find key by name");

        let found = found
            .current()
            .expect("cursor should have current, this is a bug");

        assert_eq!(found._name.as_ref(), c"foo");
        assert!(found._path.is_none());
        assert_eq!(found.dstidx, 0);
        // new key should have provided keys
        assert!(found.flags.contains(RLookupKeyFlag::Numeric));
        // new key should inherit any old flags
        assert!(found.flags.contains(RLookupKeyFlag::Unresolved));
    }

    #[test]
    fn keylist_override_key_iterate() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Unresolved),
        ));
        keylist
            .cursor_front_mut()
            .override_current(make_bitflags!(RLookupKeyFlag::Numeric));

        let mut c = keylist.cursor_front();

        // we expect the first item to be the tombstone of the old key
        assert!(c.current().unwrap().is_tombstone());

        // and the next item to be the new key
        c.move_next();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"foo");
    }

    #[test]
    fn keylist_override_key_tail_handling() {
        let mut keylist = KeyList::new();

        // push two keys, so we can override one without altering the tail and another one to override it.
        keylist.push(RLookupKey::new(
            c"foo",
            make_bitflags!(RLookupKeyFlag::Unresolved),
        ));
        let secoond = keylist.push(RLookupKey::new(
            c"bar",
            make_bitflags!(RLookupKeyFlag::Unresolved),
        ));
        let second = unsafe { NonNull::from(Pin::into_inner_unchecked(secoond)) };

        // store first override key
        let override1 = keylist
            .cursor_front_mut()
            .override_current(make_bitflags!(RLookupKeyFlag::Numeric));
        let override1 = unsafe { NonNull::from(Pin::into_inner_unchecked(override1.unwrap())) };

        // we expect the tail to be the second key still
        assert_ne!(override1, keylist.tail.unwrap());
        assert_eq!(second, keylist.tail.unwrap());

        // now we override the second key, which is the tail
        let mut cursor = keylist.cursor_front_mut();
        cursor.move_next(); // move to the first override
        cursor.move_next(); // move to the second key
        let override2 = cursor.override_current(make_bitflags!(RLookupKeyFlag::Numeric));
        let override2 = unsafe { NonNull::from(Pin::into_inner_unchecked(override2.unwrap())) };

        // we expect the tail to be the new key
        assert_eq!(override2, keylist.tail.unwrap());
    }

    #[test]
    fn rlookup_init() {
        let mut rlookup = RLookup::new();

        let spcache = Box::new(ffi::IndexSpecCache {
            fields: ptr::null_mut(),
            nfields: 0,
            refcount: 1,
        });
        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };

        rlookup.init(spcache);

        assert!(rlookup.index_spec_cache.is_some());
    }

    #[test]
    #[cfg_attr(debug_assertions, should_panic)]
    fn rlookup_no_reinit() {
        let mut rlookup = RLookup::new();

        let spcache = Box::new(ffi::IndexSpecCache {
            fields: ptr::null_mut(),
            nfields: 0,
            refcount: 1,
        });
        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };

        rlookup.init(spcache);
        assert!(rlookup.index_spec_cache.is_some());

        let spcache = Box::new(ffi::IndexSpecCache {
            fields: ptr::null_mut(),
            nfields: 0,
            refcount: 1,
        });
        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };

        // this should panic
        rlookup.init(spcache);
    }

    // Assert that we can successfully write keys to the rlookup
    #[test]
    fn rlookup_write_new_key() {
        let name = CString::new("new_key").unwrap();
        let flags = RLookupKeyFlags::empty();
        let mut rlookup = RLookup::new();

        // Assert that we can write a new key
        let key = rlookup.get_key_write(name.as_c_str(), flags).unwrap();
        assert_eq!(key._name.as_ref(), name.as_c_str());
        assert_eq!(key.name, name.as_ptr());
        assert!(key.flags.contains(RLookupKeyFlag::QuerySrc));
    }

    // Assert that we fail to write a key if the key already exists and no overwrite is allowed
    #[test]
    fn rlookup_write_key_multiple_times_fails() {
        let name = CString::new("new_key").unwrap();
        let flags = RLookupKeyFlags::empty();
        let mut rlookup = RLookup::new();

        // Assert that we can write a new key
        let key = rlookup.get_key_write(name.as_c_str(), flags).unwrap();
        assert_eq!(key._name.as_ref(), name.as_c_str());
        assert_eq!(key.name, name.as_ptr());
        assert!(key.flags.contains(RLookupKeyFlag::QuerySrc));

        // Assert that we cannot write the same key again without allowing overwrites
        let not_key = rlookup.get_key_write(name.as_c_str(), flags);
        assert!(not_key.is_none());
    }

    // Assert that we can override an existing key
    #[test]
    fn rlookup_write_key_override() {
        let name = CString::new("new_key").unwrap();
        let flags = RLookupKeyFlags::empty();
        let mut rlookup = RLookup::new();

        let key = rlookup.get_key_write(name.as_c_str(), flags).unwrap();
        assert_eq!(key._name.as_ref(), name.as_c_str());
        assert_eq!(key.name, name.as_ptr());
        assert!(key.flags.contains(RLookupKeyFlag::QuerySrc));

        let new_flags = make_bitflags!(RLookupKeyFlag::{ExplicitReturn | Override});

        let new_key = rlookup.get_key_write(name.as_c_str(), new_flags).unwrap();
        assert_eq!(new_key._name.as_ref(), name.as_c_str());
        assert_eq!(new_key.name, name.as_ptr());
        assert!(new_key.flags.contains(RLookupKeyFlag::QuerySrc));
        assert!(new_key.flags.contains(RLookupKeyFlag::ExplicitReturn));
    }

    #[cfg(not(miri))]
    proptest! {
         // assert that a key can in the keylist can be retrieved by its name
         #[test]
         fn rlookup_get_key_read_found(name in "\\PC+") {
             let name = CString::new(name).unwrap();

             let mut rlookup = RLookup::new();

             rlookup
                 .keys
                 .push(RLookupKey::new(&name, RLookupKeyFlags::empty()));

             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty())
                 .unwrap();
             assert_eq!(key._name.as_ref(), name.as_ref());
             assert!(key._path.is_none());
         }

         // Assert that a key cannot be retrieved by any other string
         #[test]
         fn rlookup_get_key_read_not_found(name in "\\PC+", wrong_name in "\\PC+") {
            let name = CString::new(name).unwrap();
            let wrong_name = CString::new(wrong_name).unwrap();

            if wrong_name == name {
                // skip this test if the wrong name is the same as the name
                return Ok(());
            }

             let mut rlookup = RLookup::new();

             rlookup
                 .keys
                 .push(RLookupKey::new(&name, RLookupKeyFlags::empty()));

             let not_key = rlookup
                 .get_key_read(&wrong_name, RLookupKeyFlags::empty());
             prop_assert!(not_key.is_none());
         }

         // Assert that - if the key cannot be found in the rlookups keylist - it will be loaded from the index spec cache
         // and inserted into the list
         #[test]
         fn rlookup_get_key_read_not_found_spcache_hit(name in "\\PC+", path in "\\PC+", sort_idx in 0i16..i16::MAX) {
             let name = CString::new(name).unwrap();
             let path = CString::new(path).unwrap();

             let mut rlookup = RLookup::new();

             let mut arr = unsafe {
                 [
                     MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init(),
                 ]
             };

             let field_name = name.as_c_str();
             arr[0].fieldName =
                 unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
             let field_path = path.as_c_str();
             arr[0].fieldPath =
                 unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
             arr[0].set_options(
                 ffi::FieldSpecOptions_FieldSpec_Sortable | ffi::FieldSpecOptions_FieldSpec_UNF,
             );
             arr[0].sortIdx = sort_idx;

             let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

             rlookup.init(spcache);

             // the first call will load from the index spec cache
             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty()).unwrap();

             prop_assert_eq!(key.name, name.as_ptr());
             prop_assert_eq!(key._name.as_ref(), name.as_c_str());
             prop_assert_eq!(key.path, path.as_ptr());
             prop_assert_eq!(key._path.as_ref().unwrap().as_ref(), path.as_c_str());

             // the second call will load from the keylist
             // to ensure this we zero out the cache
             rlookup.index_spec_cache = None;

             let key = rlookup
                 .get_key_read(&name, RLookupKeyFlags::empty())
                 .unwrap();
             prop_assert_eq!(key.name, name.as_ptr());
             prop_assert_eq!(key._name.as_ref(), name.as_c_str());
             prop_assert_eq!(key.path, path.as_ptr());
             prop_assert_eq!(key._path.as_ref().unwrap().as_ref(), path.as_c_str());
         }

        // Assert that, even though there is a key in the list AND a a field space in the cache, we won't load the key
        // if it is a wrong name, i.e. a name that's neither part of the list nor the cache.
         #[test]
         fn rlookup_get_key_read_not_found_no_spcache_hit(name1 in "\\PC+", name2 in "\\PC+", wrong_name in "\\PC+") {
             let name1 = CString::new(name1).unwrap();
             let name2 = CString::new(name2).unwrap();
             let wrong_name = CString::new(wrong_name).unwrap();

            if name1 == wrong_name || name2 == wrong_name {
                // skip this test if the wrong name is the same as one of the other random names
                return Ok(());
            }

             let mut rlookup = RLookup::new();

             // push a key to the keylist
             rlookup
                 .keys
                 .push(RLookupKey::new(&name1, RLookupKeyFlags::empty()));

             // push a field spec to the cache
             let mut arr = unsafe {
                 [
                     MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init(),
                 ]
             };

             let field_name = name2.as_c_str();
             arr[0].fieldName =
                 unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };

             let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

             // set the cache as the rlookup cache
             rlookup.init(spcache);

             let not_key = rlookup.get_key_read(&wrong_name, RLookupKeyFlags::empty());
             prop_assert!(not_key.is_none());
         }

        // Assert that, even though there is a key in the list AND a a field space in the cache, we won't load the key
        // if it is a wrong name, however if the flag `AllowUnresolved` is set, we will create an unresolved key instead.
         #[test]
         fn rlookup_get_key_read_not_found_no_spcache_hit_allow_unresolved(name1 in "\\PC+", name2 in "\\PC+", wrong_name in "\\PC+") {
             let name1 = CString::new(name1).unwrap();
             let name2 = CString::new(name2).unwrap();
             let wrong_name = CString::new(wrong_name).unwrap();

            if name1 == wrong_name || name2 == wrong_name {
                // skip this test if the wrong name is the same as one of the other random names
                return Ok(());
            }

             let mut rlookup = RLookup::new();

             // push a key to the keylist
             rlookup
                 .keys
                 .push(RLookupKey::new(&name1, RLookupKeyFlags::empty()));

             // push a field spec to the cache
             let mut arr = unsafe {
                 [
                     MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init(),
                 ]
             };

             let field_name = name2.as_c_str();
             arr[0].fieldName =
                 unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };

             let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

             // set the cache as the rlookup cache
             rlookup.init(spcache);

             // set the AllowUnresolved option to allow unresolved keys in this rlookup
             rlookup.options.set(RLookupOption::AllowUnresolved, true);

             let key = rlookup.get_key_read(&wrong_name, RLookupKeyFlags::empty()).unwrap();
             prop_assert!(key.flags.contains(RLookupKeyFlag::Unresolved));
             prop_assert_eq!(key.name, wrong_name.as_ptr());
             prop_assert_eq!(key._name.as_ref(), wrong_name.as_c_str());
             prop_assert_eq!(key.path, wrong_name.as_ptr());
             prop_assert!(key._path.is_none());
        }
    }
}
