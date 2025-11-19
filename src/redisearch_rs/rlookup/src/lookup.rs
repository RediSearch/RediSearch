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
#[cfg(debug_assertions)]
use crate::rlookup_id::RLookupId;
use enumflags2::{BitFlags, bitflags, make_bitflags};
use pin_project::pin_project;
use std::{
    borrow::Cow,
    cell::UnsafeCell,
    ffi::{CStr, c_char},
    mem,
    ops::{Deref, DerefMut},
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

/// Helper type to represent a set of [`RLookupKeyFlag`]s.
/// cbindgen:ignore
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

/// Helper type to represent a set of [`RLookupOption`]s.
/// cbindgen:ignore
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
///
/// cbindgen:no-export
#[pin_project(!Unpin)]
#[derive(Debug)]
#[repr(C)]
pub struct RLookupKey<'a> {
    /// RLookupKey fields exposed to C.
    // Because we must be able to re-interpret pointers to `RLookupKey` to `RLookupKeyHeader`
    // THIS MUST BE THE FIRST FIELD DONT MOVE IT
    header: RLookupKeyHeader<'a>,

    // The actual "owning" strings, we need to hold onto these
    // so the pointers in the above header stay valid. Note that you
    // MUST NEVER MOVE THESE BEFORE THE header FIELD
    #[pin]
    _name: Cow<'a, CStr>,
    #[pin]
    _path: Option<Cow<'a, CStr>>,

    #[cfg(debug_assertions)]
    rlookup_id: RLookupId,
}

#[derive(Debug)]
#[repr(C)]
pub struct RLookupKeyHeader<'a> {
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
}

/// An append-only list of [`RLookupKey`]s.
///
/// This type maintains a mapping from string names to [`RLookupKey`]s.
/// cbindgen:no-export
#[derive(Debug)]
#[repr(C)]
pub struct RLookup<'a> {
    /// RLookup fields exposed to C.
    // Because we must be able to re-interpret pointers to `RLookup` to `RLookupHeader`
    // THIS MUST BE THE FIRST FIELD DONT MOVE IT
    header: RLookupHeader<'a>,

    // Flags/options
    options: RLookupOptions,

    // If present, then GetKey will consult this list if the value is not found in
    // the existing list of keys.
    index_spec_cache: Option<IndexSpecCache>,

    #[cfg(debug_assertions)]
    id: RLookupId,
}

#[derive(Debug)]
#[repr(C)]
pub struct RLookupHeader<'a> {
    keys: KeyList<'a>,
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

/// A cursor over an [`RLookup`]s key list.
pub struct Cursor<'list, 'a> {
    _rlookup: &'list KeyList<'a>,
    current: Option<NonNull<RLookupKey<'a>>>,
}

/// A cursor over an [`RLookup`]s key list with editing operations.
pub struct CursorMut<'list, 'a> {
    _rlookup: &'list mut KeyList<'a>,
    current: Option<NonNull<RLookupKey<'a>>>,
}

// ===== impl RLookupKey =====

impl<'a> Deref for RLookupKey<'a> {
    type Target = RLookupKeyHeader<'a>;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl<'a> DerefMut for RLookupKey<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

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
    #[cfg_attr(not(debug_assertions), allow(unused_variables))]
    pub fn new(
        parent: &RLookup<'_>,
        name: impl Into<Cow<'a, CStr>>,
        flags: RLookupKeyFlags,
    ) -> Self {
        let name = match name.into() {
            Cow::Borrowed(name) if flags.contains(RLookupKeyFlag::NameAlloc) => {
                Cow::Owned(name.to_owned())
            }
            name => name,
        };

        Self {
            header: RLookupKeyHeader {
                dstidx: 0,
                svidx: 0,
                flags: flags & !TRANSIENT_FLAGS,
                name: name.as_ptr(),
                path: name.as_ptr(),
                name_len: name.count_bytes(),
                next: UnsafeCell::new(None),
            },
            _name: name,
            _path: None,
            #[cfg(debug_assertions)]
            rlookup_id: parent.id(),
        }
    }

    pub fn name(&self) -> &CStr {
        self._name.as_ref()
    }

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

    #[cfg(debug_assertions)]
    pub const fn rlookup_id(&self) -> RLookupId {
        self.rlookup_id
    }

    /// Construct an `RLookupKey` from its main parts. Prefer Self::new if you are unsure which to use.
    fn from_parts(
        name: Cow<'a, CStr>,
        path: Option<Cow<'a, CStr>>,
        dstidx: u16,
        flags: RLookupKeyFlags,
        #[cfg(debug_assertions)] rlookup_id: RLookupId,
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
            header: RLookupKeyHeader {
                dstidx,
                svidx: 0,
                flags: flags & !TRANSIENT_FLAGS,
                name: name.as_ptr(),
                // if a separate path was provided we should set the pointer accordingly
                // if not, we fall back to the name as usual
                path: path.as_ref().map_or(name.as_ptr(), |path| path.as_ptr()),
                name_len: name.count_bytes(),
                next: UnsafeCell::new(None),
            },
            _name: name,
            _path: path,
            #[cfg(debug_assertions)]
            rlookup_id,
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

    pub fn is_overridden(&self) -> bool {
        let is_overridden = self.name.is_null();

        #[cfg(any(debug_assertions, test))]
        if is_overridden {
            debug_assert!(self.name_len == usize::MAX);
            debug_assert!(self.path.is_null());
            debug_assert!(self.flags.contains(RLookupKeyFlag::Hidden))
        }

        is_overridden
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
        mem::replace(me.header.next.get_mut(), next)
    }

    #[cfg(any(debug_assertions, test))]
    fn assert_valid(&self, tail: &Self, ctx: &str) {
        assert!(
            !self.flags.intersects(TRANSIENT_FLAGS),
            "{ctx}key flags must not contain transient ({TRANSIENT_FLAGS:?}) flags. Found {:?}.",
            self.flags
        );

        if !self.is_overridden() {
            use std::ptr;

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
    #[cfg(debug_assertions)]
    pub fn cursor_front(&self) -> Cursor<'_, 'a> {
        self.assert_valid("KeyList::cursor_front");

        Cursor {
            _rlookup: self,
            current: self.head,
        }
    }
    #[cfg(not(debug_assertions))]
    pub const fn cursor_front(&self) -> Cursor<'_, 'a> {
        Cursor {
            _rlookup: self,
            current: self.head,
        }
    }

    /// Returns a [`CursorMut`] starting at the first element.
    ///
    /// The [`CursorMut`] type can be used as Iterator over this list. In addition, it may be used to manipulate the list.
    #[cfg(debug_assertions)]
    pub fn cursor_front_mut(&mut self) -> CursorMut<'_, 'a> {
        self.assert_valid("KeyList::cursor_front_mut");

        CursorMut {
            current: self.head,
            _rlookup: self,
        }
    }
    #[cfg(not(debug_assertions))]
    pub const fn cursor_front_mut(&mut self) -> CursorMut<'_, 'a> {
        CursorMut {
            current: self.head,
            _rlookup: self,
        }
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a [`Cursor`] pointing to the key if found.
    // FIXME [MOD-10315] replace with more efficient search
    fn find_by_name(&self, name: &CStr) -> Option<Cursor<'_, 'a>> {
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
    fn find_by_name_mut(&mut self, name: &CStr) -> Option<CursorMut<'_, 'a>> {
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
    /// Move the cursor to the next [`RLookupKey`].
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

    /// Consume this cursor returning an immutable reference to the current key, if any.
    pub fn into_current(self) -> Option<&'list mut RLookupKey<'a>> {
        // Safety: See Self::move_next.
        Some(unsafe { self.current?.as_mut() })
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

            old.header.name = ptr::null();
            old.header.name_len = usize::MAX;
            let name = mem::take(old._name.deref_mut());

            old.header.path = ptr::null();
            let path = mem::take(old._path.deref_mut());

            let new = RLookupKey::from_parts(
                name,
                path,
                old.header.dstidx,
                old.header.flags | flags,
                #[cfg(debug_assertions)]
                *old.rlookup_id,
            );

            // Mark the old key as hidden, so it won't show up in iteration.
            old.header.flags |= RLookupKeyFlag::Hidden;

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
}

// ===== impl RLookup =====

impl<'a> Deref for RLookup<'a> {
    type Target = RLookupHeader<'a>;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl<'a> DerefMut for RLookup<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Default for RLookup<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> RLookup<'a> {
    pub fn new() -> Self {
        Self {
            header: RLookupHeader {
                keys: KeyList::new(),
            },
            options: RLookupOptions::empty(),
            index_spec_cache: None,
            #[cfg(debug_assertions)]
            id: RLookupId::next(),
        }
    }

    pub fn init(&mut self, spcache: Option<IndexSpecCache>) {
        // c version used memset to zero initialize, We behave the same way in release, but add a debug assert to catch misuses.
        if self.index_spec_cache.is_some() {
            debug_assert!(false, "RLookup already initialized with an IndexSpecCache");
            *self = Self::new();
        }
        self.index_spec_cache = spcache;
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a [`Cursor`] pointing to the key if found.
    // FIXME [MOD-10315] replace with more efficient search
    pub fn find_key_by_name(&self, name: &CStr) -> Option<Cursor<'_, 'a>> {
        self.keys.find_by_name(name)
    }

    /// Add all non-overridden keys from `src` to `self`.
    ///
    /// For each key in src, check if it already exists *by name*.
    /// - If it does the `flag` argument controls the behaviour (skip with `RLookupKeyFlags::empty()`, override with `RLookupKeyFlag::Override`).
    /// - If it doesn't a new key will ne created.
    ///
    /// Flag handling:
    ///  * - Preserves persistent source key properties (F_SVSRC, F_HIDDEN, F_EXPLICITRETURN, etc.)
    ///  * - Filters out transient flags from source keys (F_OVERRIDE, F_FORCE_LOAD)
    ///  * - Respects caller's control flags for behavior (F_OVERRIDE, F_FORCE_LOAD, etc.)
    ///  * - Target flags = caller_flags | (source_flags & ~RLOOKUP_TRANSIENT_FLAGS)
    pub fn add_keys_from(&mut self, src: &RLookup<'a>, flags: RLookupKeyFlags) {
        let mut c = src.cursor();

        while let Some(src_key) = c.current() {
            if !src_key.is_overridden() {
                // Combine caller's control flags with source key's persistent properties
                // Only preserve non-transient flags from source (F_SVSRC, F_HIDDEN, etc.)
                // while respecting caller's control flags (F_OVERRIDE, F_FORCE_LOAD, etc.)
                let combined_flags = flags | src_key.flags & !TRANSIENT_FLAGS;

                // NB: get_key_write returns none if the key already exists and `flags` don't contain `Override`.
                // In this case, we just want to move on to the next key
                let _ = self.get_key_write(src_key._name.clone(), combined_flags);
            }

            c.move_next();
        }
    }

    /// Returns a [`Cursor`] starting at the first key.
    ///
    /// The [`Cursor`] type can be used as Iterator over the keys in this lookup.
    #[inline(always)]
    pub fn cursor(&self) -> Cursor<'_, 'a> {
        self.keys.cursor_front()
    }

    /// Returns a [`Cursor`] starting at the first key.
    ///
    /// The [`Cursor`] type can be used as Iterator over the keys in this lookup.
    #[inline(always)]
    pub fn cursor_mut(&mut self) -> CursorMut<'_, 'a> {
        self.keys.cursor_front_mut()
    }

    #[cfg(debug_assertions)]
    pub const fn id(&self) -> RLookupId {
        self.id
    }

    // ===== Get key for reading (create only if in schema and sortable) =====

    /// Gets a key by its name from the lookup table, if not found it uses the schema as a fallback to search the key.
    ///
    /// If the flag `RLookupKeyFlag::AllowUnresolved` is set, it will create a new key if it does not exist in the lookup table
    /// nor in the schema.
    pub fn get_key_read(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        flags &= GET_KEY_FLAGS;

        let name = name.into();

        let available = self.keys.find_by_name(&name).is_some();
        if available {
            // FIXME: We cannot use let-some above because of a borrow-checker false positive.
            // This duplication might have performance implications.
            // See <https://github.com/rust-lang/rust/issues/54663>
            return self.keys.find_by_name(&name).unwrap().into_current();
        }

        // If we didn't find the key at the lookup table, check if it exists in
        // the schema as SORTABLE, and create only if so.
        let name = match self.gen_key_from_spec(name, flags) {
            Ok(key) => {
                let key = self.keys.push(key);

                // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned immutable reference.
                return Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) });
            }
            Err(name) => name,
        };

        // If we didn't find the key in the schema (there is no schema) and unresolved is OK, create an unresolved key.
        if self.options.contains(RLookupOption::AllowUnresolved) {
            let mut key = RLookupKey::new(self, name, flags);
            key.flags |= RLookupKeyFlag::Unresolved;

            let key = self.keys.push(key);

            // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned immutable reference.
            return Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) });
        }

        None
    }

    // Gets a key from the schema if the field is sortable (so its data is available), unless an RP upstream
    // has promised to load the entire document.
    //
    // # Errors
    //
    // If the key cannot be created, either because there is no IndexSpecCache associated with this RLookup OR,
    // because the field is not sortable the name will be returned in the `Err` variant.
    fn gen_key_from_spec(
        &mut self,
        name: Cow<'a, CStr>,
        flags: RLookupKeyFlags,
    ) -> Result<RLookupKey<'a>, Cow<'a, CStr>> {
        let Some(fs) = self
            .index_spec_cache
            .as_ref()
            .and_then(|spcache| spcache.find_field(&name))
        else {
            return Err(name);
        };
        let fs_options = FieldSpecOptions::from_bits(fs.options()).unwrap();

        // FIXME: (from C code) LOAD ALL loads the key properties by their name, and we won't find their value by the field name
        //        if the field has a different name (alias) than its path.
        if !fs_options.contains(FieldSpecOption::Sortable)
            && !self.options.contains(RLookupOption::AllLoaded)
        {
            return Err(name);
        }

        let mut key = RLookupKey::new(self, name, flags);
        key.update_from_field_spec(fs);
        Ok(key)
    }

    /// Writes a key to the lookup table, if the key already exists, it is either overwritten if flags is set to `RLookupKeyFlag::Override`
    /// or returns `None` if the key is in exclusive mode.
    ///
    /// This will never get a key from the cache, it will either create a new key, override an existing key or return `None` if the key
    /// is in exclusive mode.
    pub fn get_key_write(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        let name = name.into();

        if let Some(c) = self.keys.find_by_name_mut(&name) {
            // A. we found the key at the lookup table:
            if flags.contains(RLookupKeyFlag::Override) {
                // We are in create mode, overwrite the key (remove schema related data, mark with new flags)
                c.override_current(flags | RLookupKeyFlag::QuerySrc)
                    .unwrap();
            } else {
                // 1. if we are in exclusive mode, return None
                return None;
            }
        } else {
            // B. we didn't find the key at the lookup table:
            // create a new key with the name and flags
            let key = RLookupKey::new(self, name.clone(), flags | RLookupKeyFlag::QuerySrc);
            self.keys.push(key);
        };

        // FIXME: Duplication because of borrow-checker false positive. Duplication means performance implications.
        // See <https://github.com/rust-lang/rust/issues/54663>
        let cursor = self
            .keys
            .find_by_name(&name)
            .expect("key should have been created above");
        Some(cursor.into_current().unwrap())
    }

    // ===== Load key from redis keyspace (include known information on the key, fail if already loaded) =====

    pub fn get_key_load(
        &mut self,
        name: impl Into<Cow<'a, CStr>>,
        field_name: &'a CStr,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        let name = name.into();

        // 1. if the key is already loaded, or it has created by earlier RP for writing, return NULL (unless override was requested)
        // 2. create a new key with the name of the field, and mark it as doc-source.
        // 3. if the key is in the schema, mark it as schema-source and apply all the relevant flags according to the field spec.
        // 4. if the key is "loaded" at this point (in schema, sortable and un-normalized), create the key but return NULL
        //    (no need to load it from the document).

        // Ensure the key is available, if it is check for flags and return None or override the key depending on flags, if key not available insert it.
        if let Some(mut c) = self.keys.find_by_name_mut(&name) {
            let key = c.current().unwrap();

            if (key.flags.contains(RLookupKeyFlag::ValAvailable)
                && !key.flags.contains(RLookupKeyFlag::IsLoaded))
                && !key
                    .flags
                    .intersects(RLookupKeyFlag::Override | RLookupKeyFlag::ForceLoad)
                || (key.flags.contains(RLookupKeyFlag::IsLoaded)
                    && !flags.contains(RLookupKeyFlag::Override))
                || (key.flags.contains(RLookupKeyFlag::QuerySrc)
                    && !flags.contains(RLookupKeyFlag::Override))
            {
                // We found a key with the same name. We return NULL if:
                // 1. The key has the origin data available (from the sorting vector, UNF) and the caller didn't
                //    request to override or forced loading.
                // 2. The key is already loaded (from the document) and the caller didn't request to override.
                // 3. The key was created by the query (upstream) and the caller didn't request to override.

                let key = key.project();

                // If the caller wanted to mark this key as explicit return, mark it as such even if we don't return it.
                key.header.flags |= flags & RLookupKeyFlag::ExplicitReturn;

                return None;
            } else {
                c.override_current(flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded)
                    .unwrap();
            }
        } else {
            let key = RLookupKey::new(
                self,
                name.clone(),
                flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded,
            );
            self.keys.push(key);
        };

        // FIXME: Duplication because of borrow-checker false positive. Duplication means performance implications.
        // See <https://github.com/rust-lang/rust/issues/54663>
        let mut cursor = self
            .header
            .keys
            .find_by_name_mut(&name)
            .expect("key should have been created above");
        let key = if let Some(fs) = self
            .index_spec_cache
            .as_ref()
            .and_then(|spcache| spcache.find_field(&name))
        {
            let key = cursor.into_current().unwrap();
            key.update_from_field_spec(fs);

            if key.flags.contains(RLookupKeyFlag::ValAvailable)
                && !flags.contains(RLookupKeyFlag::ForceLoad)
            {
                // If the key is marked as "value available", it means that it is sortable and un-normalized.
                // so we can use the sorting vector as the source, and we don't need to load it from the document.
                return None;
            }
            key
        } else {
            // Field not found in the schema.
            let mut key = cursor.current().unwrap();
            let is_borrowed = matches!(key._name, Cow::Borrowed(_));
            let mut key = key.as_mut().project();

            // We assume `field_name` is the path to load from in the document.
            if is_borrowed {
                key.header.path = field_name.as_ptr();
                *key._path = Some(Cow::Borrowed(field_name));
            } else if name.as_ref() != field_name {
                let field_name: Cow<'_, CStr> = Cow::Owned(field_name.to_owned());
                key.header.path = field_name.as_ptr();
                *key._path = Some(field_name);
            } // else
            // If the caller requested to allocate the name, and the name is the same as the path,
            // it was already set to the same allocation for the name, so we don't need to do anything.

            cursor.into_current().unwrap()
        };

        Some(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::CString;
    use std::mem::MaybeUninit;

    #[cfg(not(miri))]
    use proptest::prelude::*;

    // Compile time check to ensure that `RLookupKey` can safely be re-interpreted as `RLookupKeyHeader` (has the same
    // layout at the beginning).
    const _: () = {
        // RLookupKey is larger than RLookupKeyHeader because it has additional Rust fields
        assert!(std::mem::size_of::<RLookupKey>() >= std::mem::size_of::<RLookupKeyHeader>());
        assert!(std::mem::align_of::<RLookupKey>() == std::mem::align_of::<RLookupKeyHeader>());

        assert!(
            ::std::mem::offset_of!(RLookupKey, header.dstidx)
                == ::std::mem::offset_of!(RLookupKeyHeader, dstidx)
        );
        assert!(
            ::std::mem::offset_of!(RLookupKey, header.svidx)
                == ::std::mem::offset_of!(RLookupKeyHeader, svidx)
        );
        assert!(
            ::std::mem::offset_of!(RLookupKey, header.flags)
                == ::std::mem::offset_of!(RLookupKeyHeader, flags)
        );
        assert!(
            ::std::mem::offset_of!(RLookupKey, header.path)
                == ::std::mem::offset_of!(RLookupKeyHeader, path)
        );
        assert!(
            ::std::mem::offset_of!(RLookupKey, header.name)
                == ::std::mem::offset_of!(RLookupKeyHeader, name)
        );
        assert!(
            ::std::mem::offset_of!(RLookupKey, header.name_len)
                == ::std::mem::offset_of!(RLookupKeyHeader, name_len)
        );
        assert!(
            ::std::mem::offset_of!(RLookupKey, header.next)
                == ::std::mem::offset_of!(RLookupKeyHeader, next)
        );
    };

    // Compile time check to ensure that `RLookup` can safely be re-interpreted as `RLookupHeader` (has the same
    // layout at the beginning).
    const _: () = {
        // RLookup is larger than RLookupHeader because it has additional Rust fields
        assert!(std::mem::size_of::<RLookup>() >= std::mem::size_of::<RLookupHeader>());
        assert!(std::mem::align_of::<RLookup>() == std::mem::align_of::<RLookupHeader>());

        assert!(
            ::std::mem::offset_of!(RLookup, header.keys)
                == ::std::mem::offset_of!(RLookupHeader, keys)
        );
    };

    // Make sure that the `into_ptr` and `from_ptr` functions are inverses of each other.
    #[test]
    fn into_ptr_from_ptr_roundtrip() {
        let rlookup = RLookup::new();

        let key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());
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

        let rlookup = RLookup::new();

        let key = RLookupKey::new(&rlookup, name, make_bitflags!(RLookupKeyFlag::NameAlloc));
        assert_ne!(key.name, name.as_ptr());
        assert!(matches!(key._name, Cow::Owned(_)));
    }

    // Assert that creating a RLookupKey *without* the NameAlloc flag keeps the provided string
    #[test]
    fn rlookupkey_new_without_namealloc() {
        let name = c"test";

        let rlookup = RLookup::new();

        let key = RLookupKey::new(&rlookup, name, RLookupKeyFlags::empty());
        assert_eq!(key.name, name.as_ptr());
        assert!(matches!(key._name, Cow::Borrowed(_)));
    }

    // Assert that creating a RLookupKey with the NameAlloc flag indeed allocates a new string
    #[test]
    fn rlookupkey_new_utf8_with_namealloc() {
        let name = c"🔍🔥🎶";

        let rlookup = RLookup::new();

        let key = RLookupKey::new(&rlookup, name, make_bitflags!(RLookupKeyFlag::NameAlloc));
        assert_ne!(key.name, name.as_ptr());
        assert_eq!(key.name_len, 12); // 3 characters, 4 bytes each
        assert!(matches!(key._name, Cow::Owned(_)));
    }

    // Assert that creating a RLookupKey *without* the NameAlloc flag keeps the provided string
    #[test]
    fn rlookupkey_new_utf8_without_namealloc() {
        let name = c"🔍🔥🎶";

        let rlookup = RLookup::new();

        let key = RLookupKey::new(&rlookup, name, RLookupKeyFlags::empty());
        assert_eq!(key.name, name.as_ptr());
        assert_eq!(key.name_len, 12); // 3 characters, 4 bytes each
        assert!(matches!(key._name, Cow::Borrowed(_)));
    }

    #[test]
    fn update_from_field_spec() {
        let rlookup = RLookup::new();

        let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());

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
        let rlookup = RLookup::new();

        let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());

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
        let rlookup = RLookup::new();

        let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());

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
        let rlookup = RLookup::new();

        let mut key = RLookupKey::new(&rlookup, c"test", make_bitflags!(RLookupKeyFlag::NameAlloc));

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
        let key = RLookupKey::from_parts(
            name,
            None,
            0,
            RLookupKeyFlags::empty(),
            #[cfg(debug_assertions)]
            RLookupId::next(),
        );

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._name.as_ptr());
    }

    #[test]
    fn key_from_parts_name_and_path() {
        let name = Cow::Borrowed(c"foo");
        let path = Cow::Borrowed(c"bar");
        let key = RLookupKey::from_parts(
            name,
            Some(path),
            0,
            RLookupKeyFlags::empty(),
            #[cfg(debug_assertions)]
            RLookupId::next(),
        );

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._path.as_ref().unwrap().as_ptr());
    }

    // Assert that `RLookupKey::from_parts` catches the mismatch between owned name & missing namealloc flag
    #[test]
    #[allow(unreachable_code, unused)]
    #[cfg_attr(debug_assertions, should_panic)]
    fn key_from_parts_name_namealloc_fail() {
        let name = Cow::Owned(c"foo".to_owned());
        let key = RLookupKey::from_parts(
            name,
            None,
            0,
            RLookupKeyFlags::empty(),
            #[cfg(debug_assertions)]
            RLookupId::next(),
        );

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
        let key = RLookupKey::from_parts(
            name,
            None,
            0,
            make_bitflags!(RLookupKeyFlag::NameAlloc),
            #[cfg(debug_assertions)]
            RLookupId::next(),
        );

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
        let key = RLookupKey::from_parts(
            name,
            Some(path),
            0,
            RLookupKeyFlags::empty(),
            #[cfg(debug_assertions)]
            RLookupId::next(),
        );

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
            #[cfg(debug_assertions)]
            RLookupId::next(),
        );

        #[cfg(debug_assertions)]
        unreachable!();

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._path.as_ref().unwrap().as_ptr());
    }

    // assert that the linked list is produced and linked correctly
    #[test]
    fn keylist_push_consistency() {
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(&rlookup, c"foo", RLookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(&rlookup, c"bar", RLookupKeyFlags::empty()));
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

    // Assert the Cursor::move_next method DOES NOT skip keys marked hidden
    #[test]
    fn keylist_cursor_move_next() {
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            &rlookup,
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(&rlookup, c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(
            &rlookup,
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
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            &rlookup,
            c"foo",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keylist.push(RLookupKey::new(&rlookup, c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(
            &rlookup,
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
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(&rlookup, c"foo", RLookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(&rlookup, c"bar", RLookupKeyFlags::empty()));
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
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(&rlookup, c"foo", RLookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(&rlookup, c"bar", RLookupKeyFlags::empty()));
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
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            &rlookup,
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
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(
            &rlookup,
            c"foo",
            make_bitflags!(RLookupKeyFlag::Unresolved),
        ));
        keylist
            .cursor_front_mut()
            .override_current(make_bitflags!(RLookupKeyFlag::Numeric));

        let mut c = keylist.cursor_front();

        // we expect the first item to be the tombstone of the old key
        assert!(c.current().unwrap().is_overridden());

        // and the next item to be the new key
        c.move_next();
        assert_eq!(c.current().unwrap()._name.as_ref(), c"foo");
    }

    #[test]
    fn keylist_override_key_tail_handling() {
        let rlookup = RLookup::new();
        let mut keylist = KeyList::new();

        // push two keys, so we can override one without altering the tail and another one to override it.
        keylist.push(RLookupKey::new(
            &rlookup,
            c"foo",
            make_bitflags!(RLookupKeyFlag::Unresolved),
        ));
        let secoond = keylist.push(RLookupKey::new(
            &rlookup,
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

        rlookup.init(Some(spcache));

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

        rlookup.init(Some(spcache));
        assert!(rlookup.index_spec_cache.is_some());

        let spcache = Box::new(ffi::IndexSpecCache {
            fields: ptr::null_mut(),
            nfields: 0,
            refcount: 1,
        });
        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };

        // this should panic
        rlookup.init(Some(spcache));
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

    // Assert that a key can be loaded from the RLookup even if we have no associated index spec cache
    #[test]
    fn rlookup_get_key_load_override_no_spcache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        let mut rlookup = RLookup::new();

        let key = RLookupKey::new(&rlookup, key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(key_name, field_name, RLookupKeyFlags::empty())
            .expect("expected to find key by name");

        assert_eq!(retrieved_key._name.as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key._path.as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    // Assert that a key can be retrieved by its name and is been overridden with the `DocSrc` and `IsLoaded` flags.
    #[test]
    fn rlookup_get_key_load_override_no_field_in_cache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        // we don't use the cache
        let empty_field_array = [];
        let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

        let mut rlookup = RLookup::new();
        rlookup.init(Some(spcache));

        let key = RLookupKey::new(&rlookup, key_name, RLookupKeyFlags::empty());
        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key._name.as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key._path.as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    // Assert that a key can be retrieved by its name and is been overridden with the `DocSrc` and `IsLoaded` flags.
    #[cfg(not(miri))] // uses strncmp under the hood for HiddenString
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let mut arr = unsafe { [MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init()] };
        let field_name = key_name;
        arr[0].fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = cache_field_name;
        arr[0].fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        arr[0].set_options(ffi::FieldSpecOptions_FieldSpec_Sortable);
        arr[0].sortIdx = 12;
        let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

        let mut rlookup = RLookup::new();
        rlookup.init(Some(spcache));

        let key = RLookupKey::new(&rlookup, key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key._name.as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, cache_field_name.as_ptr());
        assert_eq!(
            retrieved_key._path.as_ref().unwrap().as_ref(),
            cache_field_name
        );
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));

        // cleanup
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldPath, false);
        }
    }

    #[cfg(not(miri))] // uses strncmp under the hood for HiddenString
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache_but_value_availabe() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let mut arr = unsafe { [MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init()] };
        let field_name = key_name;
        arr[0].fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = cache_field_name;
        arr[0].fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        arr[0].set_options(
            ffi::FieldSpecOptions_FieldSpec_Sortable | ffi::FieldSpecOptions_FieldSpec_UNF,
        );
        arr[0].sortIdx = 12;
        let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

        let mut rlookup = RLookup::new();
        rlookup.init(Some(spcache));

        let key = RLookupKey::new(&rlookup, key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup.get_key_load(
            key_name,
            field_name,
            make_bitflags!(RLookupKeyFlag::Override),
        );

        // we should access the sorting vector instead
        assert!(retrieved_key.is_none());

        // cleanup
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldPath, false);
        }
    }

    #[cfg(not(miri))] // uses strncmp under the hood for HiddenString
    #[test]
    fn rlookup_get_key_load_override_with_field_in_cache_but_value_availabe_however_force_load() {
        // setup:
        let key_name = c"key_also_cache";
        let cache_field_name = c"name_in_doc";

        // Let's create a cache with one field spec
        let mut arr = unsafe { [MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init()] };
        let field_name = key_name;
        arr[0].fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = cache_field_name;
        arr[0].fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        arr[0].set_options(
            ffi::FieldSpecOptions_FieldSpec_Sortable | ffi::FieldSpecOptions_FieldSpec_UNF,
        );
        arr[0].sortIdx = 12;
        let spcache = unsafe { IndexSpecCache::from_slice(&arr) };

        let mut rlookup = RLookup::new();
        rlookup.init(Some(spcache));

        let key = RLookupKey::new(&rlookup, key_name, RLookupKeyFlags::empty());

        rlookup.keys.push(key);

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::{Override | ForceLoad}),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key._name.as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, cache_field_name.as_ptr());
        assert_eq!(
            retrieved_key._path.as_ref().unwrap().as_ref(),
            cache_field_name
        );
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));

        // cleanup
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldName, false);
        }
        unsafe {
            ffi::HiddenString_Free(arr[0].fieldPath, false);
        }
    }

    // Assert the the cases in which None is returned also the key could be found
    #[test]
    fn rlookup_get_key_load_returns_none_although_key_is_available() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";
        let key_flags = [
            RLookupKeyFlag::ValAvailable,
            RLookupKeyFlag::IsLoaded,
            RLookupKeyFlag::QuerySrc,
        ];

        for flag in key_flags {
            // we don't use the cache
            let empty_field_array = [];
            let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

            let mut rlookup = RLookup::new();
            rlookup.init(Some(spcache));

            let key = RLookupKey::new(&rlookup, key_name, flag.into());

            rlookup.keys.push(key);

            let retrieved_key =
                rlookup.get_key_load(key_name, field_name, RLookupKeyFlags::empty());
            assert!(retrieved_key.is_none());
            if let Some(key) = rlookup.get_key_read(key_name, RLookupKeyFlags::empty()) {
                assert!(!key.flags.contains(RLookupKeyFlag::ExplicitReturn));
            } else {
                panic!("expected to find key by name");
            }

            // let's use the load to tag explicit return
            let opt =
                rlookup.get_key_load(key_name, field_name, RLookupKeyFlag::ExplicitReturn.into());
            assert!(opt.is_none(), "expected None, got {opt:?}");

            if let Some(key) = rlookup.get_key_read(key_name, RLookupKeyFlags::empty()) {
                assert!(key.flags.contains(RLookupKeyFlag::ExplicitReturn));
            } else {
                panic!("expected to find key by name");
            }
        }
    }

    #[test]
    fn rlookup_get_load_key_on_empty_rlookup_and_cache() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"name_in_doc";

        // we don't use the cache
        let empty_field_array = [];
        let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

        let mut rlookup = RLookup::new();
        rlookup.init(Some(spcache));

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key._name.as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key._path.as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    #[test]
    fn rlookup_get_load_key_name_equals_field_name() {
        // setup:
        let key_name = c"key_no_cache";
        let field_name = c"key_no_cache";

        // we don't use the cache
        let empty_field_array = [];
        let spcache = unsafe { IndexSpecCache::from_slice(&empty_field_array) };

        let mut rlookup = RLookup::new();
        rlookup.init(Some(spcache));

        let retrieved_key = rlookup
            .get_key_load(
                key_name,
                field_name,
                make_bitflags!(RLookupKeyFlag::Override),
            )
            .expect("expected to find key by name");

        assert_eq!(retrieved_key._name.as_ref(), key_name);
        assert_eq!(retrieved_key.name, key_name.as_ptr());
        assert_eq!(retrieved_key.path, field_name.as_ptr());
        assert_eq!(retrieved_key._path.as_ref().unwrap().as_ref(), field_name);
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::DocSrc));
        assert!(retrieved_key.flags.contains(RLookupKeyFlag::IsLoaded));
    }

    #[test]
    fn rlookup_add_fields_from_empty_dst() {
        let mut src = RLookup::new();
        src.get_key_write(c"foo", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"bar", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"baz", RLookupKeyFlags::empty()).unwrap();

        let mut dst = RLookup::new();
        dst.add_keys_from(&src, RLookupKeyFlags::empty());

        assert!(dst.keys.find_by_name(c"foo").is_some());
        assert!(dst.keys.find_by_name(c"bar").is_some());
        assert!(dst.keys.find_by_name(c"baz").is_some());
    }

    /// Asserts that if a key already exists in `dst` AND the `Override` flag is set, it will override that key.
    #[test]
    fn rlookup_add_fields_exists_override() {
        let mut src = RLookup::new();
        src.get_key_write(c"foo", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"bar", RLookupKeyFlags::empty()).unwrap();
        let src_baz = &raw const *src
            .get_key_write(c"baz", make_bitflags!(RLookupKeyFlag::ExplicitReturn))
            .unwrap();

        let mut dst = RLookup::new();
        let old_dst_baz = &raw const *dst.get_key_write(c"baz", RLookupKeyFlags::empty()).unwrap();

        dst.add_keys_from(&src, make_bitflags!(RLookupKeyFlag::Override));

        assert!(dst.keys.find_by_name(c"foo").is_some());
        assert!(dst.keys.find_by_name(c"bar").is_some());
        assert!(dst.keys.find_by_name(c"baz").is_some());
        let dst_baz = dst
            .keys
            .find_by_name(c"baz")
            .unwrap()
            .into_current()
            .unwrap();

        // the new key should have a different address than both src and old dst keys
        assert!(!ptr::addr_eq(src_baz, &raw const *dst_baz));
        assert!(!ptr::addr_eq(old_dst_baz, &raw const *dst_baz));

        // BUT the new key should contain the `src` flags
        assert!(dst_baz.flags == make_bitflags!(RLookupKeyFlag::{ExplicitReturn | QuerySrc}));
    }

    /// Asserts that if a key already exists in `dst` AND the `Override` flag is NOT set, it will skip copying that key.
    #[test]
    fn rlookup_add_fields_exists_skip() {
        let mut src = RLookup::new();
        src.get_key_write(c"foo", RLookupKeyFlags::empty()).unwrap();
        src.get_key_write(c"bar", RLookupKeyFlags::empty()).unwrap();
        let src_baz = &raw const *src.get_key_write(c"baz", RLookupKeyFlags::empty()).unwrap();

        let mut dst = RLookup::new();
        let old_dst_baz = &raw const *dst
            .get_key_write(c"baz", make_bitflags!(RLookupKeyFlag::ExplicitReturn))
            .unwrap();

        dst.add_keys_from(&src, RLookupKeyFlags::empty());

        assert!(dst.keys.find_by_name(c"foo").is_some());
        assert!(dst.keys.find_by_name(c"bar").is_some());
        assert!(dst.keys.find_by_name(c"baz").is_some());
        let dst_baz = dst
            .keys
            .find_by_name(c"baz")
            .unwrap()
            .into_current()
            .unwrap();

        // the new key should have a different address than the src key
        assert!(!ptr::addr_eq(src_baz, &raw const *dst_baz));
        // but the same address as the old baz
        assert!(ptr::addr_eq(old_dst_baz, &raw const *dst_baz));
        // and should still contain all the old flags
        assert!(dst_baz.flags == make_bitflags!(RLookupKeyFlag::{ExplicitReturn | QuerySrc}));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn rlookup_returns_branded_keys() {
        let mut rlookup = RLookup::new();

        rlookup.options.set(RLookupOption::AllowUnresolved, true);
        {
            let key = rlookup
                .get_key_read(c"foo", RLookupKeyFlags::empty())
                .unwrap();
            assert_eq!(key.rlookup_id(), rlookup.id());
        }
        rlookup.options.set(RLookupOption::AllowUnresolved, false);

        let key = rlookup
            .get_key_write(c"bar", RLookupKeyFlags::empty())
            .unwrap();
        assert_eq!(key.rlookup_id(), rlookup.id());

        let key = rlookup
            .get_key_load(c"baz", c"field", RLookupKeyFlags::empty())
            .unwrap();
        assert_eq!(key.rlookup_id(), rlookup.id());
    }

    #[cfg(not(miri))]
    proptest! {
         // assert that a key can in the keylist can be retrieved by its name
         #[test]
         fn rlookup_get_key_read_found(name in "\\PC+") {
             let name = CString::new(name).unwrap();

             let mut rlookup = RLookup::new();

             let key = RLookupKey::new(&rlookup, &name, RLookupKeyFlags::empty());

             rlookup.keys.push(key);

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

             let key = RLookupKey::new(&rlookup, &name, RLookupKeyFlags::empty());
             rlookup.keys.push(key);

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

             rlookup.init(Some(spcache));

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

             // cleanup
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldName, false);
             }
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldPath, false);
             }
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
             let key = RLookupKey::new(&rlookup, &name1, RLookupKeyFlags::empty());
             rlookup.keys.push(key);

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
             rlookup.init(Some(spcache));

             let not_key = rlookup.get_key_read(&wrong_name, RLookupKeyFlags::empty());
             prop_assert!(not_key.is_none());

             // cleanup
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldName, false);
             }
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

             let key = RLookupKey::new(&rlookup, &name1, RLookupKeyFlags::empty());

             rlookup.keys.push(key);

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
             rlookup.init(Some(spcache));

             // set the AllowUnresolved option to allow unresolved keys in this rlookup
             rlookup.options.set(RLookupOption::AllowUnresolved, true);

             let key = rlookup.get_key_read(&wrong_name, RLookupKeyFlags::empty()).unwrap();
             prop_assert!(key.flags.contains(RLookupKeyFlag::Unresolved));
             prop_assert_eq!(key.name, wrong_name.as_ptr());
             prop_assert_eq!(key._name.as_ref(), wrong_name.as_c_str());
             prop_assert_eq!(key.path, wrong_name.as_ptr());
             prop_assert!(key._path.is_none());

             // cleanup
             unsafe {
                 ffi::HiddenString_Free(arr[0].fieldName, false);
             }
        }
    }
}
