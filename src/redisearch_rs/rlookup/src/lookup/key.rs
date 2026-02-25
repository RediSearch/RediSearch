/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

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

use enumflags2::{BitFlags, bitflags, make_bitflags};
use pin_project::pin_project;

use crate::bindings::{FieldSpecOption, FieldSpecOptions, FieldSpecType, FieldSpecTypes};

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
pub const GET_KEY_FLAGS: RLookupKeyFlags =
    make_bitflags!(RLookupKeyFlag::{Override | Hidden | ExplicitReturn | ForceLoad});

/// Flags do not persist to the key, they are just options to [`RLookup::get_key_read`], [`RLookup::get_key_write`], or [`RLookup::get_key_load`].
pub const TRANSIENT_FLAGS: RLookupKeyFlags =
    make_bitflags!(RLookupKeyFlag::{Override | ForceLoad | NameAlloc});

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
    pub(crate) header: RLookupKeyHeader<'a>,

    // The actual "owning" strings, we need to hold onto these
    // so the pointers in the above header stay valid. Note that you
    // MUST NEVER MOVE THESE BEFORE THE header FIELD
    #[pin]
    _name: Cow<'a, CStr>,
    #[pin]
    _path: Option<Cow<'a, CStr>>,
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
    /// Constructs a new `RLookupKey` using the provided `name` and `flags`.
    pub fn new(name: impl Into<Cow<'a, CStr>>, flags: RLookupKeyFlags) -> Self {
        debug_assert!(
            !flags.contains(RLookupKeyFlag::NameAlloc),
            "The NameAlloc flag should have been handled in the FFI function. This is a bug."
        );

        let name = name.into();

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
        }
    }

    /// Constructs a new `RLookupKey` using the provided `name`, `path` and `flags`.
    pub fn new_with_path(
        name: impl Into<Cow<'a, CStr>>,
        path: impl Into<Cow<'a, CStr>>,
        flags: RLookupKeyFlags,
    ) -> Self {
        debug_assert!(
            !flags.contains(RLookupKeyFlag::NameAlloc),
            "The NameAlloc flag should have been handled in the FFI function. This is a bug."
        );

        let mut new = Self::new(name, flags);
        let path = path.into();
        new.path = path.as_ptr();
        new._path = Some(path);

        new
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
    pub(crate) unsafe fn from_ptr(ptr: NonNull<Self>) -> Pin<Box<Self>> {
        // This function must be kept in sync with `Self::into_ptr` above.

        // Safety:
        // 1 -> This function will only ever be called through `RLookup::drop`.
        //      We therefore know - because push_key creates pointers through `into_ptr` - that the invariant is upheld.
        // 2 -> Has to be upheld by the caller
        let b = unsafe { Box::from_raw(ptr.as_ptr()) };
        // Safety: 3 -> Caller has to uphold the pin contract
        unsafe { Pin::new_unchecked(b) }
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
    pub(crate) unsafe fn into_ptr(me: Pin<Box<Self>>) -> NonNull<Self> {
        // This function must be kept in sync with `Self::from_ptr`.

        // Safety: The caller promised to continue to treat the returned pointer
        // as pinned and never move out of it.
        let ptr = Box::into_raw(unsafe { Pin::into_inner_unchecked(me) });

        // Safety: we know the ptr we get from Box::into_raw is never null
        unsafe { NonNull::new_unchecked(ptr) }
    }

    pub const fn name(&self) -> &Cow<'a, CStr> {
        &self._name
    }

    pub const fn path(&self) -> &Option<Cow<'a, CStr>> {
        &self._path
    }

    pub fn is_tombstone(&self) -> bool {
        let is_tombstone = self.name.is_null();

        #[cfg(any(debug_assertions, test))]
        if is_tombstone {
            debug_assert!(self.name_len == usize::MAX);
            debug_assert!(self.path.is_null());
            debug_assert!(self.flags.contains(RLookupKeyFlag::Hidden))
        }

        is_tombstone
    }

    /// Returns `true` if this node is currently linked to a [`List`].
    #[cfg(test)]
    pub(crate) fn has_next(&self) -> bool {
        self.next().is_some()
    }

    /// Return the next pointer in the linked list
    #[inline]
    pub(crate) fn next(&self) -> Option<NonNull<RLookupKey<'a>>> {
        // Safety: RLookupKeys are created through `KeyList::push` and owned by the `List`. We
        // can therefore assume this pointer is safe to dereference at this point.
        unsafe { *self.next.get() }
    }

    /// Update the pointer to the next node
    #[inline]
    pub(crate) fn set_next(
        self: Pin<&mut Self>,
        next: Option<NonNull<RLookupKey<'a>>>,
    ) -> Option<NonNull<RLookupKey<'a>>> {
        let me = self.project();
        mem::replace(me.header.next.get_mut(), next)
    }

    #[inline]
    pub(crate) fn set_path(self: Pin<&mut Self>, path: Cow<'a, CStr>) {
        let mut me = self.project();
        me.header.path = path.as_ptr();
        *me._path = Some(path);
    }

    pub fn make_tombstone(self: Pin<&mut Self>) -> (Cow<'a, CStr>, Option<Cow<'a, CStr>>) {
        let mut me = self.project();

        me.header.name = ptr::null();
        me.header.name_len = usize::MAX;
        let name = mem::take(me._name.deref_mut());

        me.header.path = ptr::null();
        let path = mem::take(me._path.deref_mut());

        // this will exclude it from iteration
        me.header.flags |= RLookupKeyFlag::Hidden;

        (name, path)
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

    #[cfg(any(debug_assertions, test))]
    pub(crate) fn assert_valid(&self, tail: &Self, ctx: &str) {
        assert!(
            !self.flags.intersects(TRANSIENT_FLAGS),
            "{ctx}key flags must not contain transient ({TRANSIENT_FLAGS:?}) flags. Found {:?}.",
            self.flags
        );

        if !self.is_tombstone() {
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

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;

    use super::*;

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

    #[test]
    fn rlookupkey_new_ascii() {
        let name = c"test";

        let key = RLookupKey::new(name, RLookupKeyFlags::empty());
        assert_eq!(key.name, name.as_ptr());
        assert!(matches!(key._name, Cow::Borrowed(_)));
    }

    #[test]
    fn rlookupkey_new_utf8() {
        let name = c"🔍🔥🎶";

        let key = RLookupKey::new(name, RLookupKeyFlags::empty());
        assert_eq!(key.name, name.as_ptr());
        assert_eq!(key.name_len, 12); // 3 characters, 4 bytes each
        assert!(matches!(key._name, Cow::Borrowed(_)));
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri does not support FFI functions")]
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
    #[cfg_attr(miri, ignore = "miri does not support FFI functions")]
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
    #[cfg_attr(miri, ignore = "miri does not support FFI functions")]
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

    #[test]
    fn new_only_name() {
        let name = Cow::Borrowed(c"foo");
        let key = RLookupKey::new(name, RLookupKeyFlags::empty());

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._name.as_ptr());
    }

    #[test]
    fn new_name_and_path() {
        let name = Cow::Borrowed(c"foo");
        let path = Cow::Borrowed(c"bar");
        let key = RLookupKey::new_with_path(name, path, RLookupKeyFlags::empty());

        assert_eq!(key.name, key._name.as_ptr());
        assert_eq!(key.path, key._path.as_ref().unwrap().as_ptr());
    }
}
