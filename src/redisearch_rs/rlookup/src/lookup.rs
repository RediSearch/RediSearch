/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::bindings::{FieldSpecOption, FieldSpecOptions, FieldSpecType, FieldSpecTypes};
use enumflags2::{BitFlags, bitflags, make_bitflags};
use pin_project::pin_project;
use std::{
    borrow::Cow,
    ffi::{CStr, c_char},
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

    /// The opposite of F_HIDDEN. This field is specified as an explicit return in
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
#[expect(unused, reason = "used by later stacked PRs")]
const GET_KEY_FLAGS: RLookupKeyFlags =
    make_bitflags!(RLookupKeyFlag::{Override | Hidden | ExplicitReturn | ForceLoad});

/// Flags do not persist to the key, they are just options to [`RLookup::get_key_read`], [`RLookup::get_key_write`], or [`RLookup::get_key_load`].
const TRANSIENT_FLAGS: RLookupKeyFlags = make_bitflags!(RLookupKeyFlag::{Override | ForceLoad});

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
#[derive(Debug, PartialEq, Eq)]
#[pin_project]
#[repr(C)]
pub struct RLookupKey<'a> {
    /// Index into the dynamic values array within the associated `RLookupRow`.
    dstidx: u16,

    /// If the source for this key is a sorting vector, this is the index
    /// into the `RSSortingVector` within the associated `RLookupRow`.
    svidx: u16,

    /// Various flags dictating the behavior of looking up the value of this key.
    /// Most notably, `Flags::SVSRC` means the source is an `RSSortingVector` and
    /// `Self::svidx` should be used to look up the value.
    flags: RLookupKeyFlags,

    /// The path of this key.
    ///
    /// For fields *not* loaded from a [`FieldSpec`][ffi::FieldSpec], this points to the *same* string
    /// as `Self::path`.
    path: *const c_char,

    /// The name of this key.
    name: *const c_char,
    /// The length of this key, without the null-terminator.
    /// Should be used to avoid repeated `strlen` computations.
    name_len: usize,

    /// Pointer to next field in the list
    #[pin]
    next: Option<NonNull<RLookupKey<'a>>>,

    // Private Rust fields
    /// The actual "owning" strings, we need to hold onto these
    /// so the pointers above stay valid. Note that you
    /// MUST NEVER MOVE THESE BEFORE THE name AND path FIELDS LESS
    /// YOU WANT POTENTIALLY RISK UB
    #[pin]
    _name: Cow<'a, CStr>,
    #[pin]
    _path: Option<Cow<'a, CStr>>,
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
    /// Constructs a new `RLookupKey` using te provided `CStr` and flags.
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
            next: None,
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
    #[cfg_attr(not(test), expect(unused, reason = "used by later stacked PRs"))]
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
    #[cfg_attr(not(test), expect(unused, reason = "used by later stacked PRs"))]
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

            if self.flags.contains(RLookupKeyFlag::NameAlloc) {
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
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;

    use super::*;

    // Make sure that the `into_ptr` and `from_ptr` functions are inverses of each other.
    #[test]
    fn into_ptr_from_ptr_roundtrip() {
        let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
        let key = Box::pin(key);

        let ptr = unsafe { RLookupKey::into_ptr(key) };
        let key = unsafe { RLookupKey::from_ptr(ptr) };

        assert_eq!(*key, RLookupKey::new(c"test", RLookupKeyFlags::empty()));
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

    #[repr(C)]
    struct UserString {
        user: *const c_char,
        length: usize,
    }

    /// Mock implementation of `HiddenString_GetUnsafe` from obfuscation/hidden.h for testing purposes
    #[unsafe(no_mangle)]
    extern "C" fn HiddenString_GetUnsafe(
        value: *const ffi::HiddenString,
        length: *mut usize,
    ) -> *const c_char {
        let text = unsafe { value.cast::<UserString>().as_ref().unwrap() };
        if text.length != 0 {
            unsafe {
                *length = text.length;
            }
        }

        text.user
    }

    /// Mock implementation of `NewHiddenString` from obfuscation/hidden.h for testing purposes
    #[unsafe(no_mangle)]
    extern "C" fn NewHiddenString(
        user: *const c_char,
        length: usize,
        take_ownership: bool,
    ) -> *mut ffi::HiddenString {
        assert!(
            !take_ownership,
            "tests are not allowed to move ownership to C"
        );
        let value = Box::new(UserString { user, length });
        Box::into_raw(value).cast()
    }

    /// Mock implementation of `HiddenString_Free` from obfuscation/hidden.h for testing purposes
    #[unsafe(no_mangle)]
    extern "C" fn HiddenString_Free(value: *const ffi::HiddenString, took_ownership: bool) {
        assert!(
            !took_ownership,
            "tests are not allowed to move ownership to C"
        );

        drop(unsafe { Box::from_raw(value.cast_mut().cast::<UserString>()) });
    }
}
