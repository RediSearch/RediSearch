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
    marker::PhantomPinned,
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
const GET_KEY_FLAGS: RLookupKeyFlags =
    make_bitflags!(RLookupKeyFlag::{Override | Hidden | ExplicitReturn | ForceLoad});

/// Flags do not persist to the key, they are just options to [`RLookup::get_key_read`], [`RLookup::get_key_write`], or [`RLookup::get_key_load`].
const TRANSIENT_FLAGS: RLookupKeyFlags = make_bitflags!(RLookupKeyFlag::{Override | ForceLoad});

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
pub type RlookupOptions = BitFlags<RLookupOption>;

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
    next: Link<'a>,

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

/// An append-only list of [`RlookupKey`]s.
///
/// This type allows the creation and retrieval of [`RlookupKey`]s.
#[derive(Debug)]
#[repr(C)]
pub struct RLookup<'a> {
    keys: KeyList<'a>,

    // Flags/options
    options: RlookupOptions,

    // If present, then GetKey will consult this list if the value is not found in
    // the existing list of keys.
    spcache: Option<IndexSpecCache>,
}

#[derive(Debug)]
#[repr(C)]
struct KeyList<'a> {
    head: Option<NonNull<RLookupKey<'a>>>,
    tail: Option<NonNull<RLookupKey<'a>>>,
    // Length of the data row. This is not necessarily the number
    // of lookup keys. Hidden keys created through [`CursorMut::override_current`] increase
    // the number of actually allocated keys without increasing the conceptual rowlen.
    rowlen: u32,
}

/// Link to other keys in a [`KeyList`].
#[derive(Debug)]
#[repr(C)]
struct Link<'a> {
    inner: UnsafeCell<LinkInner<'a>>,
}

#[derive(Debug, PartialEq, Eq)]
#[repr(C)]
struct LinkInner<'a> {
    next: Option<NonNull<RLookupKey<'a>>>,
    /// Linked list links must always be `!Unpin`, in order to ensure that they
    /// never receive LLVM `noalias` annotations; see also
    /// <https://github.com/rust-lang/rust/issues/63818>.
    _unpin: PhantomPinned,
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
            next: Link::new(),
        }
    }

    pub fn from_parts(
        name: Cow<'a, CStr>,
        path: Option<Cow<'a, CStr>>,
        dstidx: u16,
        flags: RLookupKeyFlags,
    ) -> Self {
        debug_assert_eq!(
            matches!(name, Cow::Owned(_)),
            flags.contains(RLookupKeyFlag::NameAlloc),
            "RLookupKeyFlag::NameAlloc but name is Cow::Borrowed"
        );
        if let Some(path) = &path {
            debug_assert_eq!(
                matches!(path, Cow::Owned(_)),
                flags.contains(RLookupKeyFlag::NameAlloc),
                "RLookupKeyFlag::NameAlloc but path is Cow::Borrowed"
            );
        }

        Self {
            dstidx,
            svidx: 0,
            flags: flags & !TRANSIENT_FLAGS,
            name: name.as_ptr(),
            path: name.as_ptr(),
            name_len: name.count_bytes(),
            _name: name,
            _path: path,
            next: Link::new(),
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

    /// Updates this `RLookupKey`'s fields according to the provided [`ffi::FieldSpec`].
    pub fn update_from_field_spec(self: Pin<&mut Self>, fs: &ffi::FieldSpec) {
        let mut me = self.project();

        *me.flags |= RLookupKeyFlag::DocSrc | RLookupKeyFlag::SchemaSrc;

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

            if me.flags.contains(RLookupKeyFlag::NameAlloc) {
                Cow::Owned(path.to_owned())
            } else {
                Cow::Borrowed(path)
            }
        };
        *me._path = Some(path);
        *me.path = me._path.as_deref().unwrap().as_ptr();

        let fs_options = FieldSpecOptions::from_bits(fs.options()).unwrap();

        if fs_options.contains(FieldSpecOption::Sortable) {
            *me.flags |= RLookupKeyFlag::SvSrc;
            *me.svidx = u16::try_from(fs.sortIdx).unwrap();

            if fs_options.contains(FieldSpecOption::Unf) {
                // If the field is sortable and not normalized (UNF), the available data in the
                // sorting vector is the same as the data in the document.
                *me.flags |= RLookupKeyFlag::ValAvailable;
            }
        }

        let fs_types = FieldSpecTypes::from_bits(fs.types()).unwrap();

        if fs_types.contains(FieldSpecType::Numeric) {
            *me.flags |= RLookupKeyFlag::Numeric;
        }
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
            options: RlookupOptions::empty(),
            spcache: None,
        }
    }

    pub const fn with_options(options: RlookupOptions) -> Self {
        Self {
            keys: KeyList::new(),
            options,
            spcache: None,
        }
    }

    pub fn init(&mut self, spcache: IndexSpecCache) {
        debug_assert!(self.spcache.is_none());
        self.spcache = Some(spcache);
    }

    // ===== Load key from redis keyspace (include known information on the key, fail if already loaded) =====

    pub fn get_key_load(
        &mut self,
        name: &'a CStr,
        field_name: &'a CStr,
        mut flags: RLookupKeyFlags,
    ) -> Option<&RLookupKey<'a>> {
        // remove all flags that are not relevant to getting a key
        flags &= GET_KEY_FLAGS;

        // 1. if the key is already loaded, or it has created by earlier RP for writing, return NULL (unless override was requested)
        // 2. create a new key with the name of the field, and mark it as doc-source.
        // 3. if the key is in the schema, mark it as schema-source and apply all the relevant flags according to the field spec.
        // 4. if the key is "loaded" at this point (in schema, sortable and un-normalized), create the key but return NULL
        //    (no need to load it from the document).

        // Safety: The non-lexical lifetime analysis of current Rust, incorrectly handles borrows in early
        // return statements, expanding the borrow of `self` to the scope of the entire method. This is
        // obviously not correct as `key` is either found and borrowed in which case we never reach the code below
        // this early return OR it is *not* found and *not* borrowed which means the code below is fine to
        // borrow self again. The current compiler is not smart enough to get this though, so we create a disjoint
        // borrow below.
        // TODO remove once <https://github.com/rust-lang/rust/issues/54663> is fixed.
        let me = unsafe { &mut *(self as *mut Self) };

        let mut key = if let Some(mut c) = me.keys.find_by_name_mut(name) {
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
                *key.flags |= flags & RLookupKeyFlag::ExplicitReturn;

                return None;
            } else {
                c.override_current(flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded)
                    .unwrap()
            }
        } else {
            self.keys.push(RLookupKey::new(
                name,
                flags | RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded,
            ))
        };

        if let Some(fs) = self.spcache.as_ref()?.find_field(name) {
            key.as_mut().update_from_field_spec(fs);

            if key.flags.contains(RLookupKeyFlag::ValAvailable)
                && !flags.contains(RLookupKeyFlag::ForceLoad)
            {
                // If the key is marked as "value available", it means that it is sortable and un-normalized.
                // so we can use the sorting vector as the source, and we don't need to load it from the document.
                return None;
            }
        } else {
            // Field not found in the schema.
            let mut key = key.as_mut().project();

            // We assume `field_name` is the path to load from in the document.
            if !key.flags.contains(RLookupKeyFlag::NameAlloc) {
                *key.path = field_name.as_ptr();
                *key._path = Some(Cow::Borrowed(field_name));
            } else if name != field_name {
                let field_name: Cow<'_, CStr> = Cow::Owned(field_name.to_owned());
                *key.path = field_name.as_ptr();
                *key._path = Some(field_name);
            } // else
            // If the caller requested to allocate the name, and the name is the same as the path,
            // it was already set to the same allocation for the name, so we don't need to do anything.
        }

        // Safety: We treat the pointer as pinned internally and safe Rust cannot move out of the returned reference.
        Some(unsafe { Pin::into_inner_unchecked(key.into_ref()) })
    }
}

// ===== impl KeyList =====

#[cfg_attr(not(test), expect(unused, reason = "used by later stacked PRs"))]
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

        key.dstidx = u16::try_from(self.rowlen).expect("conversion from u32 RLookup::rowlen to u16 RLookupRow::dstidx overflowed. This is a bug!");

        // Safety: RLookup never hands out mutable references to the key (except `Pin<&mut T>` which is fine)
        // and never copies, or memmoves the memory internally.
        let mut ptr = unsafe { RLookupKey::into_ptr(Box::pin(key)) };

        if let Some(mut tail) = self.tail.take() {
            // if we have a tail we also must have a head
            debug_assert!(self.head.is_some());

            // Safety: We know we can borrow tail here, since we mutably borrow the RLookup
            // which owns all keys allocated within it. This ensures the RLookup and all keys outlive
            // this method call AND that we have exclusive access to mutate the key.
            let tail = unsafe { tail.as_mut() };

            tail.next.set_next(Some(ptr));
            self.tail = Some(ptr);
        } else {
            // if we have no tail we also must have no head
            debug_assert!(self.head.is_none());
            self.head = Some(ptr);
            self.tail = Some(ptr);
        }

        // Increase the RLookup table row length. (all rows have the same length).
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
        let head_link = unsafe { &head.as_ref().next };
        // Safety: see abvove
        let tail_link = unsafe { &tail.as_ref().next };

        if ptr::addr_eq(head.as_ptr(), tail.as_ptr()) {
            assert_eq!(
                NonNull::from(head_link),
                NonNull::from(tail_link),
                "{ctx}if the head and tail nodes are the same, their links must be the same"
            );
            assert_eq!(
                head_link.next(),
                None,
                "{ctx}if the linked list has only one node, it must not be linked"
            );
            return;
        }

        let mut curr = Some(head);
        let mut actual_len = 0;
        while let Some(node) = curr {
            // Safety: see abvove
            let link = unsafe { &node.as_ref().next };
            link.assert_valid(tail_link);
            curr = link.next();
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

            self.head = head.next.next();

            if head.next.next().is_none() {
                self.tail = None;
            }

            // clear the pointer before dropping the key, just to be sure
            head.next.set_next(None);

            // Safety:
            // 1 -> all keys here are created through `push_key`, which correctly calls into_ptr.
            // 2 -> after this destructor runs, this RLookup is inaccessible making double frees impossible.
            // 3 -> RLookupKey is about to be freed, we don't need to worry about pinning anymore.
            drop(unsafe { RLookupKey::from_ptr(head_ptr) });
        }
    }
}

// ===== impl Links =====

impl<'a> Link<'a> {
    /// Returns new links for a [doubly-linked intrusive list](List).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: UnsafeCell::new(LinkInner {
                next: None,
                _unpin: PhantomPinned,
            }),
        }
    }

    /// Returns `true` if this node is currently linked to a [`List`].
    #[cfg(test)]
    pub fn has_next(&self) -> bool {
        self.next().is_some()
    }

    /// Return the next pointer in the linked list
    #[inline]
    fn next(&self) -> Option<NonNull<RLookupKey<'a>>> {
        // Safety: RLookupKeys are created through `KeyList::push` and owned by the `List`. We
        // can therefore assume this pointer is safe to dereference at this point.
        unsafe { (*self.inner.get()).next }
    }

    /// Update the pointer to the next node
    #[inline]
    fn set_next(
        &mut self,
        next: Option<NonNull<RLookupKey<'a>>>,
    ) -> Option<NonNull<RLookupKey<'a>>> {
        mem::replace(&mut self.inner.get_mut().next, next)
    }

    fn assert_valid(&self, tail: &Self) {
        if ptr::eq(self, tail) {
            assert_eq!(
                self.next(),
                None,
                "tail node must not have a next link; node={self:#?}",
            );
        }

        if let Some(next) = self.next() {
            assert_ne!(
                // Safety:
                NonNull::from(unsafe { &next.as_ref().next }),
                NonNull::from(self),
                "node's next link cannot be to itself; node={self:#?}",
            );
        }
    }
}

// ===== impl Cursor =====

impl<'a> Cursor<'_, 'a> {
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

            self.current = curr.next.next();
        }
    }

    /// If the cursor currently points to a key, return an immutable reference to it.
    pub fn current(&self) -> Option<&RLookupKey<'a>> {
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
            self.current = curr.next.next();
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

    /// Override the [`RLookupKey`] currently being pointed to by this cursor (if any) with the given flags.
    ///
    /// The new key will inherit the `name`, `path`, and `dstidx` of the current key but receive a
    /// **new pointer identity**. The new key is returned.
    pub fn override_current(
        mut self,
        flags: RLookupKeyFlags,
    ) -> Option<Pin<&'list mut RLookupKey<'a>>> {
        let old = self.current()?;
        let mut old = old.project();

        *old.name = ptr::null();
        *old.name_len = usize::MAX;
        let name = mem::take(old._name.deref_mut());

        *old.path = ptr::null();
        let path = mem::take(old._path.deref_mut());

        let mut new = RLookupKey::from_parts(name, path, *old.dstidx, flags);

        // link the new key into the linked-list. Since KeyList is singly-linked and we don't know yet
        // if C code is still holding on to pointers to nodes, we replicate the C behaviour here:
        //
        // 1. We copy the next pointer from old to new
        // 2. We mark the old as "Hidden" so it doesn't show up in iteration anymore
        // 3. We point old.next to the new key, so the chain isn't broken
        //
        // This in effect, replaces the old key but turning it into a "tombstone value" and forcing iteration
        // to follow this indirection.
        new.next.set_next(old.next.next());

        // Safety: we treat the pointer as pinned below and only hand out a pinned mutable reference.
        let mut new = unsafe { RLookupKey::into_ptr(Box::pin(new)) };

        *old.flags |= RLookupKeyFlag::Hidden;
        old.next.set_next(Some(new));

        // If the old key was the tail, set the new key as the tail
        if self._rlookup.tail == self.current {
            self._rlookup.tail = Some(new);
        }

        // Safety: we have allocated the memory above, this pointer is safe to dereference.
        let new = unsafe { new.as_mut() };

        // Safety: We treat the pointer as pinned internally and never hand out references that could be moved out of (in safe Rust)
        // publicly.
        Some(unsafe { Pin::new_unchecked(new) })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        mem::{MaybeUninit, offset_of},
        pin::pin,
        sync::atomic::{AtomicUsize, Ordering},
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
        let mut key = pin!(RLookupKey::new(c"test", RLookupKeyFlags::empty()));

        let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
        let field_name = c"this is the field name";
        fs.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = c"this is the field path";
        fs.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };

        key.as_mut().update_from_field_spec(&fs);

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
        let mut key = pin!(RLookupKey::new(c"test", RLookupKeyFlags::empty()));

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

        key.as_mut().update_from_field_spec(&fs);

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
        let mut key = pin!(RLookupKey::new(c"test", RLookupKeyFlags::empty()));

        let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
        let field_name = c"this is the field name";
        fs.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = c"this is the field path";
        fs.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        fs.set_types(ffi::FieldType_INDEXFLD_T_NUMERIC);

        key.as_mut().update_from_field_spec(&fs);

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
        let mut key = pin!(RLookupKey::new(
            c"test",
            make_bitflags!(RLookupKeyFlag::NameAlloc)
        ));

        let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
        let field_name = c"this is the field name";
        fs.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        let field_path = c"this is the field path";
        fs.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };

        key.as_mut().update_from_field_spec(&fs);

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
            assert!(foo.as_ref().next.has_next());
        }
        unsafe {
            assert!(!bar.as_ref().next.has_next());
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
    fn keylist_override_key() {
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
        // new key should not inherit any old flags
        assert!(!found.flags.contains(RLookupKeyFlag::Unresolved));

        let mut c = keylist.cursor_front();

        // we expect the first item to be the tombstone of the old key
        assert!(c.current().unwrap()._name.is_empty());
        assert!(c.current().unwrap().flags.contains(RLookupKeyFlag::Hidden));

        // and the next item to be the new key
        c.move_next();
        assert_eq!(found._name.as_ref(), c"foo");
    }

    #[test]
    fn rlookup_init() {
        let mut rlookup = RLookup::new();

        let spcache = Box::new(ffi::IndexSpecCache {
            fields: ptr::null_mut(),
            nfields: 0,
            refcount: 0,
        });
        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };

        rlookup.init(spcache);
    }

    #[test]
    #[should_panic]
    fn rlookup_no_reinit() {
        let mut rlookup = RLookup::new();

        let spcache = Box::new(ffi::IndexSpecCache {
            fields: ptr::null_mut(),
            nfields: 0,
            refcount: 0,
        });
        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };

        rlookup.init(spcache);

        let spcache = Box::new(ffi::IndexSpecCache {
            fields: ptr::null_mut(),
            nfields: 0,
            refcount: 0,
        });
        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };

        // this should panic
        rlookup.init(spcache);
    }

    // ===== mock implementations for testing purposes =====

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

    /// Mock implementation of `IndexSpecCache_Decref` from spec.h for testing purposes
    #[unsafe(no_mangle)]
    extern "C" fn IndexSpecCache_Decref(s: Option<NonNull<ffi::IndexSpecCache>>) {
        let s = s.unwrap();
        let refcount = unsafe {
            s.byte_add(offset_of!(ffi::IndexSpecCache, refcount))
                .cast::<usize>()
        };

        let refcount = unsafe { AtomicUsize::from_ptr(refcount.as_ptr()) };
        if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
            drop(unsafe { Box::from_raw(s.as_ptr()) });
        }
    }
}
