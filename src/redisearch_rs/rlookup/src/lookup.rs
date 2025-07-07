/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use enumflags2::{BitFlags, bitflags, make_bitflags};
use pin_project::pin_project;
use std::{
    borrow::Cow,
    cell::UnsafeCell,
    ffi::{CStr, c_char},
    marker::PhantomPinned,
    mem,
    pin::Pin,
    ptr::{self, NonNull},
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
#[expect(unused, reason = "used by later stacked PRs")]
const GET_KEY_FLAGS: RLookupKeyFlags =
    make_bitflags!(RLookupKeyFlag::{Override | Hidden | ExplicitReturn | ForceLoad});

/// Flags do not persist to the key, they are just options to [`RLookup::get_key_read`], [`RLookup::get_key_write`], or [`RLookup::get_key_load`].
const TRANSIENT_FLAGS: RLookupKeyFlags =
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
#[pin_project]
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
    #[pin]
    pub next: Option<NonNull<RLookupKey<'a>>>,

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

/// An append-only list of [`RlookupKey`]s.
///
/// This type allows the creation and retrieval of [`RlookupKey`]s.
#[derive(Debug)]
#[repr(C)]
struct KeyList<'a> {
    head: Option<NonNull<RLookupKey<'a>>>,
    tail: Option<NonNull<RLookupKey<'a>>>,
    // Length of the data row. This is not necessarily the number
    // of lookup keys
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

/// Iterates over the keys in a [`KeyList`] by reference.
pub struct Iter<'list, 'a> {
    _rlookup: &'list KeyList<'a>,
    curr: Option<NonNull<RLookupKey<'a>>>,
}

/// Iterates over the keys in a [`KeyList`] by mutable reference.
pub struct IterMut<'list, 'a> {
    _rlookup: &'list mut KeyList<'a>,
    curr: Option<NonNull<RLookupKey<'a>>>,
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

        key.dstidx = u16::try_from(self.rowlen).unwrap();

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

    /// Returns an Iterator over the `RLookupKey`s in this `KeyList`.
    ///
    /// The iteration order is **not specified** and must not be relied on for correctness.
    pub fn iter(&self) -> Iter<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::iter");

        Iter {
            _rlookup: self,
            curr: self.head,
        }
    }

    /// Returns an Iterator over the `RLookupKey`s in this `KeyList`.
    ///
    /// The iteration order is **not specified** and must not be relied on for correctness.
    pub fn iter_mut(&mut self) -> IterMut<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::iter_mut");

        IterMut {
            curr: self.head,
            _rlookup: self,
        }
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return an immutable reference to it if found.
    fn find_by_name(&self, name: &'a CStr) -> Option<&RLookupKey<'a>> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::find");

        // FIXME [MOD-10315] replace with more efficient search
        self.iter().find(|key| key._name.as_ref() == name)
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a mutable reference to it if found.
    fn find_by_name_mut(&mut self, name: &'a CStr) -> Option<Pin<&mut RLookupKey<'a>>> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::find_mut");

        // FIXME [MOD-10315] replace with more efficient search
        self.iter_mut().find(|key| key._name.as_ref() == name)
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

        assert_eq!(
            self.rowlen, actual_len,
            "{ctx}linked list's actual length did not match its `len` variable"
        );
    }
}

impl Drop for KeyList<'_> {
    fn drop(&mut self) {
        while let Some(mut head_ptr) = self.head.take() {
            // Safety: This ptr has been created through `push_key` and is owned by this list,
            // which means it is valid & safe to deref at this point.
            let head = unsafe { head_ptr.as_mut() };

            self.head = head.next.next();

            if head.next.next().is_none() {
                self.tail = None;
            }

            head.next.unlink();

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

    fn unlink(&mut self) {
        self.inner.get_mut().next = None;
    }

    #[inline]
    fn next(&self) -> Option<NonNull<RLookupKey<'a>>> {
        // Safety: RLookupKeys are created through `KeyList::push` and owned by the `List`. We
        // can therefore assume this pointer is safe to dereference at this point.
        unsafe { (*self.inner.get()).next }
    }

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

// ===== impl Iter =====

impl<'list, 'a> Iterator for Iter<'list, 'a> {
    type Item = &'list RLookupKey<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let curr = self.curr.take()?;

        // Safety: It is safe for us to borrow `curr`, because the iteraror mutably borrows the `KeyList`,
        // ensuring it will not be dropped while the iterator exists AND we have exclusive access
        // to the keys it owns (and can therefore hand out mutable references).
        // The returned item will not outlive the iterator.
        self.curr = unsafe { curr.as_ref().next.next() };

        // Safety: See above.
        let curr = unsafe { curr.as_ref() };

        Some(curr)
    }
}

// ===== impl IterMut =====

impl<'list, 'a> Iterator for IterMut<'list, 'a> {
    type Item = Pin<&'list mut RLookupKey<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut curr = self.curr.take()?;

        // Safety: It is safe for us to borrow `curr`, because the iteraror mutably borrows the `KeyList`,
        // ensuring it will not be dropped while the iterator exists AND we have exclusive access
        // to the keys it owns (and can therefore hand out mutable references).
        // The returned item will not outlive the iterator.
        self.curr = unsafe { curr.as_ref().next.next() };

        // Safety: See above.
        let curr = unsafe { curr.as_mut() };

        // Safety: RLookup treats the keys are pinned always, we just need consumers of this
        // iterator to uphold the pinning invariant too
        Some(unsafe { Pin::new_unchecked(curr) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // assert that the linked list is produced and linked correctly
    #[test]
    fn keylist_push_consistency() {
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(c"foo", RlookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(c"bar", RlookupKeyFlags::empty()));
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
    fn keylist_find() {
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(c"foo", RlookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(c"bar", RlookupKeyFlags::empty()));
        let bar = unsafe { NonNull::from(Pin::into_inner_unchecked(bar)) };

        keylist.assert_valid("tests::keylist_find after insertions");

        let found = keylist.find_by_name(c"foo").unwrap();
        assert_eq!(NonNull::from(found), foo);

        let found = keylist.find_by_name(c"bar").unwrap();
        assert_eq!(NonNull::from(found), bar);
    }

    #[test]
    fn keylist_find_mut() {
        let mut keylist = KeyList::new();

        let foo = keylist.push(RLookupKey::new(c"foo", RlookupKeyFlags::empty()));
        let foo = unsafe { NonNull::from(Pin::into_inner_unchecked(foo)) };

        let bar = keylist.push(RLookupKey::new(c"bar", RlookupKeyFlags::empty()));
        let bar = unsafe { NonNull::from(Pin::into_inner_unchecked(bar)) };

        keylist.assert_valid("tests::keylist_find_mut after insertions");

        let found = keylist.find_by_name_mut(c"foo").unwrap();
        assert_eq!(
            NonNull::from(unsafe { Pin::into_inner_unchecked(found) }),
            foo
        );

        let found = keylist.find_by_name_mut(c"bar").unwrap();
        assert_eq!(
            NonNull::from(unsafe { Pin::into_inner_unchecked(found) }),
            bar
        );
    }

    #[test]
    fn keylist_iter() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RlookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RlookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"baz", RlookupKeyFlags::empty()));
        keylist.assert_valid("tests::keylist_iter after insertions");

        let mut iter = keylist.iter();
        assert_eq!(iter.next().unwrap()._name.as_ref(), c"foo");
        assert_eq!(iter.next().unwrap()._name.as_ref(), c"bar");
        assert_eq!(iter.next().unwrap()._name.as_ref(), c"baz");
        assert!(iter.next().is_none());
    }

    #[test]
    fn keylist_iter_mut() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RlookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RlookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"baz", RlookupKeyFlags::empty()));
        keylist.assert_valid("tests::keylist_iter_mut after insertions");

        let mut iter = keylist.iter_mut();

        assert_eq!(iter.next().unwrap()._name.as_ref(), c"foo");
        assert_eq!(iter.next().unwrap()._name.as_ref(), c"bar");
        assert_eq!(iter.next().unwrap()._name.as_ref(), c"baz");
        assert!(iter.next().is_none());
    }
}
