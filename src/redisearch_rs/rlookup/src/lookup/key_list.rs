/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RLookupKey, RLookupKeyFlags};
use std::{ffi::CStr, iter::FusedIterator, marker::PhantomData, pin::Pin, ptr::NonNull};

#[cfg(any(debug_assertions, test))]
use std::ptr;

#[derive(Debug)]
#[repr(C)]
pub struct KeyList<'a> {
    // The head and tail nodes of this linked-list.
    // FIXME [MOD-10314] make this more type-safe when we no longer have direct field access from C
    head: Option<NonNull<RLookupKey<'a>>>,
    tail: Option<NonNull<RLookupKey<'a>>>,
    // Length of the data row. This is not necessarily the number
    // of lookup keys. Overridden keys created through `CursorMut::override_current` increase
    // the number of actually allocated keys without increasing the conceptual rowlen.
    pub(crate) rowlen: u32,
}

/// A cursor over an [`RLookup`][crate::RLookup]'s key list.
pub struct Cursor<'list, 'a> {
    _list: PhantomData<&'list KeyList<'a>>,
    current: Option<NonNull<RLookupKey<'a>>>,
}

/// A cursor over an [`RLookup`][crate::RLookup]'s key list with editing operations.
pub struct CursorMut<'list, 'a> {
    list: &'list mut KeyList<'a>,
    current: Option<NonNull<RLookupKey<'a>>>,
}

/// Internal iterator yielding raw pointers to keys.
struct IterRaw<'a> {
    current: Option<NonNull<RLookupKey<'a>>>,
}

/// Iterator over an [`RLookup`][crate::RLookup]'s key list, yielding immutable references to keys.
pub struct Iter<'list, 'a> {
    _list: PhantomData<&'list KeyList<'a>>,
    raw: IterRaw<'a>,
}

/// Iterator over an [`RLookup`][crate::RLookup]'s key list, yielding pinned mutable references to keys.
///
/// Use [`CursorMut`] to override a key during traversal.
pub struct IterMut<'list, 'a> {
    _list: PhantomData<&'list mut KeyList<'a>>,
    raw: IterRaw<'a>,
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
    //
    // TODO remove the 'a and 'b lifetimes borrow-checker hack when we refactor this code. refer to Jira ticket MOD-13907.
    pub(crate) fn push<'b>(&mut self, mut key: RLookupKey<'a>) -> Pin<&'b mut RLookupKey<'a>>
    where
        'a: 'b,
    {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::push (before)");

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
        self.assert_valid("KeyList::push (after)");

        // Safety: we have allocated the memory above, this pointer is safe to dereference.
        let key = unsafe { ptr.as_mut() };
        // Safety: We treat the pointer as pinned internally and never hand out references that could be moved out of (in safe Rust)
        // publicly.
        unsafe { Pin::new_unchecked(key) }
    }

    /// Return a cursor over an [`crate::RLookup`]'s key list.
    #[cfg_attr(not(debug_assertions), expect(clippy::missing_const_for_fn))]
    pub fn cursor_front(&self) -> Cursor<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::cursor_front");

        Cursor {
            current: self.head,
            _list: PhantomData,
        }
    }

    /// Return a cursor over an [`crate::RLookup`]'s key list with editing operations.
    #[cfg_attr(not(debug_assertions), expect(clippy::missing_const_for_fn))]
    pub fn cursor_front_mut(&mut self) -> CursorMut<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::cursor_front_mut");

        CursorMut {
            current: self.head,
            list: self,
        }
    }

    /// Returns an iterator over immutable references to keys.
    #[cfg_attr(not(debug_assertions), expect(clippy::missing_const_for_fn))]
    pub fn iter(&self) -> Iter<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::iter");

        Iter {
            raw: IterRaw { current: self.head },
            _list: PhantomData,
        }
    }

    /// Returns an iterator over pinned mutable references to keys.
    #[cfg_attr(not(debug_assertions), expect(clippy::missing_const_for_fn))]
    pub fn iter_mut(&mut self) -> IterMut<'_, 'a> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::iter_mut");

        IterMut {
            raw: IterRaw { current: self.head },
            _list: PhantomData,
        }
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a [`Cursor`] pointing to the key if found.
    // FIXME [MOD-10315] replace with more efficient search
    pub(crate) fn find_by_name(&self, name: &CStr) -> Option<Cursor<'_, 'a>> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::find_by_name");

        let mut c = self.cursor_front();
        while let Some(key) = c.current() {
            if key.name().as_ref() == name {
                return Some(c);
            }
            c.move_next();
        }
        None
    }

    /// Find a [`RLookupKey`] in this `KeyList` by its [`name`][RLookupKey::name]
    /// and return a [`CursorMut`] pointing to the key if found.
    // FIXME [MOD-10315] replace with more efficient search
    pub(crate) fn find_by_name_mut(&mut self, name: &CStr) -> Option<CursorMut<'_, 'a>> {
        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::find_by_name_mut");

        let mut c = self.cursor_front_mut();
        while let Some(key) = c.current() {
            if key.name().as_ref() == name {
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
    pub(crate) fn assert_valid(&self, ctx: &str) {
        let Some(head) = self.head else {
            assert!(
                self.tail.is_none(),
                "{ctx} - if the linked list's head is null, the tail must also be null"
            );
            assert_eq!(
                self.rowlen, 0,
                "{ctx} - if a linked list's head is null, its length must be 0"
            );
            return;
        };

        assert_ne!(
            self.rowlen, 0,
            "{ctx} - if a linked list's head is not null, its length must be greater than 0"
        );
        assert_ne!(
            self.tail, None,
            "{ctx} - if the linked list has a head, it must also have a tail"
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
                "{ctx} - if the head and tail nodes are the same, their links must be the same"
            );
            assert_eq!(
                head.next(),
                None,
                "{ctx} - if the linked list has only one node, it must not be linked"
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
            "{ctx} - linked list's rowlen was greater than its actual length"
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
            let (name, path) = old.as_mut().make_tombstone();
            let mut key = if let Some(path) = path {
                RLookupKey::new_with_path(name, path, flags)
            } else {
                RLookupKey::new(name, flags)
            };
            key.dstidx = old.dstidx;

            key
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

        if self.list.tail == self.current {
            self.list.tail = Some(new_ptr);
        }

        Some(new)
    }
}

impl<'a> Iterator for IterRaw<'a> {
    type Item = NonNull<RLookupKey<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut curr = self.current.take()?;
        loop {
            // Safety: The `KeyList` ensures the linked list pointers are valid for as long as the
            // wrapping iterator's borrow lives.
            let curr_ref = unsafe { curr.as_ref() };
            if !curr_ref.is_tombstone() {
                self.current = curr_ref.next();
                return Some(curr);
            }
            curr = curr_ref.next()?;
        }
    }
}

impl<'list, 'a> Iterator for Iter<'list, 'a> {
    type Item = &'list RLookupKey<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.raw.next()?;

        // Safety: we immutably borrow (through a PhantomData but nonetheless) the `KeyList` ensuring
        // it cannot be dropped or mutated while we hold the iterator. The returned reference cannot outlive the list.
        Some(unsafe { next.as_ref() })
    }
}

impl<'list, 'a> Iterator for IterMut<'list, 'a> {
    type Item = Pin<&'list mut RLookupKey<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.raw.next()?;

        // Safety: we mutably borrow (through a PhantomData but nonetheless) the `KeyList` ensuring
        // it cannot be dropped or mutated while we hold the iterator. The returned reference cannot outlive the list.
        let next = unsafe { next.as_mut() };

        // Safety: all keys are treated as pinned by the crate
        Some(unsafe { Pin::new_unchecked(next) })
    }
}

// Once `IterRaw::next` returns `None`, `self.current` is `None` and stays that way, so all three
// iterators are naturally fused.
impl FusedIterator for IterRaw<'_> {}
impl FusedIterator for Iter<'_, '_> {}
impl FusedIterator for IterMut<'_, '_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RLookupKeyFlag;
    use enumflags2::make_bitflags;

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
        assert_eq!(c.current().unwrap().name().as_ref(), c"foo");
        c.move_next();
        assert_eq!(c.current().unwrap().name().as_ref(), c"bar");
        c.move_next();
        assert_eq!(c.current().unwrap().name().as_ref(), c"baz");
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
        assert_eq!(c.current().unwrap().name().as_ref(), c"foo");
        c.move_next();
        assert_eq!(c.current().unwrap().name().as_ref(), c"bar");
        c.move_next();
        assert_eq!(c.current().unwrap().name().as_ref(), c"baz");
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

        assert_eq!(found.name().as_ref(), c"foo");
        assert!(found.path().is_none());
        assert_eq!(found.dstidx, 0);
        // new key should have provided keys
        assert!(found.flags.contains(RLookupKeyFlag::Numeric));
        // new key should not inherit any old flags
        assert!(!found.flags.contains(RLookupKeyFlag::Unresolved));
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
        assert_eq!(c.current().unwrap().name().as_ref(), c"foo");
    }

    // Assert that Iter skips tombstones (hidden/overridden keys)
    #[test]
    fn iter_skips_tombstones() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"baz", RLookupKeyFlags::empty()));

        // Override "foo" to create a tombstone
        keylist
            .cursor_front_mut()
            .override_current(RLookupKeyFlags::empty());

        let names: Vec<_> = keylist
            .iter()
            .map(|k| k.name().as_ref().to_owned())
            .collect();

        // The tombstone for old "foo" should be skipped; new "foo" replacement + bar + baz remain
        assert_eq!(
            names,
            vec![c"foo".to_owned(), c"bar".to_owned(), c"baz".to_owned()]
        );
    }

    // Assert that Iter yields nothing for an empty list
    #[test]
    fn iter_empty_list() {
        let keylist = KeyList::new();
        assert_eq!(keylist.iter().count(), 0);
    }

    // Assert that Iter yields all elements when there are no tombstones
    #[test]
    fn iter_no_tombstones() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"a", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"b", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"c", RLookupKeyFlags::empty()));

        let names: Vec<_> = keylist
            .iter()
            .map(|k| k.name().as_ref().to_owned())
            .collect();
        assert_eq!(
            names,
            vec![c"a".to_owned(), c"b".to_owned(), c"c".to_owned()]
        );
    }

    // Assert that IterMut skips tombstones
    #[test]
    fn iter_mut_skips_tombstones() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"baz", RLookupKeyFlags::empty()));

        // Override "bar" (middle element) to create a tombstone
        let mut cursor = keylist.cursor_front_mut();
        cursor.move_next(); // move to "bar"
        cursor.override_current(RLookupKeyFlags::empty());

        let names: Vec<_> = keylist
            .iter_mut()
            .map(|k| k.name().as_ref().to_owned())
            .collect();
        assert_eq!(
            names,
            vec![c"foo".to_owned(), c"bar".to_owned(), c"baz".to_owned()]
        );
    }

    // Assert that IterMut yields nothing for an empty list
    #[test]
    fn iter_mut_empty_list() {
        let mut keylist = KeyList::new();
        assert_eq!(keylist.iter_mut().count(), 0);
    }

    // Assert that Iter skips multiple consecutive tombstones
    #[test]
    fn iter_skips_consecutive_tombstones() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));

        // Override both keys to create consecutive tombstones with their replacements
        keylist
            .cursor_front_mut()
            .override_current(RLookupKeyFlags::empty());

        let mut cursor = keylist.cursor_front_mut(); // at foo tombstone
        cursor.move_next(); // at foo
        cursor.move_next(); // at bar
        cursor.override_current(RLookupKeyFlags::empty());

        // All tombstones should be skipped, only replacement keys should be yielded
        let names: Vec<_> = keylist
            .iter()
            .map(|k| k.name().as_ref().to_owned())
            .collect();
        assert_eq!(names, vec![c"foo".to_owned(), c"bar".to_owned()]);
    }

    // Assert that mutations performed through IterMut are observable on a subsequent iter() pass.
    #[test]
    fn iter_mut_mutation_visible() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"a", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"b", RLookupKeyFlags::empty()));

        for key in keylist.iter_mut() {
            key.project().header.flags |= RLookupKeyFlag::ExplicitReturn;
        }

        for key in keylist.iter() {
            assert!(key.flags.contains(RLookupKeyFlag::ExplicitReturn));
        }
    }

    // Assert that Iter handles a tombstone at the tail with no successor (returns None instead of dereferencing past the end).
    #[test]
    fn iter_skips_tail_tombstone() {
        let mut keylist = KeyList::new();

        keylist.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keylist.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));

        // Tombstone the tail without inserting a replacement.
        let mut cursor = keylist.cursor_front_mut();
        cursor.move_next(); // now at "bar"
        cursor.current().unwrap().make_tombstone();

        let names: Vec<_> = keylist
            .iter()
            .map(|k| k.name().as_ref().to_owned())
            .collect();
        assert_eq!(names, vec![c"foo".to_owned()]);
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
}
