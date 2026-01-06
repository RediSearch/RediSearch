/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use crate::{RLookupKey, RLookupKeyFlag, RLookupKeyFlags};
use std::ops::DerefMut;
use std::{ffi::CStr, pin::Pin, ptr::NonNull};
use std::{mem, ptr};

#[derive(Debug)]
#[repr(C)]
pub struct KeyList<'a> {
    // The head and tail nodes of this linked-list.
    // FIXME [MOD-10314] make this more type-safe when we no longer have direct field access from C
    head: Option<NonNull<RLookupKey<'a>>>,
    tail: Option<NonNull<RLookupKey<'a>>>,
    // Length of the data row. This is not necessarily the number
    // of lookup keys. Overridden keys created through [`CursorMut::override_current`] increase
    // the number of actually allocated keys without increasing the conceptual rowlen.
    pub(crate) rowlen: u32,
}

/// A cursor over an [`RLookup`]'s key list usable as [`Iterator`]
///
/// This types `Iterator` implementation skips all hidden keys, i.e. the keys
/// with hidden flags, also including keys that been overridden.
///
/// If you need to obtain the hidden keys use [`Cursor::move_next`].
pub struct Cursor<'list, 'a> {
    _rlookup: &'list KeyList<'a>,
    current: Option<NonNull<RLookupKey<'a>>>,
}

/// A cursor over an [`RLookup`]s key list with editing operations.
pub struct CursorMut<'list, 'a> {
    _rlookup: &'list mut KeyList<'a>,
    current: Option<NonNull<RLookupKey<'a>>>,
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
    pub(crate) fn push(&mut self, mut key: RLookupKey<'a>) -> Pin<&mut RLookupKey<'a>> {
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
    /// The returned Cursor's `Iterator` implementation skips hidden keys, i.e. the keys that have
    /// been overridden.
    ///
    /// If you need to obtain the hidden keys use [`Cursor::move_next`].
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
    pub(crate) fn find_by_name(&self, name: &CStr) -> Option<Cursor<'_, 'a>> {
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
    pub(crate) fn find_by_name_mut(&mut self, name: &CStr) -> Option<CursorMut<'_, 'a>> {
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
    pub(crate) fn assert_valid(&self, ctx: &str) {
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
                // NAME_ALLOC is a transient flag in Rust and must not be copied over,
                // thus we can safely just set the provided flags here.
                flags,
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
