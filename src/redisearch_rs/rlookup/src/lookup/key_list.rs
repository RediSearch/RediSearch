/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RLookupKey, RLookupKeyFlags};
use std::{ffi::CStr, iter::FusedIterator, mem, pin::Pin, ptr::NonNull, slice};

/// Owns a pinned key while keeping the key's address independent of vector reallocations.
#[derive(Debug)]
#[repr(transparent)]
struct OwnedKey<'a>(NonNull<RLookupKey<'a>>);

impl<'a> OwnedKey<'a> {
    fn new(key: RLookupKey<'a>) -> Self {
        // SAFETY: `OwnedKey` keeps ownership of the allocation and never moves the pointee.
        Self(unsafe { RLookupKey::into_ptr(Box::pin(key)) })
    }

    const fn get(&self) -> &RLookupKey<'a> {
        // SAFETY: the allocation is owned by `self` and remains valid until `Drop`.
        unsafe { self.0.as_ref() }
    }

    const fn as_non_null(&self) -> NonNull<RLookupKey<'a>> {
        self.0
    }

    const fn get_pin_mut(&mut self) -> Pin<&mut RLookupKey<'a>> {
        // SAFETY: the allocation is owned exclusively by `self`.
        let key = unsafe { self.0.as_mut() };
        // SAFETY: the allocation was pinned when it was created.
        unsafe { Pin::new_unchecked(key) }
    }
}

impl Drop for OwnedKey<'_> {
    fn drop(&mut self) {
        // SAFETY: `OwnedKey::new` created this pointer, and this is its unique owning drop.
        drop(unsafe { RLookupKey::from_ptr(self.0) });
    }
}

#[derive(Debug)]
struct KeyStore<'a> {
    /// The current logical key at every row slot, in row order.
    live: Vec<OwnedKey<'a>>,
    /// Replaced keys whose addresses must remain valid for C consumers.
    retired: Vec<OwnedKey<'a>>,
}

impl KeyStore<'_> {
    const fn new() -> Self {
        Self {
            live: Vec::new(),
            retired: Vec::new(),
        }
    }

    fn find_slot(&self, name: &CStr) -> Option<u16> {
        let slot = self
            .live
            .iter()
            .position(|key| key.get().name().as_ref() == name)?;
        Some(u16::try_from(slot).expect("RLookup key count exceeds u16::MAX"))
    }
}

/// Compact owner for an [`RLookup`][crate::RLookup]'s keys.
///
/// The store is allocated lazily because most pipeline lookups are empty. Once allocated, current
/// keys are kept in row-slot order and replaced keys are retained separately for pointer stability.
#[derive(Debug)]
#[repr(C)]
pub struct KeyList<'a> {
    store: Option<Box<KeyStore<'a>>>,
    sealed: bool,
}

/// A cursor over an [`RLookup`][crate::RLookup]'s current keys.
pub struct Cursor<'list, 'a> {
    list: &'list KeyList<'a>,
    current: Option<usize>,
}

/// A cursor over an [`RLookup`][crate::RLookup]'s current keys with editing operations.
pub struct CursorMut<'list, 'a> {
    list: &'list mut KeyList<'a>,
    current: Option<usize>,
}

/// Iterator over an [`RLookup`][crate::RLookup]'s current keys.
pub struct Iter<'list, 'a> {
    inner: slice::Iter<'list, OwnedKey<'a>>,
}

/// Mutable iterator over an [`RLookup`][crate::RLookup]'s current keys.
pub struct IterMut<'list, 'a> {
    inner: slice::IterMut<'list, OwnedKey<'a>>,
}

impl<'a> KeyList<'a> {
    pub const fn new() -> Self {
        Self {
            store: None,
            sealed: false,
        }
    }

    pub(crate) const fn seal(&mut self) {
        self.sealed = true;
    }

    pub(crate) const fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub(crate) fn row_len(&self) -> u32 {
        u32::try_from(self.live().len()).expect("RLookup row length exceeds u32::MAX")
    }

    pub(crate) fn push_slot(&mut self, mut key: RLookupKey<'a>) -> u16 {
        let store = self.store.get_or_insert_with(|| Box::new(KeyStore::new()));
        let slot = u16::try_from(store.live.len()).expect("RLookup key count exceeds u16::MAX");
        key.dstidx = slot;
        store.live.push(OwnedKey::new(key));

        #[cfg(debug_assertions)]
        self.assert_valid("KeyList::push");

        slot
    }

    /// Insert a key at the next logical row slot.
    pub(crate) fn push(&mut self, key: RLookupKey<'a>) -> Pin<&mut RLookupKey<'a>> {
        let slot = self.push_slot(key);

        self.store
            .as_mut()
            .unwrap()
            .live
            .get_mut(usize::from(slot))
            .unwrap()
            .get_pin_mut()
    }

    pub fn cursor_front(&self) -> Cursor<'_, 'a> {
        Cursor {
            list: self,
            current: (!self.live().is_empty()).then_some(0),
        }
    }

    pub fn iter(&self) -> Iter<'_, 'a> {
        Iter {
            inner: self.live().iter(),
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, 'a> {
        IterMut {
            inner: self.live_mut().iter_mut(),
        }
    }

    pub(crate) fn find_by_name(&self, name: &CStr) -> Option<Cursor<'_, 'a>> {
        let current = Some(usize::from(self.find_slot(name)?));
        Some(Cursor {
            list: self,
            current,
        })
    }

    #[cfg(test)]
    pub(crate) fn find_by_name_mut(&mut self, name: &CStr) -> Option<CursorMut<'_, 'a>> {
        let current = Some(usize::from(self.find_slot(name)?));
        Some(CursorMut {
            list: self,
            current,
        })
    }

    pub(crate) fn cursor_at_mut(&mut self, slot: u16) -> CursorMut<'_, 'a> {
        debug_assert!(usize::from(slot) < self.live().len());
        CursorMut {
            list: self,
            current: Some(usize::from(slot)),
        }
    }

    pub(crate) fn find_slot(&self, name: &CStr) -> Option<u16> {
        self.store.as_ref()?.find_slot(name)
    }

    pub(crate) fn get(&self, slot: u16) -> Option<&RLookupKey<'a>> {
        self.live().get(usize::from(slot)).map(OwnedKey::get)
    }

    pub(crate) fn get_ptr(&self, slot: u16) -> Option<NonNull<RLookupKey<'a>>> {
        self.live()
            .get(usize::from(slot))
            .map(OwnedKey::as_non_null)
    }

    /// Return the contiguous C-facing pointer view and its length.
    ///
    /// Mutation may reallocate this pointer array, so the caller must keep the lookup immutable
    /// until it finishes consuming the returned range. The key allocations themselves stay stable.
    pub(crate) fn raw_parts(&self) -> (*const *const RLookupKey<'a>, usize) {
        let live = self.live();
        (live.as_ptr().cast(), live.len())
    }

    fn live(&self) -> &[OwnedKey<'a>] {
        self.store.as_ref().map_or(&[], |store| &store.live)
    }

    fn live_mut(&mut self) -> &mut [OwnedKey<'a>] {
        self.store.as_mut().map_or(&mut [], |store| &mut store.live)
    }

    #[track_caller]
    #[cfg(any(debug_assertions, test))]
    pub(crate) fn assert_valid(&self, ctx: &str) {
        self.assert_structure_valid(ctx);

        let Some(store) = &self.store else {
            return;
        };

        // Per-key checks only: this runs on every list operation, so it must
        // stay linear. A per-key `find_slot` cross-check would make each call
        // quadratic (and cubic across a lookup's construction) — and on this
        // representation it is tautological anyway, since `find_slot` is
        // itself a first-match scan in slot order.
        for owned in &store.live {
            owned.get().assert_valid(ctx);
        }
        for owned in &store.retired {
            owned.get().assert_valid(ctx);
        }
    }

    /// Validate invariants that do not require dereferencing borrowed key data.
    #[track_caller]
    #[cfg(any(debug_assertions, test))]
    pub(crate) fn assert_structure_valid(&self, ctx: &str) {
        let Some(store) = &self.store else {
            return;
        };

        for (slot, owned) in store.live.iter().enumerate() {
            let key = owned.get();
            assert!(
                !key.is_tombstone(),
                "{ctx} - live key at slot {slot} is a tombstone"
            );
            assert_eq!(
                usize::from(key.dstidx),
                slot,
                "{ctx} - dstidx does not match slot"
            );
        }
        for owned in &store.retired {
            assert!(
                owned.get().is_tombstone(),
                "{ctx} - retired key is not a tombstone"
            );
        }
    }
}

impl<'list, 'a> Cursor<'list, 'a> {
    pub fn move_next(&mut self) {
        self.current = self
            .current
            .and_then(|slot| (slot + 1 < self.list.live().len()).then_some(slot + 1));
    }

    pub fn current(&self) -> Option<&RLookupKey<'a>> {
        self.current
            .and_then(|slot| self.list.live().get(slot))
            .map(OwnedKey::get)
    }

    pub fn into_current(self) -> Option<&'list RLookupKey<'a>> {
        self.current
            .and_then(|slot| self.list.live().get(slot))
            .map(OwnedKey::get)
    }
}

impl<'list, 'a> CursorMut<'list, 'a> {
    pub fn move_next(&mut self) {
        self.current = self
            .current
            .and_then(|slot| (slot + 1 < self.list.live().len()).then_some(slot + 1));
    }

    pub fn current(&mut self) -> Option<Pin<&mut RLookupKey<'a>>> {
        let slot = self.current?;
        Some(self.list.live_mut().get_mut(slot)?.get_pin_mut())
    }

    pub fn into_current(self) -> Option<&'list mut RLookupKey<'a>> {
        let slot = self.current?;
        // SAFETY: `OwnedKey` owns a pinned allocation; this consumes the list's exclusive borrow.
        Some(unsafe { self.list.live_mut().get_mut(slot)?.0.as_mut() })
    }

    /// Replace the current logical key while preserving both its row slot and old pointer.
    pub fn override_current(
        self,
        flags: RLookupKeyFlags,
    ) -> Option<Pin<&'list mut RLookupKey<'a>>> {
        assert!(
            !self.list.sealed,
            "cannot override a key in a sealed RLookup (sealed lookups are append-only)"
        );

        let slot = self.current?;
        let store = self.list.store.as_mut().unwrap();
        let old = &mut store.live[slot];
        let dstidx = old.get().dstidx;
        let (name, path) = old.get_pin_mut().make_tombstone();
        let mut replacement = if let Some(path) = path {
            RLookupKey::new_with_path(name, path, flags)
        } else {
            RLookupKey::new(name, flags)
        };
        replacement.dstidx = dstidx;

        let retired = mem::replace(old, OwnedKey::new(replacement));
        store.retired.push(retired);

        #[cfg(debug_assertions)]
        self.list.assert_valid("CursorMut::override_current");

        Some(self.list.store.as_mut().unwrap().live[slot].get_pin_mut())
    }
}

impl<'list, 'a> Iterator for Iter<'list, 'a> {
    type Item = &'list RLookupKey<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(OwnedKey::get)
    }
}

impl<'list, 'a> Iterator for IterMut<'list, 'a> {
    type Item = Pin<&'list mut RLookupKey<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(OwnedKey::get_pin_mut)
    }
}

impl FusedIterator for Iter<'_, '_> {}
impl FusedIterator for IterMut<'_, '_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RLookupKeyFlag;
    use enumflags2::make_bitflags;

    #[test]
    fn append_and_lookup_preserve_row_order() {
        let mut keys = KeyList::new();
        keys.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keys.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keys.push(RLookupKey::new(c"baz", RLookupKeyFlags::empty()));

        let names: Vec<_> = keys.iter().map(|key| key.name().as_ref()).collect();
        assert_eq!(names, [c"foo", c"bar", c"baz"]);
        assert_eq!(keys.find_slot(c"bar"), Some(1));
        assert_eq!(keys.find_slot(c"missing"), None);
        assert_eq!(keys.row_len(), 3);
    }

    #[test]
    fn duplicate_names_resolve_to_first_slot() {
        let mut keys = KeyList::new();
        keys.push(RLookupKey::new(c"same", RLookupKeyFlags::empty()));
        keys.push(RLookupKey::new(c"same", RLookupKeyFlags::empty()));

        assert_eq!(keys.find_slot(c"same"), Some(0));
    }

    #[test]
    fn hidden_keys_remain_in_logical_row_order() {
        let mut keys = KeyList::new();
        keys.push(RLookupKey::new(
            c"hidden",
            make_bitflags!(RLookupKeyFlag::Hidden),
        ));
        keys.push(RLookupKey::new(c"visible", RLookupKeyFlags::empty()));

        let names: Vec<_> = keys.iter().map(|key| key.name().as_ref()).collect();
        assert_eq!(names, [c"hidden", c"visible"]);
    }

    #[test]
    fn override_keeps_slot_order_and_old_pointer_valid() {
        let mut keys = KeyList::new();
        keys.push(RLookupKey::new_with_path(
            c"foo",
            c"$.foo",
            RLookupKeyFlags::empty(),
        ));
        let old = keys.get_ptr(0).unwrap();
        keys.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));

        let replacement = keys
            .find_by_name_mut(c"foo")
            .unwrap()
            .override_current(make_bitflags!(RLookupKeyFlag::Numeric))
            .unwrap();

        assert_ne!(NonNull::from(replacement.as_ref().get_ref()), old);
        assert_eq!(replacement.dstidx, 0);
        assert!(replacement.flags.contains(RLookupKeyFlag::Numeric));
        assert_eq!(keys.find_slot(c"foo"), Some(0));
        assert_eq!(
            keys.iter()
                .map(|key| key.name().as_ref())
                .collect::<Vec<_>>(),
            [c"foo", c"bar"]
        );
        // SAFETY: replaced keys remain owned by `keys.retired` until the list is dropped.
        let old = unsafe { old.as_ref() };
        assert!(old.is_tombstone());
        // SAFETY: `path` still points to the retained key's original NUL-terminated path.
        assert_eq!(unsafe { CStr::from_ptr(old.path) }, c"$.foo");
    }

    #[test]
    fn vector_growth_does_not_move_key_allocations() {
        let mut keys = KeyList::new();
        keys.push(RLookupKey::new(c"first", RLookupKeyFlags::empty()));
        let first = keys.get_ptr(0).unwrap();
        let initial_capacity = keys.store.as_ref().unwrap().live.capacity();

        for i in 0..initial_capacity {
            let name = std::ffi::CString::new(format!("key-{i}")).unwrap();
            keys.push(RLookupKey::new(name, RLookupKeyFlags::empty()));
        }

        assert!(keys.store.as_ref().unwrap().live.capacity() > initial_capacity);
        assert_eq!(NonNull::from(keys.get(0).unwrap()), first);
    }

    #[test]
    fn repeated_overrides_retain_every_previous_allocation() {
        let mut keys = KeyList::new();
        keys.push(RLookupKey::new(c"key", RLookupKeyFlags::empty()));
        let mut old = Vec::new();

        for _ in 0..8 {
            old.push(keys.get_ptr(0).unwrap());
            keys.cursor_at_mut(0)
                .override_current(RLookupKeyFlags::empty())
                .unwrap();
        }

        assert_eq!(keys.row_len(), 1);
        assert_eq!(keys.store.as_ref().unwrap().retired.len(), 8);
        assert!(old.into_iter().all(|pointer| {
            // SAFETY: every previous allocation is retained by this list.
            unsafe { pointer.as_ref() }.is_tombstone()
        }));
    }

    #[test]
    fn raw_parts_contains_only_live_keys() {
        let mut keys = KeyList::new();
        keys.push(RLookupKey::new(c"foo", RLookupKeyFlags::empty()));
        keys.push(RLookupKey::new(c"bar", RLookupKeyFlags::empty()));
        keys.find_by_name_mut(c"foo")
            .unwrap()
            .override_current(RLookupKeyFlags::empty());

        let (raw, len) = keys.raw_parts();
        assert_eq!(len, 2);
        // SAFETY: the lookup is not mutated while the returned pointer array is read.
        let names: Vec<_> = unsafe { slice::from_raw_parts(raw, len) }
            .iter()
            .map(|key| {
                // SAFETY: `raw_parts` returns aligned, initialized, non-null pointers owned by
                // `keys`, and the lookup remains immutable for the duration of this iteration.
                unsafe { &**key }.name().as_ref()
            })
            .collect();
        assert_eq!(names, [c"foo", c"bar"]);
    }
}
