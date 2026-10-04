/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ref_mode::{Active, Ref, SharedPtr, Suspended};
use thin_vec::MediumThinVec;

use super::core::{RSIndexResult, RawIndexResult};
use super::kind::RSResultKindMask;

/// Represents an aggregate array of values in an index record.
///
/// How the children are held is what separates the two variants, and each one
/// carries its own type — [`RawBorrowedAggregateResult`] and
/// [`RawOwnedAggregateResult`]. An operation that only one of them supports
/// therefore lives on that type: pushing a borrowed child, or handing out a `&mut`
/// to a child, cannot be attempted on the wrong kind of aggregate. Reach the
/// payload with [`as_borrowed`](Self::as_borrowed), [`as_owned`](Self::as_owned)
/// or their `_mut` counterparts.
///
/// `RawAggregateResult` is part of a union in
/// [`super::result_data::RawResultData`], so it needs to have a known size. That
/// is why both payloads hold their children in a [`MediumThinVec`] rather than the
/// std `Vec`, which is not `#[repr(C)]`.
///
/// The C code should always use `AggregateResult_New` to construct a new instance of this type
/// using Rust since the internals cannot be constructed directly in C. The reason is because of
/// the `ThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
#[cheadergen::config(prefix_with_name)]
#[derive(Debug)]
#[repr(u8)]
pub enum RawAggregateResult<'query, R: Ref> {
    Borrowed(RawBorrowedAggregateResult<'query, R>),
    Owned(RawOwnedAggregateResult<'query, R>),
}

/// The [`Active`] instantiation of [`RawAggregateResult`].
#[cheadergen::config(export)]
pub type RSAggregateResult<'a> = RawAggregateResult<'a, Active<'a>>;

/// An aggregate result whose children live elsewhere — in the composite iterator
/// that built it, typically — and are only pointed at from here.
///
/// The [`Borrowed`](RawAggregateResult::Borrowed) payload of
/// [`RawAggregateResult`].
#[derive(Debug)]
#[repr(C)]
pub struct RawBorrowedAggregateResult<'query, R: Ref> {
    /// The records making up this aggregate result.
    ///
    /// Each child is stored as a [`SharedPtr<R, RawIndexResult<R>>`]. In [`Active`] mode this is
    /// equivalent to a `&'a RSIndexResult<'a>`; in [`ref_mode::Suspended`] mode it is
    /// an inert raw pointer that survives lock release/reacquire cycles.
    records: MediumThinVec<SharedPtr<R, RawIndexResult<'query, R>>>,

    /// A map of the aggregate kind of the underlying records
    kind_mask: RSResultKindMask,
}

/// The [`Active`] instantiation of [`RawBorrowedAggregateResult`].
pub type RSBorrowedAggregateResult<'a> = RawBorrowedAggregateResult<'a, Active<'a>>;

/// An aggregate result that owns its children, holding each one in its own heap
/// allocation.
///
/// The [`Owned`](RawAggregateResult::Owned) payload of [`RawAggregateResult`].
#[derive(Debug)]
#[repr(C)]
pub struct RawOwnedAggregateResult<'query, R: Ref> {
    /// The records making up this aggregate result, each owned by this aggregate.
    records: MediumThinVec<Box<RawIndexResult<'query, R>>>,

    /// A map of the aggregate kind of the underlying records
    kind_mask: RSResultKindMask,
}

/// The [`Active`] instantiation of [`RawOwnedAggregateResult`].
pub type RSOwnedAggregateResult<'a> = RawOwnedAggregateResult<'a, Active<'a>>;

// Compile-time proof that the `Active` and `Suspended` instantiations of the two
// payloads — and hence of `RawAggregateResult` itself — are layout-identical.
// Field offsets are checked on the payloads, which `offset_of!` can address;
// for the enum wrapping them only `size_of`/`align_of` are available, since
// `offset_of!` cannot reach the fields of a `#[repr(u8)]` enum variant. The
// child `RawIndexResult<R>` read through a `records` pointer is guarded by the
// `core/mod.rs` block. Part of the recursive net backing the conversions on
// `RawIndexResult`.
const _: () = {
    use ref_mode::Suspended;
    use std::mem::{align_of, offset_of, size_of};

    type ActiveBorrowed = RawBorrowedAggregateResult<'static, Active<'static>>;
    type SuspendedBorrowed = RawBorrowedAggregateResult<'static, Suspended>;
    assert!(size_of::<ActiveBorrowed>() == size_of::<SuspendedBorrowed>());
    assert!(align_of::<ActiveBorrowed>() == align_of::<SuspendedBorrowed>());
    assert!(offset_of!(ActiveBorrowed, records) == offset_of!(SuspendedBorrowed, records));
    assert!(offset_of!(ActiveBorrowed, kind_mask) == offset_of!(SuspendedBorrowed, kind_mask));

    type ActiveOwned = RawOwnedAggregateResult<'static, Active<'static>>;
    type SuspendedOwned = RawOwnedAggregateResult<'static, Suspended>;
    assert!(size_of::<ActiveOwned>() == size_of::<SuspendedOwned>());
    assert!(align_of::<ActiveOwned>() == align_of::<SuspendedOwned>());
    assert!(offset_of!(ActiveOwned, records) == offset_of!(SuspendedOwned, records));
    assert!(offset_of!(ActiveOwned, kind_mask) == offset_of!(SuspendedOwned, kind_mask));

    type ActiveAggregate = RawAggregateResult<'static, Active<'static>>;
    type SuspendedAggregate = RawAggregateResult<'static, Suspended>;
    assert!(size_of::<ActiveAggregate>() == size_of::<SuspendedAggregate>());
    assert!(align_of::<ActiveAggregate>() == align_of::<SuspendedAggregate>());
};

// Manual (rather than derived) because the records are
// `SharedPtr<R, RawIndexResult<R>>`, which only implements `PartialEq` in `Active`
// mode. Restricted to the `Active` alias accordingly.
impl<'a> PartialEq for RSBorrowedAggregateResult<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.kind_mask == other.kind_mask
            && self.records.len() == other.records.len()
            && self
                .records
                .iter()
                .zip(other.records.iter())
                .all(|(x, y)| x.get() == y.get())
    }
}

// Manual for the same reason as the borrowed payload above: the child
// `RawIndexResult<R>` is only `PartialEq` in `Active` mode.
impl<'a> PartialEq for RSOwnedAggregateResult<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.kind_mask == other.kind_mask && self.records == other.records
    }
}

impl<'a> PartialEq for RSAggregateResult<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Borrowed(a), Self::Borrowed(b)) => a == b,
            (Self::Owned(a), Self::Owned(b)) => a == b,
            _ => false,
        }
    }
}

impl<'query, R: Ref> RawBorrowedAggregateResult<'query, R> {
    /// Create a new empty borrowed aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            records: MediumThinVec::with_capacity(cap),
            kind_mask: RSResultKindMask::empty(),
        }
    }

    /// The number of results in this aggregate result
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        self.records.capacity()
    }

    /// The current type mask of the aggregate result
    pub const fn kind_mask(&self) -> RSResultKindMask {
        self.kind_mask
    }

    /// The pointers to the children of this aggregate result, in order.
    pub fn records(&self) -> &[SharedPtr<R, RawIndexResult<'query, R>>] {
        &self.records
    }

    /// Reset the aggregate result, clearing the children vector and resetting the kind mask.
    pub fn reset(&mut self) {
        self.records.clear();
        self.kind_mask = RSResultKindMask::empty();
    }
}

impl<'a> RSBorrowedAggregateResult<'a> {
    /// Get the child at the given index, if it exists.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'a>> {
        self.records.get(index).map(|p| p.get())
    }

    /// Get the child at the given index, if it exists.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    pub unsafe fn get_unchecked(&self, index: usize) -> &RSIndexResult<'a> {
        debug_assert!(
            index < self.records.len(),
            "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
            self.records.len()
        );
        // SAFETY:
        // - Thanks to precondition 1., we know that the index is within bounds.
        unsafe { self.records.get_unchecked(index) }.get()
    }

    /// Add a child to the aggregate result and update the kind mask.
    ///
    /// `'a` ties the child to this aggregate, so a Rust caller cannot outlive it.
    /// A caller reaching this across FFI, where `'a` was fabricated from a raw
    /// pointer, owes the guarantee the borrow checker would otherwise give:
    /// reading a dead child back out with [`Self::get`] is undefined behaviour.
    pub fn push_borrowed(&mut self, child: &'a RSIndexResult<'a>) {
        self.records.push(SharedPtr::from_ref(child));

        self.kind_mask |= child.kind();
    }

    /// Create an owned copy of this aggregate result, allocating new memory for the records.
    ///
    /// The returned aggregate result will have the same lifetime as the original one,
    /// since it may borrow terms from the original result.
    pub fn to_owned(&'a self) -> RSOwnedAggregateResult<'a> {
        let mut records = MediumThinVec::with_capacity(self.records.len());

        records.extend(
            self.records
                .iter()
                .map(|c| RSIndexResult::to_owned(c.get()))
                .map(Box::new),
        );

        RSOwnedAggregateResult {
            records,
            kind_mask: self.kind_mask,
        }
    }
}

impl<'query, R: Ref> RawOwnedAggregateResult<'query, R> {
    /// Create a new empty owned aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            records: MediumThinVec::with_capacity(cap),
            kind_mask: RSResultKindMask::empty(),
        }
    }

    /// The number of results in this aggregate result
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        self.records.capacity()
    }

    /// The current type mask of the aggregate result
    pub const fn kind_mask(&self) -> RSResultKindMask {
        self.kind_mask
    }

    /// The children owned by this aggregate result, in order.
    pub fn records(&self) -> &[Box<RawIndexResult<'query, R>>] {
        &self.records
    }

    /// Take the children out of this aggregate result, transferring ownership of
    /// each one to the caller.
    pub fn into_records(self) -> MediumThinVec<Box<RawIndexResult<'query, R>>> {
        self.records
    }

    /// Reset the aggregate result, clearing (and thereby dropping) the children
    /// and resetting the kind mask.
    pub fn reset(&mut self) {
        self.records.clear();
        self.kind_mask = RSResultKindMask::empty();
    }
}

impl<'a> RSOwnedAggregateResult<'a> {
    /// Get the child at the given index, if it exists.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'a>> {
        self.records.get(index).map(AsRef::as_ref)
    }

    /// Get the child at the given index, if it exists.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    pub unsafe fn get_unchecked(&self, index: usize) -> &RSIndexResult<'a> {
        debug_assert!(
            index < self.records.len(),
            "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
            self.records.len()
        );
        // SAFETY:
        // - Thanks to precondition 1., we know that the index is within bounds.
        unsafe { self.records.get_unchecked(index) }
    }

    /// Get a mutable reference to the child at the given index, if it exists
    pub fn get_mut(&mut self, index: usize) -> Option<&mut RSIndexResult<'a>> {
        self.records.get_mut(index).map(AsMut::as_mut)
    }

    /// Get a mutable reference to the child at the given index, without checking bounds.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    pub unsafe fn get_mut_unchecked(&mut self, index: usize) -> &mut RSIndexResult<'a> {
        debug_assert!(
            index < self.records.len(),
            "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
            self.records.len()
        );
        // SAFETY: Thanks to precondition 1., we know that the index is within bounds.
        unsafe { self.records.get_unchecked_mut(index) }
    }

    /// Add a heap owned child to the aggregate result and update the kind mask
    pub fn push_boxed(&mut self, child: Box<RSIndexResult<'a>>) {
        self.kind_mask |= child.kind();
        self.records.push(child);
    }

    /// Create an owned copy of this aggregate result, allocating new memory for the records.
    ///
    /// The returned aggregate result will have the same lifetime as the original one,
    /// since it may borrow terms from the original result.
    pub fn to_owned(&'a self) -> RSOwnedAggregateResult<'a> {
        let mut records = MediumThinVec::with_capacity(self.records.len());

        records.extend(
            self.records
                .iter()
                .map(|c| RSIndexResult::to_owned(c))
                .map(Box::new),
        );

        RSOwnedAggregateResult {
            records,
            kind_mask: self.kind_mask,
        }
    }
}

impl<'query, R: Ref> RawAggregateResult<'query, R> {
    /// Create a new empty aggregate result (of the borrowed kind) with the given capacity
    pub fn borrowed_with_capacity(cap: usize) -> Self {
        Self::Borrowed(RawBorrowedAggregateResult::with_capacity(cap))
    }

    /// Create a new empty aggregate result (of the owned kind) with the given capacity
    pub fn owned_with_capacity(cap: usize) -> Self {
        Self::Owned(RawOwnedAggregateResult::with_capacity(cap))
    }

    /// The borrowed payload, if this aggregate result borrows its children.
    pub const fn as_borrowed(&self) -> Option<&RawBorrowedAggregateResult<'query, R>> {
        match self {
            Self::Borrowed(agg) => Some(agg),
            Self::Owned(_) => None,
        }
    }

    /// The borrowed payload to mutate, if this aggregate result borrows its
    /// children. The only route to
    /// [`push_borrowed`](RSBorrowedAggregateResult::push_borrowed).
    pub const fn as_borrowed_mut(&mut self) -> Option<&mut RawBorrowedAggregateResult<'query, R>> {
        match self {
            Self::Borrowed(agg) => Some(agg),
            Self::Owned(_) => None,
        }
    }

    /// The owned payload, if this aggregate result owns its children.
    pub const fn as_owned(&self) -> Option<&RawOwnedAggregateResult<'query, R>> {
        match self {
            Self::Owned(agg) => Some(agg),
            Self::Borrowed(_) => None,
        }
    }

    /// The owned payload to mutate, if this aggregate result owns its children.
    /// The only route to [`push_boxed`](RSOwnedAggregateResult::push_boxed) and
    /// [`get_mut`](RSOwnedAggregateResult::get_mut).
    pub const fn as_owned_mut(&mut self) -> Option<&mut RawOwnedAggregateResult<'query, R>> {
        match self {
            Self::Owned(agg) => Some(agg),
            Self::Borrowed(_) => None,
        }
    }

    /// The number of results in this aggregate result
    pub fn len(&self) -> usize {
        match self {
            Self::Borrowed(agg) => agg.len(),
            Self::Owned(agg) => agg.len(),
        }
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Borrowed(agg) => agg.is_empty(),
            Self::Owned(agg) => agg.is_empty(),
        }
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        match self {
            Self::Borrowed(agg) => agg.capacity(),
            Self::Owned(agg) => agg.capacity(),
        }
    }

    /// The current type mask of the aggregate result
    pub const fn kind_mask(&self) -> RSResultKindMask {
        match self {
            Self::Borrowed(agg) => agg.kind_mask(),
            Self::Owned(agg) => agg.kind_mask(),
        }
    }

    /// Reset the aggregate result, clearing the children vector and resetting the
    /// kind mask. Dispatches to
    /// [`RawBorrowedAggregateResult::reset`] or [`RawOwnedAggregateResult::reset`],
    /// which differ in what becomes of the children.
    pub fn reset(&mut self) {
        match self {
            Self::Borrowed(agg) => agg.reset(),
            Self::Owned(agg) => agg.reset(),
        }
    }
}

impl<'query> RawBorrowedAggregateResult<'query, Suspended> {
    /// Append an entry for `child`, taking its provenance from that live
    /// reference — the push counterpart of
    /// [`push_borrowed`](RSBorrowedAggregateResult::push_borrowed), for a caller
    /// holding a [`Suspended`] aggregate and an [`Active`] child.
    ///
    /// # Why a suspended aggregate needs this
    ///
    /// Every entry is a pointer derived from a `&` to a child's own
    /// [`RawIndexResult`]. A resume re-narrows them all in one whole-allocation
    /// cast, which the addresses survive — the children never leave their slots
    /// — but their *provenance* does not: transitioning a child hands its
    /// allocation through a by-value `Box<Self>`, and that retag invalidates the
    /// borrow each entry was derived from. Rebuilding the list while the result
    /// is still suspended writes every entry from a reference that post-dates
    /// the retag.
    ///
    /// # What the caller still owes
    ///
    /// [`reset`](RawBorrowedAggregateResult::reset) drops the kind mask with the
    /// records and this rebuilds it, but `freq` and `field_mask` live on the
    /// enclosing [`RawIndexResult`] and are the caller's to re-accumulate.
    /// `metrics` are not: [`RSIndexResult::push_borrowed`] **moves** them out of
    /// the children when the aggregate is first built, so there is nothing to
    /// re-accumulate and they must be carried across untouched.
    ///
    /// The lifetime tie does not keep the child *alive*, since the stored
    /// pointer is raw — that stays [`RawIndexResult::into_active`]'s
    /// preconditions (3) and (4), along with taking no `&mut` to the child
    /// before the result is re-narrowed.
    ///
    /// # Why the child is tied to `'query`
    ///
    /// A child admitted at a shorter lifetime would have its query-pipeline
    /// pointers — the [`RLookupKey`](ffi::RLookupKey) in its `metrics`, a term
    /// record's borrowed query term — silently widened to `'query`.
    /// [`RawIndexResult::into_active`] excludes exactly those from the caller's
    /// obligations because the `'query: 'a` bound already covers them, so
    /// widening would remove the only thing that clause rests on, in safe code
    /// and with no diagnostic. A composite whose children sit at a shorter `'a`
    /// narrows its *result* to `'a` instead — the sound direction, which `&mut`
    /// invariance stops the compiler from taking on its own.
    pub fn push_borrowed_ptr_from_ref(&mut self, child: &RSIndexResult<'query>) {
        // The cast is between the two `Ref` modes of one `#[repr(C)]` type,
        // proven layout-identical in `core`, and changes neither address nor
        // provenance.
        let live = NonNull::from_ref(child).cast::<RawIndexResult<'query, Suspended>>();
        self.records.push(SharedPtr::from_non_null(live));

        self.kind_mask |= child.kind();
    }
}

impl<'a> RSAggregateResult<'a> {
    /// Get an iterator over the children of this aggregate result
    pub const fn iter(&'a self) -> RSAggregateResultIter<'a> {
        RSAggregateResultIter {
            agg: self,
            index: 0,
        }
    }

    /// Get the child at the given index, if it exists.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'a>> {
        match self {
            Self::Borrowed(agg) => agg.get(index),
            Self::Owned(agg) => agg.get(index),
        }
    }

    /// Get the child at the given index, if it exists.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    pub unsafe fn get_unchecked(&self, index: usize) -> &RSIndexResult<'a> {
        match self {
            // SAFETY: precondition 1. is forwarded to the payload unchanged.
            Self::Borrowed(agg) => unsafe { agg.get_unchecked(index) },
            // SAFETY: precondition 1. is forwarded to the payload unchanged.
            Self::Owned(agg) => unsafe { agg.get_unchecked(index) },
        }
    }

    /// Create an owned copy of this aggregate result, allocating new memory for the records.
    ///
    /// The returned aggregate result will have the same lifetime as the original one,
    /// since it may borrow terms from the original result.
    pub fn to_owned(&'a self) -> RSAggregateResult<'a> {
        match self {
            Self::Borrowed(agg) => Self::Owned(agg.to_owned()),
            Self::Owned(agg) => Self::Owned(agg.to_owned()),
        }
    }
}

/// An iterator over the results in an [`RSAggregateResult`].
pub struct RSAggregateResultIter<'a> {
    agg: &'a RSAggregateResult<'a>,
    index: usize,
}

impl<'a> Iterator for RSAggregateResultIter<'a> {
    type Item = &'a RSIndexResult<'a>;

    /// Get the next item in the iterator
    ///
    /// # Safety
    /// The caller must ensure that all memory pointers in the aggregate result are still valid.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.agg.get(self.index) {
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}
