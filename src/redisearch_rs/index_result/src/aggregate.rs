/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ref_mode::{Active, Ptr, Ref};
use thin_vec::SmallThinVec;

use super::core::{RSIndexResult, RawIndexResult};
use super::kind::RSResultKindMask;

/// Represents an aggregate array of values in an index record.
///
/// The C code should always use `AggregateResult_New` to construct a new instance of this type
/// using Rust since the internals cannot be constructed directly in C. The reason is because of
/// the `ThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
#[cheadergen::config(prefix_with_name)]
#[repr(u8)]
pub enum RawAggregateResult<R: Ref> {
    Borrowed {
        /// The records making up this aggregate result
        ///
        /// The `RawAggregateResult` is part of a union in [`super::result_data::RawResultData`], so
        /// it needs to have a known size. The std `Vec` won't have this since it is not
        /// `#[repr(C)]`, so we use our own `ThinVec` type which is `#[repr(C)]` and has a known
        /// size instead.
        ///
        /// Each child is stored as a [`Ptr<R, RawIndexResult<R>>`]. In [`Active`] mode this is
        /// equivalent to a `&'a RSIndexResult<'a>`; in [`ref_mode::Suspended`] mode it is
        /// an inert raw pointer that survives lock release/reacquire cycles.
        records: SmallThinVec<Ptr<R, RawIndexResult<R>>>,

        /// A map of the aggregate kind of the underlying records
        kind_mask: RSResultKindMask,
    },
    Owned {
        /// The records making up this aggregate result
        ///
        /// The `RawAggregateResult` is part of a union in [`super::result_data::RawResultData`], so it needs to have a
        /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
        /// own `ThinVec` type which is `#[repr(C)]` and has a known size instead.
        records: SmallThinVec<Box<RawIndexResult<R>>>,

        /// A map of the aggregate kind of the underlying records
        kind_mask: RSResultKindMask,
    },
}

/// The [`Active`] instantiation of [`RawAggregateResult`].
pub type RSAggregateResult<'a> = RawAggregateResult<Active<'a>>;

impl<'a> std::fmt::Debug for RSAggregateResult<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawAggregateResult::Borrowed { records, kind_mask } => f
                .debug_struct("RSAggregateResult::Borrowed")
                .field(
                    "records",
                    &records.iter().map(|p| p.get()).collect::<Vec<_>>(),
                )
                .field("kind_mask", kind_mask)
                .finish(),
            RawAggregateResult::Owned { records, kind_mask } => f
                .debug_struct("RSAggregateResult::Owned")
                .field("records", records)
                .field("kind_mask", kind_mask)
                .finish(),
        }
    }
}

impl<'a> PartialEq for RSAggregateResult<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                RawAggregateResult::Borrowed {
                    records: a,
                    kind_mask: ma,
                },
                RawAggregateResult::Borrowed {
                    records: b,
                    kind_mask: mb,
                },
            ) => {
                ma == mb
                    && a.len() == b.len()
                    && a.iter().zip(b.iter()).all(|(x, y)| x.get() == y.get())
            }
            (
                RawAggregateResult::Owned {
                    records: a,
                    kind_mask: ma,
                },
                RawAggregateResult::Owned {
                    records: b,
                    kind_mask: mb,
                },
            ) => ma == mb && a == b,
            _ => false,
        }
    }
}

impl<R: Ref> RawAggregateResult<R> {
    /// Create a new empty aggregate result (of the borrowed kind) with the given capacity
    pub fn borrowed_with_capacity(cap: usize) -> Self {
        Self::Borrowed {
            records: SmallThinVec::with_capacity(cap),
            kind_mask: RSResultKindMask::empty(),
        }
    }

    /// Create a new empty aggregate result (of the owned kind) with the given capacity
    pub fn owned_with_capacity(cap: usize) -> Self {
        Self::Owned {
            records: SmallThinVec::with_capacity(cap),
            kind_mask: RSResultKindMask::empty(),
        }
    }

    /// The number of results in this aggregate result
    pub fn len(&self) -> usize {
        match self {
            RawAggregateResult::Borrowed { records, .. } => records.len(),
            RawAggregateResult::Owned { records, .. } => records.len(),
        }
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        match self {
            RawAggregateResult::Borrowed { records, .. } => records.is_empty(),
            RawAggregateResult::Owned { records, .. } => records.is_empty(),
        }
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        match self {
            RawAggregateResult::Borrowed { records, .. } => records.capacity(),
            RawAggregateResult::Owned { records, .. } => records.capacity(),
        }
    }

    /// The current type mask of the aggregate result
    pub const fn kind_mask(&self) -> RSResultKindMask {
        match self {
            RawAggregateResult::Borrowed { kind_mask, .. } => *kind_mask,
            RawAggregateResult::Owned { kind_mask, .. } => *kind_mask,
        }
    }

    /// Reset the aggregate result, clearing the children vector and resetting the kind mask.
    pub fn reset(&mut self) {
        match self {
            RawAggregateResult::Borrowed {
                records, kind_mask, ..
            } => {
                records.clear();
                *kind_mask = RSResultKindMask::empty();
            }
            RawAggregateResult::Owned { records, kind_mask } => {
                records.clear();
                *kind_mask = RSResultKindMask::empty();
            }
        }
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
            RawAggregateResult::Borrowed { records, .. } => records.get(index).map(|p| p.get()),
            RawAggregateResult::Owned { records, .. } => records.get(index).map(AsRef::as_ref),
        }
    }

    /// Get the child at the given index, if it exists.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    pub unsafe fn get_unchecked(&self, index: usize) -> &RSIndexResult<'a> {
        match self {
            RawAggregateResult::Borrowed { records, .. } => {
                debug_assert!(
                    index < records.len(),
                    "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
                    records.len()
                );
                // SAFETY:
                // - Thanks to precondition 1., we know that the index is within bounds.
                unsafe { records.get_unchecked(index) }.get()
            }
            RawAggregateResult::Owned { records, .. } => {
                debug_assert!(
                    index < records.len(),
                    "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
                    records.len()
                );
                // SAFETY:
                // - Thanks to precondition 1., we know that the index is within bounds.
                unsafe { records.get_unchecked(index) }
            }
        }
    }

    /// Add a child to the aggregate result and update the kind mask
    ///
    /// # Safety
    /// The given `child` has to stay valid for the lifetime of this aggregate result. Else reading
    /// the child with [`Self::get()`] will cause undefined behavior.
    pub fn push_borrowed(&mut self, child: &'a RSIndexResult<'a>) {
        match self {
            RawAggregateResult::Borrowed {
                records, kind_mask, ..
            } => {
                records.push(Ptr::new(child));

                *kind_mask |= child.kind();
            }
            RawAggregateResult::Owned { .. } => {
                panic!("Cannot push a borrowed child to an owned aggregate result");
            }
        }
    }

    /// Create an owned copy of this aggregate result, allocating new memory for the records.
    ///
    /// The returned aggregate result will have the same lifetime as the original one,
    /// since it may borrow terms from the original result.
    pub fn to_owned(&'a self) -> RSAggregateResult<'a> {
        match self {
            RawAggregateResult::Borrowed { records, kind_mask } => {
                let mut new_records = SmallThinVec::with_capacity(records.len());

                new_records.extend(
                    records
                        .iter()
                        .map(|c| RSIndexResult::to_owned(c.get()))
                        .map(Box::new),
                );

                RawAggregateResult::Owned {
                    records: new_records,
                    kind_mask: *kind_mask,
                }
            }
            RawAggregateResult::Owned { records, kind_mask } => {
                let mut new_records = SmallThinVec::with_capacity(records.len());

                new_records.extend(
                    records
                        .iter()
                        .map(|c| RSIndexResult::to_owned(c))
                        .map(Box::new),
                );

                RawAggregateResult::Owned {
                    records: new_records,
                    kind_mask: *kind_mask,
                }
            }
        }
    }

    /// Add a heap owned child to the aggregate result and update the kind mask
    pub fn push_boxed(&mut self, child: Box<RSIndexResult<'a>>) {
        match self {
            RawAggregateResult::Borrowed { .. } => {
                panic!("Cannot push a borrowed child to an owned aggregate result");
            }
            RawAggregateResult::Owned { records, kind_mask } => {
                *kind_mask |= child.kind();
                records.push(child);
            }
        }
    }

    /// Get a mutable reference to the child at the given index, if it exists
    pub fn get_mut(&mut self, index: usize) -> Option<&mut RSIndexResult<'a>> {
        match self {
            RawAggregateResult::Borrowed { .. } => {
                panic!("Cannot get a mutable reference to a borrowed aggregate result");
            }
            RawAggregateResult::Owned { records, .. } => records.get_mut(index).map(AsMut::as_mut),
        }
    }

    /// Get a mutable reference to the child at the given index, without checking bounds.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    /// 2. The aggregate result must be of the `Owned` variant.
    pub unsafe fn get_mut_unchecked(&mut self, index: usize) -> &mut RSIndexResult<'a> {
        match self {
            RawAggregateResult::Borrowed { .. } => {
                debug_assert!(
                    false,
                    "Safety violation: trying to get a mutable reference from a borrowed aggregate result"
                );
                // SAFETY: Thanks to precondition 2., we'll never reach this statement.
                unsafe { std::hint::unreachable_unchecked() }
            }
            RawAggregateResult::Owned { records, .. } => {
                debug_assert!(
                    index < records.len(),
                    "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
                    records.len()
                );
                // SAFETY: Thanks to precondition 1., we know that the index is within bounds.
                unsafe { records.get_unchecked_mut(index) }
            }
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
