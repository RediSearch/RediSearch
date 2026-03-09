/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use thin_vec::SmallThinVec;

use super::core::RSIndexResult;
use super::kind::RSResultKindMask;

/// Represents an aggregate array of values in an index record.
///
/// The C code should always use `AggregateResult_New` to construct a new instance of this type
/// using Rust since the internals cannot be constructed directly in C. The reason is because of
/// the `ThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
/// cbindgen:prefix-with-name=true
#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum RSAggregateResult<'index> {
    Borrowed {
        /// The records making up this aggregate result
        ///
        /// The `RSAggregateResult` is part of a union in [`super::result_data::RSResultData`], so it needs to have a
        /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
        /// own `ThinVec` type which is `#[repr(C)]` and has a known size instead.
        ///
        /// This requires `'index` on the reference because adding a new lifetime will cause the
        /// type to be `ThinVec<&'refs RSIndexResult<'index, 'refs>>` which will require
        /// `'index: 'refs` else it would mean the `'index` can be cleaned up while some reference
        /// will still try to access it (ie a dangling pointer). Now the decoders will never return
        /// any aggregate results so `'refs == 'static` when decoding. Because of the requirement
        /// above, this means `'index: 'static` which is just incorrect since the index data will
        /// never be `'static` when decoding.
        records: SmallThinVec<&'index RSIndexResult<'index>>,

        /// A map of the aggregate kind of the underlying records
        kind_mask: RSResultKindMask,
    },
    Owned {
        /// The records making up this aggregate result
        ///
        /// The `RSAggregateResult` is part of a union in [`super::result_data::RSResultData`], so it needs to have a
        /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
        /// own `ThinVec` type which is `#[repr(C)]` and has a known size instead.
        records: SmallThinVec<Box<RSIndexResult<'index>>>,

        /// A map of the aggregate kind of the underlying records
        kind_mask: RSResultKindMask,
    },
}

impl<'index> RSAggregateResult<'index> {
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
            RSAggregateResult::Borrowed { records, .. } => records.len(),
            RSAggregateResult::Owned { records, .. } => records.len(),
        }
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        match self {
            RSAggregateResult::Borrowed { records, .. } => records.is_empty(),
            RSAggregateResult::Owned { records, .. } => records.is_empty(),
        }
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        match self {
            RSAggregateResult::Borrowed { records, .. } => records.capacity(),
            RSAggregateResult::Owned { records, .. } => records.capacity(),
        }
    }

    /// The current type mask of the aggregate result
    pub const fn kind_mask(&self) -> RSResultKindMask {
        match self {
            RSAggregateResult::Borrowed { kind_mask, .. } => *kind_mask,
            RSAggregateResult::Owned { kind_mask, .. } => *kind_mask,
        }
    }

    /// Get an iterator over the children of this aggregate result
    pub const fn iter(&'index self) -> RSAggregateResultIter<'index> {
        RSAggregateResultIter {
            agg: self,
            index: 0,
        }
    }

    /// Get the child at the given index, if it exists.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'index>> {
        match self {
            RSAggregateResult::Borrowed { records, .. } => records.get(index).copied(),
            RSAggregateResult::Owned { records, .. } => records.get(index).map(AsRef::as_ref),
        }
    }

    /// Get the child at the given index, if it exists.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    pub unsafe fn get_unchecked(&self, index: usize) -> &RSIndexResult<'index> {
        match self {
            RSAggregateResult::Borrowed { records, .. } => {
                debug_assert!(
                    index < records.len(),
                    "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
                    records.len()
                );
                // SAFETY:
                // - Thanks to precondition 1., we know that the index is within bounds.
                unsafe { records.get_unchecked(index) }
            }
            RSAggregateResult::Owned { records, .. } => {
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

    /// Reset the aggregate result, clearing the children vector and resetting the kind mask.
    pub fn reset(&mut self) {
        match self {
            RSAggregateResult::Borrowed {
                records, kind_mask, ..
            } => {
                records.clear();
                *kind_mask = RSResultKindMask::empty();
            }
            RSAggregateResult::Owned { records, kind_mask } => {
                records.clear();
                *kind_mask = RSResultKindMask::empty();
            }
        }
    }

    /// Add a child to the aggregate result and update the kind mask
    ///
    /// # Safety
    /// The given `child` has to stay valid for the lifetime of this aggregate result. Else reading
    /// the child with [`Self::get()`] will cause undefined behavior.
    pub fn push_borrowed(&mut self, child: &'index RSIndexResult) {
        match self {
            RSAggregateResult::Borrowed {
                records, kind_mask, ..
            } => {
                records.push(child);

                *kind_mask |= child.data.kind();
            }
            RSAggregateResult::Owned { .. } => {
                panic!("Cannot push a borrowed child to an owned aggregate result");
            }
        }
    }

    /// Create an owned copy of this aggregate result, allocating new memory for the records.
    ///
    /// The returned aggregate result will have the same lifetime as the original one,
    /// since it may borrow terms from the original result.
    pub fn to_owned<'a>(&'a self) -> RSAggregateResult<'a> {
        match self {
            RSAggregateResult::Borrowed { records, kind_mask } => {
                let mut new_records = SmallThinVec::with_capacity(records.len());

                new_records.extend(
                    records
                        .iter()
                        .map(|c| RSIndexResult::to_owned(c))
                        .map(Box::new),
                );

                RSAggregateResult::Owned {
                    records: new_records,
                    kind_mask: *kind_mask,
                }
            }
            RSAggregateResult::Owned { records, kind_mask } => {
                let mut new_records = SmallThinVec::with_capacity(records.len());

                new_records.extend(
                    records
                        .iter()
                        .map(|c| RSIndexResult::to_owned(c))
                        .map(Box::new),
                );

                RSAggregateResult::Owned {
                    records: new_records,
                    kind_mask: *kind_mask,
                }
            }
        }
    }

    /// Add a heap owned child to the aggregate result and update the kind mask
    pub fn push_boxed(&mut self, child: Box<RSIndexResult<'index>>) {
        match self {
            RSAggregateResult::Borrowed { .. } => {
                panic!("Cannot push a borrowed child to an owned aggregate result");
            }
            RSAggregateResult::Owned { records, kind_mask } => {
                *kind_mask |= child.data.kind();
                records.push(child);
            }
        }
    }

    /// Get a mutable reference to the child at the given index, if it exists
    pub fn get_mut(&mut self, index: usize) -> Option<&mut RSIndexResult<'index>> {
        match self {
            RSAggregateResult::Borrowed { .. } => {
                panic!("Cannot get a mutable reference to a borrowed aggregate result");
            }
            RSAggregateResult::Owned { records, .. } => records.get_mut(index).map(AsMut::as_mut),
        }
    }
}

/// An iterator over the results in an [`RSAggregateResult`].
pub struct RSAggregateResultIter<'index> {
    agg: &'index RSAggregateResult<'index>,
    index: usize,
}

impl<'index> Iterator for RSAggregateResultIter<'index> {
    type Item = &'index RSIndexResult<'index>;

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
