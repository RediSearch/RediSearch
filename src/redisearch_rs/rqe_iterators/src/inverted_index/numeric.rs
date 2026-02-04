/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{f64, ptr::NonNull};

use ffi::t_docId;
use inverted_index::{NumericReader, RSIndexResult};
use numeric_range_tree::NumericRangeTree;

use crate::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    expiration_checker::{ExpirationChecker, NoOpChecker},
};

use super::core::InvIndIterator;

/// An iterator over numeric inverted index entries.
///
/// This iterator can be used to query a numeric inverted index.
///
/// The [`inverted_index::IndexReader`] API can be used to fully scan an inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The type of the numeric reader.
/// * `E` - The expiration checker type used to check for expired documents.
pub struct Numeric<'index, R, E = NoOpChecker> {
    it: InvIndIterator<'index, R, E>,
    /// The numeric range tree and its revision ID, used to detect changes during revalidation.
    range_tree_info: Option<RangeTreeInfo>,
    /// Minimum numeric range, only used in debug print.
    range_min: f64,
    /// Maximum numeric range, only used in debug print.
    range_max: f64,
}

/// Information about the numeric range tree backing a [`Numeric`] iterator.
struct RangeTreeInfo {
    /// Pointer to the numeric range tree.
    tree: NonNull<NumericRangeTree>,
    /// The revision ID at the time the iterator was created.
    /// Used to detect if the tree has been modified.
    revision_id: u32,
}

impl<'index, R, E> Numeric<'index, R, E>
where
    R: NumericReader<'index>,
    E: ExpirationChecker,
{
    /// Create an iterator returning results from a numeric inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`NumericReader`] such as [`inverted_index::FilterNumericReader`]
    /// or [`inverted_index::FilterGeoReader`].
    ///
    /// `expiration_checker` is used to check for expired documents when reading from the inverted index.
    ///
    /// `range_tree` is the underlying range tree backing the iterator.
    /// It is used during revalidation to check if the iterator is still valid.
    ///
    /// `range_min` and `range_max` are the minimum and maximum numeric ranges,
    /// respectively. They are only used in debug print.
    ///
    /// # Safety
    ///
    /// 1. If `range_tree` is Some, it must be a valid pointer to a `NumericRangeTree`.
    /// 2. If `range_tree` is Some, it must stay valid during the iterator's lifetime.
    pub unsafe fn new(
        reader: R,
        expiration_checker: E,
        range_tree: Option<&NumericRangeTree>,
        range_min: Option<f64>,
        range_max: Option<f64>,
    ) -> Self {
        let result = RSIndexResult::numeric(0.0);

        let range_tree_info = range_tree.map(|tree| {
            let revision_id = tree.revision_id();
            RangeTreeInfo {
                tree: NonNull::from_ref(tree),
                revision_id,
            }
        });

        let range_min = range_min.unwrap_or(f64::NEG_INFINITY);
        let range_max = range_max.unwrap_or(f64::INFINITY);
        assert!(range_min <= range_max);

        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
            range_tree_info,
            range_min,
            range_max,
        }
    }

    const fn should_abort(&self) -> bool {
        // If there's no range tree, we can't check for changes
        let Some(ref info) = self.range_tree_info else {
            return false;
        };

        let current_revision_id = {
            // SAFETY: 5. from [`Self::new`]
            let tree = unsafe { info.tree.as_ref() };
            tree.revision_id()
        };
        // If the revision id changed the numeric tree was either completely deleted or a node was split or removed.
        // The cursor is invalidated so we cannot revalidate the iterator.
        current_revision_id != info.revision_id
    }

    pub const fn range_min(&self) -> f64 {
        self.range_min
    }

    pub const fn range_max(&self) -> f64 {
        self.range_max
    }

    /// Get a reference to the underlying reader.
    ///
    /// This is used by FFI code to access the reader.
    pub const fn reader(&self) -> &R {
        &self.it.reader
    }
}

impl<'index, R, E> RQEIterator<'index> for Numeric<'index, R, E>
where
    R: NumericReader<'index>,
    E: ExpirationChecker,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.should_abort() {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate()
    }
}
