/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`OptionalOptimized`].
//!
//! This is the optimized variant of the optional iterator. Instead of scanning
//! all doc IDs from 1 to `maxDocId`, it uses a [`WildcardIterator`] over
//! `spec.existingDocs` to visit only real document IDs, yielding real or virtual
//! results accordingly.

use ffi::{RS_FIELDMASK_ALL, t_docId};
use inverted_index::RSIndexResult;

use crate::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, maybe_empty::MaybeEmpty,
    wildcard::WildcardIterator,
};

/// An iterator that emits results for all document IDs present in the index,
/// driven by a [`WildcardIterator`] over the existing-documents inverted index.
///
/// For each doc ID that `wcii` yields:
/// - If the query child also has a hit at that doc ID, a **real** result is
///   returned with [`OptionalOptimized::weight`] applied.
/// - Otherwise a **virtual** result is returned with zero weight.
///
/// This avoids scanning doc IDs 1..=maxDocId sequentially. When the index is
/// sparse (few documents relative to `maxDocId`), the optimized variant is
/// significantly faster.
pub struct OptionalOptimized<'index, W, I> {
    /// Wildcard iterator over `spec.existingDocs` — the authoritative source of doc IDs.
    wcii: W,
    /// Query child — provides real hits at positions where it has a match.
    /// Wrapped in [`MaybeEmpty`] so it can be replaced with an empty iterator
    /// when it is aborted during [`RQEIterator::revalidate`].
    child: MaybeEmpty<I>,
    /// Virtual result returned when `wcii` has a doc but `child` does not.
    virt: RSIndexResult<'index>,
    /// Inclusive upper bound (matches C `maxDocId`).
    max_doc_id: t_docId,
    /// Weight applied to real results from `child`.
    weight: f64,
    /// Tracks the doc ID of the last result yielded.
    last_doc_id: t_docId,
    /// Whether the iterator has reached EOF.
    at_eof: bool,
}

impl<'index, W, I> OptionalOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    /// Creates a new [`OptionalOptimized`] iterator.
    ///
    /// * `wcii` — wildcard iterator over `spec.existingDocs`; drives which doc IDs
    ///   are visited.
    /// * `child` — query child iterator that provides real hits.
    /// * `max_doc_id` — inclusive upper bound on doc IDs.
    /// * `weight` — applied to results produced by `child`.
    pub fn new(wcii: W, child: I, max_doc_id: t_docId, weight: f64) -> Self {
        Self {
            wcii,
            child: MaybeEmpty::new(child),
            virt: RSIndexResult::build_virt()
                .frequency(1)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            max_doc_id,
            weight,
            last_doc_id: 0,
            at_eof: false,
        }
    }
}

impl<'index, W, I> RQEIterator<'index> for OptionalOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.child.last_doc_id() == self.last_doc_id
            && let Some(result) = self.child.current()
        {
            return Some(result);
        }

        Some(&mut self.virt)
    }

    // Mirrors `OI_Read_Optimized` in `optional_iterator.c`.
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof {
            return Ok(None);
        }

        if self.last_doc_id >= self.max_doc_id {
            self.at_eof = true;
            return Ok(None);
        }

        // Advance wcii to the next existing document.
        let wcii_doc_id = match self.wcii.read()? {
            None => {
                self.at_eof = true;
                return Ok(None);
            }
            Some(r) => r.doc_id,
        };

        // Advance child to catch up with wcii.
        // The index may not be up to date, so loop until child reaches or passes wcii.
        while wcii_doc_id > self.child.last_doc_id() && !self.child.at_eof() {
            let _ = self.child.read()?;
        }

        self.last_doc_id = wcii_doc_id;

        let weight = self.weight;
        if self.child.last_doc_id() == wcii_doc_id {
            // Real hit: child has a result at this position.
            let result = self
                .child
                .current()
                .expect("child has a result at wcii_doc_id");
            result.weight = weight;
            Ok(Some(result))
        } else {
            // Virtual hit: wcii has a doc ID but child does not.
            self.virt.doc_id = wcii_doc_id;
            Ok(Some(&mut self.virt))
        }
    }

    // Mirrors `OI_SkipTo_Optimized` in `optional_iterator.c`.
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.last_doc_id);

        if doc_id > self.max_doc_id || self.at_eof {
            self.at_eof = true;
            return Ok(None);
        }

        // Promote wcii to doc_id. It may land on a different doc if doc_id is not
        // present in the existing-documents index.
        let (found, effective_id) = match self.wcii.skip_to(doc_id)? {
            None => {
                self.at_eof = true;
                return Ok(None);
            }
            Some(SkipToOutcome::Found(r)) => (true, r.doc_id),
            Some(SkipToOutcome::NotFound(r)) => (false, r.doc_id),
        };

        // Advance child to effective_id if needed.
        if effective_id > self.child.last_doc_id() {
            let _ = self.child.skip_to(effective_id)?;
        }

        self.last_doc_id = effective_id;

        let weight = self.weight;
        if self.child.last_doc_id() == effective_id {
            // Real hit — outcome (Found/NotFound) mirrors wcii.
            let result = self
                .child
                .current()
                .expect("child has a result at effective_id");
            result.weight = weight;
            if found {
                Ok(Some(SkipToOutcome::Found(result)))
            } else {
                Ok(Some(SkipToOutcome::NotFound(result)))
            }
        } else {
            // Virtual hit — outcome (Found/NotFound) mirrors wcii.
            self.virt.doc_id = effective_id;
            if found {
                Ok(Some(SkipToOutcome::Found(&mut self.virt)))
            } else {
                Ok(Some(SkipToOutcome::NotFound(&mut self.virt)))
            }
        }
    }

    // Mirrors `OI_Revalidate_Optimized` in `optional_iterator.c`.
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Simple enum to avoid holding a borrow through the match.
        enum ValidateOutcome {
            Ok,
            Moved,
            Aborted,
        }

        // Step 1: Revalidate wcii. If it aborts, the whole iterator must abort.
        let wcii_outcome = match self.wcii.revalidate()? {
            RQEValidateStatus::Ok => ValidateOutcome::Ok,
            RQEValidateStatus::Moved { .. } => ValidateOutcome::Moved,
            RQEValidateStatus::Aborted => ValidateOutcome::Aborted,
        };
        self.at_eof = self.wcii.at_eof();

        if matches!(wcii_outcome, ValidateOutcome::Aborted) {
            return Ok(RQEValidateStatus::Aborted);
        }

        // Capture before child revalidation to detect "current is virtual".
        let current_was_virtual = self.child.last_doc_id() != self.last_doc_id;

        // Step 2: Revalidate child. If it aborts, replace with an empty iterator.
        let child_outcome = match self.child.revalidate()? {
            RQEValidateStatus::Ok => ValidateOutcome::Ok,
            RQEValidateStatus::Moved { .. } => ValidateOutcome::Moved,
            RQEValidateStatus::Aborted => {
                let _ = self.child.take_iterator(); // replace with Empty
                ValidateOutcome::Aborted
            }
        };

        // Step 3: Determine the outcome based on wcii's and child's status.
        match wcii_outcome {
            ValidateOutcome::Ok => {
                if matches!(child_outcome, ValidateOutcome::Ok) || current_was_virtual {
                    // Child is still valid, or the current result was virtual — no change.
                    return Ok(RQEValidateStatus::Ok);
                }
                // Child moved or aborted while current was a real result.
                // Advance to the next valid state.
                let current = self.read()?;
                Ok(RQEValidateStatus::Moved { current })
            }
            ValidateOutcome::Moved => {
                // wcii moved to a new position; update child accordingly.
                let wcii_doc_id = self.wcii.last_doc_id();

                if wcii_doc_id > self.child.last_doc_id() {
                    let _ = self.child.skip_to(wcii_doc_id)?;
                }

                self.last_doc_id = wcii_doc_id;

                let weight = self.weight;
                if self.child.last_doc_id() == wcii_doc_id {
                    // Real hit at the new wcii position.
                    let result = self
                        .child
                        .current()
                        .expect("child has a result at wcii_doc_id");
                    result.weight = weight;
                    Ok(RQEValidateStatus::Moved {
                        current: Some(result),
                    })
                } else {
                    // Virtual hit at the new wcii position.
                    self.virt.doc_id = wcii_doc_id;
                    Ok(RQEValidateStatus::Moved {
                        current: Some(&mut self.virt),
                    })
                }
            }
            ValidateOutcome::Aborted => unreachable!("already handled above"),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.at_eof = false;
        self.virt.doc_id = 0;
        self.wcii.rewind();
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.at_eof
    }
}
