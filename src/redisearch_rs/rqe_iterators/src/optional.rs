/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Optional iterator implementation

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Iterator that extends a [`RQEIterator`] up to a given upper bound
/// by emitting virtual results after the child iterator is exhausted.
pub struct Optional<'index, I> {
    /// Inclusive upper bound on document identifiers to iterate over.
    /// Reads from the `child` beyond this bound are ignored.
    /// If the child ends before this bound, the iterator yields virtual
    /// results with no weight applied until [`Optional::max_id`] is reached.
    max_doc_id: t_docId,

    /// Weight applied to results produced by the inner child iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    result: RSIndexResult<'index>,

    /// Child iterator provided at construction time.
    /// Used while it can produce results. After it is exhausted
    /// the iterator yields virtual results until [`Optional::max_id`] is reached.
    ///
    /// Set to `Some(I)` when creating the [`Optional`] iterator,
    /// but set to `None` in case the child iterator results in
    /// [`RQEValidateStatus::Aborted`] during a delegated call
    /// to [`RQEIterator::revalidate`]. In which case it will
    /// start acting like an "Empty" iterator.
    child: Option<I>,
    /// Delayed cleanup / shortcut to work around lifetime issues
    child_abort: bool,
}

impl<'index, I> Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a new [`Optional`] iterator.
    ///
    /// * `max_id` is the upper bound of document identifiers visited by
    ///   [`RQEIterator::read`] and [`RQEIterator::skip_to`].
    /// * `weight` is applied to [`RSIndexResult`] values returned by the
    ///   child [`RQEIterator`]. When the child is exhausted, the iterator
    ///   yields virtual [`RSIndexResult`] values without weight until `max_id` is reached.
    /// * `child` [`RQEIterator`] used, can be deallocated early in case of an abort status in
    ///   a call to [`RQEIterator::revalidate`]
    pub const fn new(max_id: t_docId, weight: f64, child: I) -> Self {
        Self {
            max_doc_id: max_id,
            weight,
            result: RSIndexResult::virt(),
            child: Some(child),
            child_abort: false,
        }
    }
}

impl<'index, I> RQEIterator<'index> for Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        if self.child_abort && self.child.is_some() {
            // clean up from abort during revalidate
            self.child = None;
        }

        match self
            .child
            .as_mut()
            .map(|child| child.read())
            .transpose()?
            .flatten()
        {
            Some(real) => {
                real.weight = self.weight;
                self.result.doc_id = real.doc_id;
                Ok(Some(real))
            }
            None => {
                self.result.doc_id += 1;
                Ok(Some(&mut self.result))
            }
        }
    }

    // [OG C Comment] SkipTo for OPTIONAL iterator - Non-optimized version.
    // Skip to a specific docId. If the child has a hit on this docId, return it.
    // Otherwise, return a virtual hit.
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.result.doc_id);

        if doc_id > self.max_doc_id || self.at_eof() {
            self.result.doc_id = self.max_doc_id;
            return Ok(None);
        }

        if !self.child_abort
            && let Some(child) = &mut self.child
            && doc_id > child.last_doc_id()
            && let Some(SkipToOutcome::Found(real)) = child.skip_to(doc_id)?
            && real.doc_id == doc_id
        {
            real.weight = self.weight;
            self.result.doc_id = real.doc_id;
            return Ok(Some(SkipToOutcome::Found(real)));
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let last_child_doc_id = self.child.as_ref().map(|child| child.last_doc_id());
        match self
            .child
            .as_mut()
            .map(|child| child.revalidate())
            .transpose()?
        {
            Some(RQEValidateStatus::Ok) => Ok(RQEValidateStatus::Ok),
            Some(RQEValidateStatus::Moved {
                current: Some(real),
            }) => {
                self.result.doc_id += 1;
                if last_child_doc_id
                    .map(|id| id != self.result.doc_id)
                    .unwrap_or(true)
                {
                    Ok(RQEValidateStatus::Moved {
                        current: Some(&mut self.result),
                    })
                } else {
                    real.weight = self.weight;
                    Ok(RQEValidateStatus::Moved {
                        current: Some(real),
                    })
                }
            }
            Some(RQEValidateStatus::Aborted) => {
                // ideally we would already clean up here,
                // but as we share &mut reference derived from it in
                // the Moved { Some ( .. ) } branch we sadly cannot do that,
                self.child_abort = true;
                Ok(
                    if last_child_doc_id
                        .map(|id| id != self.result.doc_id)
                        .unwrap_or(true)
                    {
                        // virtual
                        self.result.doc_id += 1;
                        RQEValidateStatus::Moved {
                            current: Some(&mut self.result),
                        }
                    } else {
                        // real
                        RQEValidateStatus::Ok
                    },
                )
            }
            None | Some(RQEValidateStatus::Moved { current: None }) => {
                Ok(if self.result.doc_id >= self.max_doc_id {
                    RQEValidateStatus::Ok
                } else {
                    self.result.doc_id += 1;
                    RQEValidateStatus::Moved {
                        current: Some(&mut self.result),
                    }
                })
            }
        }
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
        if let Some(child) = self.child.as_mut() {
            child.rewind();
        }
    }

    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.max_doc_id
    }
}
