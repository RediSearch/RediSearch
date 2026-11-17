/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use inverted_index::offsets_only::OffsetsOnly;
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    ExpirationChecker, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus,
    SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::{Tag, TagLookup};

/// Whether an [`ElementTag`] reports, as the positions of its results, the array elements
/// where the tag occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[cheadergen::config(export, prefix_with_name)]
pub enum TagElements {
    /// Every result is a plain tag result: no positions, and matching in every field.
    Hidden,
    /// The positions of a result are the element numbers. Only a reader that looks for a
    /// common element asks for them, because they are not word positions.
    Reported,
}

/// A tag iterator over the index of a member of a NESTED group, whose entries record, as
/// positions, the array elements where the tag occurs.
pub struct ElementTag<'index, L, C> {
    tag: Tag<'index, OffsetsOnly, L, C>,
    elements: TagElements,
}

impl<'index, L, C> ElementTag<'index, L, C>
where
    L: TagLookup<OffsetsOnly>,
    C: ExpirationChecker,
{
    /// Wrap `tag`, reporting its results as `elements` says.
    pub const fn new(tag: Tag<'index, OffsetsOnly, L, C>, elements: TagElements) -> Self {
        Self { tag, elements }
    }

    /// The current result of the inner iterator, as this reader reports it. Called after
    /// every operation that can decode an entry into the result.
    fn report(&mut self) -> Option<&mut RSIndexResult<'index>> {
        let result = self.tag.current()?;
        if self.elements == TagElements::Hidden {
            if let Some(record) = result.as_term_mut() {
                record.set_offsets(RSOffsetSlice::empty());
            }
            result.field_mask = RS_FIELDMASK_ALL;
        }
        Some(result)
    }
}

impl<'index, L, C> RQEIterator<'index> for ElementTag<'index, L, C>
where
    L: TagLookup<OffsetsOnly>,
    C: ExpirationChecker,
{
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.tag.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.tag.read()?.is_none() {
            return Ok(None);
        }
        Ok(self.report())
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.tag.skip_to(doc_id)?.is_none() {
            return Ok(None);
        }
        Ok(self.report().map(|result| {
            if result.doc_id == doc_id {
                SkipToOutcome::Found(result)
            } else {
                SkipToOutcome::NotFound(result)
            }
        }))
    }

    fn rewind(&mut self) {
        self.tag.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.tag.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.tag.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.tag.at_eof()
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match self.tag.revalidate(spec)? {
            RQEValidateStatus::Aborted => Ok(RQEValidateStatus::Aborted),
            // A re-seek decodes the entry again, even when it lands on the same document.
            RQEValidateStatus::Ok => {
                self.report();
                Ok(RQEValidateStatus::Ok)
            }
            RQEValidateStatus::Moved { .. } => Ok(RQEValidateStatus::Moved {
                current: self.report(),
            }),
        }
    }

    fn type_(&self) -> IteratorType {
        self.tag.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.tag.intersection_sort_weight(prioritize_union_children)
    }
}

impl<L, C> ProfilePrint for ElementTag<'_, L, C>
where
    L: TagLookup<OffsetsOnly>,
    C: ExpirationChecker,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.tag.print_profile(map, ctx);
    }
}
