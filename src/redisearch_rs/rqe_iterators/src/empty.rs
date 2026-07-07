/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Empty`].

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

/// An iterator that yields no results.
///
/// The [`Empty`] iterator is a sentinel iterator that represents an empty result set.
///
/// `#[repr(C)]` makes `Empty` trivially layout-compatible with itself across
/// `Active`/`Suspended` instantiations of any containing iterator — it has no
/// `Rf` fields of its own, so its Suspended counterpart is just `Empty` itself.
#[derive(Default)]
#[repr(C)]
pub struct Empty;

impl<'index> RQEIterator<'index> for Empty {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        None
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        _doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        Ok(None)
    }

    #[inline(always)]
    fn rewind(&mut self) {}

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        0
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        0
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        true
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Empty
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl ProfilePrint for Empty {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(c"EMPTY", map);
    }
}
