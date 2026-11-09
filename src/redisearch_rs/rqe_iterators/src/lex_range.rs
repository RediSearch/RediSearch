/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The documents a TAG lex range covers, collected up front.

use std::ffi::{CStr, CString};

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    id_list::IdListSorted,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

/// An [`IdListSorted`] over the documents of every value a lex range covers.
///
/// Iteration is the ID list's. What this adds is the profile: the range prints
/// under its own name and bound, as the per-value union it replaces did, so two
/// range clauses on one field can still be told apart, and it reports how many
/// values the walk visited, which is the walk's own progress rather than the
/// pipeline's.
pub struct LexRange<'index> {
    ids: IdListSorted<'index>,
    /// `LEXRANGE`, or `LEXRANGE - <bound>` when the clause carries one.
    profile_name: CString,
    values_walked: usize,
}

impl<'index> LexRange<'index> {
    /// Wrap the sorted, deduplicated `ids` a range collected after walking
    /// `values_walked` values, labelled with the clause's `bound`, if any.
    pub fn new(
        ids: Vec<DocId>,
        result: RSIndexResult<'index>,
        bound: Option<&CStr>,
        values_walked: usize,
    ) -> Self {
        let profile_name = match bound {
            Some(bound) => format!("LEXRANGE - {}", bound.to_string_lossy()),
            None => "LEXRANGE".to_owned(),
        };
        Self {
            ids: IdListSorted::with_result(ids, result),
            profile_name: CString::new(profile_name)
                .expect("a bound is a C string, so it carries no interior NUL"),
            values_walked,
        }
    }
}

impl<'index> RQEIterator<'index> for LexRange<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.ids.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.ids.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.ids.skip_to(doc_id)
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.ids.revalidate(spec)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.ids.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        RQEIterator::num_estimated(&self.ids)
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.ids.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.ids.at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.ids.type_()
    }

    #[inline(always)]
    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.ids.intersection_sort_weight(prioritize_union_children)
    }
}

impl ProfilePrint for LexRange<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(&self.profile_name, map);
        map.kv_long_long(c"Values walked", self.values_walked as i64);
    }
}
