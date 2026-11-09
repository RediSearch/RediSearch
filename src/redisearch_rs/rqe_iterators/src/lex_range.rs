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
    ExpirationChecker, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus,
    SkipToOutcome,
    id_list::IdListSorted,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

/// An [`IdListSorted`] over the documents of every value a lex range covers.
///
/// The documents are collected when the query is evaluated, but whether their
/// field has expired is checked as each is read, as a tag value's reader does:
/// a cursor can read a later batch at a later time.
///
/// The profile prints the range under its own name and bound, as the per-value
/// union it replaces did, so two range clauses on one field can still be told
/// apart, and reports how many values the walk visited, which is the walk's own
/// progress rather than the pipeline's.
pub struct LexRange<'index, C> {
    ids: IdListSorted<'index>,
    expiration: C,
    /// Whether any document in the index can have an expiring field, decided at
    /// construction as an inverted-index iterator decides it.
    check_expiration: bool,
    /// The last document returned. Tracked here rather than read off the ID
    /// list, which is positioned on an expired document it skipped over too.
    last_doc_id: DocId,
    /// `LEXRANGE`, or `LEXRANGE - <bound>` when the clause carries one.
    profile_name: CString,
    values_walked: usize,
}

impl<'index, C: ExpirationChecker> LexRange<'index, C> {
    /// Wrap the sorted, deduplicated `ids` a range collected after walking
    /// `values_walked` values, labelled with the clause's `bound`, if any.
    pub fn new(
        ids: Vec<DocId>,
        result: RSIndexResult<'index>,
        expiration: C,
        bound: Option<&CStr>,
        values_walked: usize,
    ) -> Self {
        let profile_name = match bound {
            Some(bound) => format!("LEXRANGE - {}", bound.to_string_lossy()),
            None => "LEXRANGE".to_owned(),
        };
        Self {
            ids: IdListSorted::with_result(ids, result),
            check_expiration: expiration.has_expiration(),
            expiration,
            last_doc_id: 0,
            profile_name: CString::new(profile_name)
                .expect("a bound is a C string, so it carries no interior NUL"),
            values_walked,
        }
    }
}

impl<'index, C: ExpirationChecker> LexRange<'index, C> {
    /// Whether the document the ID list is positioned on has an expired field.
    fn current_expired(&mut self) -> bool {
        let current = self
            .ids
            .current()
            .expect("checked only right after a read or skip that found a document");
        self.expiration.is_expired(current)
    }
}

impl<'index, C: ExpirationChecker> RQEIterator<'index> for LexRange<'index, C> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.ids.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        loop {
            if self.ids.read()?.is_none() {
                return Ok(None);
            }
            if !self.check_expiration || !self.current_expired() {
                self.last_doc_id = self.ids.last_doc_id();
                return Ok(self.ids.current());
            }
        }
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let found = match self.ids.skip_to(doc_id)? {
            None => return Ok(None),
            Some(outcome) => matches!(outcome, SkipToOutcome::Found(_)),
        };
        if !self.check_expiration || !self.current_expired() {
            self.last_doc_id = self.ids.last_doc_id();
            let current = self.ids.current().expect("the skip found a document");
            return Ok(Some(if found {
                SkipToOutcome::Found(current)
            } else {
                SkipToOutcome::NotFound(current)
            }));
        }
        // The skip landed on an expired document, so the next valid one, if
        // any, is past the requested id.
        if self.read()?.is_none() {
            return Ok(None);
        }
        let current = self.ids.current().expect("the read found a document");
        Ok(Some(SkipToOutcome::NotFound(current)))
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
        self.last_doc_id = 0;
        self.ids.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        RQEIterator::num_estimated(&self.ids)
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
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

impl<C> ProfilePrint for LexRange<'_, C> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(&self.profile_name, map);
        map.kv_long_long(c"Values walked", self.values_walked as i64);
    }
}
