/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared machinery for node types that read from a search-on-disk index.

use std::ptr::NonNull;

use query_error::QueryErrorCode;
use query_term::RSQueryTerm;
use rqe_core::FieldMask;
use rqe_iterators::RQEIteratorPrintable;
use search_disk::SearchDiskHandle;

use crate::QueryEvalContext;

/// Build a search-on-disk reader for a single term, resolving the query's disk
/// snapshot and turning a failure into a query error.
///
/// `disk` must wrap the spec's own disk index. `term` must already carry its
/// scoring metadata: the document count its IDF is derived from is looked up
/// differently per caller, so computing it stays with the caller.
/// `needs_offsets` selects the offset-carrying iterator variant.
///
/// Returns `None` — after recording a
/// [`DiskIteratorCreation`](QueryErrorCode::DiskIteratorCreation) error, so the
/// query aborts rather than silently reading fewer results — when the iterator
/// cannot be built.
pub(crate) fn new_term_iterator<'index>(
    ctx: &'index mut QueryEvalContext,
    disk: SearchDiskHandle,
    term: Box<RSQueryTerm>,
    field_mask: FieldMask,
    weight: f64,
    needs_offsets: bool,
) -> Option<Box<dyn RQEIteratorPrintable<'index> + 'index>> {
    let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
        .expect("query.sctx.diskSnapshot is null for a disk-backed query");
    // SAFETY: `disk` wraps the spec's disk index, valid for `'index`
    // (`QueryEvalContext` invariants 1/2), and single-threaded query evaluation
    // gives us the only live reference to it; the enterprise iterators are
    // registered whenever a disk index is in use; `snapshot` is the disk
    // snapshot taken at query start.
    match unsafe { disk.new_term_iterator(term, field_mask, weight, needs_offsets, snapshot) } {
        Ok(it) => Some(it),
        Err(err) => {
            ctx.status()
                .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
            None
        }
    }
}
