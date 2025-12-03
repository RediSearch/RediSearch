/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Intersection iterator - finds documents appearing in ALL child iterators.

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{Empty, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Yields documents appearing in ALL child iterators using a merge/zipper algorithm.
///
/// Children are sorted by estimated result count (smallest first) to minimize iterations.
/// A document is only yielded when ALL children have a matching entry for it.
pub struct Intersection<'index, I> {
    /// Child iterators, sorted by estimated count (smallest first).
    children: Vec<I>,
    /// Cached last doc_id from each child.
    doc_ids: Vec<t_docId>,
    /// Last doc_id read from the first child (returned by `last_doc_id()`).
    last_doc_id: t_docId,
    /// Last doc_id successfully found in ALL children (returned by `last_doc_id()`).
    last_found_id: t_docId,
    len: usize,
    num_expected: usize,
    is_eof: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RSIndexResult<'index>,
}

enum AgreeResult {
    Agreed,
    Ahead(t_docId),
    Eof,
}

impl<'index, I> Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a new intersection iterator. Children are sorted by estimated count. If `children`
    /// is empty, returns an iterator immediately at EOF.
    pub fn new(mut children: Vec<I>) -> Self {
        let num_children = children.len();

        if num_children == 0 {
            return Self {
                children,
                doc_ids: Vec::new(),
                last_doc_id: 0,
                last_found_id: 0,
                len: 0,
                num_expected: 0,
                is_eof: true,
                result: RSIndexResult::intersect(0),
            };
        }

        children.sort_by_cached_key(|c| c.num_estimated());
        let num_expected = children.first().map(|c| c.num_estimated()).unwrap_or(0);
        let doc_ids = vec![0; num_children];

        Self {
            children,
            doc_ids,
            last_doc_id: 0,
            last_found_id: 0,
            len: 0,
            num_expected,
            is_eof: false,
            result: RSIndexResult::intersect(num_children),
        }
    }

    /// Reads from the first child. Returns the doc_id or None if EOF.
    fn read_from_first_child(&mut self) -> Result<Option<t_docId>, RQEIteratorError> {
        debug_assert!(!self.children.is_empty());

        match self.children[0].read()? {
            Some(r) => {
                self.doc_ids[0] = r.doc_id;
                Ok(Some(r.doc_id))
            }
            None => {
                self.is_eof = true;
                Ok(None)
            }
        }
    }

    /// Tries to get all children to agree on the target doc_id.
    fn agree_on_doc_id(&mut self, target: t_docId) -> Result<AgreeResult, RQEIteratorError> {
        for (i, child) in self.children.iter_mut().enumerate() {
            if self.doc_ids[i] == target {
                continue;
            }

            match child.skip_to(target)? {
                None => {
                    self.is_eof = true;
                    return Ok(AgreeResult::Eof);
                }
                Some(SkipToOutcome::Found(r)) => {
                    self.doc_ids[i] = r.doc_id;
                }
                Some(SkipToOutcome::NotFound(r)) => {
                    self.doc_ids[i] = r.doc_id;
                    return Ok(AgreeResult::Ahead(r.doc_id));
                }
            }
        }
        Ok(AgreeResult::Agreed)
    }

    /// Loops until all children agree on a doc_id, or EOF is reached.
    fn find_consensus(&mut self, initial_target: t_docId) -> Result<Option<t_docId>, RQEIteratorError> {
        let mut curr_target = initial_target;
        loop {
            match self.agree_on_doc_id(curr_target)? {
                AgreeResult::Agreed => return Ok(Some(curr_target)),
                AgreeResult::Ahead(new_target) => curr_target = new_target,
                AgreeResult::Eof => return Ok(None),
            }
        }
    }

    /// Builds the aggregate result from all children's current results.
    ///
    /// # Safety
    ///
    /// Uses `unsafe` for split borrow: we need to iterate `&mut self.children` while
    /// mutating `self.result`. These are disjoint fields, but the borrow checker can't
    /// verify this through `&mut self`

    fn build_aggregate_result(&mut self, doc_id: t_docId) {
        self.last_found_id = doc_id;
        self.last_doc_id = doc_id;
        self.len += 1;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = doc_id;

        // SAFETY: `children` and `result` are disjoint fields. We only call `current()`
        // on children (doesn't invalidate results). Result is reset before adding new refs.
        let result_ptr: *mut RSIndexResult<'index> = &mut self.result;
        for child in &mut self.children {
            if let Some(child_result) = child.current() {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                unsafe {
                    (*result_ptr).push_borrowed(&*child_ptr);
                }
            }
        }
    }
}

impl<'index, I> RQEIterator<'index> for Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        let target = match self.read_from_first_child()? {
            Some(doc_id) => doc_id,
            None => return Ok(None),
        };

        match self.find_consensus(target)? {
            Some(doc_id) => {
                self.build_aggregate_result(doc_id);
                Ok(Some(&mut self.result))
            }
            None => Ok(None),
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        match self.find_consensus(doc_id)? {
            Some(found_id) => {
                self.build_aggregate_result(found_id);
                if found_id == doc_id {
                    Ok(Some(SkipToOutcome::Found(&mut self.result)))
                } else {
                    Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
                }
            }
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.last_found_id = 0;
        self.len = 0;
        self.is_eof = self.children.is_empty();
        self.doc_ids.fill(0);
        self.children.iter_mut().for_each(|c| c.rewind());
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_expected
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.last_found_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let mut any_child_moved = false;
        let mut max_child_doc_id: t_docId = 0;
        let mut moved_to_eof = false;

        for (i, child) in self.children.iter_mut().enumerate() {
            match child.revalidate()? {
                RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
                RQEValidateStatus::Moved { current } => {
                    any_child_moved = true;
                    match current {
                        Some(result) => {
                            self.doc_ids[i] = result.doc_id;
                            max_child_doc_id = max_child_doc_id.max(result.doc_id);
                        }
                        None => moved_to_eof = true,
                    }
                }
                RQEValidateStatus::Ok => {}
            }
        }

        if !any_child_moved || self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        if moved_to_eof {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        match self.skip_to(max_child_doc_id)? {
            Some(_) => Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            }),
            None => Ok(RQEValidateStatus::Moved { current: None }),
        }
    }
}

/// Result of reducing an intersection's children.
///
/// Optimizations applied:
/// - Empty child → Empty (intersection with empty set is empty)
/// - Wildcard children removed (intersection with "all" is identity)
/// - All wildcards → return last wildcard
/// - Single child → return directly
/// - Multiple children → Intersection
pub enum ReducedIntersection<'index, I> {
    Empty(Empty),
    Single(I),
    Intersection(Intersection<'index, I>),
}

/// Reduces children by removing wildcards and optimizing edge cases.
pub fn reduce<'index, I>(children: Vec<I>) -> ReducedIntersection<'index, I>
where
    I: RQEIterator<'index>,
{
    if children.is_empty() || children.iter().any(|c| c.is_empty()) {
        return ReducedIntersection::Empty(Empty);
    }

    let mut non_wildcards = Vec::with_capacity(children.len());
    let mut last_wildcard: Option<I> = None;

    for child in children {
        if child.is_wildcard() {
            last_wildcard = Some(child);
        } else {
            non_wildcards.push(child);
        }
    }

    match non_wildcards.len() {
        0 => match last_wildcard {
            Some(w) => ReducedIntersection::Single(w),
            None => ReducedIntersection::Empty(Empty),
        },
        1 => ReducedIntersection::Single(non_wildcards.pop().unwrap()),
        _ => ReducedIntersection::Intersection(Intersection::new(non_wildcards)),
    }
}

impl<'index, I> RQEIterator<'index> for ReducedIntersection<'index, I>
where
    I: RQEIterator<'index>,
{
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        match self {
            ReducedIntersection::Empty(e) => e.read(),
            ReducedIntersection::Single(s) => s.read(),
            ReducedIntersection::Intersection(i) => i.read(),
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        match self {
            ReducedIntersection::Empty(e) => e.skip_to(doc_id),
            ReducedIntersection::Single(s) => s.skip_to(doc_id),
            ReducedIntersection::Intersection(i) => i.skip_to(doc_id),
        }
    }

    fn rewind(&mut self) {
        match self {
            ReducedIntersection::Empty(e) => e.rewind(),
            ReducedIntersection::Single(s) => s.rewind(),
            ReducedIntersection::Intersection(i) => i.rewind(),
        }
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        match self {
            ReducedIntersection::Empty(e) => e.current(),
            ReducedIntersection::Single(s) => s.current(),
            ReducedIntersection::Intersection(i) => i.current(),
        }
    }

    fn last_doc_id(&self) -> t_docId {
        match self {
            ReducedIntersection::Empty(e) => e.last_doc_id(),
            ReducedIntersection::Single(s) => s.last_doc_id(),
            ReducedIntersection::Intersection(i) => i.last_doc_id(),
        }
    }

    fn at_eof(&self) -> bool {
        match self {
            ReducedIntersection::Empty(e) => e.at_eof(),
            ReducedIntersection::Single(s) => s.at_eof(),
            ReducedIntersection::Intersection(i) => i.at_eof(),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            ReducedIntersection::Empty(e) => e.num_estimated(),
            ReducedIntersection::Single(s) => s.num_estimated(),
            ReducedIntersection::Intersection(i) => i.num_estimated(),
        }
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match self {
            ReducedIntersection::Empty(e) => e.revalidate(),
            ReducedIntersection::Single(s) => s.revalidate(),
            ReducedIntersection::Intersection(i) => i.revalidate(),
        }
    }
}
