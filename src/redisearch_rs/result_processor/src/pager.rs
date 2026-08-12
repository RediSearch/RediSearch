/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The paging processor, implementing the `LIMIT <offset> <num>` clause.

use crate::ResultProcessor;
use search_result::SearchResult;

// The C symbols this crate's tests need are linked and stubbed once, in `counter.rs`.

/// Which half of the `LIMIT <offset> <num>` window the pager is currently working on.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Phase {
    /// Discarding the first `offset` results.
    Skip,
    /// Forwarding results until `remaining` hits zero.
    Limit,
}

/// A processor yielding only the results in the `LIMIT <offset> <num>` window.
///
/// The sorter upstream builds a heap of `offset + num` results; the pager takes results
/// `offset..offset + num` out of it. For `LIMIT 40 10` the sorter builds a heap of 50 and the pager
/// discards the first 40, yielding just 10.
///
/// The two are separate processors so that the sorter's heap can later be cached and paged again
/// without re-running the whole query.
#[derive(Debug)]
pub struct Pager {
    /// How many results are still to be discarded before yielding any.
    offset: u32,
    /// How many results may still be yielded.
    remaining: u32,
    phase: Phase,
}

impl ResultProcessor for Pager {
    const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_PAGER_LIMITER;

    fn next(
        &mut self,
        mut cx: crate::Context,
        res: &mut SearchResult<'_>,
    ) -> Result<Option<()>, crate::Error> {
        if self.phase == Phase::Skip {
            // The skip phase may bail out early, in which case we stay in it and retry the
            // remaining offset on the next call.
            if self.skip(&mut cx, res)?.is_none() {
                return Ok(None);
            }
            self.phase = Phase::Limit;
        }

        // If we've reached LIMIT:
        if self.remaining == 0 {
            return Ok(None);
        }

        let mut upstream = cx
            .upstream()
            .expect("There is no processor upstream of this pager.");

        let result = upstream.next(res)?;
        // Account for the result only if we got one.
        if result.is_some() {
            self.remaining -= 1;
        }

        Ok(result)
    }
}

impl Pager {
    /// Create a new pager. `offset` and `limit` are taken from the user's `LIMIT` clause.
    pub const fn new(offset: u32, limit: u32) -> Self {
        Self {
            offset,
            remaining: limit,
            phase: Phase::Skip,
        }
    }

    /// Pull and discard the first `offset` results.
    ///
    /// Returns `Ok(Some(()))` once the whole offset has been skipped, and `Ok(None)` if upstream hit
    /// EOF first. In the `Ok(None)` and `Err(_)` cases `self.offset` still holds what is left to
    /// skip, so the caller stays in [`Phase::Skip`].
    fn skip(
        &mut self,
        cx: &mut crate::Context,
        res: &mut SearchResult<'_>,
    ) -> Result<Option<()>, crate::Error> {
        // A pager is never called more than offset+limit times, because the whole pipeline —
        // upstream and downstream — is limited to offset+limit.
        let downstream_limit = cx
            .result_limit()
            .expect("This pager has no parent QueryProcessingCtx.");
        let limit = self.remaining.min(downstream_limit);
        // Matches the C pager's unsigned arithmetic. Both operands come from a user-supplied
        // `LIMIT`, which the request parser caps well below `u32::MAX`.
        cx.set_result_limit(self.offset.wrapping_add(limit));

        while self.offset > 0 {
            let mut upstream = cx
                .upstream()
                .expect("There is no processor upstream of this pager.");

            if upstream.next(res)?.is_none() {
                // Deliberately leaves `resultLimit` at the lowered value, as the C pager does: the
                // query is over, so nothing downstream will read it again.
                return Ok(None);
            }

            // Re-read rather than tracking the limit locally: the C pager decrements whatever the
            // upstream left behind, so an upstream that lowers the limit itself is respected.
            // Cannot underflow — the limit starts at `offset + limit` and is decremented at most
            // `offset` times — but wrap like C rather than panicking if that ever stops holding.
            if let Some(limit) = cx.result_limit() {
                cx.set_result_limit(limit.wrapping_sub(1));
            }
            self.offset -= 1;

            res.clear();
        }

        // Restore the limit, so that it seems untouched to the downstream.
        cx.set_result_limit(downstream_limit);

        Ok(Some(()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::{Chain, from_iter};
    use std::iter;

    /// Build a chain of `upstream_results` dummy results feeding a `Pager::new(offset, limit)`,
    /// then drain the pager and return the number of results it yielded.
    fn run(offset: u32, limit: u32, upstream_results: usize) -> (usize, Chain) {
        let mut chain = Chain::new();
        chain.append(from_iter(
            iter::from_fn(|| Some(SearchResult::default())).take(upstream_results),
        ));
        chain.append(Pager::new(offset, limit));

        let mut yielded = 0;
        loop {
            let (cx, rp) = chain.last_as_context_and_inner::<Pager>();
            let mut res = SearchResult::default();
            match rp.next(cx, &mut res) {
                Ok(Some(())) => {
                    yielded += 1;
                    res.clear();
                }
                _ => break,
            }
        }

        (yielded, chain)
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn yields_the_limit_window() {
        // 10 upstream results, LIMIT 4 3 -> results 4, 5, 6.
        let (yielded, _chain) = run(4, 3, 10);
        assert_eq!(yielded, 3);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn zero_offset_yields_from_the_start() {
        let (yielded, _chain) = run(0, 3, 10);
        assert_eq!(yielded, 3);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn zero_limit_yields_nothing() {
        let (yielded, _chain) = run(0, 0, 10);
        assert_eq!(yielded, 0);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn stops_at_upstream_eof_inside_the_window() {
        // Only 6 results upstream but the window asks for 4..14, so we get 6 - 4 = 2.
        let (yielded, _chain) = run(4, 10, 6);
        assert_eq!(yielded, 2);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn offset_past_upstream_eof_yields_nothing() {
        let (yielded, _chain) = run(20, 10, 6);
        assert_eq!(yielded, 0);
    }

    /// The skip phase lowers `resultLimit` to cap its upstream, then has to put it back so the
    /// downstream sees the value it set.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn restores_the_downstream_result_limit() {
        let mut chain = Chain::new();
        chain.append(from_iter(
            iter::from_fn(|| Some(SearchResult::default())).take(10),
        ));
        chain.append(Pager::new(4, 3));
        chain.set_result_limit(7);

        let (cx, rp) = chain.last_as_context_and_inner::<Pager>();
        let mut res = SearchResult::default();
        assert_eq!(rp.next(cx, &mut res), Ok(Some(())));
        res.clear();

        assert_eq!(chain.result_limit(), 7);
    }

    /// The pager must not re-run its skip phase once it has been completed.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn skips_only_once() {
        let mut chain = Chain::new();
        chain.append(from_iter(
            iter::from_fn(|| Some(SearchResult::default())).take(10),
        ));
        chain.append(Pager::new(4, 3));

        for _ in 0..3 {
            let (cx, rp) = chain.last_as_context_and_inner::<Pager>();
            let mut res = SearchResult::default();
            assert_eq!(rp.next(cx, &mut res), Ok(Some(())));
            res.clear();
        }

        let (cx, rp) = chain.last_as_context_and_inner::<Pager>();
        assert_eq!(rp.offset, 0);
        assert_eq!(rp.remaining, 0);
        assert_eq!(rp.next(cx, &mut SearchResult::default()), Ok(None));
    }
}
