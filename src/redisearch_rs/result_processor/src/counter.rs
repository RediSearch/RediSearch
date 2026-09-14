/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::ResultProcessor;
use search_result::SearchResult;
use std::sync::atomic::{AtomicUsize, Ordering};

// Link both Rust-provided and C-provided symbols
#[cfg(all(test, feature = "unittest"))]
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
#[cfg(all(test, feature = "unittest"))]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// A processor to track the number of entries yielded by the previous processor in the chain.
#[derive(Debug)]
pub struct Counter {
    count: AtomicUsize,
}

impl ResultProcessor for Counter {
    const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_COUNTER;

    fn drain(
        &self,
        cx: crate::DrainContext,
        res: &mut SearchResult<'_>,
    ) -> Result<Option<()>, crate::DrainError> {
        let upstream = cx
            .upstream()
            .expect("There is no processor upstream of this counter.");
        while upstream.drain(res)?.is_some() {
            self.count.fetch_add(1, Ordering::Relaxed);
            res.clear();
        }
        // Profiling and reply bookkeeping are not owned by the drain consumer.
        Ok(None)
    }

    fn next(
        &self,
        mut cx: crate::Context,
        res: &mut SearchResult<'_>,
    ) -> Result<Option<()>, crate::Error> {
        let mut upstream = cx
            .upstream()
            .expect("There is no processor upstream of this counter.");

        while upstream.next(res)?.is_some() {
            // Publish progress before the next upstream call can block.
            self.count.fetch_add(1, Ordering::Relaxed);
            res.clear();
        }

        // In profiling mode, RPProfile is interleaved into the result processor chain: A chain of
        // processors A -> B -> C becomes A -> RPProfile -> B -> RPProfile -> C -> RPProfile, to
        // profile each of the individual result processors.
        //
        // Because the Counter result processor returns Ok(None), this is equivalent to returning
        // ffi::RPStatus_RS_RESULT_EOF (see ResultProcessorWrapper::result_processor_next). This
        // apparently (in a way enricozb cannot figure out) prevents the very last RPProfile from
        // appropriately counting, so this patches that by manually incrementing the counter.
        if upstream.ty() == ffi::ResultProcessorType_RP_PROFILE {
            // Safety: We trust that the result processor parent structure (QueryProcessingCtx) was
            // constructed correctly, and thus has a valid pointer to the end processor.
            let end_proc = unsafe {
                *cx.parent()
                    .expect("This processor has no parent.")
                    .endProc
                    .get()
            };

            // Safety: If the previous (upstream) result processor is a profiling result processor,
            // then we are in profiling mode, and every other result processor is an RPProfile.
            // Thus, the last result processor is also an RPProfile.
            unsafe { ffi::RPProfile_IncrementCount(end_proc) };
        }

        Ok(None)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl Counter {
    pub const fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::test_utils::{Chain, from_iter};
    use std::iter;
    use std::sync::{Arc, Barrier};
    use std::thread;

    struct DrainSource {
        cursor: AtomicUsize,
        terminal: Result<Option<()>, crate::DrainError>,
    }

    impl ResultProcessor for DrainSource {
        const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

        fn next(
            &self,
            _cx: crate::Context,
            _res: &mut SearchResult,
        ) -> Result<Option<()>, crate::Error> {
            panic!("counter Drain must not call Next")
        }

        fn drain(
            &self,
            _cx: crate::DrainContext,
            res: &mut SearchResult,
        ) -> Result<Option<()>, crate::DrainError> {
            assert_eq!(res.score(), 0.0, "the previous row must have been cleared");
            let id = self.cursor.fetch_add(1, Ordering::Relaxed);
            if id < 3 {
                res.set_doc_id(id as u64 + 1);
                res.set_score(id as f64 + 1.0);
                Ok(Some(()))
            } else {
                self.terminal
            }
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn drain_counts_and_clears_rows_before_eof_or_error() {
        for (terminal, expected) in [
            (Ok(None), ffi::RPDrainStatus_RP_DRAIN_EOF),
            (Err(crate::DrainError), ffi::RPDrainStatus_RP_DRAIN_ERROR),
        ] {
            let mut chain = Chain::new();
            chain.append(DrainSource {
                cursor: AtomicUsize::new(0),
                terminal,
            });
            chain.append(Counter::new());
            // SAFETY: The chain owns and pins the initialized wrapper through both calls.
            let rp = unsafe { *chain.last_raw() };
            // SAFETY: The header is initialized, and no Next call mutates it.
            let drain = unsafe { rp.as_ref().drain.unwrap() };
            let mut result = SearchResult::new();
            for _ in 0..2 {
                // SAFETY: The wrapper is live and result storage is exclusive.
                assert_eq!(unsafe { drain(rp.as_ptr(), &mut result) }, expected);
                assert_eq!(result.score(), 0.0);
            }
            let (_, counter) = chain.last_as_context_and_inner::<Counter>();
            assert_eq!(counter.count.load(Ordering::Relaxed), 3);
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn drain_counts_while_next_is_parked_without_losing_progress() {
        struct Source {
            cursor: AtomicUsize,
            next_calls: AtomicUsize,
            paused: Arc<Barrier>,
            resume: Arc<Barrier>,
        }
        impl Source {
            fn row(&self, res: &mut SearchResult) -> Option<()> {
                assert_eq!(res.score(), 0.0);
                let id = self.cursor.fetch_add(1, Ordering::Relaxed);
                if id < 4 {
                    res.set_doc_id(id as u64 + 1);
                    res.set_score(id as f64 + 1.0);
                    Some(())
                } else {
                    None
                }
            }
        }
        impl ResultProcessor for Source {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

            fn next(
                &self,
                _cx: crate::Context,
                res: &mut SearchResult,
            ) -> Result<Option<()>, crate::Error> {
                if self.next_calls.fetch_add(1, Ordering::Relaxed) == 1 {
                    self.paused.wait();
                    self.resume.wait();
                }
                Ok(self.row(res))
            }

            fn drain(
                &self,
                _cx: crate::DrainContext,
                res: &mut SearchResult,
            ) -> Result<Option<()>, crate::DrainError> {
                Ok(self.row(res))
            }
        }
        let paused = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let mut chain = Chain::new();
        chain.append(Source {
            cursor: AtomicUsize::new(0),
            next_calls: AtomicUsize::new(0),
            paused: Arc::clone(&paused),
            resume: Arc::clone(&resume),
        });
        chain.append(Counter::new());
        // SAFETY: The chain pins the wrapper until the scoped thread completes.
        let rp = unsafe { *chain.last_raw() };
        // SAFETY: The initialized, immutable Drain callback is read before concurrent entry.
        let drain = unsafe { rp.as_ref().drain.unwrap() };
        let address = rp.as_ptr().expose_provenance();
        let (cx, counter) = chain.last_as_context_and_inner::<Counter>();
        let (next_status, observation) = thread::scope(|scope| {
            let drainer = scope.spawn(|| {
                paused.wait();
                let before = counter.count.load(Ordering::Relaxed);
                let ptr = std::ptr::with_exposed_provenance_mut::<crate::Header>(address);
                let mut result = SearchResult::new();
                // SAFETY: The pinned wrapper stays live; Drain uses distinct result storage
                // and the source's only shared mutable state is atomic.
                let status = unsafe { drain(ptr, &mut result) };
                let after = counter.count.load(Ordering::Relaxed);
                resume.wait();
                (before, after, status, result.score())
            });
            let status = counter.next(cx, &mut SearchResult::new());
            (status, drainer.join().unwrap())
        });
        assert_eq!(next_status, Ok(None));
        assert_eq!(observation, (1, 4, ffi::RPDrainStatus_RP_DRAIN_EOF, 0.0));
        assert_eq!(counter.count.load(Ordering::Relaxed), 4);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn count_is_visible_while_upstream_is_blocked() {
        let paused = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let source_paused = Arc::clone(&paused);
        let source_resume = Arc::clone(&resume);
        let mut remaining = 3;
        let mut chain = Chain::new();
        chain.append(from_iter(iter::from_fn(move || {
            if remaining > 0 {
                remaining -= 1;
                Some(SearchResult::default())
            } else {
                source_paused.wait();
                source_resume.wait();
                None
            }
        })));
        chain.append(Counter::new());
        let (cx, rp) = chain.last_as_context_and_inner::<Counter>();

        let (status, observed_count) = thread::scope(|scope| {
            let observer = scope.spawn(|| {
                paused.wait();
                let count = rp.count.load(Ordering::Relaxed);
                resume.wait();
                count
            });
            let status = rp.next(cx, &mut SearchResult::default());
            (status, observer.join().unwrap())
        });

        assert_eq!(status, Ok(None));
        assert_eq!(observed_count, 3);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn basically_works() {
        // Set up the result processor chain
        let mut chain = Chain::new();
        chain.append(from_iter(
            iter::from_fn(|| Some(SearchResult::default())).take(3),
        ));
        chain.append(Counter::new());

        let (cx, rp) = chain.last_as_context_and_inner::<Counter>();

        assert!(rp.next(cx, &mut SearchResult::default()).unwrap().is_none());
        assert_eq!(rp.count.load(Ordering::Relaxed), 3);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn retains_count_when_upstream_errors() {
        struct FailingSource(AtomicUsize);

        impl ResultProcessor for FailingSource {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

            fn next(
                &self,
                _cx: crate::Context,
                _res: &mut SearchResult<'_>,
            ) -> Result<Option<()>, crate::Error> {
                if self.0.fetch_add(1, Ordering::Relaxed) < 3 {
                    Ok(Some(()))
                } else {
                    Err(crate::Error::TimedOut)
                }
            }
        }

        let mut chain = Chain::new();
        chain.append(FailingSource(AtomicUsize::new(0)));
        chain.append(Counter::new());
        let (cx, rp) = chain.last_as_context_and_inner::<Counter>();

        assert_eq!(
            rp.next(cx, &mut SearchResult::default()),
            Err(crate::Error::TimedOut)
        );
        assert_eq!(rp.count.load(Ordering::Relaxed), 3);
    }
}
