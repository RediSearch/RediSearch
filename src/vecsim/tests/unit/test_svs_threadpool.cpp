/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"

#if HAVE_SVS
#include <atomic>
#include <latch>
#include <set>
#include <thread>

#include <chrono>

#include "VecSim/algorithms/svs/svs.h"
#include "VecSim/algorithms/svs/svs_utils.h"
#include "VecSim/vec_sim.h"
#include "VecSim/vec_sim_interface.h"

// std::latch::wait() blocks indefinitely. Use this to fail with a diagnostic
// message instead of hanging if a bug causes a deadlock.
static bool wait_with_timeout(std::latch &l, std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!l.try_wait()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return true;
}

static constexpr auto kTestTimeout = std::chrono::seconds(5);

class SVSThreadPoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save and clear global log callback so that pool operations
        // don't assert on nullptr log_ctx (we don't have an index context).
        saved_callback_ = VecSimIndexInterface::logCallback;
        VecSimIndexInterface::logCallback = nullptr;
        // Mark the shared pool as having an attached index so resize() behaves
        // eagerly throughout the test (these unit tests exercise the pool in
        // isolation, without constructing real SVS indexes). Idempotent.
        VecSimSVSThreadPoolImpl::instance()->onIndexAttached();
        // Reset the shared singleton pool to size 1 — earlier test suites may have
        // resized it via VecSim_UpdateThreadPoolSize() and left it in that state.
        VecSimSVSThreadPool::resize(1);
    }
    void TearDown() override {
        // Reset the shared singleton pool to size 1 so tests don't leak state.
        VecSimSVSThreadPool::resize(1);
        VecSimIndexInterface::logCallback = saved_callback_;
    }

    // Allocator used by VecSimSVSThreadPool wrappers constructed in tests.
    std::shared_ptr<VecSimAllocator> allocator_ = VecSimAllocator::newVecsimAllocator();

private:
    logCallbackFunction saved_callback_ = nullptr;
};

// ---------------------------------------------------------------------------
// Test 1: VecSimSVSThreadPool::resize — grow and shrink
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, ResizeGrowAndShrink) {
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);

    // Grow 1 → 4
    VecSimSVSThreadPool::resize(4);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 4);

    // Shrink 4 → 2
    VecSimSVSThreadPool::resize(2);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 2);

    // No-op 2 → 2
    VecSimSVSThreadPool::resize(2);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 2);

    // Shrink to minimum 2 → 1
    VecSimSVSThreadPool::resize(1);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);

    // resize(0) clamps to 1
    VecSimSVSThreadPool::resize(0);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);

    // Grow from 1 → 8 and verify parallel_for works at new size
    VecSimSVSThreadPool::resize(8);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 8);
    std::atomic_int counter{0};
    VecSimSVSThreadPoolImpl::instance()->parallel_for([&counter](size_t) { counter++; }, 8);
    ASSERT_EQ(counter, 8);
}

// ---------------------------------------------------------------------------
// Test 2: Scheduled jobs defer shrink until they end
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, ScheduledJobDefersShrinkUntilEnd) {
    VecSimSVSThreadPool::resize(4);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 4);

    auto pool = VecSimSVSThreadPoolImpl::instance();
    size_t snapshot = pool->beginScheduledJob();
    ASSERT_EQ(snapshot, 4);

    VecSimSVSThreadPool::resize(1);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 4);

    pool->endScheduledJob();
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);
}

// ---------------------------------------------------------------------------
// Test 3: Shrink while threads are rented
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, ShrinkWhileRented) {
    // Pool size 5: 4 worker slots [s0, s1, s2, s3].
    VecSimSVSThreadPool::resize(5);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 5);

    // Wrapper A uses parallelism 3 → rents 2 workers (s0, s1).
    VecSimSVSThreadPool wrapperA{allocator_};
    wrapperA.setParallelism(3);

    std::latch hold(1);          // blocks rented workers
    std::latch workers_ready(2); // signaled when both workers start
    std::atomic_int resultA{0};

    // Spawned thread: run parallel_for that blocks until we release the latch.
    // parallel_for runs tid=0 on the calling thread (no slot rented) and
    // tid=1,2 on rented worker threads (s0, s1). Only the worker threads
    // (tid > 0) count down workers_ready, so main can wait until both workers
    // are actually running and holding their slots before proceeding to shrink.
    std::thread t([&] {
        wrapperA.parallel_for(
            [&](size_t tid) {
                if (tid > 0) {
                    workers_ready.count_down();
                }
                hold.wait();
                resultA++;
            },
            3);
    });

    // Wait until both workers are running (blocked on latch).
    ASSERT_TRUE(wait_with_timeout(workers_ready, kTestTimeout))
        << "Timed out waiting for wrapper A's 2 workers to start. "
           "resultA="
        << resultA << ", pool_size=" << VecSimSVSThreadPool::poolSize();

    // Shrink pool from 5 → 4 (slots become [s0, s1, s2]). s3 is free and
    // gets destroyed. s0, s1 are occupied by wrapperA but remain in the vector.
    // s2 is free and available for rental.
    VecSimSVSThreadPool::resize(4);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 4);

    // While wrapperA's threads are still alive (blocked on latch), run
    // parallel_for on the shrunk pool with a second wrapper using a free slot.
    VecSimSVSThreadPool wrapperB{allocator_};
    // Parallelism 2 = 1 rented worker + calling thread. The pool has 3 slots
    // [s0, s1, s2] after shrink; s0 and s1 are occupied by wrapperA, so the
    // single rented worker will get s2 (the only free slot).
    wrapperB.setParallelism(2);
    std::atomic_int resultB{0};
    wrapperB.parallel_for([&](size_t) { resultB++; }, 2);
    ASSERT_EQ(resultB, 2);

    // Release the latch — wrapperA's parallel_for completes.
    hold.count_down();
    t.join();
    ASSERT_EQ(resultA, 3);

    // After the renter's RentedThreads guard is destroyed, the old slots'
    // shared_ptrs drop to refcount 0 and the threads are destroyed.
    // Pool size remains at the shrunk value.
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 4);
}

// ---------------------------------------------------------------------------
// Test 4: Grow while threads are rented
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, GrowWhileRented) {
    // Pool size 3: 2 worker slots [s0, s1].
    VecSimSVSThreadPool::resize(3);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 3);

    // Wrapper A uses parallelism 3 → rents 2 workers (s0, s1).
    VecSimSVSThreadPool wrapperA{allocator_};
    wrapperA.setParallelism(3);

    std::latch hold(1);          // blocks rented workers
    std::latch workers_ready(2); // signaled when both workers start
    std::atomic_int resultA{0};

    // Spawned thread: parallel_for blocks all 3 partitions (tid=0 on calling
    // thread, tid=1,2 on rented workers). Only workers (tid > 0) count down
    // workers_ready so main knows the slots are occupied before growing.
    std::thread t([&] {
        wrapperA.parallel_for(
            [&](size_t tid) {
                if (tid > 0) {
                    workers_ready.count_down();
                }
                hold.wait();
                resultA++;
            },
            3);
    });

    ASSERT_TRUE(wait_with_timeout(workers_ready, kTestTimeout))
        << "Timed out waiting for wrapper A's 2 workers to start. "
           "resultA="
        << resultA << ", pool_size=" << VecSimSVSThreadPool::poolSize();

    // Grow pool from 3 → 5 while s0, s1 are occupied. New slots [s2, s3] are
    // appended. The in-flight parallel_for is unaffected — it holds independent
    // shared_ptr refs to s0, s1.
    VecSimSVSThreadPool::resize(5);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 5);

    // Wrapper B uses parallelism 3 → rents 2 workers. s0, s1 are occupied by
    // wrapperA, so it gets the 2 newly created slots s2, s3... but we only
    // need 2 of the 3 free slots (s2, s3 are free, only need 2).
    VecSimSVSThreadPool wrapperB{allocator_};
    wrapperB.setParallelism(3);
    std::atomic_int resultB{0};
    wrapperB.parallel_for([&](size_t) { resultB++; }, 3);
    ASSERT_EQ(resultB, 3);

    // Release wrapperA's blocked threads.
    hold.count_down();
    t.join();
    ASSERT_EQ(resultA, 3);

    // Pool size remains at the grown value after renter releases.
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 5);

    // After all threads are free, verify the full pool is usable at new size.
    wrapperA.setParallelism(5);
    std::atomic_int resultFull{0};
    wrapperA.parallel_for([&](size_t) { resultFull++; }, 5);
    ASSERT_EQ(resultFull, 5);
}

// ---------------------------------------------------------------------------
// Test 4: Parallelism propagation across copies
// The VecSimSVSThreadPool wrapper is copied internally by SVS (via
// ThreadPoolHandle). Both the original and the copy must share the same
// parallelism_ (shared_ptr<atomic<size_t>>), so that setParallelism() on
// the original is visible to SVS's copy when it calls size().
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, ParallelismPropagationAcrossCopies) {
    VecSimSVSThreadPool::resize(8);

    VecSimSVSThreadPool original{allocator_};
    original.setParallelism(2);
    ASSERT_EQ(original.size(), 2);

    // Copy the wrapper — simulates what SVS does internally.
    VecSimSVSThreadPool copy = original;
    ASSERT_EQ(copy.size(), 2);

    // Change parallelism on the original. The copy must see it.
    original.setParallelism(6);
    ASSERT_EQ(original.size(), 6);
    ASSERT_EQ(copy.size(), 6);

    // Change via the copy. The original must see it too.
    copy.setParallelism(3);
    ASSERT_EQ(copy.size(), 3);
    ASSERT_EQ(original.size(), 3);

    // Both share the same pool.
    ASSERT_EQ(original.poolSize(), 8);
    ASSERT_EQ(copy.poolSize(), 8);
}

// ---------------------------------------------------------------------------
// Test 5: Two indexes sharing one pool with independent parallelism
// Two wrappers from the same shared pool have independent parallelism values.
// Changing one does not affect the other. Both can run parallel_for
// sequentially using disjoint thread budgets.
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, TwoIndexesIndependentParallelism) {
    VecSimSVSThreadPool::resize(8);

    VecSimSVSThreadPool wrapperA{allocator_};
    VecSimSVSThreadPool wrapperB{allocator_};

    wrapperA.setParallelism(2);
    wrapperB.setParallelism(5);

    // Each wrapper reports its own parallelism via size().
    ASSERT_EQ(wrapperA.size(), 2);
    ASSERT_EQ(wrapperB.size(), 5);

    // Both report the same shared pool size.
    ASSERT_EQ(wrapperA.poolSize(), 8);
    ASSERT_EQ(wrapperB.poolSize(), 8);

    // Changing A's parallelism does not affect B.
    wrapperA.setParallelism(4);
    ASSERT_EQ(wrapperA.size(), 4);
    ASSERT_EQ(wrapperB.size(), 5);

    // Both can run parallel_for sequentially — all threads are free between calls.
    std::atomic_int resultA{0};
    wrapperA.parallel_for([&](size_t) { resultA++; }, 4);
    ASSERT_EQ(resultA, 4);

    std::atomic_int resultB{0};
    wrapperB.parallel_for([&](size_t) { resultB++; }, 5);
    ASSERT_EQ(resultB, 5);

    // Verify both wrappers can use the full pool capacity sequentially.
    wrapperA.setParallelism(8);
    std::atomic_int resultFullA{0};
    wrapperA.parallel_for([&](size_t) { resultFullA++; }, 8);
    ASSERT_EQ(resultFullA, 8);

    wrapperB.setParallelism(8);
    std::atomic_int resultFullB{0};
    wrapperB.parallel_for([&](size_t) { resultFullB++; }, 8);
    ASSERT_EQ(resultFullB, 8);
}

// ---------------------------------------------------------------------------
// Test 6: VecSim_UpdateThreadPoolSize mode transitions
// The C API always sets write mode immediately. The pool resize is lazy: it is
// applied at first index attach if no index has attached yet, otherwise
// immediately.
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, UpdateThreadPoolSizeModeTransitions) {
    // Reset to a fresh state — previous tests may have attached indexes via
    // VecSimSVSThreadPool wrappers, leaving has_attached_index_ set.
    VecSimSVSThreadPoolImpl::instance()->resetForTest();

    // 0 → 4: switch to async mode immediately. Pool size stays at 1 because
    // no SVS index has attached yet (resize is deferred).
    VecSim_UpdateThreadPoolSize(4);
    ASSERT_EQ(VecSimIndex::asyncWriteMode, VecSim_WriteAsync);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);

    // Attaching an index applies the deferred size.
    VecSimSVSThreadPool wrapper{allocator_};
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 4);

    // 4 → 8: now that an index is attached, resize is immediate.
    VecSim_UpdateThreadPoolSize(8);
    ASSERT_EQ(VecSimIndex::asyncWriteMode, VecSim_WriteAsync);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 8);

    // 8 → 0: switch to in-place mode, pool resizes to 1.
    VecSim_UpdateThreadPoolSize(0);
    ASSERT_EQ(VecSimIndex::asyncWriteMode, VecSim_WriteInPlace);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);

    // 0 → 0: idempotent, stays in-place, no crash.
    VecSim_UpdateThreadPoolSize(0);
    ASSERT_EQ(VecSimIndex::asyncWriteMode, VecSim_WriteInPlace);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);

    // 0 → 2: back to async mode.
    VecSim_UpdateThreadPoolSize(2);
    ASSERT_EQ(VecSimIndex::asyncWriteMode, VecSim_WriteAsync);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 2);

    // Restore to in-place mode so we don't leak state to other tests.
    VecSim_UpdateThreadPoolSize(0);
}

// ---------------------------------------------------------------------------
// Test 7: Concurrent rental from two indexes
// Two wrappers share a pool. Both call parallel_for concurrently from
// different threads with parallelism values that sum to the pool size.
// No thread should be double-rented — the mutex serializes rental scans.
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, ConcurrentRentalFromTwoIndexes) {
    // Pool size 8: wrappers A (4) and B (4) sum to exactly 8.
    VecSimSVSThreadPool::resize(8);

    VecSimSVSThreadPool wrapperA{allocator_};
    wrapperA.setParallelism(4);
    VecSimSVSThreadPool wrapperB{allocator_};
    wrapperB.setParallelism(4);

    std::atomic_int resultA{0};
    std::atomic_int resultB{0};

    // Use a latch as a barrier: all 8 tasks (4 from A + 4 from B) must
    // arrive before any can proceed. If a thread were double-rented, only
    // 7 tasks could run simultaneously and the latch would never complete.
    std::latch barrier(8);
    std::mutex mtx;
    std::set<std::thread::id> idsA, idsB;

    std::thread tA([&] {
        wrapperA.parallel_for(
            [&](size_t) {
                {
                    std::lock_guard lock(mtx);
                    idsA.insert(std::this_thread::get_id());
                }
                barrier.count_down();
                barrier.wait();
                resultA++;
            },
            4);
    });
    std::thread tB([&] {
        wrapperB.parallel_for(
            [&](size_t) {
                {
                    std::lock_guard lock(mtx);
                    idsB.insert(std::this_thread::get_id());
                }
                barrier.count_down();
                barrier.wait();
                resultB++;
            },
            4);
    });

    // Wait for both threads to complete. If the barrier deadlocks (e.g., due
    // to double-renting), these joins would hang — use a timed check.
    auto deadline = std::chrono::steady_clock::now() + kTestTimeout;
    tA.join();
    tB.join();
    ASSERT_LT(std::chrono::steady_clock::now(), deadline)
        << "Timed out waiting for concurrent parallel_for calls. "
           "resultA="
        << resultA << ", resultB=" << resultB << ", idsA.size=" << idsA.size()
        << ", idsB.size=" << idsB.size() << ", pool_size=" << wrapperA.poolSize();

    ASSERT_EQ(resultA, 4);
    ASSERT_EQ(resultB, 4);

    // Each wrapper used 4 distinct OS threads, and no thread was shared.
    ASSERT_EQ(idsA.size(), 4);
    ASSERT_EQ(idsB.size(), 4);
    for (auto &id : idsA) {
        ASSERT_EQ(idsB.count(id), 0) << "Thread was double-rented across wrappers";
    }
}

// ---------------------------------------------------------------------------
// Test 8: All threads occupied — graceful degradation
// When all pool threads are rented by wrapper A, wrapper B's parallel_for
// cannot rent any workers. In debug builds, rent() asserts. In release
// builds, parallel_for uses rented.count() (0 workers) and runs only
// partition 0 on the calling thread.
//
// NOTE: This should never happen in production. RediSearch's reserve job
// mechanism guarantees that the number of concurrent renters never exceeds
// the pool size. This test exercises the defensive fallback path.
// ---------------------------------------------------------------------------
TEST_F(SVSThreadPoolTest, AllThreadsOccupied) {
    // Pool size 4 (3 worker slots). Wrapper A rents all 3.
    VecSimSVSThreadPool::resize(4);

    VecSimSVSThreadPool wrapperA{allocator_};
    wrapperA.setParallelism(4);

    std::latch hold(1);
    std::latch workers_ready(3);
    std::atomic_int resultA{0};

    // Spawned thread: wrapper A grabs all 3 worker slots and blocks.
    std::thread t([&] {
        wrapperA.parallel_for(
            [&](size_t tid) {
                if (tid > 0) {
                    workers_ready.count_down();
                }
                hold.wait();
                resultA++;
            },
            4);
    });

    ASSERT_TRUE(wait_with_timeout(workers_ready, kTestTimeout))
        << "Timed out waiting for wrapper A's 3 workers to start. "
           "resultA="
        << resultA << ", pool_size=" << wrapperA.poolSize();

    // All 3 worker slots are occupied. Wrapper B tries to rent 1 worker.
    VecSimSVSThreadPool wrapperB{allocator_};
    wrapperB.setParallelism(2);

#ifdef NDEBUG
    // Release: graceful degradation — rent() returns 0 workers, parallel_for
    // runs only partition 0 on the calling thread. Work is silently dropped.
    std::atomic_int resultB{0};
    wrapperB.parallel_for([&](size_t) { resultB++; }, 2);
    ASSERT_EQ(resultB, 1); // only partition 0 ran
#else
    // Debug: rent() asserts because it can't fulfill the request.
    ASSERT_DEATH(wrapperB.parallel_for([&](size_t) {}, 2),
                 "Failed to rent the expected number of SVS threads");
#endif

    // Clean up: release wrapper A's blocked threads.
    hold.count_down();
    t.join();
    ASSERT_EQ(resultA, 4);
}

#endif // HAVE_SVS
