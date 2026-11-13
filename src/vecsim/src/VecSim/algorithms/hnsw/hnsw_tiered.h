/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <optional>
#include <span>
#include <tuple>
#include <utility>

#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "VecSim/quantization/sq8_training_state.h"
#include "VecSim/vec_sim_tiered_index.h"
#include "hnsw.h"

/**
 * Definition of a job that inserts a new vector from flat into HNSW Index.
 */
struct HNSWInsertJob : public TieredInsertJob {
    HNSWInsertJob(std::shared_ptr<VecSimAllocator> allocator, labelType label_, idType id_,
                  JobCallback insertCb, VecSimIndex *index_)
        : TieredInsertJob(allocator, HNSW_INSERT_VECTOR_JOB, label_, id_, insertCb, index_) {}
};

/**
 * Definition of a job that swaps last id with a deleted id in HNSW Index after delete operation.
 */
struct HNSWSwapJob : public VecsimBaseObject {
    idType deleted_id;
    std::atomic_int
        pending_repair_jobs_counter; // number of repair jobs left to complete before this job
                                     // is ready to be executed (atomic counter).
    HNSWSwapJob(std::shared_ptr<VecSimAllocator> allocator, idType deletedId)
        : VecsimBaseObject(allocator), deleted_id(deletedId), pending_repair_jobs_counter(0) {}
    void setRepairJobsNum(long num_repair_jobs) { pending_repair_jobs_counter = num_repair_jobs; }
    int atomicDecreasePendingJobsNum() {
        int ret = --pending_repair_jobs_counter;
        assert(pending_repair_jobs_counter >= 0);
        return ret;
    }
};

static const size_t DEFAULT_PENDING_SWAP_JOBS_THRESHOLD = DEFAULT_BLOCK_SIZE;
static const size_t MAX_PENDING_SWAP_JOBS_THRESHOLD = 100000;

/**
 * Definition of a job that repairs a certain node's connection in HNSW Index after delete
 * operation.
 */
struct HNSWRepairJob : public AsyncJob {
    idType node_id;
    unsigned short level;
    vecsim_stl::vector<HNSWSwapJob *> associatedSwapJobs;

    HNSWRepairJob(std::shared_ptr<VecSimAllocator> allocator, idType id_, unsigned short level_,
                  JobCallback repairCb, VecSimIndex *index_, HNSWSwapJob *swapJob)
        : AsyncJob(allocator, HNSW_REPAIR_NODE_CONNECTIONS_JOB, repairCb, index_), node_id(id_),
          level(level_),
          // Insert the first swap job from which this repair job was created.
          associatedSwapJobs(1, swapJob, this->allocator) {}
    // In case that a repair job is required for deleting another neighbor of the node, save a
    // reference to additional swap job.
    void appendAnotherAssociatedSwapJob(HNSWSwapJob *swapJob) {
        associatedSwapJobs.push_back(swapJob);
    }
};

template <typename DataType, typename DistType>
class TieredHNSWIndex : public VecSimTieredIndex<DataType, DistType> {
private:
    // Index guard hierarchy: acquire nested guards in this order, skipping guards not needed:
    //   1. flatIndexGuard: flat vectors, pending insert lookups and insert-job fields.
    //   2. mainIndexGuard: backend allocation and internal graph-ID stability. Batch iterators
    //      retain shared ownership between batches to prevent physical removal and swapping.
    //   3. labelGuard: excludes relabeling while queries read packed, potentially unaligned
    //      metadata labels. Queries hold it shared only during a backend query or batch.
    //   4. HNSW indexDataGuard: backend label lookups and global index state.
    //
    // Structural writers take mainIndexGuard exclusively before labelGuard exclusively.
    // Relabel takes flatIndexGuard and labelGuard exclusively, then the backend data guard;
    // it must not acquire mainIndexGuard, even shared, since a paused iterator can block a
    // pending exclusive writer and shared acquisition can then wait behind that writer.
    // Release the backend data guard before upgrading mainIndexGuard to exclusive ownership.
    mutable std::shared_mutex labelGuard;
    SharedMutexLockable labelLockable{labelGuard};

    /// Mappings from id/label to associated jobs, for invalidating and update ids if necessary.
    // **This map is protected with the flat buffer lock**
    vecsim_stl::unordered_map<idType, vecsim_stl::vector<HNSWRepairJob *>> idToRepairJobs;
    vecsim_stl::unordered_map<idType, HNSWSwapJob *> idToSwapJob;

    // This threshold is tested upon deleting a label from HNSW, and once the number of deleted
    // vectors reached this limit, we apply swap jobs *only for vectors that has no more pending
    // repair jobs*, and are ready to be removed from the graph.
    size_t pendingSwapJobsThreshold;
    std::atomic_size_t readySwapJobs;

    // Cost of defrag(), for `debugInfo()`. Statistics only, so relaxed ordering is enough.
    std::atomic<uint64_t> defragRuns{0};
    std::atomic<uint64_t> defragTimeNs{0};

    // Protect the both idToRepairJobs lookup and the pending_repair_jobs_counter for the
    // associated swap jobs.
    std::mutex idToRepairJobsGuard;

    // Counter for vectors inserted directly into HNSW by the main thread (bypassing flat buffer).
    // This happens in WriteInPlace mode or when the flat buffer is full.
    // Not atomic since it's only accessed from the main thread.
    size_t directHNSWInsertions{0};

    bool isQuantized;

    // Writer-owned state exists only while SQ8 vectors are accumulating in FLAT.
    // Reads and maintenance always use the real, initially empty backend.
    std::optional<SQ8TrainingState> sqAccumulationState;

#ifdef BUILD_TESTS
    std::function<void()> beforeQuantizationFinalization;
    std::function<void()> afterQuantizationFinalization;
    std::function<void()> afterBackendInsertBeforeFlatRemoval;
#endif

    void addToSum(std::span<const DataType> vector) {
        if constexpr (QuantInput<DataType>) {
            sqAccumulationState->add(vector);
        }
    }

    void subtractFromSum(std::span<const DataType> vector) {
        if constexpr (QuantInput<DataType>) {
            sqAccumulationState->subtract(vector);
        }
    }

    int addVectorDuringAccumulation(const void *blob, labelType label);
    int deleteVectorDuringAccumulation(labelType label);
    void finalizeQuantizationAndSubmitJobs(size_t count);

    void executeInsertJob(HNSWInsertJob *job);
    void executeRepairJob(HNSWRepairJob *job);

    // To be executed synchronously upon deleting a vector, doesn't require a wrapper. Main HNSW
    // lock is assumed to be held exclusive here.
    void fixJobsAfterSwap(idType deleted_id, vecsim_stl::vector<idType> &idsToRemove);

    // Drop the pending repair jobs *of* a disposed element and account for them in the swap jobs
    // that were waiting on them. Takes `idToRepairJobsGuard` itself, so it may be called while the
    // main index guard is held for shared ownership -- which is what lets `isolateRepairedElement`
    // use it. Callers must not hold that guard.
    void invalidateRepairJobs(idType deleted_id);

    // Execute the ready swap jobs, run no more than 'maxSwapsToRun' jobs (run all of them for -1).
    // Returns the number of swap jobs executed.
    size_t executeReadySwapJobs(size_t maxSwapsToRun = -1);

    // Wrappers static functions to be sent as callbacks upon creating the jobs (since members
    // functions cannot serve as callback, this serve as the "gateway" to the appropriate index).
    static void executeInsertJobWrapper(AsyncJob *job);
    static void executeRepairJobWrapper(AsyncJob *job);

    inline HNSWIndex<DataType, DistType> *getHNSWIndex() const;

    // Helper function for performing in place mark delete of vector(s) associated with a label
    // and creating the appropriate repair jobs for the effected connections. This should be called
    // while *HNSW shared lock is held* (shared locked).
    int deleteLabelFromHNSW(labelType label);

    // Take a deleted element out of the graph once the last repair job created for its deletion is
    // done - no edge in or out of it is left, so the swap job that disposes of it later only has to
    // reclaim its slot. Called in the repair context, while the main index guard is held for shared
    // ownership and `idToRepairJobsGuard` is not held.
    void isolateRepairedElement(idType deleted_id);
    void executeSwapJob(vecsim_stl::vector<idType> &idsToRemove, HNSWSwapJob *swap_job);

    // Insert a single vector to HNSW. This can be called in both write modes - insert async and
    // in-place. For the async mode, we have to release the flat index guard that is held for shared
    // ownership (we do it right after we update the HNSW global data and receive the new state).
    template <bool releaseFlatGuard>
    void insertVectorToHNSW(HNSWIndex<DataType, DistType> *hnsw_index, labelType label,
                            const void *blob);

    // Handle deletion of vector inplace considering that async deletion might occurred beforehand.
    int deleteLabelFromHNSWInplace(labelType label);

    // Drop the label's buffered vector(s) from the flat buffer and invalidate their pending insert
    // jobs, without touching the backend. Takes `flatIndexGuard` exclusively,
    // so a caller must not already hold it.
    int deleteFromFlatAndInsertJobs(labelType label);

    VecSimUpdateCode updateVectorSingleValue(labelType label, const void *new_blobs, size_t n);
    int addVectorInPlace(const void *blob, labelType label);
    HNSWInsertJob *addVectorToFlat(const void *blob, labelType label, int &ret);

#ifdef BUILD_TESTS
#include "VecSim/algorithms/hnsw/hnsw_tiered_tests_friends.h"
#endif

protected:
    // Adapts HNSW's own data-guard mutex to Lockable
    class HnswDataGuardLockable : public Lockable {
        HNSWIndex<DataType, DistType> *hnsw;

    public:
        explicit HnswDataGuardLockable(HNSWIndex<DataType, DistType> *hnsw) : hnsw(hnsw) {}
        void lock() const override { hnsw->lockSharedIndexDataGuard(); }
        void unlock() const override { hnsw->unlockSharedIndexDataGuard(); }
    };
    HnswDataGuardLockable hnswDataGuardLockable{this->getHNSWIndex()};

    ScopedLocks lockMainIndexForQuery() const override {
        return ScopedLocks(this->mainIndexLockable, labelLockable);
    }

    ScopedLocks lockMainIndexForInfo() const override { return lockMainIndexForQuery(); }

    ScopedLocks lockIndexForSize() const override {
        return ScopedLocks(this->flatIndexLockable, hnswDataGuardLockable);
    }

    ScopedLocks lockIndexForCapacity() const override {
        // No-op: No locking required for capacity()
        return ScopedLocks();
    }

public:
    class TieredHNSW_BatchIterator : public TieredIndex_BatchIterator {
    private:
        const TieredHNSWIndex<DataType, DistType> *index;
        std::shared_lock<std::shared_mutex> backend_index_lock;
        VecSimQueryParams *queryParams;

        VecSimBatchIterator *flat_iterator;
        VecSimBatchIterator *hnsw_iterator;

        VecSimQueryReply *getNextBackendResults(size_t n_res) {
            std::shared_lock<std::shared_mutex> label_lock(index->labelGuard);
            return hnsw_iterator->getNextResults(n_res, BY_SCORE_THEN_ID);
        }

    public:
        TieredHNSW_BatchIterator(const void *query_vector,
                                 const TieredHNSWIndex<DataType, DistType> *index,
                                 VecSimQueryParams *queryParams,
                                 std::shared_ptr<VecSimAllocator> allocator);

        ~TieredHNSW_BatchIterator();

        const void *getQueryBlob() const override { return flat_iterator->getQueryBlob(); }

        VecSimQueryReply *getNextResults(size_t n_res, VecSimQueryReply_Order order) override;

        bool isDepleted() override;

        void reset() override;

#ifdef BUILD_TESTS
        VecSimBatchIterator *getHNSWIterator() { return hnsw_iterator; }
#endif
    };

public:
    TieredHNSWIndex(HNSWIndex<DataType, DistType> *hnsw_index,
                    BruteForceIndex<DataType, DistType> *bf_index,
                    const TieredIndexParams &tieredParams,
                    std::shared_ptr<VecSimAllocator> allocator);
    virtual ~TieredHNSWIndex();

#ifdef BUILD_TESTS
    void setBeforeQuantizationFinalizationHook(std::function<void()> hook) {
        beforeQuantizationFinalization = std::move(hook);
    }

    void setAfterQuantizationFinalizationHook(std::function<void()> hook) {
        afterQuantizationFinalization = std::move(hook);
    }

    void setAfterBackendInsertBeforeFlatRemovalHook(std::function<void()> hook) {
        afterBackendInsertBeforeFlatRemoval = std::move(hook);
    }
#endif

    int addVector(const void *blob, labelType label) override;
    // This will do nothing (and return 0) if this label doesn't exist. Otherwise, it may
    // remove vector from the flat buffer and/or the HNSW index.
    int deleteVector(labelType label) override;
    VecSimRelabelCode relabelVector(labelType old_label, labelType new_label) override;

    VecSimUpdateCode updateVectors(labelType label, const void *new_blobs, size_t n) override;
    size_t getNumMarkedDeleted() const override { return getHNSWIndex()->getNumMarkedDeleted(); }
    auto shouldDefrag();
    // Do nothing here, each tier (flat buffer and HNSW) should increase capacity for itself when
    // needed.
    VecSimIndexDebugInfo debugInfo() const override;
    VecSimIndexBasicInfo basicInfo() const override;
    VecSimIndexStatsInfo statisticInfo() const override;
    VecSimDebugInfoIterator *debugInfoIterator() const override;
    VecSimBatchIterator *newBatchIterator(const void *queryBlob,
                                          VecSimQueryParams *queryParams) const override {
        // The query blob will be processed and copied by the internal indexes's batch iterator.
        return new (this->allocator)
            TieredHNSW_BatchIterator(queryBlob, this, queryParams, this->allocator);
    }
    inline void setLastSearchMode(VecSearchMode mode) override {
        this->backendIndex->setLastSearchMode(mode);
    }
    // swap latest elements deleted slots
    void defrag() {
        const auto start = std::chrono::steady_clock::now();
        const size_t executed = this->executeReadySwapJobs(this->pendingSwapJobsThreshold);
        // Includes the wait for the main index guard, which is part of what defrag costs a caller.
        const auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now() - start)
                                   .count();
        defragRuns.fetch_add(1, std::memory_order_relaxed);
        defragTimeNs.fetch_add(elapsedNs, std::memory_order_relaxed);
        TIERED_LOG(VecSimCommonStrings::LOG_DEBUG_STRING,
                   "Tiered HNSW index defrag: executed %zu swap jobs in %lld ns", executed,
                   static_cast<long long>(elapsedNs));
    }
    void runGC() override {
        TIERED_LOG(VecSimCommonStrings::LOG_VERBOSE_STRING,
                   "running asynchronous GC for tiered HNSW index");
        this->defrag();
    }
    void acquireSharedLocks() override {
        this->flatIndexGuard.lock_shared();
        this->mainIndexGuard.lock_shared();
        labelGuard.lock_shared();
        this->getHNSWIndex()->lockSharedIndexDataGuard();
    }

    void releaseSharedLocks() override {
        this->getHNSWIndex()->unlockSharedIndexDataGuard();
        labelGuard.unlock_shared();
        this->mainIndexGuard.unlock_shared();
        this->flatIndexGuard.unlock_shared();
    }

    VecSimDebugCommandCode getHNSWElementNeighbors(size_t label, int ***neighborsData) {
        std::shared_lock<std::shared_mutex> main_index_lock(this->mainIndexGuard);
        return this->getHNSWIndex()->getHNSWElementNeighbors(label, neighborsData);
    }

#ifdef BUILD_TESTS
    size_t indexMetaDataCapacity() const override {
        return this->frontendIndex->indexMetaDataCapacity() +
               this->backendIndex->indexMetaDataCapacity();
    }
#endif
};

/**
 ******************************* Implementation **************************
 */

/* Helper methods */
template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::executeInsertJobWrapper(AsyncJob *job) {
    auto *insert_job = static_cast<HNSWInsertJob *>(job);
    auto *job_index = static_cast<TieredHNSWIndex<DataType, DistType> *>(insert_job->index);
    job_index->executeInsertJob(insert_job);
    delete job;
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::executeRepairJobWrapper(AsyncJob *job) {
    auto *repair_job = static_cast<HNSWRepairJob *>(job);
    auto *job_index = static_cast<TieredHNSWIndex<DataType, DistType> *>(repair_job->index);
    job_index->executeRepairJob(repair_job);
    delete job;
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::fixJobsAfterSwap(
    idType deleted_id, vecsim_stl::vector<idType> &idsToRemove) {
    // Get the id that was last and had been swapped with the job's deleted id.
    idType prev_last_id = this->getHNSWIndex()->indexSize();

    // Swap the ids in the pending jobs for the current last id (if exist).
    if (idToRepairJobs.find(prev_last_id) != idToRepairJobs.end()) {
        for (auto &job_it : idToRepairJobs.at(prev_last_id)) {
            job_it->node_id = deleted_id;
        }
        idToRepairJobs.insert({deleted_id, idToRepairJobs.at(prev_last_id)});
        idToRepairJobs.erase(prev_last_id);
    }
    // Update the swap jobs if the last id also needs a swap, otherwise just collect to deleted id
    // to be removed from the swap jobs.
    if (prev_last_id != deleted_id && idToSwapJob.find(prev_last_id) != idToSwapJob.end() &&
        std::find(idsToRemove.begin(), idsToRemove.end(), prev_last_id) == idsToRemove.end()) {
        // Update the curr_last_id pending swap job id after the removal that renamed curr_last_id
        // with the deleted id.
        idsToRemove.push_back(prev_last_id);
        idToSwapJob.at(prev_last_id)->deleted_id = deleted_id;
        // If id was deleted in-place and there is no swap job for it, this will create a new entry
        // in idToSwapJob for the swapped id, otherwise it will update the existing entry.
        idToSwapJob[deleted_id] = idToSwapJob.at(prev_last_id);
    } else {
        idsToRemove.push_back(deleted_id);
    }
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::invalidateRepairJobs(idType deleted_id) {
    // Invalidate repair jobs for the disposed id (if exist), and update the associated swap jobs.
    // Under the guard rather than under the main guard held exclusively: a job still listed here
    // has not been claimed by a worker yet -- `executeRepairJob` claims it under this same guard
    // before touching it -- so it is alive for as long as this is held.
    std::lock_guard<std::mutex> lock(this->idToRepairJobsGuard);
    if (idToRepairJobs.find(deleted_id) == idToRepairJobs.end()) {
        return;
    }

    for (auto &job_it : idToRepairJobs.at(deleted_id)) {
        job_it->node_id = this->setAndSaveInvalidJob(job_it);
        for (auto &swap_job_it : job_it->associatedSwapJobs) {
            if (swap_job_it->atomicDecreasePendingJobsNum() == 0) {
                readySwapJobs++;
            }
        }
    }
    idToRepairJobs.erase(deleted_id);
}

template <typename DataType, typename DistType>
HNSWIndex<DataType, DistType> *TieredHNSWIndex<DataType, DistType>::getHNSWIndex() const {
    return dynamic_cast<HNSWIndex<DataType, DistType> *>(this->backendIndex);
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::isolateRepairedElement(idType deleted_id) {
    this->getHNSWIndex()->isolateDeletedElement(deleted_id);
    // No edge in or out of the element is left, so a repair job waiting to fix *its* connections
    // has nothing to fix. Dropping them here rather than leaving them to the swap job also
    // releases the swap jobs they were counted against, which is what they were holding up.
    // Note that a swap job released here is not isolated in turn: it is only marked ready, and
    // `executeReadySwapJobs` will later remove its element by walking its edges, as it does for any
    // element whose repairs completed without this shortcut. Cascading the isolation instead would
    // make that removal the cheap one as well - left for a follow-up.
    this->invalidateRepairJobs(deleted_id);
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::executeSwapJob(vecsim_stl::vector<idType> &idsToRemove,
                                                         HNSWSwapJob *swap_job) {
    auto deleted_id = swap_job->deleted_id;
    this->getHNSWIndex()->removeFromGraph(deleted_id);
    this->getHNSWIndex()->swapDeletedElement(deleted_id);
    this->invalidateRepairJobs(deleted_id);
    this->fixJobsAfterSwap(deleted_id, idsToRemove);
}

template <typename DataType, typename DistType>
size_t TieredHNSWIndex<DataType, DistType>::executeReadySwapJobs(size_t maxJobsToRun) {

    // Execute swap jobs - acquire hnsw write lock.
    const auto main_index_lock = this->acquireMainIndexGuard();
    std::unique_lock<std::shared_mutex> label_lock(labelGuard);
    TIERED_LOG(VecSimCommonStrings::LOG_VERBOSE_STRING,
               "Tiered HNSW index GC: there are %zu ready swap jobs. Start executing %zu swap jobs",
               readySwapJobs.load(), std::min(readySwapJobs.load(), maxJobsToRun));

    vecsim_stl::vector<idType> idsToRemove(this->allocator);
    idsToRemove.reserve(idToSwapJob.size());
    for (auto &it : idToSwapJob) {
        auto *swap_job = it.second;
        // Swap job is ready for execution - execute and delete it.
        if (swap_job->pending_repair_jobs_counter.load() == 0) {
            executeSwapJob(idsToRemove, swap_job);
            delete swap_job;
        }
        if (maxJobsToRun > 0 && idsToRemove.size() >= maxJobsToRun) {
            break;
        }
    }
    for (idType id : idsToRemove) {
        idToSwapJob.erase(id);
    }
    readySwapJobs -= idsToRemove.size();
    TIERED_LOG(VecSimCommonStrings::LOG_VERBOSE_STRING,
               "Tiered HNSW index GC: done executing %zu swap jobs", idsToRemove.size());
    return idsToRemove.size();
}

template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::deleteLabelFromHNSW(labelType label) {
    auto *hnsw_index = getHNSWIndex();
    this->mainIndexGuard.lock_shared();

    // Get the required data about the relevant ids to delete.
    // Internally, this will hold the index data lock.
    auto internal_ids = hnsw_index->markDelete(label);

    for (size_t i = 0; i < internal_ids.size(); i++) {
        idType id = internal_ids[i];
        vecsim_stl::vector<AsyncJob *> repair_jobs(this->allocator);
        auto *swap_job = new (this->allocator) HNSWSwapJob(this->allocator, id);

        // Go over all the deleted element links in every level and create repair jobs.
        auto incomingEdges = hnsw_index->safeCollectAllNodeIncomingNeighbors(id);

        // Protect the id->repair_jobs lookup while we update it with the new jobs.
        this->idToRepairJobsGuard.lock();
        for (pair<idType, unsigned short> &node : incomingEdges) {
            bool repair_job_exists = false;
            HNSWRepairJob *repair_job = nullptr;
            if (idToRepairJobs.find(node.first) != idToRepairJobs.end()) {
                for (auto it : idToRepairJobs.at(node.first)) {
                    if (it->level == node.second) {
                        // There is already an existing pending repair job for this node due to
                        // the deletion of another node - avoid creating another job.
                        repair_job_exists = true;
                        repair_job = it;
                        break;
                    }
                }
            } else {
                // There is no repair jobs at all for this element, create a new array for it.
                idToRepairJobs.insert(
                    {node.first, vecsim_stl::vector<HNSWRepairJob *>(this->allocator)});
            }
            if (repair_job_exists) {
                repair_job->appendAnotherAssociatedSwapJob(swap_job);
            } else {
                repair_job =
                    new (this->allocator) HNSWRepairJob(this->allocator, node.first, node.second,
                                                        executeRepairJobWrapper, this, swap_job);
                repair_jobs.emplace_back(repair_job);
                idToRepairJobs.at(node.first).push_back(repair_job);
            }
        }
        swap_job->setRepairJobsNum(incomingEdges.size());
        if (incomingEdges.size() == 0) {
            // No pending repair jobs, so swap jobs is ready from the beginning.
            readySwapJobs++;
        }
        this->idToRepairJobsGuard.unlock();

        if (incomingEdges.size() == 0) {
            // No repair job will ever run for this element, so this is already the point at which
            // it can be taken out of the graph (outside the repair jobs guard, as isolating takes
            // the per-element links locks).
            this->isolateRepairedElement(id);
        }

        this->submitJobs(repair_jobs);
        // Insert the swap job into the swap jobs lookup (for fast update in case that the
        // node id is changed due to swap job).
        assert(idToSwapJob.find(id) == idToSwapJob.end());
        idToSwapJob[id] = swap_job;
    }
    this->mainIndexGuard.unlock_shared();
    return internal_ids.size();
}

template <typename DataType, typename DistType>
template <bool releaseFlatGuard>
void TieredHNSWIndex<DataType, DistType>::insertVectorToHNSW(
    HNSWIndex<DataType, DistType> *hnsw_index, labelType label, const void *blob) {

    // Preprocess for storage and indexing in the hnsw index
    ProcessedBlobs processed_blobs = hnsw_index->preprocess(blob);
    const void *processed_storage_blob = processed_blobs.getStorageBlob();
    const void *processed_for_index = processed_blobs.getQueryBlob();

    // Acquire the index data lock, so we know what is the exact index size at this time. Acquire
    // the main r/w lock before to avoid deadlocks.
    this->mainIndexGuard.lock_shared();
    hnsw_index->lockIndexDataGuard();
    // Check if resizing is needed for HNSW index (requires write lock).
    if (hnsw_index->isCapacityFull()) {
        // Release the inner HNSW data lock before we re-acquire the global HNSW lock.
        this->mainIndexGuard.unlock_shared();
        hnsw_index->unlockIndexDataGuard();
        this->lockMainIndexGuard();
        std::unique_lock<std::shared_mutex> label_lock(labelGuard);
        hnsw_index->lockIndexDataGuard();

        // Hold the index data lock while we store the new element. If the new node's max level is
        // higher than the current one, hold the lock through the entire insertion to ensure that
        // graph scans will not occur, as they will try access the entry point's neighbors.
        // If an index resize is still needed, `storeNewElement` will perform it. This is OK since
        // we hold the main index lock for exclusive access.
        auto state = hnsw_index->storeNewElement(label, processed_storage_blob);
        if constexpr (releaseFlatGuard) {
            this->flatIndexGuard.unlock_shared();
        }

        // If we're still holding the index data guard, we cannot take the main index lock for
        // shared ownership as it may cause deadlocks, and we also cannot release the main index
        // lock between, since we cannot allow swap jobs to happen, as they will make the
        // saved state invalid. Hence, we insert the vector with the current exclusive lock held.
        if (state.elementMaxLevel <= state.currMaxLevel) {
            hnsw_index->unlockIndexDataGuard();
        }
        // Take the vector from the flat buffer and insert it to HNSW (overwrite should not occur).
        hnsw_index->indexVector(processed_for_index, label, state);
        if (state.elementMaxLevel > state.currMaxLevel) {
            hnsw_index->unlockIndexDataGuard();
        }
        label_lock.unlock();
        this->unlockMainIndexGuard();
    } else {
        // Do the same as above except for changing the capacity, but with *shared* lock held:
        // Hold the index data lock while we store the new element. If the new node's max level is
        // higher than the current one, hold the lock through the entire insertion to ensure that
        // graph scans will not occur, as they will try access the entry point's neighbors.
        // At this point we are certain that the index has enough capacity for the new element, and
        // this call will not resize the index.
        auto state = hnsw_index->storeNewElement(label, processed_storage_blob);
        if constexpr (releaseFlatGuard) {
            this->flatIndexGuard.unlock_shared();
        }

        if (state.elementMaxLevel <= state.currMaxLevel) {
            hnsw_index->unlockIndexDataGuard();
        }
        // Take the vector from the flat buffer and insert it to HNSW (overwrite should not occur).
        hnsw_index->indexVector(processed_for_index, label, state);
        if (state.elementMaxLevel > state.currMaxLevel) {
            hnsw_index->unlockIndexDataGuard();
        }
        this->mainIndexGuard.unlock_shared();
    }
}

template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::deleteLabelFromHNSWInplace(labelType label) {
    auto *hnsw_index = this->getHNSWIndex();

    auto ids = hnsw_index->getElementIds(label);
    // Dispose pending repair and swap jobs for the removed ids.
    vecsim_stl::vector<idType> idsToRemove(this->allocator);
    idsToRemove.reserve(ids.size());
    readySwapJobs += ids.size(); // account for the current ids that are going to be removed.
    for (size_t id_ind = 0; id_ind < ids.size(); id_ind++) {
        // Get the id in every iteration, since the ids can be swapped in every iteration.
        idType id = hnsw_index->getElementIds(label).at(id_ind);
        hnsw_index->removeVectorInPlace(id);
        this->invalidateRepairJobs(id);
        this->fixJobsAfterSwap(id, idsToRemove);
    }
    hnsw_index->removeLabel(label);
    for (idType id : idsToRemove) {
        idToSwapJob.erase(id);
    }
    readySwapJobs -= idsToRemove.size();
    return ids.size();
}

/******************** Job's callbacks **********************************/
template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::executeInsertJob(HNSWInsertJob *job) {
    // Note that accessing the job fields should occur with flat index guard held (here and later).
    this->flatIndexGuard.lock_shared();
    if (!job->isValid) {
        this->flatIndexGuard.unlock_shared();
        // Job has been invalidated in the meantime - nothing to execute, and remove it from the
        // lookup.
        this->invalidJobsLookupGuard.lock();
        this->invalidJobs.erase(job->id);
        this->invalidJobsLookupGuard.unlock();
        return;
    }

    HNSWIndex<DataType, DistType> *hnsw_index = this->getHNSWIndex();
    // Copy the vector blob from the flat buffer, so we can release the flat lock while we are
    // indexing the vector into HNSW index.
    size_t data_size = this->frontendIndex->getStoredDataSize();
    auto blob_copy = this->getAllocator()->allocate_unique(data_size);
    // Assuming the size of the blob stored in the frontend index matches the size of the blob
    // stored in the HNSW index.
    memcpy(blob_copy.get(), this->frontendIndex->getDataByInternalId(job->id), data_size);

    this->insertVectorToHNSW<true>(hnsw_index, job->label, blob_copy.get());

#ifdef BUILD_TESTS
    if (afterBackendInsertBeforeFlatRemoval) {
        afterBackendInsertBeforeFlatRemoval();
    }
#endif

    // Remove the vector and the insert job from the flat buffer.
    this->removeIngestedVectorFromFlat(job);
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::executeRepairJob(HNSWRepairJob *job) {
    // Lock the HNSW shared lock before accessing its internals.
    this->mainIndexGuard.lock_shared();

    // Whether this job was invalidated, and whether it is still listed as pending, are decided
    // together under one lock. An invalidation landing between the two -- which
    // `isolateRepairedElement` can do while holding the main guard for shared ownership, so it
    // does not exclude us -- rewrites `node_id` with the invalid-job key, and the lookup below
    // would miss.
    this->idToRepairJobsGuard.lock();
    if (!job->isValid) {
        this->idToRepairJobsGuard.unlock();
        this->mainIndexGuard.unlock_shared();
        // The current node has already been removed and disposed.
        this->invalidJobsLookupGuard.lock();
        this->invalidJobs.erase(job->node_id);
        this->invalidJobsLookupGuard.unlock();
        return;
    }
    HNSWIndex<DataType, DistType> *hnsw_index = this->getHNSWIndex();

    // Remove this job pointer from the repair jobs lookup BEFORE it has been executed. Had we done
    // it after executing the repair job, we might have see that there is a pending repair job for
    // this node id upon deleting another neighbor of this node, and we may avoid creating another
    // repair job even though *it has already been executed*.
    auto &repair_jobs = this->idToRepairJobs.at(job->node_id);
    assert(repair_jobs.size() > 0);
    if (repair_jobs.size() == 1) {
        // This was the only pending repair job for this id.
        this->idToRepairJobs.erase(job->node_id);
    } else {
        // There are more pending jobs for the current id, remove just this job from the pending
        // repair jobs list for this element id by replacing it with the last one (and trim the
        // last job in the list).
        auto it = std::find(repair_jobs.begin(), repair_jobs.end(), job);
        assert(it != repair_jobs.end());
        *it = repair_jobs.back();
        repair_jobs.pop_back();
    }
    this->idToRepairJobsGuard.unlock();

    hnsw_index->repairNodeConnections(job->node_id, job->level);

    // Account for this job only now that its repair has actually been performed. Decreasing the
    // counter beforehand would let a swap job be seen as ready, and its element isolated, while
    // an element still points to it from a repair that has not run yet.
    vecsim_stl::vector<idType> fully_repaired_ids(this->allocator);
    this->idToRepairJobsGuard.lock();
    for (auto &it : job->associatedSwapJobs) {
        if (it->atomicDecreasePendingJobsNum() == 0) {
            readySwapJobs++;
            fully_repaired_ids.push_back(it->deleted_id);
        }
    }
    this->idToRepairJobsGuard.unlock();

    // These deleted elements have no pending repair job left, so nothing points to them anymore.
    // Take them out of the graph entirely, leaving no edge in or out for the swap job to deal with.
    // Done outside the repair jobs guard, as isolating takes the per-element links locks.
    for (idType deleted_id : fully_repaired_ids) {
        this->isolateRepairedElement(deleted_id);
    }

    this->mainIndexGuard.unlock_shared();
}

/******************** Index API ****************************************/

template <typename DataType, typename DistType>
TieredHNSWIndex<DataType, DistType>::TieredHNSWIndex(HNSWIndex<DataType, DistType> *hnsw_index,
                                                     BruteForceIndex<DataType, DistType> *bf_index,
                                                     const TieredIndexParams &tiered_index_params,
                                                     std::shared_ptr<VecSimAllocator> allocator)
    : VecSimTieredIndex<DataType, DistType>(hnsw_index, bf_index, tiered_index_params, allocator),
      idToRepairJobs(this->allocator), idToSwapJob(this->allocator), readySwapJobs(0),
      isQuantized(hnsw_index->usesQuantizedStorage()) {
    const size_t normalization_set_size =
        tiered_index_params.specificParams.tieredHnswParams.QuantNormalizationSetSize;
    if (isQuantized && normalization_set_size > 0) {
        sqAccumulationState.emplace(this->allocator, hnsw_index->getDim(), normalization_set_size);
    }
    // If the param for swapJobThreshold is 0 use the default value, if it exceeds the maximum
    // allowed, use the maximum value.
    this->pendingSwapJobsThreshold =
        tiered_index_params.specificParams.tieredHnswParams.swapJobThreshold == 0
            ? DEFAULT_PENDING_SWAP_JOBS_THRESHOLD
            : std::min(tiered_index_params.specificParams.tieredHnswParams.swapJobThreshold,
                       MAX_PENDING_SWAP_JOBS_THRESHOLD);
}

template <typename DataType, typename DistType>
TieredHNSWIndex<DataType, DistType>::~TieredHNSWIndex() {
    // Delete all the pending repair jobs.
    for (auto &jobs : this->idToRepairJobs) {
        for (auto *job : jobs.second) {
            delete job;
        }
    }
    // Delete all the pending swap jobs.
    for (auto &it : this->idToSwapJob) {
        delete it.second;
    }
}

// Training writes stay in FLAT in both write modes, even beyond flatBufferLimit.
// No worker can own an insertion job until finalization submits the whole pending set.
template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::addVectorDuringAccumulation(const void *blob,
                                                                     labelType label) {
    const auto [result, count, should_finalize] = [&] {
        std::lock_guard flat_lock(this->flatIndexGuard);
        idType id = this->frontendIndex->indexSize();
        TieredInsertJob *job = nullptr;
        if (!this->frontendIndex->isMultiValue() && this->frontendIndex->isLabelExists(label)) {
            id = static_cast<BruteForceIndex_Single<DataType, DistType> *>(this->frontendIndex)
                     ->getIdOfLabel(label);
            subtractFromSum(
                {this->frontendIndex->getDataByInternalId(id), this->frontendIndex->getDim()});
            job = this->labelToInsertJobs.at(label).front();
        }
        const int result = this->frontendIndex->addVector(blob, label);
        addToSum({this->frontendIndex->getDataByInternalId(id), this->frontendIndex->getDim()});
        if (!job) {
            job = new (this->allocator)
                HNSWInsertJob(this->allocator, label, id, executeInsertJobWrapper, this);
            this->registerInsertJob(label, job);
        }
        const size_t count = this->frontendIndex->indexSize();
        assert(sqAccumulationState->getCount() == count);
        const bool should_finalize = sqAccumulationState->shouldFinalize();
        return std::tuple{result, count, should_finalize};
    }();

    if (should_finalize) {
        finalizeQuantizationAndSubmitJobs(count);
    }
    return result;
}

template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::deleteVectorDuringAccumulation(labelType label) {
    std::lock_guard flat_lock(this->flatIndexGuard);
    auto it = this->labelToInsertJobs.find(label);
    if (it == this->labelToInsertJobs.end()) {
        return 0;
    }
    const int removed = it->second.size();
    for (auto *job : it->second) {
        subtractFromSum(
            {this->frontendIndex->getDataByInternalId(job->id), this->frontendIndex->getDim()});
        delete job; // Never submitted, so no worker can still reference this job.
    }
    this->labelToInsertJobs.erase(it);
    auto updated_ids = this->frontendIndex->deleteVectorAndGetUpdatedIds(label);
    for (const auto &[new_id, previous] : updated_ids) {
        this->updateInsertJobInternalId(previous.first, new_id, previous.second);
    }
    assert(sqAccumulationState->getCount() == this->frontendIndex->indexSize());
    return removed;
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::finalizeQuantizationAndSubmitJobs(size_t count) {
    const auto mean = sqAccumulationState->calculateMean();
    // Allocate the snapshot before finishing accumulation. Synchronous execution removes jobs
    // from labelToInsertJobs, so iterating the map while dispatching would invalidate iterators.
    vecsim_stl::vector<AsyncJob *> jobs(this->allocator);
    jobs.reserve(count);
    {
        std::shared_lock flat_lock(this->flatIndexGuard);
        for (const auto &[label, pending] : this->labelToInsertJobs) {
            jobs.insert(jobs.end(), pending.begin(), pending.end());
        }
    }
#ifdef BUILD_TESTS
    if (beforeQuantizationFinalization) {
        beforeQuantizationFinalization();
    }
#endif
    {
        const auto main_index_lock = this->acquireMainIndexGuard();
        std::unique_lock<std::shared_mutex> label_lock(labelGuard);
        getHNSWIndex()->setQuantizationMean(mean);
        // The optional state is consumed once. Emptying a trained index never restarts training.
        sqAccumulationState.reset();
    }
#ifdef BUILD_TESTS
    if (afterQuantizationFinalization) {
        afterQuantizationFinalization();
    }
#endif
    if (this->getWriteMode() == VecSim_WriteInPlace) {
        for (auto *job : jobs) {
            executeInsertJobWrapper(job);
        }
    } else {
        this->submitJobs(jobs);
    }
}

template <typename DataType, typename DistType>
auto TieredHNSWIndex<DataType, DistType>::shouldDefrag() {
    return readySwapJobs.load() >= this->pendingSwapJobsThreshold;
}
template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::addVectorInPlace(const void *blob, labelType label) {
    int ret = 1;
    auto hnsw_index = this->getHNSWIndex();
    auto storage_blob = this->frontendIndex->preprocessForStorage(blob);
    if (!this->backendIndex->isMultiValue()) {
        // A label ingested into HNSW whose flat copy is not dropped yet exists in both tiers, so
        // `deleteVector` may report 2 deletions. It is still one overwrite, not a new label.
        ret = this->deleteVector(label) == 0 ? 1 : 0;
    }
    {
        const auto main_index_lock = this->acquireMainIndexGuard();
        std::unique_lock<std::shared_mutex> label_lock(labelGuard);
        hnsw_index->addVector(storage_blob.get(), label);
    }
    // Track direct insertion to HNSW (bypassing flat buffer)
    ++this->directHNSWInsertions;
    return ret;
}
template <typename DataType, typename DistType>
HNSWInsertJob *TieredHNSWIndex<DataType, DistType>::addVectorToFlat(const void *blob,
                                                                    labelType label, int &ret) {
    this->flatIndexGuard.lock();
    idType new_flat_id = this->frontendIndex->indexSize();
    if (this->frontendIndex->isLabelExists(label) && !this->frontendIndex->isMultiValue()) {
        ret = 0;
        // We are going to update the internal id that currently holds the vector associated with
        // the given label.
        new_flat_id = this->invalidatePendingInsertJob(label);
        // If we are adding a new element (rather than updating an exiting one) we may need to
        // increase index capacity.
    }
    // If this label already exists, this will do overwrite.
    this->frontendIndex->addVector(blob, label);

    auto new_insert_job = new (this->allocator)
        HNSWInsertJob(this->allocator, label, new_flat_id, executeInsertJobWrapper, this);
    this->registerInsertJob(label, new_insert_job);
    this->flatIndexGuard.unlock();
    return new_insert_job;
}
// In the tiered index, we assume that the blobs are processed by the flat buffer
// before being transferred to the HNSW index.
// When inserting vectors directly into the HNSW index—such as in VecSim_WriteInPlace mode— or when
// the flat buffer is full, we must manually preprocess the blob according to the **frontend** index
// parameters.
template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::addVector(const void *blob, labelType label) {
    if (sqAccumulationState) {
        return addVectorDuringAccumulation(blob, label);
    }
    // writeMode is not protected since it is assumed to be called only from the "main thread"
    // (that is the thread that is exclusively calling add/delete vector).
    if (this->getWriteMode() == VecSim_WriteInPlace) {
        return addVectorInPlace(blob, label);
    }
    int ret = 1;
    if (this->frontendIndex->indexSize() >= this->flatBufferLimit) {
        // Handle overwrite situation.
        if (!this->backendIndex->isMultiValue()) {
            ret -= this->deleteVector(label);
        }
        if (this->frontendIndex->indexSize() >= this->flatBufferLimit) {
            // We didn't remove a vector from flat buffer due to overwrite, insert the new vector
            // directly to HNSW. Since flat buffer guard was not held, no need to release it
            // internally.
            auto storage_blob = this->frontendIndex->preprocessForStorage(blob);
            auto hnsw_index = this->getHNSWIndex();
            this->insertVectorToHNSW<false>(hnsw_index, label, storage_blob.get());
            // Track direct insertion to HNSW (flat buffer was full)
            ++this->directHNSWInsertions;
            return ret;
        }
        // Otherwise, we fall back to the "regular" insertion into the flat buffer
        // (since it is not full anymore after removing the previous vector stored under the label).
    }
    auto new_insert_job = addVectorToFlat(blob, label, ret);
    // Here, a worker might ingest the previous vector that was stored under "label"
    // (in case of override in non-MULTI index) - so if it's there, we remove it (and create the
    // required repair jobs), *before* we submit the insert job.
    if (!this->backendIndex->isMultiValue()) {
        // If we removed the previous vector from both HNSW and flat in the overwrite process,
        // we still return 0 (not -1).
        ret = std::max(ret - this->deleteLabelFromHNSW(label), 0);
    }
    // Apply ready swap jobs if number of deleted vectors reached the threshold (under exclusive
    // lock of the main index guard).
    if (shouldDefrag()) {
        this->defrag();
    }

    // Insert job to the queue and signal the workers' updater.
    this->submitSingleJob(new_insert_job);
    return ret;
}

template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::deleteFromFlatAndInsertJobs(labelType label) {
    int num_deleted_vectors = 0;
    this->flatIndexGuard.lock_shared();
    if (this->frontendIndex->isLabelExists(label)) {
        this->flatIndexGuard.unlock_shared();
        this->flatIndexGuard.lock();
        // Check again if the label exists, as it may have been removed while we released the lock.
        if (this->frontendIndex->isLabelExists(label)) {
            // Invalidate the pending insert job(s) into HNSW associated with this label
            auto &insert_jobs = this->labelToInsertJobs.at(label);
            for (auto *job : insert_jobs) {
                job->id = this->setAndSaveInvalidJob(job);
            }
            num_deleted_vectors += insert_jobs.size();
            // Remove the pending insert job(s) from the labelToInsertJobs mapping.
            this->labelToInsertJobs.erase(label);
            // Go over the every id that corresponds the label and remove it from the flat buffer.
            // Every delete may cause a swap of the deleted id with the last id, and we return a
            // mapping from id to the original id that resides in this id after the deletion(s) (see
            // an example in this function implementation in MULTI index).
            auto updated_ids = this->frontendIndex->deleteVectorAndGetUpdatedIds(label);
            for (auto &it : updated_ids) {
                idType prev_id = it.second.first;
                labelType updated_vec_label = it.second.second;
                this->updateInsertJobInternalId(prev_id, it.first, updated_vec_label);
            }
        }
        this->flatIndexGuard.unlock();
    } else {
        this->flatIndexGuard.unlock_shared();
    }
    return num_deleted_vectors;
}

template <typename DataType, typename DistType>
int TieredHNSWIndex<DataType, DistType>::deleteVector(labelType label) {
    if (sqAccumulationState) {
        return deleteVectorDuringAccumulation(label);
    }
    int num_deleted_vectors = this->deleteFromFlatAndInsertJobs(label);

    // Next, check if there vector(s) stored under the given label in HNSW and delete them as well.
    // Note that we may remove the same vector that has been removed from the flat index, if it was
    // being ingested at that time.
    // writeMode is not protected since it is assumed to be called only from the "main thread"
    // (that is the thread that is exclusively calling add/delete vector).
    if (this->getWriteMode() == VecSim_WriteAsync) {
        num_deleted_vectors += this->deleteLabelFromHNSW(label);
        // Apply ready swap jobs if number of deleted vectors reached the threshold
        // (under exclusive lock of the main index guard).
        if (shouldDefrag()) {
            this->defrag();
        }
    } else {
        // delete in place.
        const auto main_index_lock = this->acquireMainIndexGuard();
        std::unique_lock<std::shared_mutex> label_lock(labelGuard);
        num_deleted_vectors += this->deleteLabelFromHNSWInplace(label);
    }

    return num_deleted_vectors;
}

template <typename DataType, typename DistType>
VecSimUpdateCode TieredHNSWIndex<DataType, DistType>::updateVectorSingleValue(labelType label,
                                                                              const void *new_blobs,
                                                                              size_t n) {
    if (n > 1) {
        return VecSimUpdate_MultiNotSupported;
    }
    if (n == 0) {
        // Nothing to store, so replacing the label's content means removing it.
        this->deleteVector(label);
        return VecSimUpdate_OK;
    }
    // One vector replacing a single-value label *is* an overwrite, and `addVector` serves that
    // better than a deleted and an insert would: it writes over the buffered copy in place, keeping
    // its id and the pending job that will ingest it, and only then marks the backend's copy
    // deleted - so the label is never without a vector, where removing first would leave a window
    // with none.
    this->addVector(new_blobs, label);
    return VecSimUpdate_OK;
}
/**
 * Replace everything stored under a label with the given vectors.
 *
 * Both halves go through this index's own delete and add, which is what makes every mode and tier
 * fall out for free: the delete drops the label's buffered copies and invalidates their pending
 * insert jobs, and marks its indexed elements deleted (or removes them in place, in the
 * corresponding write mode); each insert then lands wherever a fresh vector would - the flat
 * buffer, or HNSW directly once that buffer is full or in in-place mode. The elements of the label
 * that survive are none: unlike `relabelVector` this is not a bookkeeping change, and unlike a
 * quantized index's stored data it needs no comparison against the values, so nothing here refuses
 * a quantized backend.
 *
 * The consequence to know is the one the delete already has: an indexed element leaves a tombstone
 * behind, to be repaired and disposed of by the usual jobs. A label whose vectors are all still
 * buffered costs no tombstone at all.
 *
 * Not atomic - a query landing between the two halves sees the label with none of its vectors -
 * which is the same window the delete-then-insert it replaces would leave.
 */
template <typename DataType, typename DistType>
VecSimUpdateCode TieredHNSWIndex<DataType, DistType>::updateVectors(labelType label,
                                                                    const void *new_blobs,
                                                                    size_t n) {
    if (!this->backendIndex->isMultiValue())
        return updateVectorSingleValue(label, new_blobs, n);

    // -- Multi Value --
    this->deleteVector(label);
    const char *blob = static_cast<const char *>(new_blobs);
    for (size_t i = 0; i < n; i++) {
        this->addVector(blob + i * this->frontendIndex->getInputBlobSize(), label);
    }
    return VecSimUpdate_OK;
}

/**
 * Move a label across every place the tiered index records one. Unlike add/delete this needs no
 * jobs and no vector data movement - the swap/repair machinery is keyed purely on internal ids, so
 * it is unaffected.
 *
 * A label can legitimately live in *both* tiers at once: `executeInsertJob` inserts into HNSW
 * before removing the vector from the flat buffer, so all four updates below are independently
 * guarded by their own existence check rather than treated as mutually exclusive.
 *
 * Locking follows the class's index guard hierarchy. Ingestion without resizing can overlap
 * relabeling; HNSW's data guard serializes their backend label lookup changes.
 */
template <typename DataType, typename DistType>
VecSimRelabelCode TieredHNSWIndex<DataType, DistType>::relabelVector(labelType old_label,
                                                                     labelType new_label) {
    if (old_label == new_label) {
        return VecSimRelabel_SameLabel;
    }

    std::unique_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);
    std::unique_lock<std::shared_mutex> label_lock(labelGuard);

    auto *hnsw_index = this->getHNSWIndex();
    // A worker can still insert into an existing HNSW allocation while sharing the main guard.
    // Protect lookup reads against that insertion, then release before HNSW relabel takes the
    // data guard exclusively. The flat guard prevents a new insert job being claimed for either
    // label, and structural removal cannot proceed while the label guard is held.
    bool in_hnsw;
    bool target_in_hnsw;
    {
        auto data_lock = hnsw_index->acquireSharedIndexDataGuard();
        in_hnsw = hnsw_index->isLabelExists(old_label);
        target_in_hnsw = hnsw_index->isLabelExists(new_label);
    }
    // A label can live in any subset of its three homes, so both questions are asked of all of
    // them: it is present if any home holds it, and the target is free only if none does. Reject if
    // the target is taken anywhere, so that a partially applied move is impossible - a free target
    // in all three homes means none of the updates below can collide.
    const bool source_exists =
        this->frontendIndex->isLabelExists(old_label) ||
        this->labelToInsertJobs.find(old_label) != this->labelToInsertJobs.end() || in_hnsw;
    const bool target_taken =
        this->frontendIndex->isLabelExists(new_label) ||
        this->labelToInsertJobs.find(new_label) != this->labelToInsertJobs.end() || target_in_hnsw;

    // Each home is asked to move the label only once it reported holding it, and the checks above
    // ruled out every other rejection - the label is present, the target is free everywhere, and
    // the labels differ - so while both guards are held a home that is asked can only answer OK.
    // The asserts below state that; a refusal would mean the state changed underneath us.
    if (source_exists && !target_taken) {
        if (this->frontendIndex->isLabelExists(old_label)) {
            const VecSimRelabelCode flat_ret =
                this->frontendIndex->relabelVector(old_label, new_label);
#ifdef BUILD_TESTS
            assert(flat_ret == VecSimRelabel_OK &&
                   "the flat buffer just reported holding this label");
#endif
            UNUSED(flat_ret);
        }

        // Re-key the pending insert jobs *and* rewrite each job's own copy of the label. Both must
        // move together: `executeInsertJob` indexes into HNSW under `job->label` and then looks the
        // job up with `labelToInsertJobs.at(job->label)`, so a half-applied move either indexes the
        // vector under the stale label or throws out of the worker thread.
        auto jobs_it = this->labelToInsertJobs.find(old_label);
        if (jobs_it != this->labelToInsertJobs.end()) {
            auto jobs = std::move(jobs_it->second);
            this->labelToInsertJobs.erase(jobs_it);
            for (TieredInsertJob *job : jobs) {
                job->label = new_label;
            }
            this->labelToInsertJobs.emplace(new_label, std::move(jobs));
        }

        // `relabelVector` takes the HNSW index data guard internally.
        if (in_hnsw) {
            const VecSimRelabelCode hnsw_ret = hnsw_index->relabelVector(old_label, new_label);
#ifdef BUILD_TESTS
            assert(hnsw_ret == VecSimRelabel_OK && "HNSW just reported holding this label");
#endif
            UNUSED(hnsw_ret);
        }
    }

    // An absent source outranks an occupied target: a caller that resolves `NewLabelTaken` by
    // freeing the target would otherwise drop an unrelated vector for a move with nothing to move.
    if (!source_exists) {
        return VecSimRelabel_OldLabelMissing;
    }
    if (target_taken) {
        return VecSimRelabel_NewLabelTaken;
    }
    return VecSimRelabel_OK;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//  TieredHNSW_BatchIterator                                                                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

/******************** Ctor / Dtor *****************/

// Defining spacial values for the hnsw_iterator field, to indicate if the iterator is uninitialized
// or depleted when we don't have a valid iterator.
#define UNINITIALIZED ((VecSimBatchIterator *)0)
#define DEPLETED      ((VecSimBatchIterator *)1)

template <typename DataType, typename DistType>
TieredHNSWIndex<DataType, DistType>::TieredHNSW_BatchIterator::TieredHNSW_BatchIterator(
    const void *query_vector, const TieredHNSWIndex<DataType, DistType> *index,
    VecSimQueryParams *queryParams, std::shared_ptr<VecSimAllocator> allocator)
    // Tiered batch iterator doesn't hold its own copy of the query vector.
    // Instead, each internal batch iterators (flat_iterator and hnsw_iterator) create their own
    // copies: flat_iterator copy is created during TieredHNSW_BatchIterator construction When
    // TieredHNSW_BatchIterator::getNextResults() is called and hnsw_iterator is not initialized, it
    // retrieves the blob from flat_iterator
    : TieredIndex_BatchIterator(nullptr, queryParams ? queryParams->timeoutCtx : nullptr,
                                std::move(allocator)),
      index(index), backend_index_lock(index->mainIndexGuard, std::defer_lock),
      flat_iterator(UNINITIALIZED), hnsw_iterator(UNINITIALIZED) {
    {
        std::shared_lock<std::shared_mutex> flat_index_lock(this->index->flatIndexGuard);
        this->flat_iterator =
            this->index->frontendIndex->newBatchIterator(query_vector, queryParams);
    }

    // Save a copy of the query params to initialize the HNSW iterator with (on first batch and
    // first batch after reset).
    if (queryParams) {
        this->queryParams =
            (VecSimQueryParams *)this->allocator->allocate(sizeof(VecSimQueryParams));
        *this->queryParams = *queryParams;
    } else {
        this->queryParams = nullptr;
    }
}

template <typename DataType, typename DistType>
TieredHNSWIndex<DataType, DistType>::TieredHNSW_BatchIterator::~TieredHNSW_BatchIterator() {
    delete this->flat_iterator;

    if (this->hnsw_iterator != UNINITIALIZED && this->hnsw_iterator != DEPLETED) {
        delete this->hnsw_iterator;
    }

    this->allocator->free_allocation(this->queryParams);
}

/******************** Implementation **************/

template <typename DataType, typename DistType>
VecSimQueryReply *TieredHNSWIndex<DataType, DistType>::TieredHNSW_BatchIterator::getNextResults(
    size_t n_res, VecSimQueryReply_Order order) {

    const bool needsDedup = this->index->frontendIndex->isMultiValue() || this->index->isQuantized;
    auto hnsw_code = VecSim_QueryReply_OK;

    if (this->hnsw_iterator == UNINITIALIZED) {
        // First call to getNextResults. The call to the BF iterator will include calculating all
        // the distances and access the BF index. We take the lock on this call.
        this->index->flatIndexGuard.lock_shared();
        auto cur_flat_results = this->flat_iterator->getNextResults(n_res, BY_SCORE_THEN_ID);
        this->index->flatIndexGuard.unlock_shared();
        // This is also the only time `getNextResults` on the BF iterator can fail.
        if (VecSim_OK != cur_flat_results->code) {
            return cur_flat_results;
        }
        this->flat_results.swap(cur_flat_results->results);
        VecSimQueryReply_Free(cur_flat_results);
        // We also take the lock on the main index on the first call to getNextResults, and we hold
        // it until the iterator is depleted or freed.
        this->backend_index_lock.lock();
        this->hnsw_iterator = this->index->backendIndex->newBatchIterator(
            this->flat_iterator->getQueryBlob(), queryParams);
        auto cur_hnsw_results = this->getNextBackendResults(n_res);
        hnsw_code = cur_hnsw_results->code;
        this->backend_results.swap(cur_hnsw_results->results);
        VecSimQueryReply_Free(cur_hnsw_results);
        if (this->hnsw_iterator->isDepleted()) {
            delete this->hnsw_iterator;
            this->hnsw_iterator = DEPLETED;
            this->backend_index_lock.unlock();
        }
    } else {
        while (this->flat_results.size() < n_res && !this->flat_iterator->isDepleted()) {
            auto tail = this->flat_iterator->getNextResults(n_res - this->flat_results.size(),
                                                            BY_SCORE_THEN_ID);
            this->flat_results.insert(this->flat_results.end(), tail->results.begin(),
                                      tail->results.end());
            VecSimQueryReply_Free(tail);

            if (!needsDedup) {
                // On single-value indexes, duplicates will never appear in the hnsw results before
                // they appear in the flat results (at the same time or later if the approximation
                // misses) so we don't need to try and filter the flat results (and recheck
                // conditions).
                break;
            } else {
                // On multi-value and quantized indexes, the flat results may contain results that
                // were already returned from the hnsw index. We need to filter them out.
                filter_irrelevant_results(this->flat_results);
            }
        }

        while (this->backend_results.size() < n_res && this->hnsw_iterator != DEPLETED &&
               hnsw_code == VecSim_OK) {
            auto tail = this->getNextBackendResults(n_res - this->backend_results.size());
            hnsw_code = tail->code; // Set the hnsw_results code to the last `getNextResults` code.
            // New batch may contain better results than the previous batch, so we need to merge.
            // We don't expect duplications (hence the <false>), as the iterator guarantees that
            // no result is returned twice.
            VecSimQueryResultContainer cur_hnsw_results(this->allocator);
            merge_results<false>(cur_hnsw_results, this->backend_results, tail->results, n_res);
            VecSimQueryReply_Free(tail);
            this->backend_results.swap(cur_hnsw_results);
            filter_irrelevant_results(this->backend_results);
            if (this->hnsw_iterator->isDepleted()) {
                delete this->hnsw_iterator;
                this->hnsw_iterator = DEPLETED;
                this->backend_index_lock.unlock();
            }
        }
    }

    if (VecSim_OK != hnsw_code) {
        return new VecSimQueryReply(this->allocator, hnsw_code);
    }

    VecSimQueryReply *batch;
    if (needsDedup)
        batch = compute_current_batch<true>(n_res);
    else
        batch = compute_current_batch<false>(n_res);

    if (order == BY_ID) {
        sort_results_by_id(batch);
    }
    size_t batch_len = VecSimQueryReply_Len(batch);
    this->updateResultsCount(batch_len);

    return batch;
}

// DISCLAIMER: After the last batch, one of the iterators may report that it is not depleted,
// while all of its remaining results were already returned from the other iterator.
// (On single-value indexes, this can happen to the hnsw iterator only, on multi-value
//  indexes, this can happen to both iterators).
// The next call to `getNextResults` will return an empty batch, and then the iterators will
// correctly report that they are depleted.
template <typename DataType, typename DistType>
bool TieredHNSWIndex<DataType, DistType>::TieredHNSW_BatchIterator::isDepleted() {
    return this->flat_results.empty() && this->flat_iterator->isDepleted() &&
           this->backend_results.empty() && this->hnsw_iterator == DEPLETED;
}

template <typename DataType, typename DistType>
void TieredHNSWIndex<DataType, DistType>::TieredHNSW_BatchIterator::reset() {
    if (this->hnsw_iterator != UNINITIALIZED && this->hnsw_iterator != DEPLETED) {
        delete this->hnsw_iterator;
        this->backend_index_lock.unlock();
    }
    this->resetResultsCount();
    this->flat_iterator->reset();
    this->hnsw_iterator = UNINITIALIZED;
    this->flat_results.clear();
    this->backend_results.clear();
    returned_results_set.clear();
}

template <typename DataType, typename DistType>
VecSimIndexStatsInfo TieredHNSWIndex<DataType, DistType>::statisticInfo() const {
    auto stats = VecSimTieredIndex<DataType, DistType>::statisticInfo();
    stats.directHNSWInsertions = this->directHNSWInsertions;
    return stats;
}

template <typename DataType, typename DistType>
VecSimIndexDebugInfo TieredHNSWIndex<DataType, DistType>::debugInfo() const {
    auto info = VecSimTieredIndex<DataType, DistType>::debugInfo();

    HnswTieredInfo hnswTieredInfo = {.pendingSwapJobsThreshold = this->pendingSwapJobsThreshold,
                                     .defragRuns = defragRuns.load(std::memory_order_relaxed),
                                     .defragTimeNs = defragTimeNs.load(std::memory_order_relaxed)};
    info.tieredInfo.specificTieredBackendInfo.hnswTieredInfo = hnswTieredInfo;

    info.tieredInfo.backgroundIndexing =
        info.tieredInfo.frontendCommonInfo.indexSize > 0 ? VecSimBool_TRUE : VecSimBool_FALSE;

    return info;
}

template <typename DataType, typename DistType>
VecSimDebugInfoIterator *TieredHNSWIndex<DataType, DistType>::debugInfoIterator() const {
    VecSimIndexDebugInfo info = this->debugInfo();
    // Get the base tiered fields.
    auto *infoIterator = VecSimTieredIndex<DataType, DistType>::debugInfoIterator();

    // Tiered HNSW specific param.
    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::TIERED_HNSW_SWAP_JOBS_THRESHOLD_STRING,
        .fieldType = INFOFIELD_UINT64,
        .fieldValue = {FieldValue{.uintegerValue = info.tieredInfo.specificTieredBackendInfo
                                                       .hnswTieredInfo.pendingSwapJobsThreshold}}});

    const auto &tieredInfo = info.tieredInfo.specificTieredBackendInfo.hnswTieredInfo;
    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::TIERED_HNSW_DEFRAG_RUNS_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = tieredInfo.defragRuns}}});
    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::TIERED_HNSW_DEFRAG_TIME_NS_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = tieredInfo.defragTimeNs}}});

    return infoIterator;
}

template <typename DataType, typename DistType>
VecSimIndexBasicInfo TieredHNSWIndex<DataType, DistType>::basicInfo() const {
    VecSimIndexBasicInfo info = this->backendIndex->getBasicInfo();
    info.isTiered = true;
    info.algo = VecSimAlgo_HNSWLIB;
    return info;
}
