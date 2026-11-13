/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "vec_sim_index.h"
#include "algorithms/brute_force/brute_force.h"
#include "algorithms/brute_force/brute_force_single.h"
#include "VecSim/batch_iterator.h"
#include "VecSim/tombstone_interface.h"
#include "VecSim/utils/query_result_utils.h"
#include "VecSim/utils/alignment.h"
#include "VecSim/utils/scoped_locks.h"

#include <atomic>
#include <cmath>
#include <mutex>
#include <shared_mutex>

#if HAVE_SVS
// For the compressed-backend check in getDataByLabel.
#include "VecSim/algorithms/svs/svs.h"
#endif

#define TIERED_LOG this->backendIndex->log

/**
 * Definition of generic job structure for asynchronous tiered index.
 */
struct AsyncJob : public VecsimBaseObject {
    JobType jobType;
    JobCallback Execute; // A callback that receives a job as its input and executes the job.
    VecSimIndex *index;
    bool isValid;

    AsyncJob(std::shared_ptr<VecSimAllocator> allocator, JobType type, JobCallback callback,
             VecSimIndex *index_ref)
        : VecsimBaseObject(allocator), jobType(type), Execute(callback), index(index_ref),
          isValid(true) {}
};

/**
 * Definition of a job that inserts a new vector from flat into the backend index.
 * Backend specific insert jobs derive from it to set their own job type.
 */
struct TieredInsertJob : public AsyncJob {
    labelType label;
    idType id;

    TieredInsertJob(std::shared_ptr<VecSimAllocator> allocator, JobType type, labelType label_,
                    idType id_, JobCallback insertCb, VecSimIndex *index_)
        : AsyncJob(allocator, type, insertCb, index_), label(label_), id(id_) {}
};

class TieredIndex_BatchIterator : public VecSimBatchIterator {
protected:
    VecSimQueryResultContainer flat_results;
    VecSimQueryResultContainer backend_results;

    // On single value indices, this set holds the IDs of the results that were returned from
    // the flat buffer.
    // On multi value indices, this set holds the IDs of all the results that were returned.
    // The difference between the two cases is that on multi value indices, the same ID can
    // appear in both indexes and results with different scores, and therefore we can't tell in
    // advance when we expect a possibility of a duplicate.
    vecsim_stl::unordered_set<labelType> returned_results_set;

    TieredIndex_BatchIterator(void *query_vector, void *tctx,
                              std::shared_ptr<VecSimAllocator> allocator)
        : VecSimBatchIterator(query_vector, tctx, std::move(allocator)),
          flat_results(this->allocator), backend_results(this->allocator),
          returned_results_set(this->allocator) {}

    template <bool needsDedup>
    VecSimQueryReply *compute_current_batch(size_t n_res) {
        // Merge results
        auto batch_res = new VecSimQueryReply(this->allocator);
        std::pair<size_t, size_t> p;
        if (needsDedup) {
            p = merge_results<true>(batch_res->results, this->backend_results, this->flat_results,
                                    n_res);
        } else {
            p = merge_results<false>(batch_res->results, this->backend_results, this->flat_results,
                                     n_res);
        }
        auto [from_backend, from_flat] = p;

        if (!needsDedup) {
            // Update the set of results returned from the FLAT
            // index before popping them.
            for (size_t i = 0; i < from_flat; ++i) {
                this->returned_results_set.insert(this->flat_results[i].id);
            }
        } else {
            // Update the set of results returned (from `batch_res`)
            for (size_t i = 0; i < batch_res->results.size(); ++i) {
                this->returned_results_set.insert(batch_res->results[i].id);
            }
        }

        // Update results
        this->flat_results.erase(this->flat_results.begin(),
                                 this->flat_results.begin() + from_flat);
        this->backend_results.erase(this->backend_results.begin(),
                                    this->backend_results.begin() + from_backend);

        // clean up the results
        // One (or both) results lists may contain results that are already
        // returned form the other list (with a different score). We need to filter them out.
        if (needsDedup) {
            this->filter_irrelevant_results(this->flat_results);
            this->filter_irrelevant_results(this->backend_results);
        }

        // Return current batch
        return batch_res;
    }

    void filter_irrelevant_results(VecSimQueryResultContainer &results) {
        // Filter out results that were already returned.
        const auto it = std::remove_if(results.begin(), results.end(), [this](const auto &r) {
            return returned_results_set.count(r.id) != 0;
        });
        results.erase(it, results.end());
    }
};

// All read operations (including KNN, range, batch iterators and get-distance-from) are guaranteed
// to consider all vectors that were added to the index before the query was submitted. The results
// may include vectors that were added after the query was submitted, with no guarantees.
template <typename DataType, typename DistType>
class VecSimTieredIndex : public VecSimIndexInterface {
protected:
    VecSimIndexAbstract<DataType, DistType> *backendIndex;
    BruteForceIndex<DataType, DistType> *frontendIndex;

    void *jobQueue;
    void *jobQueueCtx; // External context to be sent to the submit callback.
    SubmitCB SubmitJobsToQueue;

    mutable std::shared_mutex flatIndexGuard;
    mutable std::shared_mutex mainIndexGuard;
    SharedMutexLockable flatIndexLockable{flatIndexGuard};
    SharedMutexLockable mainIndexLockable{mainIndexGuard};

    // Locking behavior for topKQuery/rangeQuery
    virtual ScopedLocks lockMainIndexForQuery() const = 0;

    virtual ScopedLocks lockMainIndexForInfo() const { return ScopedLocks(mainIndexLockable); }

    // Locking behavior for indexSize()
    virtual ScopedLocks lockIndexForSize() const = 0;

    // Locking behavior for indexCapacity()
    virtual ScopedLocks lockIndexForCapacity() const = 0;

    void lockMainIndexGuard() const {
        mainIndexGuard.lock();
#ifdef BUILD_TESTS
        mainIndexGuard_write_lock_count++;
#endif
    }

    void unlockMainIndexGuard() const { mainIndexGuard.unlock(); }

    [[nodiscard]] std::lock_guard<std::shared_mutex> acquireMainIndexGuard() const {
        lockMainIndexGuard();
        return std::lock_guard<std::shared_mutex>(mainIndexGuard, std::adopt_lock);
    }
#ifdef BUILD_TESTS
    // Cumulative exclusive acquisitions; unlocking does not decrement this counter.
    mutable std::atomic_int mainIndexGuard_write_lock_count = 0;
#endif
    size_t flatBufferLimit;

    void submitSingleJob(AsyncJob *job) {
        this->SubmitJobsToQueue(this->jobQueue, this->jobQueueCtx, &job, &job->Execute, 1);
    }

    void submitJobs(vecsim_stl::vector<AsyncJob *> &jobs) {
        vecsim_stl::vector<JobCallback> callbacks(jobs.size(), this->allocator);
        for (size_t i = 0; i < jobs.size(); i++) {
            callbacks[i] = jobs[i]->Execute;
        }
        this->SubmitJobsToQueue(this->jobQueue, this->jobQueueCtx, jobs.data(), callbacks.data(),
                                jobs.size());
    }

    /**
     * @brief Return the union of unique labels in both index tiers (which are not deleted).
     * This is a debug-only method for tiered indexes that computes the union of labels
     * from both frontend and backend indexes. It assumes that caller holds the appropriate
     * locks and it is time-consuming.
     * !!! Note: this should only be called in debug mode for tiered indexes !!!
     *
     * @return index label count for debug purposes.
     */
    vecsim_stl::vector<labelType> computeUnifiedIndexLabelsSetUnsafe() const {
        auto [flat_labels, backend_labels] =
            std::make_pair(this->frontendIndex->getLabelsSet(), this->backendIndex->getLabelsSet());

        // Compute the union of the two sets.
        vecsim_stl::vector<labelType> labels_union(this->allocator);
        labels_union.reserve(flat_labels.size() + backend_labels.size());
        std::set_union(flat_labels.begin(), flat_labels.end(), backend_labels.begin(),
                       backend_labels.end(), std::back_inserter(labels_union));
        return labels_union;
    }

#ifdef BUILD_TESTS
public:
    int getMainIndexGuardWriteLockCount() const { return mainIndexGuard_write_lock_count; }
#endif
    VecSimQueryReply *topKQueryImp(const void *queryBlob, size_t k,
                                   VecSimQueryParams *queryParams) const;

    VecSimQueryReply *rangeQueryImp(const void *queryBlob, double radius,
                                    VecSimQueryParams *queryParams,
                                    VecSimQueryReply_Order order) const;

#ifdef BUILD_TESTS
public:
#endif
    /// Mappings from id/label to associated jobs, for invalidating and update ids if necessary.
    // In MULTI, we can have more than one insert job pending per label.
    // **This map is protected with the flat buffer lock**
    vecsim_stl::unordered_map<labelType, vecsim_stl::vector<TieredInsertJob *>> labelToInsertJobs;

    // Helper function for updating the pending insert job(s) of a label after the flat buffer
    // swapped the vector's internal id
    virtual void updateInsertJobInternalId(idType prev_id, idType new_id, labelType label) {
        // Update the pending job id, due to a swap that was caused after the removal of new_id.
        assert(new_id != INVALID_ID && prev_id != INVALID_ID);
        auto it = this->labelToInsertJobs.find(label);
        if (it != this->labelToInsertJobs.end()) {
            // There is a pending job for the label of the swapped last id - update its id.
            for (TieredInsertJob *job_it : it->second) {
                if (job_it->id == prev_id) {
                    job_it->id = new_id;
                }
            }
        }
    }

    // A mapping to hold invalid jobs, so we can dispose them upon index deletion.
    vecsim_stl::unordered_map<idType, AsyncJob *> invalidJobs;
    idType currInvalidJobId; // A unique arbitrary identifier for accessing invalid jobs
    std::mutex invalidJobsLookupGuard;

    // Set an insert/repair job as invalid, put the job pointer in the invalid jobs lookup under
    // the current available id, increase it and return it (while holding invalidJobsLookupGuard).
    // Returns the id that the job was stored under (to be set in the job id field).
    virtual idType setAndSaveInvalidJob(AsyncJob *job) {
        std::lock_guard<std::mutex> lock(this->invalidJobsLookupGuard);
        job->isValid = false;
        idType curInvalidId = currInvalidJobId++;
        this->invalidJobs.insert({curInvalidId, job});
        return curInvalidId;
    }

    // Remove the job pointer from the labelToInsertJobs mapping. Must hold flatIndexGuard.
    void detachInsertJob(TieredInsertJob *job) {
        auto it = this->labelToInsertJobs.find(job->label);
        if (it == this->labelToInsertJobs.end()) {
            return;
        }
        auto &jobs = it->second;
        for (size_t i = 0; i < jobs.size(); i++) {
            if (jobs[i] == job) {
                jobs.erase(jobs.begin() + (long)i);
                break;
            }
        }
        if (jobs.empty()) {
            this->labelToInsertJobs.erase(it);
        }
    }

    // A caller must hold flatIndexGuard.
    idType invalidatePendingInsertJob(labelType label) {
        auto *old_job = this->labelToInsertJobs.at(label).at(0);
        old_job->id = this->setAndSaveInvalidJob(old_job);
        this->labelToInsertJobs.erase(label);
        return dynamic_cast<BruteForceIndex_Single<DataType, DistType> *>(this->frontendIndex)
            ->getIdOfLabel(label);
    }

    // A caller must hold flatIndexGuard.
    void registerInsertJob(labelType label, TieredInsertJob *job) {
        // Construct the job vector only for a new label; multi-value labels append to the existing
        // one.
        auto [it, inserted] = this->labelToInsertJobs.try_emplace(label, 1, job, this->allocator);
        if (!inserted) {
            assert(this->backendIndex->isMultiValue());
            it->second.push_back(job);
        }
    }

    // Remove a vector and its insert job from the flat buffer
    void removeIngestedVectorFromFlat(TieredInsertJob *job) {
        std::lock_guard<std::shared_mutex> flat_lock(this->flatIndexGuard);
        // The job might have been invalidated due to overwrite in the meantime. In this case,
        // it was already deleted and the job has been evicted. Otherwise, we need to do it now.
        if (!job->isValid) {
            // Remove the current job from the invalid jobs' lookup, as we are about to delete it
            // now.
            std::lock_guard<std::mutex> invalid_jobs_lock(this->invalidJobsLookupGuard);
            this->invalidJobs.erase(job->id);
            return;
        }
        this->detachInsertJob(job);
        // Remove the vector from the flat buffer. This may cause the last vector id to swap with
        // the deleted id. Hold the label for the last id, so we can later on update its
        // corresponding job id. Note that after calling deleteVectorById, the last id's label
        // shouldn't be available, since it is removed from the lookup.
        labelType last_vec_label =
            this->frontendIndex->getVectorLabel(this->frontendIndex->indexSize() - 1);
        int deleted = this->frontendIndex->deleteVectorById(job->label, job->id);
        if (deleted && job->id != this->frontendIndex->indexSize()) {
            // If the vector removal caused a swap with the last id, update the relevant insert job.
            this->updateInsertJobInternalId(this->frontendIndex->indexSize(), job->id,
                                            last_vec_label);
        }
    }

public:
    /**
     * @brief Get the vector elements stored under a label, in insertion order.
     *
     * Contract on `VecSimIndexAbstract::getDataByLabel`, including that `vectors_output` arrives
     * empty, with two caveats:
     * - The vectors are the buffer's followed by the backend's, which for a multi-value label
     *   split across the tiers is not insertion order.
     * - An ingest job inserts into the backend before removing from the buffer, so a vector
     *   caught inside that window is reported by both tiers and appears twice.
     *
     * An SQ8 HNSW backend cannot report vector elements. Reads append nothing, including during
     * training, when the requested label is still buffered in the flat tier.
     *
     * Which tiers are read follows `getDistanceFrom_Unsafe`: a single-value label found in the
     * buffer is the whole answer, but a multi-value label's vectors are routinely split across
     * the tiers while an ingest is pending, so there the backend is read as well. Reading only
     * the backend, as this used to, reports nothing for a vector written recently enough to
     * still be buffered -- which is exactly when a document is most likely to be written again.
     *
     * A compressed SVS backend cannot report its stored vectors as values. Hence, for
     * a multi-value label that already got a buffer contribution, we check `isLabelExists`
     * in the backend before trusting the buffer alone.
     */
    void getDataByLabel(labelType label, std::vector<std::vector<DataType>> &vectors_output) const {
#ifdef BUILD_TESTS
        // The base contract asks for an empty output. A caller reusing a vector would otherwise
        // get this label's vectors appended to the previous label's, with nothing to notice it by.
        assert(vectors_output.empty() && "getDataByLabel expects an empty output vector");
#endif

        // SQ8 HNSW indexes report no values, including during training.
        if (this->backendIndex->usesQuantizedStorage()) {
            return;
        }

#if HAVE_SVS
        const auto *svs_backend = dynamic_cast<const SVSIndexBase *>(this->backendIndex);
        const bool backend_cannot_report = svs_backend && svs_backend->isCompressed();
#endif

        std::shared_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);
        const size_t before_flat = vectors_output.size();
        this->frontendIndex->getDataByLabel(label, vectors_output);
#if HAVE_SVS
        // There is no buffer contribution, and the compressed backend cannot report values.
        if (backend_cannot_report && vectors_output.size() == before_flat) {
            return;
        }
#endif
        // Read the backend for multi-value labels or labels absent from FLAT.
        if (this->frontendIndex->isMultiValue() || vectors_output.size() == before_flat) {
            std::shared_lock<std::shared_mutex> main_lock(this->mainIndexGuard);
#if HAVE_SVS
            if (backend_cannot_report && vectors_output.size() > before_flat &&
                svs_backend->isLabelExists(label)) {
                // The buffer's contribution alone would look like the whole answer; report
                // nothing instead, the same rule `SVSIndex::getDataByLabel` applies to a
                // single tier.
                vectors_output.resize(before_flat);
                return;
            }
#endif
            this->backendIndex->getDataByLabel(label, vectors_output);
        }
    }

    // `getDistanceFrom` returns the minimum distance between the given blob and the vector with
    // the given label. If the label doesn't exist, the distance will be NaN.
    // Therefore, it's better to just call `getDistanceFrom` on both indexes and return the minimum
    // instead of checking if the label exists in each index. We first try to get the distance from
    // the flat buffer, as vectors in the buffer might move to the backend while we're "between"
    // the locks.
    // Behavior for single (regular) index:
    // 1. label doesn't exist in both indexes - return NaN
    // 2. label exists in one of the indexes only - return the distance from that index (valid)
    // 3. label exists in both indexes - return the value from the flat buffer (valid and equal to
    //    the value from the backend index), saving us from locking the backend index.
    // Behavior for multi index:
    // 1. label doesn't exist in both indexes - return NaN
    // 2. label exists in one of the indexes only - return the distance from that index (valid)
    // 3. label exists in both indexes - we may have some of the vectors with the same label in the
    //    flat buffer only and some in the backend index only (and maybe temporal duplications). So,
    //    we get the distance from both indexes and return the minimum.
    //
    // IMPORTANT: this should be called when the *tiered index locks are locked for shared
    // ownership*, along with the backend index's own data guard lock if it has one. That is since
    // the internal getDistanceFrom calls access the indexes' data, and it is not safe to run
    // insert/delete operations in parallel. Also, we avoid acquiring the locks internally, since
    // this is usually called for every vector individually, and the overhead of acquiring and
    // releasing the locks is significant in that case.
    double getDistanceFrom_Unsafe(labelType label, const void *blob) const override {
        // Try to get the distance from the flat buffer.
        // If the label doesn't exist, the distance will be NaN.
        auto flat_dist = this->frontendIndex->getDistanceFrom_Unsafe(label, blob);

        // Optimization. TODO: consider having different implementations for single and multi
        // indexes, to avoid checking the index type on every query.
        if (!this->backendIndex->isMultiValue() && !std::isnan(flat_dist)) {
            // If the index is single value, and we got a valid distance from the flat buffer,
            // we can return the distance without querying the backend index.
            return flat_dist;
        }

        // Try to get the distance from the backend index.
        auto backend_dist = this->backendIndex->getDistanceFrom_Unsafe(label, blob);

        // Return the minimum distance that is not NaN.
        return std::fmin(flat_dist, backend_dist);
    }

    VecSimTieredIndex(VecSimIndexAbstract<DataType, DistType> *backendIndex_,
                      BruteForceIndex<DataType, DistType> *frontendIndex_,
                      TieredIndexParams tieredParams, std::shared_ptr<VecSimAllocator> allocator)
        : VecSimIndexInterface(allocator), backendIndex(backendIndex_),
          frontendIndex(frontendIndex_), jobQueue(tieredParams.jobQueue),
          jobQueueCtx(tieredParams.jobQueueCtx), SubmitJobsToQueue(tieredParams.submitCb),
          flatBufferLimit(tieredParams.flatBufferLimit), labelToInsertJobs(this->allocator),
          invalidJobs(this->allocator), currInvalidJobId(0) {
        assert(backendIndex != nullptr);
    }

    virtual ~VecSimTieredIndex() {
        // Delete all the pending insert jobs.
        for (auto &jobs : this->labelToInsertJobs) {
            for (auto *job : jobs.second) {
                delete job;
            }
        }
        // Delete all the pending invalid jobs.
        for (auto &it : this->invalidJobs) {
            delete it.second;
        }
        VecSimIndex_Free(backendIndex);
        VecSimIndex_Free(frontendIndex);
    }

    VecSimQueryReply *topKQuery(const void *queryBlob, size_t k,
                                VecSimQueryParams *queryParams) const override;

    VecSimQueryReply *rangeQuery(const void *queryBlob, double radius,
                                 VecSimQueryParams *queryParams,
                                 VecSimQueryReply_Order order) const override;

    size_t indexSize() const override {
        auto locks = lockIndexForSize();
        return this->frontendIndex->indexSize() + this->backendIndex->indexSize();
    }

    size_t indexCapacity() const override {
        auto locks = lockIndexForCapacity();
        return this->frontendIndex->indexCapacity() + this->backendIndex->indexCapacity();
    }

    virtual inline uint64_t getAllocationSize() const override {
        return this->allocator->getAllocationSize() + this->backendIndex->getAllocationSize() +
               this->frontendIndex->getAllocationSize();
    }
    virtual size_t getNumMarkedDeleted() const = 0;
    size_t indexLabelCount() const override;
    VecSimIndexStatsInfo statisticInfo() const override;
    virtual VecSimIndexDebugInfo debugInfo() const override;
    virtual VecSimDebugInfoIterator *debugInfoIterator() const override;

    bool preferAdHocSearch(size_t subsetSize, size_t k, bool initial_check) const override {
        // For now, decide according to the bigger index.
        return this->backendIndex->indexSize() > this->frontendIndex->indexSize()
                   ? this->backendIndex->preferAdHocSearch(subsetSize, k, initial_check)
                   : this->frontendIndex->preferAdHocSearch(subsetSize, k, initial_check);
    }

    // Return the current state of the global write mode (async/in-place).
    static VecSimWriteMode getWriteMode() { return VecSimIndexInterface::asyncWriteMode; }

#ifdef BUILD_TESTS
    inline BruteForceIndex<DataType, DistType> *getFlatBufferIndex() { return this->frontendIndex; }
    inline size_t getFlatBufferLimit() { return this->flatBufferLimit; }

    virtual void fitMemory() override {
        this->backendIndex->fitMemory();
        this->frontendIndex->fitMemory();
    }
#endif
};

template <typename DataType, typename DistType>
VecSimQueryReply *
VecSimTieredIndex<DataType, DistType>::topKQueryImp(const void *queryBlob, size_t k,
                                                    VecSimQueryParams *queryParams) const {
    std::shared_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);

    // If the flat buffer is empty, we can simply query the main index.
    if (this->frontendIndex->indexSize() == 0) {
        // Release the flat lock and acquire the main lock.
        flat_lock.unlock();

        // Simply query the main index and return the results while holding the lock.
        auto processed_query_ptr = this->frontendIndex->preprocessQuery(queryBlob);
        const void *processed_query = processed_query_ptr.get();
        auto mainLock = lockMainIndexForQuery();
        auto res = this->backendIndex->topKQuery(processed_query, k, queryParams);

        return res;
    } else {
        // No luck... first query the flat buffer and release the lock.
        // The query blob is already processed according to the frontend index.
        auto flat_results = this->frontendIndex->topKQuery(queryBlob, k, queryParams);
        flat_lock.unlock();

        // If the query failed (currently only on timeout), return the error code.
        if (flat_results->code != VecSim_QueryReply_OK) {
            assert(flat_results->results.empty());
            return flat_results;
        }

        auto processed_query_ptr = this->frontendIndex->preprocessQuery(queryBlob);
        const void *processed_query = processed_query_ptr.get();
        VecSimQueryReply *main_results;
        {
            // Lock the main index and query it.
            auto mainLock = lockMainIndexForQuery();
            main_results = this->backendIndex->topKQuery(processed_query, k, queryParams);
        }

        // If the query failed (currently only on timeout), return the error code.
        if (main_results->code != VecSim_QueryReply_OK) {
            // Free the flat results.
            VecSimQueryReply_Free(flat_results);

            assert(main_results->results.empty());
            return main_results;
        }

        return merge_result_lists(main_results, flat_results, k);
    }
}
template <typename DataType, typename DistType>
VecSimQueryReply *
VecSimTieredIndex<DataType, DistType>::topKQuery(const void *queryBlob, size_t k,
                                                 VecSimQueryParams *queryParams) const {
    return this->topKQueryImp(queryBlob, k, queryParams);
}

template <typename DataType, typename DistType>
VecSimQueryReply *
VecSimTieredIndex<DataType, DistType>::rangeQuery(const void *queryBlob, double radius,
                                                  VecSimQueryParams *queryParams,
                                                  VecSimQueryReply_Order order) const {
    return this->rangeQueryImp(queryBlob, radius, queryParams, order);
}

template <typename DataType, typename DistType>
VecSimQueryReply *
VecSimTieredIndex<DataType, DistType>::rangeQueryImp(const void *queryBlob, double radius,
                                                     VecSimQueryParams *queryParams,
                                                     VecSimQueryReply_Order order) const {
    this->flatIndexGuard.lock_shared();

    // If the flat buffer is empty, we can simply query the main index.
    if (this->frontendIndex->indexSize() == 0) {
        // Release the flat lock and acquire the main lock.
        this->flatIndexGuard.unlock_shared();

        auto processed_query_ptr = this->frontendIndex->preprocessQuery(queryBlob);
        const void *processed_query = processed_query_ptr.get();
        VecSimQueryReply *res;
        {
            auto mainLock = lockMainIndexForQuery();
            // Simply query the main index and return the results while holding the lock.
            res = this->backendIndex->rangeQuery(processed_query, radius, queryParams);
        }

        // We could have passed the order to the main index, but we can sort them here after
        // unlocking it instead.
        sort_results(res, order);
        return res;
    } else {
        // No luck... first query the flat buffer and release the lock.
        // The query blob is already processed according to the frontend index.
        auto flat_results = this->frontendIndex->rangeQuery(queryBlob, radius, queryParams);
        this->flatIndexGuard.unlock_shared();

        // If the query failed (currently only on timeout), return the error code and the partial
        // results.
        if (flat_results->code != VecSim_QueryReply_OK) {
            return flat_results;
        }

        auto processed_query_ptr = this->frontendIndex->preprocessQuery(queryBlob);
        const void *processed_query = processed_query_ptr.get();
        // Lock the main index and query it.

        VecSimQueryReply *main_results;
        {
            auto mainLock = lockMainIndexForQuery();
            main_results = this->backendIndex->rangeQuery(processed_query, radius, queryParams);
        }

        // Merge the results and return, avoiding duplicates.
        // At this point, the return code of the FLAT index is OK, and the return code of the MAIN
        // index is either OK or TIMEOUT. Make sure to return the return code of the MAIN index.
        if (BY_SCORE == order) {
            sort_results_by_score_then_id(main_results);
            sort_results_by_score_then_id(flat_results);

            // Keep the return code of the main index.
            auto code = main_results->code;

            // Merge the sorted results with no limit (all the results are valid).
            VecSimQueryReply *ret = merge_result_lists(main_results, flat_results, -1);
            // Restore the return code and return.
            ret->code = code;
            return ret;

        } else { // BY_ID
            // Notice that we don't modify the return code of the main index in any step.
            concat_results(main_results, flat_results);
            filter_results_by_id(main_results);
            return main_results;
        }
    }
}

template <typename DataType, typename DistType>
VecSimIndexStatsInfo VecSimTieredIndex<DataType, DistType>::statisticInfo() const {
    auto stats = VecSimIndexStatsInfo{
        .memory = this->getAllocationSize(),
        .numberOfMarkedDeleted = this->getNumMarkedDeleted(),
        .directHNSWInsertions = 0, // Base tiered index returns 0; TieredHNSWIndex overrides
        .flatBufferSize = this->frontendIndex->indexSize(),
    };

    return stats;
}

template <typename DataType, typename DistType>
size_t VecSimTieredIndex<DataType, DistType>::indexLabelCount() const {
    // This is a debug-only method for tiered indexes that computes the union of labels
    // from both frontend and backend indexes. It requires locking and is time-consuming.
    // !!! Note: this should only be called in debug mode for tiered indexes !!!
    std::shared_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);
    std::shared_lock<std::shared_mutex> main_lock(this->mainIndexGuard);
    return computeUnifiedIndexLabelsSetUnsafe().size();
}

template <typename DataType, typename DistType>
VecSimIndexDebugInfo VecSimTieredIndex<DataType, DistType>::debugInfo() const {
    VecSimIndexDebugInfo info;
    this->flatIndexGuard.lock_shared();
    this->mainIndexGuard.lock_shared();

    VecSimIndexDebugInfo frontendInfo = this->frontendIndex->debugInfo();
    VecSimIndexDebugInfo backendInfo = this->backendIndex->debugInfo();

    info.commonInfo.indexLabelCount = this->computeUnifiedIndexLabelsSetUnsafe().size();

    this->flatIndexGuard.unlock_shared();
    this->mainIndexGuard.unlock_shared();

    info.commonInfo.indexSize =
        frontendInfo.commonInfo.indexSize + backendInfo.commonInfo.indexSize;
    info.commonInfo.memory = this->getAllocationSize();
    info.commonInfo.lastMode = backendInfo.commonInfo.lastMode;

    VecSimIndexBasicInfo basic_info{
        .algo = backendInfo.commonInfo.basicInfo.algo,
        .metric = backendInfo.commonInfo.basicInfo.metric,
        .type = backendInfo.commonInfo.basicInfo.type,
        .isMulti = this->backendIndex->isMultiValue(),
        .isTiered = true,
        .isDisk = backendInfo.commonInfo.basicInfo.isDisk,
        .blockSize = backendInfo.commonInfo.basicInfo.blockSize,
        .dim = backendInfo.commonInfo.basicInfo.dim,
    };
    info.commonInfo.basicInfo = basic_info;

    // NOTE: backgroundIndexing needs to be set by the backend index.
    info.tieredInfo.backgroundIndexing = VecSimBool_UNSET;

    switch (backendInfo.commonInfo.basicInfo.algo) {
    case VecSimAlgo_HNSWLIB:
        info.tieredInfo.backendInfo.hnswInfo = backendInfo.hnswInfo;
        break;
    case VecSimAlgo_SVS:
        info.tieredInfo.backendInfo.svsInfo = backendInfo.svsInfo;
        break;
    case VecSimAlgo_BF:
    case VecSimAlgo_TIERED:
        assert(false && "Invalid backend algorithm");
    }

    info.tieredInfo.backendCommonInfo = backendInfo.commonInfo;
    // For now, this is hard coded to FLAT
    info.tieredInfo.frontendCommonInfo = frontendInfo.commonInfo;
    info.tieredInfo.bfInfo = frontendInfo.bfInfo;

    info.tieredInfo.management_layer_memory = this->allocator->getAllocationSize();
    info.tieredInfo.bufferLimit = this->flatBufferLimit;
    return info;
}

template <typename DataType, typename DistType>
VecSimDebugInfoIterator *VecSimTieredIndex<DataType, DistType>::debugInfoIterator() const {
    VecSimIndexDebugInfo info = this->debugInfo();
    // For readability. Update this number when needed.
    size_t numberOfInfoFields = 14;
    auto *infoIterator = new VecSimDebugInfoIterator(numberOfInfoFields, this->allocator);

    // Set tiered explicitly as algo name for root iterator.
    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::ALGORITHM_STRING,
        .fieldType = INFOFIELD_STRING,
        .fieldValue = {FieldValue{.stringValue = VecSimCommonStrings::TIERED_STRING}}});

    this->backendIndex->addCommonInfoToIterator(infoIterator, info.commonInfo);

    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::TIERED_MANAGEMENT_MEMORY_STRING,
        .fieldType = INFOFIELD_UINT64,
        .fieldValue = {FieldValue{.uintegerValue = info.tieredInfo.management_layer_memory}}});

    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::TIERED_BACKGROUND_INDEXING_STRING,
        .fieldType = INFOFIELD_INT64,
        .fieldValue = {FieldValue{.integerValue = info.tieredInfo.backgroundIndexing}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::TIERED_BUFFER_LIMIT_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.tieredInfo.bufferLimit}}});

    // Acquire shared locks to safely access each sub-index's debug info during background indexing.
    // Only hold each lock while accessing its respective index to minimize contention.
    {
        std::shared_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);
        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::FRONTEND_INDEX_STRING,
            .fieldType = INFOFIELD_ITERATOR,
            .fieldValue = {FieldValue{.iteratorValue = this->frontendIndex->debugInfoIterator()}}});
    }

    {
        auto main_lock = lockMainIndexForInfo();
        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::BACKEND_INDEX_STRING,
            .fieldType = INFOFIELD_ITERATOR,
            .fieldValue = {FieldValue{.iteratorValue = this->backendIndex->debugInfoIterator()}}});
    }
    return infoIterator;
};
