/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "VecSim/vec_sim_tiered_index.h"
#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "algorithms/hnsw/hnsw_disk.h"
#include "storage/hnsw_storage.h"
#include "utils/consistency_lock.h"
#include "throttle.h"
#include <algorithm>
#include <atomic>
#include <concepts>
#include <memory>
#include <optional>
#include <utility>

// Just use shared_ptr directly, but always create via factory
// TODO: move to VecSim repo for use across repos

template <std::derived_from<VecsimBaseObject> T>
struct SafeVecSimDeleter {
    void operator()(T* p) const {
        if (!p)
            return;
        auto allocator = p->getAllocator();
        std::destroy_at(p);
        allocator->free_allocation(p);
    }
};

template <std::derived_from<VecsimBaseObject> T>
std::shared_ptr<T> make_vecsim_shared_ptr(T* raw) {
    return std::shared_ptr<T>(raw, SafeVecSimDeleter<T>{});
}

template <std::derived_from<VecsimBaseObject> T, typename... Args>
std::shared_ptr<T> make_vecsim_shared_ptr(std::shared_ptr<VecSimAllocator> allocator, Args&&... args) {
    T* raw = new (allocator) T(allocator, std::forward<Args>(args)...);
    return make_vecsim_shared_ptr(raw);
}

enum DiskJobType {
    DISK_HNSW_INSERT_VECTOR_JOB,
    DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB,
    DISK_HNSW_DELETE_VECTOR_INIT_JOB,
    DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB,
    DISK_HNSW_QUERY_MARKER_JOB,
};

struct AsyncDiskJob : public AsyncJob {
    DiskJobType type;

    AsyncDiskJob(std::shared_ptr<VecSimAllocator> allocator, DiskJobType type_, JobCallback callback,
                 VecSimIndex* index_)
        : AsyncJob(allocator, HNSW_DISK_JOB, callback, index_), type(type_), pending_jobs(allocator) {}

private:
    // Vector of pending jobs that are waiting for this job to complete
    vecsim_stl::vector<std::shared_ptr<AsyncDiskJob>> pending_jobs;

    template <typename DataType, typename DistType>
    friend class TieredHNSWDiskIndex; // Allow API to access `pending_jobs`
};

// Repair job structure that stores node id and level for tracking
struct RepairDiskJob : public AsyncDiskJob {
    idType node_id;
    levelType level;

    RepairDiskJob(std::shared_ptr<VecSimAllocator> allocator, idType id_, levelType level_, JobCallback callback,
                  VecSimIndex* index_)
        : AsyncDiskJob(allocator, DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB, callback, index_), node_id(id_),
          level(level_) {}
};

// Delete job structure that stores the deleted node id
struct DeleteDiskJob : public AsyncDiskJob {
    idType deleted_id;

    DeleteDiskJob(std::shared_ptr<VecSimAllocator> allocator, DiskJobType type_, idType id_, JobCallback callback,
                  VecSimIndex* index_)
        : AsyncDiskJob(allocator, type_, callback, index_), deleted_id(id_) {}
};

// Insert job structure - simplified to match RAM version
// State capture and ID allocation all happen in the worker thread
// NOTE: No overwrite handling needed - caller guarantees no duplicate labels
struct InsertDiskJob : public AsyncDiskJob {
    labelType label; // User-provided label for the vector

    InsertDiskJob(std::shared_ptr<VecSimAllocator> allocator, labelType label_, JobCallback callback,
                  VecSimIndex* index_)
        : AsyncDiskJob(allocator, DISK_HNSW_INSERT_VECTOR_JOB, callback, index_), label(label_) {}
};

// Query marker job - tracks active queries to prevent ID reuse during search
// No-op execution, just for tracking in currently_running
struct QueryMarkerJob : public AsyncDiskJob {
    QueryMarkerJob(std::shared_ptr<VecSimAllocator> allocator)
        : AsyncDiskJob(allocator, DISK_HNSW_QUERY_MARKER_JOB, nullptr, nullptr) {}
};

template <typename DataType, typename DistType>
class TieredHNSWDiskIndex : public VecSimTieredIndex<DataType, DistType> {
private:
    // Job tracking for pending repair jobs
    std::mutex pending_repairs_guard;
    vecsim_stl::unordered_map<GraphNodeType, std::shared_ptr<AsyncDiskJob>> pending_repairs;

    // Job tracking for currently running jobs (non-owning raw pointers)
    mutable std::mutex running_guard;
    mutable vecsim_stl::vector<AsyncDiskJob*> currently_running;

    // Jobs that have been submitted but not yet started - keeps them alive
    // Map from raw pointer to owning shared_ptr for efficient lookup
    std::mutex submitted_jobs_guard;
    vecsim_stl::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs;

    // Flag to indicate if the index is being destroyed
    std::atomic<bool> is_destroyed{false};

    // Test hook: callback invoked after validity check but BEFORE indexVector.
    // Used to reproduce race condition in tests.
    // NOTE: Must be declared outside #ifdef to maintain consistent class layout between
    // library and tests (avoiding ODR violations that cause mutex corruption and crashes).
    std::function<void()> testHookBeforeIndexVector;

    VecSimQueryReply* topKQueryImp(const void* queryBlob, size_t k, VecSimQueryParams* queryParams) const;

    // Custom deleter for pending jobs that auto-submits when ref count reaches 0
    // This is used for jobs like DELETE_FINALIZE that should be submitted when all
    // jobs they're pending on have completed.
    //
    // SAFETY: The destroyed_flag is accessed during index destructor execution (when
    // pending_repairs.clear() and submitted_jobs.clear() are called), not after the
    // index is freed. At that point, the index memory is still valid and the flag can
    // be safely read. This assumes jobs are only triggered with valid index references,
    // so no jobs will be running or triggered after the destructor starts.
    struct PendingJobDeleter {
        TieredHNSWDiskIndex* index;
        std::atomic<bool>* destroyed_flag;

        void operator()(AsyncDiskJob* job) const {
            if (job && index && destroyed_flag && !destroyed_flag->load()) {
                // Create a new shared_ptr with SafeVecSimDeleter
                auto job_ptr = make_vecsim_shared_ptr(job);
                index->submitDiskJob(job_ptr);
            } else if (job) {
                // Index is destroyed, just delete the job
                SafeVecSimDeleter<AsyncDiskJob>{}(job);
            }
        }
    };

    // Create a pending job with auto-submit behavior when ref count reaches 0
    // IMPORTANT: Jobs created with this function should NEVER be manually submitted
    // via submitDiskJob() - they will auto-submit when all references are released.
    // Manual submission would cause double-submission and segfault.
    template <typename JobType, typename... Args>
    std::shared_ptr<AsyncDiskJob> createAutoSubmitJob(Args&&... args) {
        JobType* job = new (this->allocator) JobType(std::forward<Args>(args)..., executeDiskJobWrapper, this);
        return std::shared_ptr<AsyncDiskJob>(job, PendingJobDeleter{this, &is_destroyed});
    }

    std::shared_ptr<AsyncDiskJob> createInsertJob(labelType label) {
        return make_vecsim_shared_ptr<InsertDiskJob>(this->allocator, label, executeDiskJobWrapper, this);
    }

    std::shared_ptr<AsyncDiskJob> createRepairJob(idType node_id, levelType level) {
        return make_vecsim_shared_ptr<RepairDiskJob>(this->allocator, node_id, level, executeDiskJobWrapper, this);
    }

    std::shared_ptr<AsyncDiskJob> createDeleteInitJob(idType deleted_id) {
        return createAutoSubmitJob<DeleteDiskJob>(this->allocator, DISK_HNSW_DELETE_VECTOR_INIT_JOB, deleted_id);
    }

    std::shared_ptr<AsyncDiskJob> createDeleteFinalizeJob(idType deleted_id) {
        return createAutoSubmitJob<DeleteDiskJob>(this->allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, deleted_id);
    }

    // Submit a single job and add to submitted_jobs to keep it alive
    void submitDiskJob(std::shared_ptr<AsyncDiskJob> job) {
        {
            std::lock_guard<std::mutex> lock(submitted_jobs_guard);
            // Assert that we're not double-submitting a job
            // Double submission is DANGEROUS: job gets queued twice but only has one ref count
            // First execution removes from submitted_jobs and deletes, second execution segfaults
            assert(submitted_jobs.find(job.get()) == submitted_jobs.end() &&
                   "Double submission detected! Job already in submitted_jobs. "
                   "This indicates a bug: jobs should only be submitted once.");
            submitted_jobs[job.get()] = job;
        }
        this->submitSingleJob(job.get());
    }

    // Submit multiple jobs and add to submitted_jobs to keep them alive
    void submitDiskJobs(const vecsim_stl::vector<std::shared_ptr<AsyncDiskJob>>& jobs) {
        if (jobs.empty()) {
            return;
        }

        vecsim_stl::vector<AsyncJob*> raw_jobs(this->allocator);
        raw_jobs.reserve(jobs.size());

        {
            std::lock_guard<std::mutex> lock(submitted_jobs_guard);
            for (const auto& job : jobs) {
                // Assert that we're not double-submitting a job
                assert(submitted_jobs.find(job.get()) == submitted_jobs.end() &&
                       "Double submission detected! Job already in submitted_jobs.");
                submitted_jobs[job.get()] = job;
                raw_jobs.push_back(job.get());
            }
        }

        this->submitJobs(raw_jobs);
    }

    HNSWDiskIndex<DataType, DistType>* get_hnsw_index() const {
        return static_cast<HNSWDiskIndex<DataType, DistType>*>(this->backendIndex);
    }

    // Delete a label from HNSW disk index - marks as deleted and submits async cleanup jobs
    // Returns the number of internal IDs that were deleted (0 if label didn't exist).
    size_t deleteLabelFromHNSW(labelType label) {
        auto* hnsw_index = get_hnsw_index();

        // Mark the label as deleted in the HNSW index, and get the associated internal IDs.
        // markDelete returns empty vector if label doesn't exist.
        vecsim_stl::vector<idType> deleted_ids = hnsw_index->markDelete(label);

        // Create the init delete job for each deleted ID
        for (idType internal_id : deleted_ids) {
            // Create job with auto-submit deleter, pended by all currently running jobs.
            // If no running jobs, it will be submitted immediately.
            pendDeleteInitJobByCurrentlyRunning(internal_id);
        }
        return deleted_ids.size();
    }

    // Job execution methods
    void executeInsertJob(AsyncDiskJob* job) {
        auto* insert_job = static_cast<InsertDiskJob*>(job);
        auto* hnsw_index = get_hnsw_index();

        // Step 1: Copy full-precision data from flat buffer while holding the lock.
        // We copy (not just take a pointer) so we can release the lock before the expensive
        // indexing work. This is required because deleteVector() can invalidate the job
        // and remove the data from the flat buffer while we're processing.
        size_t data_size = hnsw_index->getInputBlobSize();
        auto fullDataCopy = this->allocator->allocate_unique(data_size);
        {
            std::shared_lock<std::shared_mutex> lock(this->flatIndexGuard);
            // Check if job was invalidated (by deleteVector) before we even start
            if (!this->frontendIndex->isLabelExists(insert_job->label)) {
                return; // Job invalidated - abort
            }
            // Cast to BruteForceIndex_Single to access getIdOfLabel and getDataByInternalId
            auto* flatIndexSingle = static_cast<BruteForceIndex_Single<DataType, DistType>*>(this->frontendIndex);
            idType flatInternalId = flatIndexSingle->getIdOfLabel(insert_job->label);
            assert(flatInternalId != INVALID_ID &&
                   "Label not found after isLabelExists returned true - should never happen");
            const void* fullData = flatIndexSingle->getDataByInternalId(flatInternalId);
            std::memcpy(fullDataCopy.get(), fullData, data_size);
        }

        // Step 2: Preprocess to quantized format in tiered layer (outside any lock)
        // This is done here so we own the blobs and can pass them to both indexVector and storeVectorConnections
        ProcessedBlobs processedBlobs = hnsw_index->preprocess(fullDataCopy.get());
        const void* quantizedQuery = processedBlobs.getQueryBlob();
        const void* quantizedStorage = processedBlobs.getStorageBlob();

#ifdef VECSIM_DISK_BUILD_TESTS
        // Test hook: allow tests to inject code for testing
        if (testHookBeforeIndexVector) {
            testHookBeforeIndexVector();
        }
#endif

        // Step 3: Search phase (read-only) - indexVector()
        // Generates random level internally and performs pure graph search.
        // No writes or locks held during this potentially expensive operation.
        auto indexResult = hnsw_index->indexVector(quantizedQuery);

        // Step 4: Acquire consistency guard (outermost lock for fork safety)
        // This must be held from now through all HNSW writes
        vecsim_disk::ConsistencySharedGuard consistency_guard;

        // Step 5: Atomic transition from flat buffer to HNSW under flat lock
        // Validate label still exists, remove from flat, allocate ID and register in HNSW
        idType internalId;
        {
            std::unique_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);

            // Validate: if deleted during indexVector, abort
            if (!this->frontendIndex->isLabelExists(insert_job->label)) {
                return; // Deleted during search phase - abort
            }

            // Remove from flat buffer
            this->frontendIndex->deleteVector(insert_job->label);

            // Disable throttling if flat buffer has space
            size_t flat_size = this->frontendIndex->indexSize();
            if (flat_size == this->flatBufferLimit - 1) {
                TIERED_LOG(VecSimCommonStrings::LOG_DEBUG_STRING,
                           "executeInsertJob: Flat buffer has space (size=%zu, limit=%zu), disabling throttle",
                           flat_size, this->flatBufferLimit);
                VecSimDisk_InvokeDisableThrottle();
            }

            // Allocate ID, store quantized data, register label atomically in HNSW
            internalId =
                hnsw_index->allocateAndRegister(insert_job->label, indexResult.elementMaxLevel, quantizedStorage);
        }
        // flat_lock released - expensive graph work can now proceed

        // Step 6: Complete graph insertion (writes full-precision data to disk, connects to graph)
        // This is the expensive part - done without holding flat lock
        auto nodesToRepair =
            hnsw_index->storeVectorConnections(internalId, fullDataCopy.get(), quantizedQuery, std::move(indexResult));

        // Step 7: Submit repair jobs for overflowed nodes (can run concurrently)
        submitRepairs(nodesToRepair);

        // consistency_guard released here automatically
    }

    void executeRepairJob(RepairDiskJob* job) {
        auto* hnsw_index = get_hnsw_index();
        hnsw_index->repairNode(job->node_id, job->level);
    }

    void executeDeleteInitJob(DeleteDiskJob* delete_job) {
        idType deleted_id = delete_job->deleted_id;

        auto* hnsw_index = get_hnsw_index();
        auto* storage = hnsw_index->getStorage();

        hnsw_index->replaceEntryPointOnDelete(deleted_id);

        levelType max_level = hnsw_index->getElementMaxLevel(deleted_id);

        GraphNodeList all_neighbors(this->allocator);
        for (levelType level = 0; level <= max_level; level++) {
            vecsim_stl::vector<idType> incoming_neighbors(this->allocator);
            storage->get_incoming_edges(deleted_id, level, incoming_neighbors);
            for (idType neighbor_id : incoming_neighbors) {
                all_neighbors.emplace_back(neighbor_id, level);
            }
        }

        auto finalize_job = createDeleteFinalizeJob(deleted_id);

        submitRepairs(all_neighbors, std::move(finalize_job));
    }

    void executeDeleteFinalizeJob(DeleteDiskJob* delete_job) {
        idType deleted_id = delete_job->deleted_id;

        auto* hnsw_index = get_hnsw_index();
        auto* storage = hnsw_index->getStorage();

        levelType max_level = hnsw_index->getElementMaxLevel(deleted_id);

        vecsim_stl::vector<std::tuple<idType, levelType, idType, EdgeOperation>> batch_ops(this->allocator);

        for (levelType level = 0; level <= max_level; level++) {
            vecsim_stl::vector<idType> outgoing_neighbors(this->allocator);
            storage->get_outgoing_edges(deleted_id, level, outgoing_neighbors);
            for (idType neighbor_id : outgoing_neighbors) {
                batch_ops.emplace_back(neighbor_id, level, deleted_id, EdgeOperation::Delete);
            }
        }

        // Acquire ConsistencySharedGuard before any writes (disk or in-memory).
        // This matches executeInsertJob pattern where the guard is held during all mutations.
        vecsim_disk::ConsistencySharedGuard consistency_guard;

        if (!batch_ops.empty()) {
            storage->batch_merge_incoming_edges(batch_ops);
        }

        storage->del_vector(deleted_id);
        for (levelType level = 0; level <= max_level; level++) {
            storage->del_outgoing_edges(deleted_id, level);
            storage->del_incoming_edges(deleted_id, level);
        }

        hnsw_index->recycleId(deleted_id);
        hnsw_index->decrementElementCount();
    }

    // Generic static wrapper that dispatches to the right executor based on job type
    static void executeDiskJobWrapper(AsyncJob* job) {
        AsyncDiskJob* disk_job = static_cast<AsyncDiskJob*>(job);
        auto index = static_cast<TieredHNSWDiskIndex<DataType, DistType>*>(job->index);

        // Remove from submitted_jobs (we now own a reference on the stack if it was there)
        std::shared_ptr<AsyncDiskJob> job_owner;
        {
            std::lock_guard<std::mutex> lock(index->submitted_jobs_guard);
            auto it = index->submitted_jobs.find(disk_job);
            assert(it != index->submitted_jobs.end());
            job_owner = std::move(it->second); // Take ownership
            index->submitted_jobs.erase(it);
        }

        // Remove repair job from pending_repairs BEFORE adding to currently_running.
        // This ensures the job is never in both structures simultaneously, preventing
        // a data race where submitRepairs (under pending_repairs_guard) and
        // pendByCurrentlyRunning (under running_guard) could both push_back to the
        // job's pending_jobs vector concurrently.
        //
        // Note: This creates a tiny window where a repair job is in neither structure.
        // If a fork happens exactly in this window, the repair job won't be replicated.
        // This is acceptable because:
        // 1. Repair jobs are non-critical optimizations, not correctness requirements
        // 2. The leader still completes the repair; replica gets repaired state via data sync
        // 3. Repairs for deletions will be re-generated on the replica anyway
        // 4. Over-linked nodes will be repaired when that graph area is traversed later
        if (disk_job->type == DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB) {
            RepairDiskJob* repair_job = static_cast<RepairDiskJob*>(disk_job);
            std::lock_guard<std::mutex> lock(index->pending_repairs_guard);
            index->pending_repairs.erase(GraphNodeType(repair_job->node_id, repair_job->level));
        }

        index->addToCurrentlyRunning(disk_job);

        if (disk_job->isValid) {
            // Dispatch to the appropriate executor based on job type
            switch (disk_job->type) {
            case DISK_HNSW_INSERT_VECTOR_JOB:
                index->executeInsertJob(disk_job);
                break;
            case DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB:
                index->executeRepairJob(static_cast<RepairDiskJob*>(disk_job));
                break;
            case DISK_HNSW_DELETE_VECTOR_INIT_JOB:
                index->executeDeleteInitJob(static_cast<DeleteDiskJob*>(disk_job));
                break;
            case DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB:
                index->executeDeleteFinalizeJob(static_cast<DeleteDiskJob*>(disk_job));
                break;
            case DISK_HNSW_QUERY_MARKER_JOB:
                // No-op - query markers are just for tracking in currently_running
                break;
            }
        }

        index->removeFromCurrentlyRunning(disk_job);

        // job_owner goes out of scope, releasing the job
        // When the job is destroyed, pending_jobs vector is destroyed, releasing all references
        // Jobs with auto-submit deleters will be automatically submitted when ref count reaches 0
    }

    // Submit multiple repair jobs for a collection of (id, level) pairs
    // All repairs will have the same pending job linked to them
    // Uses batch submission for efficiency
    void submitRepairs(const GraphNodeList& repairs, std::shared_ptr<AsyncDiskJob> pend_on = nullptr) {
        if (repairs.empty()) {
            return;
        }

        vecsim_stl::vector<std::shared_ptr<AsyncDiskJob>> new_jobs(this->allocator);

        {
            std::lock_guard<std::mutex> lock(pending_repairs_guard);
            for (const auto& repair : repairs) {
                auto it = pending_repairs.find(repair);

                if (it != pending_repairs.end()) {
                    // Repair job already exists, append pending job if provided
                    if (pend_on) {
                        it->second->pending_jobs.push_back(pend_on);
                    }
                } else {
                    // Create new repair job
                    auto job_ptr = createRepairJob(repair.id, repair.level);

                    if (pend_on) {
                        job_ptr->pending_jobs.push_back(pend_on);
                    }

                    pending_repairs[repair] = job_ptr;
                    new_jobs.push_back(job_ptr);
                }
            }
        }

        // Submit all new jobs at once for efficiency
        if (!new_jobs.empty()) {
            submitDiskJobs(new_jobs);
        }
    }

    std::shared_ptr<AsyncDiskJob> pendDeleteInitJobByCurrentlyRunning(idType deletedId) {
        auto job = createDeleteInitJob(deletedId);

        std::lock_guard<std::mutex> lock(running_guard);
        for (AsyncDiskJob* running : currently_running) {
            running->pending_jobs.push_back(job);
        }
        return job;
    }

    void addToCurrentlyRunning(AsyncDiskJob* job) const {
        std::lock_guard<std::mutex> lock(running_guard);
        currently_running.push_back(job);
    }

    void removeFromCurrentlyRunning(AsyncDiskJob* job) const {
        std::lock_guard<std::mutex> lock(running_guard);
        auto it = std::find(currently_running.begin(), currently_running.end(), job);
        assert(it != currently_running.end()); // Job must be in currently_running
        *it = currently_running.back();
        currently_running.pop_back();
    }

public:
    TieredHNSWDiskIndex(HNSWDiskIndex<DataType, DistType>* hnsw_index,
                        BruteForceIndex<DataType, DistType>* brute_force_index, const TieredIndexParams& tieredParams,
                        std::shared_ptr<VecSimAllocator> allocator);

    ~TieredHNSWDiskIndex() {
        // Mark index as destroyed to prevent any pending auto-submit deleters from trying to submit.
        // These are pending on jobs in the `pending_repairs` and `submitted_jobs` containers.
        is_destroyed.store(true);

        // Clear all pending repairs - this will release references to jobs
        {
            std::lock_guard<std::mutex> lock(pending_repairs_guard);
            pending_repairs.clear();
        }

        // Clear submitted jobs
        {
            std::lock_guard<std::mutex> lock(submitted_jobs_guard);
            submitted_jobs.clear();
        }

        // If throttle was enabled (buffer is/was full), disable it to balance the global counter.
        // No lock needed: destructor runs single-threaded after all references are released,
        // and is_destroyed prevents any concurrent addVector/executeInsertJob operations.
        //
        // Use >= for defensive safety: size > limit should never happen because:
        // 1. addVector() runs on the main thread only and adds one vector at a time
        // 2. We check == limit immediately after adding, so we can't exceed it
        // 3. Workers only remove vectors from flat buffer (decreasing size)
        // If size > limit somehow occurs, it indicates a bug - but we still need to disable.
        size_t flat_size = this->frontendIndex->indexSize();
        if (flat_size >= this->flatBufferLimit) {
            if (flat_size > this->flatBufferLimit) {
                // This should never happen given the constraints above - log for debugging
                TIERED_LOG(VecSimCommonStrings::LOG_WARNING_STRING,
                           "~TieredHNSWDiskIndex: Flat buffer size (%zu) exceeds limit (%zu) - "
                           "unexpected state, disabling throttle anyway",
                           flat_size, this->flatBufferLimit);
            }
            VecSimDisk_InvokeDisableThrottle();
        }

        // Backend and frontend indices are freed by the base class destructor
        // via VecSimIndex_Free(), which properly saves the allocator before
        // calling delete (avoiding the use-after-free in operator delete).
    }

    // VecSimIndexInterface
    int addVector(const void* blob, labelType label) override;
    int deleteVector(labelType label) override;
    double getDistanceFrom_Unsafe(labelType label, const void* blob) const override;
    size_t indexSize() const override {
        // Total vectors = flat buffer + HNSW backend.
        // We only need flatIndexGuard because:
        // 1. frontendIndex->indexSize() requires synchronization (vectors can be added/moved)
        // 2. backendIndex->indexSize() is thread-safe (atomic curElementCount_)
        std::shared_lock<std::shared_mutex> lock(this->flatIndexGuard);
        return this->frontendIndex->indexSize() + this->backendIndex->indexSize();
    }
    size_t indexCapacity() const override { /* TBD */ return 0; }
    VecSimIndexBasicInfo basicInfo() const override;
    VecSimBatchIterator* newBatchIterator(const void*, VecSimQueryParams*) const override;
    void setLastSearchMode(VecSearchMode mode) override { return this->backendIndex->setLastSearchMode(mode); }
    void runGC() override;
    void acquireSharedLocks() override { /* TBD */ }
    void releaseSharedLocks() override { /* TBD */ }

    // Override topKQuery to use query markers instead of mainIndexGuard
    VecSimQueryReply* topKQuery(const void* queryBlob, size_t k, VecSimQueryParams* queryParams) const override;

    // VecSimTieredIndex interface
    size_t getNumMarkedDeleted() const override { return get_hnsw_index()->getNumMarkedDeleted(); }

#ifdef VECSIM_DISK_BUILD_TESTS
    HNSWDiskIndex<DataType, DistType>* getBackendIndex() {
        return static_cast<HNSWDiskIndex<DataType, DistType>*>(this->backendIndex);
    }

    // Test wrapper for VecSimTieredIndex::frontendIndex (protected in VectorSimilarity)
    // Returns the flat buffer (BruteForce) frontend index
    BruteForceIndex<DataType, DistType>* getFlatBufferIndex() { return this->frontendIndex; }

    // Test helper: manually execute just the executeInsertJob logic for a job
    // This allows fine-grained control over job execution timing in tests
    void testExecuteInsertJob(AsyncDiskJob* job) { executeInsertJob(job); }

    // Test helper: get count of currently running jobs (including query markers)
    size_t getCurrentlyRunningCount() const {
        std::lock_guard<std::mutex> lock(running_guard);
        return currently_running.size();
    }

    // Test helper: get reference to flatIndexGuard for testing concurrent behavior
    // Used to block topKQuery at specific points to observe intermediate state
    std::shared_mutex& getFlatIndexGuard() { return this->flatIndexGuard; }
#endif
};

/******************** Index API ****************************************/
template <typename DataType, typename DistType>
TieredHNSWDiskIndex<DataType, DistType>::TieredHNSWDiskIndex(HNSWDiskIndex<DataType, DistType>* hnsw_index,
                                                             BruteForceIndex<DataType, DistType>* brute_force_index,
                                                             const TieredIndexParams& tieredParams,
                                                             std::shared_ptr<VecSimAllocator> allocator)
    : VecSimTieredIndex<DataType, DistType>(hnsw_index, brute_force_index, tieredParams, allocator),
      pending_repairs(allocator), currently_running(allocator), submitted_jobs(allocator) {
    assert(this->SubmitJobsToQueue != nullptr && "TieredHNSWDiskIndex requires a job queue callback");
}

template <typename DataType, typename DistType>
int TieredHNSWDiskIndex<DataType, DistType>::addVector(const void* data, labelType label) {
    auto* hnsw_index = get_hnsw_index();

    assert(!hnsw_index->isMultiValue() && "Multi-value HNSW is not supported for MVP1");

    // Step 1: Lock flat buffer and verify no duplicate labels
    // ASSUMPTION: Caller guarantees no duplicate labels. We assert this for safety.
    {
        std::lock_guard<std::shared_mutex> flat_lock(this->flatIndexGuard);

        assert(!this->frontendIndex->isLabelExists(label) && !hnsw_index->isLabelExists(label) &&
               "Duplicate label - caller must ensure uniqueness across both indices");

        this->frontendIndex->addVector(data, label);

        // Enable throttling if flat buffer is full
        size_t flat_size = this->frontendIndex->indexSize();
        if (flat_size == this->flatBufferLimit) {
            TIERED_LOG(VecSimCommonStrings::LOG_DEBUG_STRING,
                       "Flat buffer is full (size=%zu, limit=%zu), continuing to add vector", flat_size,
                       this->flatBufferLimit);
            VecSimDisk_InvokeEnableThrottle();
        }
    }

    // Step 2: Create InsertDiskJob and submit it
    // NOTE: No state is captured on the main thread.
    // All state capture (random level, entry point) happens in the worker thread
    // inside executeInsertJob().
    auto job = createInsertJob(label);

    // Step 3: Submit job to queue
    submitDiskJob(job);

    return 1; // Always a new vector (no overwrite possible per contract)
}

template <typename DataType, typename DistType>
int TieredHNSWDiskIndex<DataType, DistType>::deleteVector(labelType label) {
    {
        std::unique_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);
        if (this->frontendIndex->isLabelExists(label)) {
            this->frontendIndex->deleteVector(label);
            // Disable throttling if flat buffer has space
            size_t flat_size = this->frontendIndex->indexSize();
            if (flat_size == this->flatBufferLimit - 1) {
                TIERED_LOG(VecSimCommonStrings::LOG_DEBUG_STRING,
                           "deleteVector: Flat buffer has space (size=%zu, limit=%zu), disabling throttle", flat_size,
                           this->flatBufferLimit);
                VecSimDisk_InvokeDisableThrottle();
            }
            return 1;
        }
    }

    return deleteLabelFromHNSW(label);
}

template <typename DataType, typename DistType>
double TieredHNSWDiskIndex<DataType, DistType>::getDistanceFrom_Unsafe(labelType label, const void* blob) const {
    // TBD
    return 0.0;
}

template <typename DataType, typename DistType>
VecSimIndexBasicInfo TieredHNSWDiskIndex<DataType, DistType>::basicInfo() const {
    VecSimIndexBasicInfo info = this->backendIndex->getBasicInfo();
    info.isTiered = true;
    info.algo = VecSimAlgo_HNSWLIB;
    return info;
}

template <typename DataType, typename DistType>
VecSimBatchIterator* TieredHNSWDiskIndex<DataType, DistType>::newBatchIterator(const void* queryBlob,
                                                                               VecSimQueryParams* queryParams) const {
    // TBD
    return nullptr;
}

template <typename DataType, typename DistType>
void TieredHNSWDiskIndex<DataType, DistType>::runGC() {
    // TBD
}

template <typename DataType, typename DistType>
VecSimQueryReply* TieredHNSWDiskIndex<DataType, DistType>::topKQueryImp(const void* queryBlob, size_t k,
                                                                        VecSimQueryParams* queryParams) const {
    // Check flat buffer (scoped lock for RAII)
    VecSimQueryReply* flat_results = nullptr;
    {
        std::shared_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);
        if (this->frontendIndex->indexSize() > 0) {
            flat_results = this->frontendIndex->topKQuery(queryBlob, k, queryParams);
        }
    } // Lock released here via RAII

    // Early exit: check for query error
    if (flat_results && flat_results->code != VecSim_QueryReply_OK) {
        return flat_results;
    }

    // Query backend
    auto processed_query_ptr = this->frontendIndex->preprocessQuery(queryBlob);
    const void* processed_query = processed_query_ptr.get();

    auto main_results = this->backendIndex->topKQuery(processed_query, k, queryParams);

    if (!flat_results) {
        return main_results;
    }

    if (main_results->code != VecSim_QueryReply_OK) {
        VecSimQueryReply_Free(flat_results);
        return main_results;
    }

    // Merge results (multi-value not supported yet)
    assert(!this->backendIndex->isMultiValue() && "Multi-value indexes not supported yet");
    // We use withSet=true because after querying the flat buffer and releasing flatIndexGuard, a concurrent
    // executeInsertJob can move a vector from flat to backend before we query the backend.
    // The flat buffer stores vectors in FP32, while the backend uses SQ8
    // quantization, meaning the same vector may produce different scores due to quantization precision.
    // Without deduplication by ID, the same label appears twice with different scores, and will appear twice in the
    // final results.
    return merge_result_lists<true>(main_results, flat_results, k);
}

template <typename DataType, typename DistType>
VecSimQueryReply* TieredHNSWDiskIndex<DataType, DistType>::topKQuery(const void* queryBlob, size_t k,
                                                                     VecSimQueryParams* queryParams) const {

    // Create query marker job to track this query and prevent ID reuse
    QueryMarkerJob marker_job(this->allocator);

    // Register query as currently running
    addToCurrentlyRunning(&marker_job);

    VecSimQueryReply* results = topKQueryImp(queryBlob, k, queryParams);

    removeFromCurrentlyRunning(&marker_job);

    return results;
}
