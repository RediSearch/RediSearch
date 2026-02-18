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
    DISK_HNSW_INSERT_VECTOR_JOB = HNSW_INSERT_VECTOR_JOB,
    DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB = HNSW_REPAIR_NODE_CONNECTIONS_JOB,
    DISK_HNSW_DELETE_VECTOR_INIT_JOB,
    DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB,
};

struct AsyncDiskJob : public AsyncJob {
    DiskJobType type;

    AsyncDiskJob(std::shared_ptr<VecSimAllocator> allocator, DiskJobType type_, JobCallback callback,
                 VecSimIndex* index_)
        // TODO: Add a generic disk type and pass it here to the base class
        : AsyncJob(allocator, HNSW_INSERT_VECTOR_JOB, callback, index_), type(type_), pending_jobs(allocator) {}

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

template <typename DataType, typename DistType>
class TieredHNSWDiskIndex : public VecSimTieredIndex<DataType, DistType> {
private:
    // Job tracking for pending repair jobs
    std::mutex pending_repairs_guard;
    vecsim_stl::unordered_map<GraphNodeType, std::shared_ptr<AsyncDiskJob>> pending_repairs;

    // Job tracking for currently running jobs (non-owning raw pointers)
    std::mutex running_guard;
    vecsim_stl::vector<AsyncDiskJob*> currently_running;

    // Jobs that have been submitted but not yet started - keeps them alive
    // Map from raw pointer to owning shared_ptr for efficient lookup
    std::mutex submitted_jobs_guard;
    vecsim_stl::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs;

    // Flag to indicate if the index is being destroyed
    std::atomic<bool> is_destroyed{false};

public:
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
        JobType* job = new (this->allocator) JobType(std::forward<Args>(args)...);
        return std::shared_ptr<AsyncDiskJob>(job, PendingJobDeleter{this, &is_destroyed});
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
    void deleteLabelFromHNSW(labelType label) {
        auto* hnsw_index = get_hnsw_index();

        // Mark the label as deleted in the HNSW index, and get the associated internal IDs
        vecsim_stl::vector<idType> deleted_ids = hnsw_index->markDelete(label);

        // Create the init delete job
        for (idType internal_id : deleted_ids) {
            // Create job with auto-submit deleter, pended by all currently running jobs
            // If no running jobs, it will be submitted immediately
            pendDeleteInitJobByCurrentlyRunning(internal_id);
        }
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

#ifdef BUILD_TESTS
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

        // Get the HNSW disk index
        auto* hnsw_index = get_hnsw_index();
        auto* storage = hnsw_index->getStorage();

        // TODO (MOD-13797): Replace entry point if deleted_id is the current entry point

        // Get the maximum level for this node
        levelType max_level = 0; // TODO: Get actual max level for this node from storage

        // Collect all incoming neighbors at all levels
        GraphNodeList all_neighbors(this->allocator);
        for (levelType level = 0; level <= max_level; level++) {
            vecsim_stl::vector<idType> incoming_neighbors(this->allocator);
            storage->get_incoming_edges(deleted_id, level, incoming_neighbors);
            for (idType neighbor_id : incoming_neighbors) {
                all_neighbors.emplace_back(neighbor_id, level);
            }
        }

        // Create the finalize job with auto-submit deleter
        // It will be automatically submitted when all repair jobs complete
        std::shared_ptr<AsyncDiskJob> finalize_job = createAutoSubmitJob<DeleteDiskJob>(
            this->allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, deleted_id, executeDiskJobWrapper, this);

        // Submit repair jobs for each neighbor-level pair, linking them to the finalize job
        submitRepairs(all_neighbors, finalize_job);
    }

    void executeDeleteFinalizeJob(DeleteDiskJob* delete_job) {
        idType deleted_id = delete_job->deleted_id;

        // TODO: Implement final cleanup/swap logic
        // - Delete all disk storage keys related to deleted_id
        // - Update any internal data structures
        // - Add the deleted_id to the free list for future reuse
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

        {
            std::lock_guard<std::mutex> lock(index->running_guard);
            index->currently_running.push_back(disk_job);
        }

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
            }
        }

        {
            std::lock_guard<std::mutex> lock(index->running_guard);
            // Remove this job from currently_running (swap with last and pop)
            auto it = std::find(index->currently_running.begin(), index->currently_running.end(), disk_job);
            assert(it != index->currently_running.end()); // Job must be in currently_running
            *it = index->currently_running.back();
            index->currently_running.pop_back();
        }

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
                    // Create new repair job with the wrapper callback
                    auto job_ptr = make_vecsim_shared_ptr<RepairDiskJob>(this->allocator, repair.id, repair.level,
                                                                         executeDiskJobWrapper, this);

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

    // Create a delete init job with auto-submit deleter and pend it by all currently running jobs.
    // If there are no currently running jobs, the job will be submitted when its ref count reaches 0.
    // Returns the shared_ptr to the created job.
    std::shared_ptr<AsyncDiskJob> pendDeleteInitJobByCurrentlyRunning(idType deletedId) {
        auto job = createAutoSubmitJob<DeleteDiskJob>(this->allocator, DISK_HNSW_DELETE_VECTOR_INIT_JOB, deletedId,
                                                      executeDiskJobWrapper, this);

        std::lock_guard<std::mutex> lock(running_guard);
        for (AsyncDiskJob* running : currently_running) {
            running->pending_jobs.push_back(job);
        }
        return job;
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

    // VecSimTieredIndex interface
    size_t getNumMarkedDeleted() const override { return get_hnsw_index()->getNumMarkedDeleted(); }

#ifdef BUILD_TESTS
    HNSWDiskIndex<DataType, DistType>* getBackendIndex() {
        return static_cast<HNSWDiskIndex<DataType, DistType>*>(this->backendIndex);
    }

    // Required by VecSimIndexInterface when BUILD_TESTS is defined
    size_t indexMetaDataCapacity() const override {
        return this->backendIndex->indexMetaDataCapacity() + this->frontendIndex->indexMetaDataCapacity();
    }

    // Test hook: callback invoked after validity check but BEFORE indexVector
    // Used to reproduce race condition in tests
    std::function<void()> testHookBeforeIndexVector;

    // Test helper: manually execute just the executeInsertJob logic for a job
    // This allows fine-grained control over job execution timing in tests
    void testExecuteInsertJob(AsyncDiskJob* job) { executeInsertJob(job); }
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

    // Check if flat buffer is full - for now just log warning (throttling TBD)
    if (this->frontendIndex->indexSize() >= this->flatBufferLimit) {
        TIERED_LOG(VecSimCommonStrings::LOG_WARNING_STRING,
                   "Flat buffer is full (size=%zu, limit=%zu), continuing to add vector",
                   this->frontendIndex->indexSize(), this->flatBufferLimit);
    }

    assert(!hnsw_index->isMultiValue() && "Multi-value HNSW is not supported for MVP1");

    // Step 1: Lock flat buffer and verify no duplicate labels
    // ASSUMPTION: Caller guarantees no duplicate labels. We assert this for safety.
    std::unique_lock<std::shared_mutex> flat_lock(this->flatIndexGuard);

    assert(!this->frontendIndex->isLabelExists(label) && !hnsw_index->isLabelExists(label) &&
           "Duplicate label - caller must ensure uniqueness across both indices");

    this->frontendIndex->addVector(data, label);
    flat_lock.unlock();

    // Step 2: Create InsertDiskJob and submit it
    // NOTE: No state is captured on the main thread.
    // All state capture (random level, entry point) happens in the worker thread
    // inside executeInsertJob().
    auto job = make_vecsim_shared_ptr<InsertDiskJob>(this->allocator, label, executeDiskJobWrapper, this);

    // Step 3: Submit job to queue
    submitDiskJob(job);

    return 1; // Always a new vector (no overwrite possible per contract)
}

template <typename DataType, typename DistType>
int TieredHNSWDiskIndex<DataType, DistType>::deleteVector(labelType label) {
    // TODO(MOD-13172): Implement delete flow using DISK_IN_PROCESS flag.
    //
    // The new flow uses DISK_IN_PROCESS flag for coordination with in-flight inserts:
    // - Insert jobs call allocateAndRegister() atomically under flat lock, which marks IN_PROCESS
    // - Delete can find labels in HNSW even while insert is still connecting to graph
    // - Elements marked IN_PROCESS can be marked deleted (delete wins)
    //
    // IMPLEMENTATION:
    // 1. Acquire consistency_guard (shared) - for fork safety
    // 2. Acquire flatIndexGuard (exclusive)
    // 3. Check where the label exists:
    //
    //    CASE A: Label in flat buffer
    //      - Remove from flat buffer (insert job will fail validation when it runs)
    //      - Return 1
    //
    //    CASE B: Label in HNSW index (may be IN_PROCESS or fully indexed)
    //      - Mark deleted in HNSW (markDeleted)
    //      - Submit cleanup jobs if needed
    //      - Return 1
    //
    //    CASE C: Label doesn't exist anywhere
    //      - Return 0
    //
    // LOCK ORDER: consistency_guard -> flatIndexGuard
    return 0;
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
