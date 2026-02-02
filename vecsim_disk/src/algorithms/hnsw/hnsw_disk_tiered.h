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
#include "algorithms/hnsw/hnsw_disk.h"
#include "storage/hnsw_storage.h"
#include <algorithm>
#include <atomic>
#include <memory>
#include <utility>

enum DiskJobType {
    DISK_HNSW_INSERT_VECTOR_JOB = HNSW_INSERT_VECTOR_JOB,
    DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB = HNSW_REPAIR_NODE_CONNECTIONS_JOB,
    DISK_HNSW_DELETE_VECTOR_INIT_JOB,
    DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB,
};

// Hash function for std::pair<idType, levelType> to use in unordered_map
namespace std {
template <>
struct hash<std::pair<idType, levelType>> {
    size_t operator()(const std::pair<idType, levelType>& p) const {
        // Combine hashes of the two elements
        static_assert(sizeof(size_t) >= sizeof(idType) + sizeof(levelType));
        size_t combined = p.first + (size_t(p.second) << (sizeof(idType) * 8));
        return std::hash<size_t>()(combined);
    }
};
} // namespace std

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

template <typename DataType, typename DistType>
class TieredHNSWDiskIndex : public VecSimTieredIndex<DataType, DistType> {
private:
    // Job tracking for pending repair jobs
    std::mutex pending_repairs_guard;
    vecsim_stl::unordered_map<std::pair<idType, levelType>, std::shared_ptr<AsyncDiskJob>> pending_repairs;

    // Job tracking for currently running jobs (non-owning raw pointers)
    std::mutex running_guard;
    vecsim_stl::vector<AsyncDiskJob*> currently_running;

    // Jobs that have been submitted but not yet started - keeps them alive
    // Map from raw pointer to owning shared_ptr for efficient lookup
    std::mutex submitted_jobs_guard;
    vecsim_stl::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs;

    // Flag to indicate if the index is being destroyed
    std::atomic<bool> is_destroyed{false};

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
                // Create a new shared_ptr with default deleter
                // AsyncDiskJob inherits from VecsimBaseObject which has proper operator delete
                auto job_ptr = std::shared_ptr<AsyncDiskJob>(job);
                index->submitDiskJob(job_ptr);
            } else if (job) {
                // Index is destroyed, just delete the job
                delete job;
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
            pendByCurrentlyRunning<DeleteDiskJob>(this->allocator, DISK_HNSW_DELETE_VECTOR_INIT_JOB, internal_id,
                                                  executeDiskJobWrapper, this);
        }
    }

    // Job execution methods (stubs to be implemented with HNSW algorithm logic)
    void executeInsertJob(AsyncDiskJob* job) {
        // TODO: Implement insert vector logic
        // 1. Insert vector into HNSW disk index
        // 2. submit repair jobs for affected nodes if necessary
        // 3. remove related data from the flat index
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

        // Get the maximum level for this node
        levelType max_level = 0; // TODO: Get actual max level for this node from storage

        // Collect all incoming neighbors at all levels
        vecsim_stl::vector<std::pair<idType, levelType>> all_neighbors(this->allocator);
        for (levelType level = 0; level <= max_level; level++) {
            vecsim_stl::vector<idType> incoming_neighbors(this->allocator);
            if (storage->get_incoming_edges(deleted_id, level, incoming_neighbors)) {
                for (idType neighbor_id : incoming_neighbors) {
                    all_neighbors.push_back(std::make_pair(neighbor_id, level));
                }
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

        // Remove repair job from map before starting (releases our shared_ptr reference)
        if (disk_job->type == DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB) {
            RepairDiskJob* repair_job = static_cast<RepairDiskJob*>(disk_job);
            std::lock_guard<std::mutex> lock(index->pending_repairs_guard);
            index->pending_repairs.erase(std::make_pair(repair_job->node_id, repair_job->level));
        }

        // Remove from submitted_jobs (we now own a reference on the stack if it was there)
        std::shared_ptr<AsyncDiskJob> job_owner;
        {
            std::lock_guard<std::mutex> lock(index->submitted_jobs_guard);
            auto it = index->submitted_jobs.find(disk_job);
            assert(it != index->submitted_jobs.end());
            job_owner = std::move(it->second); // Take ownership
            index->submitted_jobs.erase(it);
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
    void submitRepairs(const vecsim_stl::vector<std::pair<idType, levelType>>& repairs,
                       std::shared_ptr<AsyncDiskJob> pend_on = nullptr) {
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
                    // Use default deleter - VecsimBaseObject has proper operator delete
                    RepairDiskJob* job = new (this->allocator)
                        RepairDiskJob(this->allocator, repair.first, repair.second, executeDiskJobWrapper, this);
                    std::shared_ptr<AsyncDiskJob> job_ptr(job);

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

    // Create a job with auto-submit deleter and pend it by all currently running jobs
    // If there are no currently running jobs, submits the job immediately
    // Returns the shared_ptr to the created job
    template <typename JobType, typename... Args>
    std::shared_ptr<AsyncDiskJob> pendByCurrentlyRunning(Args&&... args) {
        auto job = createAutoSubmitJob<JobType>(std::forward<Args>(args)...);

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
    }

    // VecSimIndexInterface
    int addVector(const void* blob, labelType label) override;
    int deleteVector(labelType label) override;
    double getDistanceFrom_Unsafe(labelType label, const void* blob) const override;
    size_t indexSize() const override { /* TBD */ return 0; }
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
#endif
};

/******************** Index API ****************************************/
template <typename DataType, typename DistType>
TieredHNSWDiskIndex<DataType, DistType>::TieredHNSWDiskIndex(HNSWDiskIndex<DataType, DistType>* hnsw_index,
                                                             BruteForceIndex<DataType, DistType>* brute_force_index,
                                                             const TieredIndexParams& tieredParams,
                                                             std::shared_ptr<VecSimAllocator> allocator)
    : VecSimTieredIndex<DataType, DistType>(hnsw_index, brute_force_index, tieredParams, allocator),
      pending_repairs(allocator), currently_running(allocator), submitted_jobs(allocator) {}

template <typename DataType, typename DistType>
int TieredHNSWDiskIndex<DataType, DistType>::addVector(const void* data, labelType label) {
    // TBD
    return 0;
}

template <typename DataType, typename DistType>
int TieredHNSWDiskIndex<DataType, DistType>::deleteVector(labelType label) {
    // TBD
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
