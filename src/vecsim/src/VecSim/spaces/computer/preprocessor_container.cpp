/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/computer/preprocessor_container.h"

ProcessedBlobs PreprocessorsContainerAbstract::preprocess(const void *original_blob,
                                                          size_t input_blob_size) const {
    return ProcessedBlobs(preprocessForStorage(original_blob, input_blob_size),
                          preprocessQuery(original_blob, input_blob_size));
}

MemoryUtils::unique_blob
PreprocessorsContainerAbstract::preprocessForStorage(const void *original_blob,
                                                     size_t input_blob_size) const {
    return wrapWithDummyDeleter(const_cast<void *>(original_blob));
}

MemoryUtils::unique_blob PreprocessorsContainerAbstract::preprocessQuery(const void *original_blob,
                                                                         size_t input_blob_size,
                                                                         bool force_copy) const {
    return maybeCopyToAlignedMem(original_blob, input_blob_size, force_copy);
}

void PreprocessorsContainerAbstract::preprocessStorageInPlace(void *blob,
                                                              size_t input_blob_size) const {}

MemoryUtils::unique_blob PreprocessorsContainerAbstract::maybeCopyToAlignedMem(
    const void *original_blob, size_t input_blob_size, bool force_copy) const {
    // This helper aligns query buffers; storage allocation paths use storage_alignment elsewhere.
    bool needs_copy = force_copy || (this->query_alignment &&
                                     ((uintptr_t)original_blob % this->query_alignment != 0));

    if (needs_copy) {
        auto aligned_mem =
            this->allocator->allocate_aligned(input_blob_size, this->query_alignment);
        memcpy(aligned_mem, original_blob, input_blob_size);
        return this->wrapAllocated(aligned_mem);
    }

    // Returning a unique_ptr with a no-op deleter
    return wrapWithDummyDeleter(const_cast<void *>(original_blob));
}
