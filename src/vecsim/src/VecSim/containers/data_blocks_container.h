/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "data_block.h"
#include "raw_data_container_interface.h"
#include "VecSim/memory/vecsim_malloc.h"
#include "VecSim/utils/serializer.h"
#include "VecSim/utils/vecsim_stl.h"

class DataBlocksContainer : public VecsimBaseObject, public RawDataContainer {

    size_t element_bytes_count;           // Element size in bytes
    size_t element_count;                 // Number of items in the container
    vecsim_stl::vector<DataBlock> blocks; // data blocks
    size_t block_size;                    // number of element in block
    unsigned char alignment;              // alignment for data allocation in each block

public:
    DataBlocksContainer(size_t blockSize, size_t elementBytesCount,
                        std::shared_ptr<VecSimAllocator> allocator, unsigned char alignment = 0);
    ~DataBlocksContainer();

    // Number of elements in the container.
    size_t size() const override;

    // Number of blocks allocated.
    size_t capacity() const;

    size_t blockSize() const;

    size_t elementByteCount() const;

    Status addElement(const void *element, size_t id) override;

    // Inlined so the hot search path (via getDataByInternalId) can fold the indexing arithmetic.
    const char *getElement(size_t id) const override {
        assert(id < element_count);
        return blocks[id / block_size].getElement(id % block_size);
    }

    Status removeElement(size_t id) override;

    Status updateElement(size_t id, const void *element) override;

    std::unique_ptr<RawDataContainer::Iterator> getIterator() const override;

    size_t numBlocks() const;
#ifdef BUILD_TESTS
    void saveVectorsData(std::ostream &output) const override;
    // Use that in deserialization when file was created with old version (v3) that serialized
    // the blocks themselves and not just thw raw vector data.
    void restoreBlocks(std::istream &input, size_t num_vectors,
                       Serializer::EncodingVersion version);
    void shrinkToFit();
#endif

    class Iterator : public RawDataContainer::Iterator {
        size_t cur_id;
        const char *cur_element;
        const DataBlocksContainer &container;

    public:
        explicit Iterator(const DataBlocksContainer &container);
        ~Iterator() override = default;

        bool hasNext() const override;
        const char *next() override;
        void reset() override;
    };
};
