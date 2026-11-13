/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "data_block.h"
#include "VecSim/memory/vecsim_malloc.h"
#include <cstring>

DataBlock::DataBlock(size_t blockSize, size_t elementBytesCount,
                     std::shared_ptr<VecSimAllocator> allocator, unsigned char alignment)
    : VecsimBaseObject(allocator), element_bytes_count(elementBytesCount), length(0),
      data((char *)this->allocator->allocate_aligned(blockSize * elementBytesCount, alignment)) {}

DataBlock::DataBlock(DataBlock &&other) noexcept
    : VecsimBaseObject(other.allocator), element_bytes_count(other.element_bytes_count),
      length(other.length), data(other.data) {
    other.data = nullptr; // take ownership of the data
}

DataBlock::~DataBlock() noexcept { this->allocator->free_allocation(data); }

void DataBlock::addElement(const void *element) {

    // Copy element data and update block size.
    memcpy(this->data + (this->length * element_bytes_count), element, element_bytes_count);
    this->length++;
}

void DataBlock::updateElement(size_t index, const void *new_element) {
    char *destinaion = (char *)getElement(index);
    memcpy(destinaion, new_element, element_bytes_count);
}
