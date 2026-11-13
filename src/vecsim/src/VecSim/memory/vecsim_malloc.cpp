/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "vecsim_malloc.h"
#include <stdlib.h>
#include <memory>
#include <string.h>
#include <sys/param.h>

std::shared_ptr<VecSimAllocator> VecSimAllocator::newVecsimAllocator() {
    std::shared_ptr<VecSimAllocator> allocator(new VecSimAllocator());
    return allocator;
}

struct VecSimAllocationHeader {
    std::size_t allocation_size : 63;
    std::size_t is_aligned : 1;
};

size_t VecSimAllocator::allocation_header_size = sizeof(VecSimAllocationHeader);

VecSimMemoryFunctions VecSimAllocator::memFunctions = {.allocFunction = malloc,
                                                       .callocFunction = calloc,
                                                       .reallocFunction = realloc,
                                                       .freeFunction = free};

void VecSimAllocator::setMemoryFunctions(VecSimMemoryFunctions memFunctions) {
    VecSimAllocator::memFunctions = memFunctions;
}

void *VecSimAllocator::allocate(size_t size) {
    auto ptr = static_cast<VecSimAllocationHeader *>(vecsim_malloc(size + allocation_header_size));
    if (ptr) {
        this->allocated += size + allocation_header_size;
        *ptr = {size, false};
        return ptr + 1;
    }
    return nullptr;
}

void *VecSimAllocator::allocate_aligned(size_t size, unsigned char alignment) {
    if (!alignment) {
        return allocate(size);
    }

    size += alignment; // Add enough space for alignment.
    auto ptr = static_cast<unsigned char *>(vecsim_malloc(size + allocation_header_size));
    if (ptr) {
        this->allocated += size + allocation_header_size;
        size_t remainder = (((uintptr_t)ptr) + allocation_header_size) % alignment;
        unsigned char offset = alignment - remainder;
        // Store the allocation header in the 8 bytes before the returned pointer.
        new (ptr + offset) VecSimAllocationHeader{size, true};
        // Store the offset in the byte right before the header.
        ptr[offset - 1] = offset;
        // Return the aligned pointer.
        return ptr + allocation_header_size + offset;
    }
    return nullptr;
}

void VecSimAllocator::deallocate(void *p, size_t size) { free_allocation(p); }

void *VecSimAllocator::reallocate(void *p, size_t size) {
    if (!p) {
        return this->allocate(size);
    }
    size_t oldSize = getPointerAllocationSize(p);
    void *new_ptr = this->allocate(size);
    if (new_ptr) {
        memcpy(new_ptr, p, MIN(oldSize, size));
        free_allocation(p);
        return new_ptr;
    }
    return nullptr;
}

void VecSimAllocator::free_allocation(void *p) {
    if (!p)
        return;

    auto hdr = ((VecSimAllocationHeader *)p) - 1;
    unsigned char offset = hdr->is_aligned ? ((unsigned char *)hdr)[-1] : 0;

    this->allocated -= (hdr->allocation_size + allocation_header_size);
    vecsim_free((char *)p - offset - allocation_header_size);
}

void *VecSimAllocator::callocate(size_t size) {
    size_t *ptr = (size_t *)vecsim_calloc(1, size + allocation_header_size);

    if (ptr) {
        this->allocated += size + allocation_header_size;
        *ptr = size;
        return ptr + 1;
    }
    return nullptr;
}

std::unique_ptr<void, VecSimAllocator::Deleter>
VecSimAllocator::allocate_aligned_unique(size_t size, size_t alignment) {
    void *ptr = this->allocate_aligned(size, alignment);
    return {ptr, Deleter(*this)};
}

std::unique_ptr<void, VecSimAllocator::Deleter> VecSimAllocator::allocate_unique(size_t size) {
    void *ptr = this->allocate(size);
    return {ptr, Deleter(*this)};
}

void *VecSimAllocator::operator new(size_t size) { return vecsim_malloc(size); }

void *VecSimAllocator::operator new[](size_t size) { return vecsim_malloc(size); }
void VecSimAllocator::operator delete(void *p, size_t size) { vecsim_free(p); }
void VecSimAllocator::operator delete[](void *p, size_t size) { vecsim_free(p); }

uint64_t VecSimAllocator::getAllocationSize() const { return this->allocated; }
