/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "info_iterator.h"
#include "VecSim/utils/vecsim_stl.h"

struct VecSimDebugInfoIterator {
private:
    vecsim_stl::vector<VecSim_InfoField> fields;
    size_t currentIndex;

public:
    VecSimDebugInfoIterator(size_t len, const std::shared_ptr<VecSimAllocator> &alloc)
        : fields(alloc), currentIndex(0) {
        this->fields.reserve(len);
    }

    inline void addInfoField(VecSim_InfoField infoField) { this->fields.push_back(infoField); }

    inline bool hasNext() { return this->currentIndex < this->fields.size(); }

    inline VecSim_InfoField *next() { return &this->fields[this->currentIndex++]; }

    inline size_t numberOfFields() { return this->fields.size(); }

    virtual ~VecSimDebugInfoIterator() {
        for (size_t i = 0; i < this->fields.size(); i++) {
            if (this->fields[i].fieldType == INFOFIELD_ITERATOR) {
                delete this->fields[i].fieldValue.iteratorValue;
            }
        }
    }
};
