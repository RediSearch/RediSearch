/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
#include <stddef.h>
#include "vec_sim_common.h"

/*
 * Defines a simple tombstone API for indexes.
 * Every index that has to implement "marking as deleted" mechanism should inherit this API and
 * implement the required functions. The implementation should also update the `numMarkedDeleted`
 * property to hold the number of vectors marked as deleted.
 */
struct VecSimIndexTombstone {
protected:
    size_t numMarkedDeleted;

public:
    VecSimIndexTombstone() : numMarkedDeleted(0) {}
    ~VecSimIndexTombstone() = default;

    inline size_t getNumMarkedDeleted() const { return numMarkedDeleted; }

    /**
     * @param label vector to mark as deleted
     * @return a vector of internal ids that has been marked as deleted (to be disposed later on).
     */
    virtual inline vecsim_stl::vector<idType> markDelete(labelType label) = 0;
};
