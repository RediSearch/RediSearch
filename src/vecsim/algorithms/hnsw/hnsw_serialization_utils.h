/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <cstddef>

#define HNSW_INVALID_META_DATA SIZE_MAX

typedef struct {
    bool valid_state;
    long memory_usage; // in bytes
    size_t double_connections;
    size_t unidirectional_connections;
    size_t min_in_degree;
    size_t max_in_degree;
    size_t connections_to_repair;
} HNSWIndexMetaData;
