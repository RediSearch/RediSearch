/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef __HYBRID_LOOKUP_CONTEXT_H__
#define __HYBRID_LOOKUP_CONTEXT_H__

#include "rlookup.h"
#include "util/arr/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * HybridLookupContext structure that provides RLookup context for field merging.
 * Contains source lookups from each upstream and the unified destination lookup.
 *
 * This structure is used to facilitate proper field mapping and data writing
 * between different search result sources (search index vs vector index) in
 * hybrid search operations.
 */
typedef struct {
  arrayof(const RLookup*) sourceLookups;  // Source lookups from each request
  RLookup *tailLookup;              // Unified destination lookup
} HybridLookupContext;

#ifdef __cplusplus
}
#endif

#endif // __HYBRID_LOOKUP_CONTEXT_H__
