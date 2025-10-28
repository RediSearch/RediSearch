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

typedef struct AREQ AREQ;

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
  RLookup *tailLookup;                    // Unified destination lookup
} HybridLookupContext;

/**
 * Initialize unified lookup schema and hybrid lookup context for field merging.
 *
 * @param requests Array of AREQ pointers containing source lookups (non-null)
 * @param tailLookup The destination lookup to populate with unified schema (non-null)
 * @return HybridLookupContext* to an initialized HybridLookupContext
 */
HybridLookupContext* InitializeHybridLookupContext(arrayof(AREQ*) requests, RLookup *tailLookup);

#ifdef __cplusplus
}
#endif

#endif // __HYBRID_LOOKUP_CONTEXT_H__
