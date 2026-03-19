/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_FIELD_PARSE_H
#define SPEC_FIELD_PARSE_H

#include "redismodule.h"
#include "query_error.h"
#include "field_spec.h"
#include "util/references.h"
#include "VecSim/vec_sim_common.h"

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;

// Initialize a field spec with the given index
void initializeFieldSpec(FieldSpec *fs, t_fieldIndex index);

// Parse a single field definition from the argument cursor
int parseFieldSpec(ArgsCursor *ac, struct IndexSpec *sp, StrongRef sp_ref, FieldSpec *fs, QueryError *status);

// Add fields to an existing (or newly created) index. isNew indicates FT.CREATE vs FT.ALTER.
int IndexSpec_AddFieldsInternal(struct IndexSpec *sp, StrongRef spec_ref, ArgsCursor *ac,
                                QueryError *status, int isNew);

// Add fields via FT.ALTER (wraps AddFieldsInternal + optional re-scan)
int IndexSpec_AddFields(StrongRef spec_ref, struct IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac,
                        bool initialScan, QueryError *status);

// Validate vector similarity params (also called from vector_index.c during RDB load)
int VecSimIndex_validate_params(RedisModuleCtx *ctx, VecSimParams *params, QueryError *status);

#ifdef __cplusplus
}
#endif

#endif  // SPEC_FIELD_PARSE_H
