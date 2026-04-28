/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#ifdef ENABLE_ASSERT

#include <stdbool.h>
#include <stddef.h>

#include "rejson_api.h"
#include "VecSim/vec_sim_common.h"

#ifdef __cplusplus
extern "C" {
#endif

// Non-static wrappers over the vector-ingestion helpers in `json.c`.
// Exposed only under `ENABLE_ASSERT` for C/C++ unit testing; not part of the
// shipped module ABI.
bool JSONTest_AcceptsJSONArrayType(VecSimType target, JSONArrayType src);
void JSONTest_ConvertFromTypedBuffer(VecSimType target_type, JSONArrayType jtype,
                                     const void *src, size_t n, char *target);

#ifdef __cplusplus
}
#endif

#endif // ENABLE_ASSERT
