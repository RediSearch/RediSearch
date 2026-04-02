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

#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RedisSearchCtx;
struct RSAddDocumentCtx;
struct IndexSpec;

struct RSIndexedTagField;
typedef struct RSIndexedTagField RSIndexedTagField;

void RSIndexedTagField_FreeList(RSIndexedTagField *head);
void RSIndexedTagFields_DecrementLive(struct RedisSearchCtx *sctx, RSIndexedTagField *head);
void RSIndexedTagFields_SnapshotBulk(struct RSAddDocumentCtx *aCtx, struct IndexSpec *spec);

void DMD_SetIndexedTagFields(RSDocumentMetadata *dmd, RSIndexedTagField *fields);

#ifdef __cplusplus
}
#endif
