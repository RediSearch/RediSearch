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
#include <stdint.h>

#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ForwardIndex;
struct RedisSearchCtx;
typedef struct RSFulltextIndexedTerms RSFulltextIndexedTerms;

void RSFulltextIndexedTerms_Free(RSFulltextIndexedTerms *terms);
RSFulltextIndexedTerms *RSFulltextIndexedTerms_CreateFromForwardIndex(struct ForwardIndex *fw);
void RSFulltextIndexedTerms_DecrementLive(struct RedisSearchCtx *sctx, RSFulltextIndexedTerms *terms);
void DMD_SetFulltextIndexedTerms(RSDocumentMetadata *dmd, RSFulltextIndexedTerms *terms);

#ifdef __cplusplus
}
#endif
