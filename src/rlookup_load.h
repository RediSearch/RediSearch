/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RLOOKUP_LOAD_H
#define RLOOKUP_LOAD_H
#include "rlookup.h"

#ifdef __cplusplus
extern "C" {
#endif

int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);
int RLookup_LoadDocument(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);
int RLookup_LoadRuleFields(RedisSearchCtx *sctx, RLookup *it, RLookupRow *dst,
                           IndexSpec *sp, const char *keyptr, QueryError *status);

#ifdef __cplusplus
}
#endif

#endif
