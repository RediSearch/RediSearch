/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef TTL_TABLE_H
#define TTL_TABLE_H
#include "redisearch.h"
#include "util/dict.h"
#include "stdbool.h"
#include "util/arr.h"

typedef struct {
  t_fieldIndex index;
  t_expirationTimePoint point;
} FieldExpiration;

typedef dict TimeToLiveTable;

void TimeToLiveTable_VerifyInit(TimeToLiveTable **table);
void TimeToLiveTable_Destroy(TimeToLiveTable **table);
void TimeToLiveTable_Add(TimeToLiveTable *table, t_docId docId, t_expirationTimePoint docExpiration, arrayof(FieldExpiration) sortedById);
void TimeToLiveTable_Remove(TimeToLiveTable *table, t_docId docId);
bool TimeToLiveTable_IsEmpty(TimeToLiveTable *table);

bool TimeToLiveTable_HasExpiration(TimeToLiveTable *table, t_docId docId);
bool TimeToLiveTable_HasDocExpired(TimeToLiveTable *table, t_docId docId, const struct timespec* expirationPoint);
bool TimeToLiveTable_VerifyDocAndFields(TimeToLiveTable *table, t_docId docId, const t_fieldIndex* sortedFieldIndices, size_t fieldCount, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint);

#endif //TTL_TABLE_H
