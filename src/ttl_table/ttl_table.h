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
#include "stdbool.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  t_fieldIndex index;
  t_expirationTimePoint point;
} FieldExpiration;

typedef struct TimeToLiveTable TimeToLiveTable;

// Callback for TimeToLiveTable_ForEach. Return value is ignored.
typedef void (*TimeToLiveTable_DocIdCallback)(t_docId docId, void *ctx);

// Lazy-init: allocates the table on first use with `maxSize` as the fixed
// modulus for the slot formula. Caller (DocTable) passes its own
// `t->maxSize` so the two tables' slot formulas are identical by
// construction and cannot drift if `search-max-doctablesize` is later
// mutated. No-op if the table is already initialized.
void TimeToLiveTable_VerifyInit(TimeToLiveTable **table, size_t maxSize);
void TimeToLiveTable_Destroy(TimeToLiveTable **table);
void TimeToLiveTable_Add(TimeToLiveTable *table, t_docId docId, t_expirationTimePoint docExpiration, arrayof(FieldExpiration) sortedById);
void TimeToLiveTable_Remove(TimeToLiveTable *table, t_docId docId);
bool TimeToLiveTable_IsEmpty(TimeToLiveTable *table);

bool TimeToLiveTable_HasDocExpired(TimeToLiveTable *table, t_docId docId, const struct timespec* expirationPoint);

bool TimeToLiveTable_VerifyDocAndField(TimeToLiveTable *table, t_docId docId, t_fieldIndex fieldIndex, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint);
bool TimeToLiveTable_VerifyDocAndFieldMask(TimeToLiveTable *table, t_docId docId, uint32_t fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex);
bool TimeToLiveTable_VerifyDocAndWideFieldMask(TimeToLiveTable *table, t_docId docId, t_fieldMask fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex);

// Invoke `cb` for every live docId in the table. Used by DocTable to clear
// per-doc expiration flags at shutdown. Order is not defined.
void TimeToLiveTable_ForEach(TimeToLiveTable *table, TimeToLiveTable_DocIdCallback cb, void *ctx);

// Test-only: number of buckets currently allocated (lazy-growth high-water
// mark). Not exposed for runtime use.
size_t TimeToLiveTable_DebugAllocatedBuckets(const TimeToLiveTable *table);

#ifdef __cplusplus
}
#endif

#endif //TTL_TABLE_H
