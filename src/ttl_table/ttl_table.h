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
#include <stdint.h>          // for uint32_t

#include "redisearch.h"      // for t_docId, t_fieldIndex, ...
#include "util/dict/dict.h"  // for dict
#include "stdbool.h"         // for bool
#include "util/arr.h"
#include "types_rs.h"        // for FieldExpirationPredicate
#include "util/arr/arr.h"    // for arrayof

struct timespec;

#ifdef __cplusplus
extern "C" {
#endif

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

bool TimeToLiveTable_HasDocExpired(TimeToLiveTable *table, t_docId docId, const struct timespec* expirationPoint);

bool TimeToLiveTable_VerifyDocAndField(TimeToLiveTable *table, t_docId docId, t_fieldIndex fieldIndex, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint);
bool TimeToLiveTable_VerifyDocAndFieldMask(TimeToLiveTable *table, t_docId docId, uint32_t fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex);
bool TimeToLiveTable_VerifyDocAndWideFieldMask(TimeToLiveTable *table, t_docId docId, t_fieldMask fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex);

#ifdef __cplusplus
}
#endif

#endif //TTL_TABLE_H
