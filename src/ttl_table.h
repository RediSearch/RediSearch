//
// Created by jonathan on 7/4/24.
//

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

typedef struct {
  dict *hashTable;
} TimeToLiveTable;

void TimeToLiveTable_Init(TimeToLiveTable *table);
void TimeToLiveTable_Destroy(TimeToLiveTable *table);
void TimeToLiveTable_Add(TimeToLiveTable *table, t_docId docId, t_expirationTimePoint docExpiration, arrayof(FieldExpiration) sortedById);
void TimeToLiveTable_Remove(TimeToLiveTable *table, t_docId docId);
bool TimeToLiveTable_Empty(TimeToLiveTable *table);
struct timespec* TimeToLiveTable_GetTimeForCurrentThread(const TimeToLiveTable *table);
void TimeToLiveTable_SetTimeForCurrentThread(TimeToLiveTable *table, const struct timespec* now);

bool TimeToLiveTable_HasDocExpired(const TimeToLiveTable *table, t_docId docId);
bool TimeToLiveTable_VerifyDocAndFieldIndexPredicate(const TimeToLiveTable *table, t_docId docId, t_fieldIndex fieldIndex, enum FieldExpirationPredicate predicate);
bool TimeToLiveTable_VerifyFieldIndicesPredicate(const TimeToLiveTable *table, t_docId docId, t_fieldIndex* sortedFieldIndices, enum FieldExpirationPredicate predicate);

#endif //TTL_TABLE_H