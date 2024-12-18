/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "index_error.h"
#include "reply.h"
#include "obfuscation/hidden.h"

// Same as AggregatedFieldSpecInfo but local to a specific field inside a shard
// Strings must be freed in its dtor
// Strings can either obfuscated or not
typedef struct {
  char *identifier; // The identifier of the field spec.
  char *attribute; // The attribute of the field spec.
  IndexError error; // Indexing error of the field spec.
} FieldSpecInfo;

// A struct to hold the information of a field specification.
// To be used while field spec is still alive with respect to object lifetime.
typedef struct {
  const char *identifier; // The identifier of the field spec, can already be obfuscated from the shard.
  const char *attribute; // The attribute of the field spec, can already be obfuscated from the shard.
  IndexError error; // Indexing error of the field spec.
} AggregatedFieldSpecInfo;


// Create stack allocated FieldSpecInfo.
FieldSpecInfo FieldSpecInfo_Init();

// Create stack allocated AggregatedFieldSpecInfo.
AggregatedFieldSpecInfo AggregatedFieldSpecInfo_Init();

// Clears the field spec info.
void FieldSpecInfo_Clear(FieldSpecInfo *info);

// Clears the aggregated field spec info.
void AggregatedFieldSpecInfo_Clear(AggregatedFieldSpecInfo *info);

// Setters
// Sets the identifier of the field spec.
void FieldSpecInfo_SetIdentifier(FieldSpecInfo *info, char *identifier);

// Sets the attribute of the field spec.
void FieldSpecInfo_SetAttribute(FieldSpecInfo *info, char *attribute);

// Sets the index error of the field spec.
void FieldSpecInfo_SetIndexError(FieldSpecInfo *, IndexError error);

// IO and cluster traits
// Reply a Field spec info.
void FieldSpecInfo_Reply(const FieldSpecInfo *info, RedisModule_Reply *reply, bool withTimestamp, bool obfuscate);

// Reply an AggregatedFieldSpecInfo.
void AggregatedFieldSpecInfo_Reply(const AggregatedFieldSpecInfo *info, RedisModule_Reply *reply, bool withTimestamp, bool obfuscate);

#include "coord/rmr/reply.h"

// Adds the index error of the other FieldSpecInfo to the FieldSpecInfo.
void AggregatedFieldSpecInfo_OpPlusEquals(AggregatedFieldSpecInfo *info, const AggregatedFieldSpecInfo *other);

// Deserializes a FieldSpecInfo from a MRReply.
AggregatedFieldSpecInfo AggregatedFieldSpecInfo_Deserialize(const MRReply *reply);
