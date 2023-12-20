/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "index_error.h"
#include "reply.h"


// A struct to hold the information of a field specification.
// To be used while field spec is still alive with respect to object lifetime.
typedef struct {
    const char *identifier; // The identifier of the field spec.
    const char *attribute; // The attribute of the field spec.
    IndexError error; // Indexing error of the field spec.
} FieldSpecInfo;

// Create stack allocated FieldSpecInfo.
FieldSpecInfo FieldSpecInfo_Init();

// Clears the field spec info.
void FieldSpecInfo_Clear(FieldSpecInfo *info);

// Setters
// Sets the identifier of the field spec.
void FieldSpecInfo_SetIdentifier(FieldSpecInfo *info, const char *identifier);

// Sets the attribute of the field spec.
void FieldSpecInfo_SetAttribute(FieldSpecInfo *info, const char *attribute);

// Sets the index error of the field spec.
void FieldSpecInfo_SetIndexError(FieldSpecInfo *, IndexError error);

// IO and cluster traits
// Reply a Field spec info.
void FieldSpecInfo_Reply(const FieldSpecInfo *info, RedisModule_Reply *reply);

#ifdef RS_COORDINATOR

#include "coord/src/rmr/reply.h"

// Adds the index error of the other FieldSpecInfo to the FieldSpecInfo.
void FieldSpecInfo_OpPlusEquals(FieldSpecInfo *info, const FieldSpecInfo *other);

// Deserializes a FieldSpecInfo from a MRReply.
FieldSpecInfo FieldSpecInfo_Deserialize(const MRReply *reply);
#endif
