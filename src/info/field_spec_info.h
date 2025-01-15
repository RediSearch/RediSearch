/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "index_error.h"
#include "reply.h"


typedef enum {
  // Newline
  INDEXFLD_T_FULLTEXT_ITZIK = 0x01,
  INDEXFLD_T_NUMERIC_ITZIK = 0x02,
  INDEXFLD_T_GEO_ITZIK = 0x04,
  INDEXFLD_T_TAG_ITZIK = 0x08,
  INDEXFLD_T_VECTOR_ITZIK = 0x10,
  INDEXFLD_T_GEOMETRY_ITZIK = 0x20,
} FieldTypeItzik;

// typedef FieldType;
typedef struct {
  size_t memory;
  size_t marked_deleted;
} VectorIndexStats;


typedef struct BaseStats {
  size_t memory;
  size_t marked_deleted;
} BaseStats;

typedef struct FieldSpecStats {
  union {
    VectorIndexStats vecStats;
    BaseStats baseStats;
  };
  FieldTypeItzik type;
} FieldSpecStats;



// A struct to hold the information of a field specification.
// To be used while field spec is still alive with respect to object lifetime.
typedef struct {
    const char *identifier; // The identifier of the field spec.
    const char *attribute; // The attribute of the field spec.
    IndexError error; // Indexing error of the field spec.
    FieldSpecStats stats;
} FieldSpecInfo;

FieldSpecStats FieldStats_Deserialize(const char* type,const MRReply* reply);


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
void FieldSpecInfo_Reply(const FieldSpecInfo *info, RedisModule_Reply *reply, bool with_timestamp);

#include "coord/rmr/reply.h"

// Adds the index error of the other FieldSpecInfo to the FieldSpecInfo.
void FieldSpecInfo_OpPlusEquals(FieldSpecInfo *info, const FieldSpecInfo *other);

// Deserializes a FieldSpecInfo from a MRReply.
FieldSpecInfo FieldSpecInfo_Deserialize(const MRReply *reply);
