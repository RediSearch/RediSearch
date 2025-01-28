/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "field_spec_info.h"
#include "reply_macros.h"
#include "coord/rmr/reply.h"

static FieldType getFieldType(const char *type){
    if(strcmp(type, "vector") == 0){
        return INDEXFLD_T_VECTOR;
    }
    return 0;
}

static void FieldSpecStats_Combine(FieldSpecStats *first, const FieldSpecStats *second) {
    if (!first->type){
        *first = *second;
        return;
    }
    switch (first->type) {
        case INDEXFLD_T_VECTOR:
            VectorIndexStats_Agg(&first->vecStats, &second->vecStats);
            break;
        default:
            break;
        }
}

FieldSpecInfo FieldSpecInfo_Init() {
    FieldSpecInfo info = {0};
    info.error = IndexError_Init();
    return info;
}

void FieldSpecInfo_Clear(FieldSpecInfo *info) {
    info->identifier = NULL;
    info->attribute = NULL;
    IndexError_Clear(info->error);
}

// Setters
void FieldSpecInfo_SetIdentifier(FieldSpecInfo *info, const char *identifier) {
    info->identifier = identifier;
}

void FieldSpecInfo_SetAttribute(FieldSpecInfo *info, const char *attribute) {
    info->attribute = attribute;
}

void FieldSpecInfo_SetIndexError(FieldSpecInfo *info, IndexError error) {
    info->error = error;
}

void FieldSpecInfo_SetStats(FieldSpecInfo *info, FieldSpecStats stats) {
    info->stats = stats;
}

static FieldSpecStats FieldStats_Deserialize(const char* type,const MRReply* reply){
    FieldSpecStats stats = {0};
    FieldType fieldType = getFieldType(type);
    switch (fieldType) {
        case INDEXFLD_T_VECTOR:
            for(int i = 0; VectorIndexStats_Metrics[i] != NULL; i++){
                size_t metricValue = MRReply_Integer(MRReply_MapElement(reply, VectorIndexStats_Metrics[i]));
                VectorIndexStats_GetSetter(VectorIndexStats_Metrics[i])(&stats.vecStats, metricValue);
            }
            stats.type = INDEXFLD_T_VECTOR;
        default:
            break;
    }
    return stats;
}

// IO and cluster traits

void FieldSpecStats_Reply(const FieldSpecStats* stats, RedisModule_Reply *reply){
    if (!stats) {
        return;
    }
    switch (stats->type) {
        case INDEXFLD_T_VECTOR:
            for (int i = 0; VectorIndexStats_Metrics[i] != NULL; i++) {
                REPLY_KVINT(VectorIndexStats_Metrics[i],
                            VectorIndexStats_GetGetter(VectorIndexStats_Metrics[i])(&stats->vecStats));
            }
            break;
        default:
            break;
    }
}

// Reply a Field spec info.
void FieldSpecInfo_Reply(const FieldSpecInfo *info, RedisModule_Reply *reply, bool with_timestamp) {
    RedisModule_Reply_Map(reply);

    REPLY_KVSTR_SAFE("identifier", info->identifier);
    REPLY_KVSTR_SAFE("attribute", info->attribute);
    // Set the error as a new object.
    RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
    IndexError_Reply(&info->error, reply, with_timestamp);
    FieldSpecStats_Reply(&info->stats, reply);

    RedisModule_Reply_MapEnd(reply);
}

// Adds the index error of the other FieldSpecInfo to the FieldSpecInfo.
void FieldSpecInfo_Combine(FieldSpecInfo *info, const FieldSpecInfo *other) {
    RedisModule_Assert(info);
    RedisModule_Assert(other);
    if(!info->identifier) {
        info->identifier = other->identifier;
    }
    if(!info->attribute) {
        info->attribute = other->attribute;
    }
    IndexError_Combine(&info->error, &other->error);
    FieldSpecStats_Combine(&info->stats, &other->stats);
}

// Deserializes a FieldSpecInfo from a MRReply.
FieldSpecInfo FieldSpecInfo_Deserialize(const MRReply *reply) {
    FieldSpecInfo info = FieldSpecInfo_Init();
    RedisModule_Assert(reply);
    // Validate the reply type - array or map.
    RedisModule_Assert(MRReply_Type(reply) == MR_REPLY_MAP || (MRReply_Type(reply) == MR_REPLY_ARRAY && MRReply_Length(reply) % 2 == 0));
    // Make sure the reply is a map, regardless of the protocol.
    MRReply_ArrayToMap((MRReply*)reply);

    MRReply *identifier = MRReply_MapElement(reply, "identifier");
    RedisModule_Assert(identifier);
    // In hiredis with resp2 '+' is a status reply.
    RedisModule_Assert(MRReply_Type(identifier) == MR_REPLY_STRING || MRReply_Type(identifier) == MR_REPLY_STATUS);
    info.identifier = MRReply_String(identifier, NULL);

    MRReply *attribute = MRReply_MapElement(reply, "attribute");
    RedisModule_Assert(attribute);
    // In hiredis with resp2 '+' is a status reply.
    RedisModule_Assert(MRReply_Type(attribute) == MR_REPLY_STRING || MRReply_Type(attribute) == MR_REPLY_STATUS);
    info.attribute = MRReply_String(attribute, NULL);

    MRReply *error = MRReply_MapElement(reply, IndexError_ObjectName);
    RedisModule_Assert(error);
    info.error = IndexError_Deserialize(error);
    // attribute used to determine field type
    info.stats = FieldStats_Deserialize(info.attribute, reply);

    return info;
}

// Assuming the spec is properly locked before calling this function.
size_t IndexSpec_VectorIndexSize(IndexSpec *sp) {
  VectorIndexStats stats = IndexSpec_GetVectorIndexesStats(sp);
  return stats.memory;
}

VectorIndexStats IndexSpec_GetVectorIndexStats(IndexSpec *sp, const FieldSpec *fs){
  VectorIndexStats stats = {0};
  RedisModuleString *vecsim_name = IndexSpec_GetFormattedKey(sp, fs, INDEXFLD_T_VECTOR);
  VecSimIndex *vecsim = openVectorIndex(sp, vecsim_name, DONT_CREATE_INDEX);
  if (!vecsim) {
    return stats;
  }
  VecSimIndexInfo info = VecSimIndex_Info(vecsim);
  stats.memory += info.commonInfo.memory;
  if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_HNSWLIB) {
    stats.marked_deleted += info.hnswInfo.numberOfMarkedDeletedNodes;
  } else if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_TIERED &&
            fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
    stats.marked_deleted += info.tieredInfo.backendInfo.hnswInfo.numberOfMarkedDeletedNodes;
  }
  return stats;
}

VectorIndexStats IndexSpec_GetVectorIndexesStats(IndexSpec *sp) {
  VectorIndexStats stats = {0};
  for (size_t i = 0; i < sp->numFields; ++i) {
    const FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_VECTOR)) {
      VectorIndexStats field_stats = IndexSpec_GetVectorIndexStats(sp, fs);
      stats.memory += field_stats.memory;
      stats.marked_deleted += field_stats.marked_deleted;
    }
  }
  return stats;
}

FieldSpecStats IndexSpec_GetFieldStats(const FieldSpec *fs, IndexSpec *sp){
  FieldSpecStats stats = {0};
  stats.type = fs->types;
  switch (stats.type) {
    case INDEXFLD_T_VECTOR:
      stats.vecStats = IndexSpec_GetVectorIndexStats(sp, fs);
      return stats;
    default:
      return (FieldSpecStats){0};
  }
}

FieldSpecInfo FieldSpec_GetInfo(const FieldSpec *fs, IndexSpec *sp) {
  FieldSpecInfo info = {0};
  FieldSpecInfo_SetIdentifier(&info, fs->path);
  FieldSpecInfo_SetAttribute(&info, fs->name);
  FieldSpecInfo_SetIndexError(&info, fs->indexError);
  FieldSpecInfo_SetStats(&info, IndexSpec_GetFieldStats(fs, sp));
  return info;
}
