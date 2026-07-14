/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "field_spec_info.h"
#include "reply_macros.h"
#include "coord/rmr/reply.h"
#include "search_disk.h"

// Per-field disk metric reply keys, shared by the shard emitter and the
// coordinator deserializer so both agree on the wire names.
#define FIELD_DISK_EXCLUSIVE_BYTES "disk_exclusive_bytes"
#define FIELD_DISK_SHARED_BYTES    "disk_shared_bytes"
#define FIELD_DISK_TOTAL_BYTES     "disk_total_bytes"
#define FIELD_DISK_NUM_KEYS        "disk_num_keys"

static FieldType getFieldType(const char *type){
    if (strcmp(type, "vector") == 0) {
        return INDEXFLD_T_VECTOR;
    }
    return 0;
}

// Per-field disk metrics are cluster-wide totals, so each metric is summed
// across shards. A shard that omits the `disk` metrics (e.g. an older,
// mixed-version shard, or a non-disk-backed shard) reports `available = false`
// and contributes nothing.
static void PerFieldTextDiskMetrics_Combine(PerFieldTextDiskMetrics *dst,
                                            const PerFieldTextDiskMetrics *src) {
    if (!src->available) {
        return;
    }
    dst->exclusive_bytes += src->exclusive_bytes;
    dst->shared_bytes += src->shared_bytes;
    dst->available = true;
}

static void PerFieldCfDiskMetrics_Combine(PerFieldCfDiskMetrics *dst,
                                          const PerFieldCfDiskMetrics *src) {
    if (!src->available) {
        return;
    }
    dst->total_bytes += src->total_bytes;
    dst->estimate_num_keys += src->estimate_num_keys;
    dst->available = true;
}

static void FieldSpecStats_Combine(FieldSpecStats *first, const FieldSpecStats *second) {
    // The field type is consistent across shards; adopt it from the first shard
    // that reports one (non-vector fields carry type 0 on the coordinator).
    if (!first->type) {
        first->type = second->type;
    }
    if (first->type == INDEXFLD_T_VECTOR) {
        VectorIndexStats_Agg(&first->vecStats, &second->vecStats);
    }
    // Disk metrics apply to every field type, independently of `type`.
    PerFieldTextDiskMetrics_Combine(&first->textDisk, &second->textDisk);
    PerFieldCfDiskMetrics_Combine(&first->cfDisk, &second->cfDisk);
}

FieldSpecInfo FieldSpecInfo_Init() {
    FieldSpecInfo info = {0};
    info.error = IndexError_Init();
    return info;
}

AggregatedFieldSpecInfo AggregatedFieldSpecInfo_Init() {
    AggregatedFieldSpecInfo info = {0};
    info.error = IndexError_Init();
    return info;
}

void FieldSpecInfo_Clear(FieldSpecInfo *info) {
    rm_free(info->identifier);
    rm_free(info->attribute);
    info->identifier = NULL;
    info->attribute = NULL;
}

void AggregatedFieldSpecInfo_Clear(AggregatedFieldSpecInfo *info) {
    info->identifier = NULL;
    info->attribute = NULL;
    IndexError_Clear(info->error);
}

// Setters
void FieldSpecInfo_SetIdentifier(FieldSpecInfo *info, char *identifier) {
    info->identifier = identifier;
}

void FieldSpecInfo_SetAttribute(FieldSpecInfo *info, char *attribute) {
    info->attribute = attribute;
}

void FieldSpecInfo_SetIndexError(FieldSpecInfo *info, IndexError error) {
    info->error = error;
}

void FieldSpecInfo_SetStats(FieldSpecInfo *info, FieldSpecStats stats) {
    info->stats = stats;
}

// Parse the optional per-field `disk` metrics emitted by a disk-backed shard.
// Keys are absent for non-disk-backed or older (mixed-version) shards, in which
// case `available` stays false and the metrics contribute nothing to the reduce.
static void FieldStats_DeserializeDiskMetrics(FieldSpecStats *stats, const MRReply *reply) {
    MRReply *exclusive = MRReply_MapElement(reply, FIELD_DISK_EXCLUSIVE_BYTES);
    MRReply *shared = MRReply_MapElement(reply, FIELD_DISK_SHARED_BYTES);
    if (exclusive || shared) {
        stats->textDisk.available = true;
        if (exclusive) stats->textDisk.exclusive_bytes = MRReply_Integer(exclusive);
        if (shared) stats->textDisk.shared_bytes = MRReply_Integer(shared);
    }

    MRReply *totalBytes = MRReply_MapElement(reply, FIELD_DISK_TOTAL_BYTES);
    MRReply *numKeys = MRReply_MapElement(reply, FIELD_DISK_NUM_KEYS);
    if (totalBytes || numKeys) {
        stats->cfDisk.available = true;
        if (totalBytes) stats->cfDisk.total_bytes = MRReply_Integer(totalBytes);
        if (numKeys) stats->cfDisk.estimate_num_keys = MRReply_Integer(numKeys);
    }
}

static FieldSpecStats FieldStats_Deserialize(const char* type, const MRReply* reply){
    FieldSpecStats stats = {0};
    FieldType fieldType = getFieldType(type);
    switch (fieldType) {
        case INDEXFLD_T_VECTOR:
            for(int i = 0; VectorIndexStats_Metrics[i] != NULL; i++){
                // Handle missing metrics gracefully (e.g., during rolling upgrades when
                // old shards don't output new metrics). Missing metrics default to 0.
                MRReply *metricReply = MRReply_MapElement(reply, VectorIndexStats_Metrics[i]);
                if (metricReply) {
                    size_t metricValue = MRReply_Integer(metricReply);
                    VectorIndexStats_GetSetter(VectorIndexStats_Metrics[i])(&stats.vecStats, metricValue);
                }
            }
            stats.type = INDEXFLD_T_VECTOR;
            break;
        default:
            break;
    }
    // Disk metrics are field-type independent and keyed by name, so parse them
    // for every field regardless of the vector switch above.
    FieldStats_DeserializeDiskMetrics(&stats, reply);
    return stats;
}

// IO and cluster traits

void FieldSpecStats_Reply(const FieldSpecStats* stats, RedisModule_Reply *reply){
    RS_ASSERT(stats);

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

    // Per-field disk metrics (disk-backed indexes only; gated on `available`).
    if (stats->textDisk.available) {
        REPLY_KVINT(FIELD_DISK_EXCLUSIVE_BYTES, stats->textDisk.exclusive_bytes);
        REPLY_KVINT(FIELD_DISK_SHARED_BYTES, stats->textDisk.shared_bytes);
    }
    if (stats->cfDisk.available) {
        REPLY_KVINT(FIELD_DISK_TOTAL_BYTES, stats->cfDisk.total_bytes);
        REPLY_KVINT(FIELD_DISK_NUM_KEYS, stats->cfDisk.estimate_num_keys);
    }
}

// Reply a Field spec info.
void FieldSpecInfo_Reply(const FieldSpecInfo *info, RedisModule_Reply *reply, bool withTimestamp, bool obfuscate) {
    RedisModule_Reply_Map(reply);

    REPLY_KVSTR("identifier", info->identifier);
    REPLY_KVSTR("attribute", info->attribute);
    // Set the error as a new object.
    RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
    IndexError_Reply(&info->error, reply, withTimestamp, obfuscate, INDEX_ERROR_WITHOUT_OOM_STATUS);
    FieldSpecStats_Reply(&info->stats, reply);

    RedisModule_Reply_MapEnd(reply);
}

void AggregatedFieldSpecInfo_Reply(const AggregatedFieldSpecInfo *info, RedisModule_Reply *reply, bool withTimestamp, bool obfuscate) {
    RedisModule_Reply_Map(reply);

    REPLY_KVSTR("identifier", info->identifier);
    REPLY_KVSTR("attribute", info->attribute);
    // Set the error as a new object.
    RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
    IndexError_Reply(&info->error, reply, withTimestamp, obfuscate, INDEX_ERROR_WITHOUT_OOM_STATUS);
    FieldSpecStats_Reply(&info->stats, reply);

    RedisModule_Reply_MapEnd(reply);
}

// Adds the index error of the other FieldSpecInfo to the FieldSpecInfo.
void AggregatedFieldSpecInfo_Combine(AggregatedFieldSpecInfo *info, const AggregatedFieldSpecInfo *other) {
    RS_ASSERT(info);
    RS_ASSERT(other);
    if (!info->identifier) {
        info->identifier = other->identifier;
    }
    if (!info->attribute) {
        info->attribute = other->attribute;
    }
    IndexError_Combine(&info->error, &other->error);
    FieldSpecStats_Combine(&info->stats, &other->stats);
}

// Deserializes a FieldSpecInfo from a MRReply.
AggregatedFieldSpecInfo AggregatedFieldSpecInfo_Deserialize(const MRReply *reply) {
    AggregatedFieldSpecInfo info = {0};
    RS_ASSERT(reply);
    // Validate the reply type - array or map.
    RS_ASSERT(MRReply_Type(reply) == MR_REPLY_MAP || (MRReply_Type(reply) == MR_REPLY_ARRAY && MRReply_Length(reply) % 2 == 0));
    // Make sure the reply is a map, regardless of the protocol.
    MRReply_ArrayToMap((MRReply*)reply);

    MRReply *identifier = MRReply_MapElement(reply, "identifier");
    RS_ASSERT(identifier);
    // In hiredis with resp2 '+' is a status reply.
    RS_ASSERT(MRReply_Type(identifier) == MR_REPLY_STRING || MRReply_Type(identifier) == MR_REPLY_STATUS);
    info.identifier = MRReply_String(identifier, NULL);

    MRReply *attribute = MRReply_MapElement(reply, "attribute");
    RS_ASSERT(attribute);
    // In hiredis with resp2 '+' is a status reply.
    RS_ASSERT(MRReply_Type(attribute) == MR_REPLY_STRING || MRReply_Type(attribute) == MR_REPLY_STATUS);
    info.attribute = MRReply_String(attribute, NULL);

    MRReply *error = MRReply_MapElement(reply, IndexError_ObjectName);
    RS_ASSERT(error);
    info.error = IndexError_Deserialize(error, INDEX_ERROR_WITHOUT_OOM_STATUS);
    // attribute used to determine field type
    info.stats = FieldStats_Deserialize(info.attribute, reply);

    return info;
}

// Returns the size of the vector indexes in the index `sp`.
size_t IndexSpec_VectorIndexesSize(IndexSpec *sp) {
  VectorIndexStats stats = IndexSpec_GetVectorIndexesStats(sp);
  return stats.memory;
}

// Get the stats of the vector field `fs`.
VectorIndexStats IndexSpec_GetVectorIndexStats(FieldSpec *fs){
  VectorIndexStats stats = {0};
  // ctx is NULL because we don't create the index here
  VecSimIndex *vecsim = openVectorIndex(NULL, fs, DONT_CREATE_INDEX);
  if (!vecsim) {
    return stats;
  }
  const VecSimIndexStatsInfo info = VecSimIndex_StatsInfo(vecsim);
  stats.memory += info.memory;
  stats.marked_deleted += info.numberOfMarkedDeleted;
  stats.direct_hnsw_insertions += info.directHNSWInsertions;
  stats.flat_buffer_size += info.flatBufferSize;
  return stats;
}

// Get the stats of the vector indexes in the index `sp`.
VectorIndexStats IndexSpec_GetVectorIndexesStats(IndexSpec *sp) {
  VectorIndexStats stats = {0};
  for (size_t i = 0; i < sp->numFields; ++i) {
    FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_VECTOR)) {
      VectorIndexStats field_stats = IndexSpec_GetVectorIndexStats(fs);
      stats.memory += field_stats.memory;
      stats.marked_deleted += field_stats.marked_deleted;
      stats.direct_hnsw_insertions += field_stats.direct_hnsw_insertions;
      stats.flat_buffer_size += field_stats.flat_buffer_size;
    }
  }
  return stats;
}

// Get the stats of the field `fs`.
FieldSpecStats IndexSpec_GetFieldStats(FieldSpec *fs){
  FieldSpecStats stats = {0};
  stats.type = fs->types;
  switch (stats.type) {
    case INDEXFLD_T_VECTOR:
      stats.vecStats = IndexSpec_GetVectorIndexStats(fs);
      return stats;
    default:
      return (FieldSpecStats){0};
  }
}

// Populate disk-backed FT.INFO metrics for field `fs`.
static void FieldSpecStats_PopulateDiskMetrics(FieldSpecStats *stats, const IndexSpec *sp,
                                               const FieldSpec *fs) {
  if (FieldSpec_IsIndexableText(fs)) {
    stats->textDisk = SearchDisk_GetTextFieldMetrics(sp->diskSpec, fs->ftId);
  }
  if (FIELD_IS(fs, INDEXFLD_T_TAG | INDEXFLD_T_NUMERIC)) {
    // TAG/NUMERIC CFs are named by the numeric field index.
    stats->cfDisk = SearchDisk_GetCfFieldMetrics(sp->diskSpec, fs->index);
  } else if (FIELD_IS(fs, INDEXFLD_T_VECTOR)) {
    // Vector CFs are named `vector_<fieldName>`, so they are keyed by name.
    size_t nameLen;
    const char *namePtr = HiddenString_GetUnsafe(fs->fieldName, &nameLen);
    stats->cfDisk = SearchDisk_GetVectorFieldMetrics(sp->diskSpec, namePtr, nameLen);
  }
}

// Get the information of the field `fs` in the index `sp`.
FieldSpecInfo FieldSpec_GetInfo(const IndexSpec *sp, FieldSpec *fs, bool obfuscate) {
  FieldSpecInfo info = {0};
  FieldSpecInfo_SetIdentifier(&info, FieldSpec_FormatPath(fs, obfuscate));
  FieldSpecInfo_SetAttribute(&info, FieldSpec_FormatName(fs, obfuscate));
  FieldSpecInfo_SetIndexError(&info, fs->indexError);
  FieldSpecStats stats = IndexSpec_GetFieldStats(fs);
  // Per-field disk metrics only exist for disk-backed indexes.
  if (sp->diskSpec) {
    FieldSpecStats_PopulateDiskMetrics(&stats, sp, fs);
  }
  FieldSpecInfo_SetStats(&info, stats);
  return info;
}
