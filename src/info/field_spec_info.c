/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "field_spec_info.h"
#include "reply_macros.h"
#include "coord/rmr/reply.h"

FieldTypeItzik getFieldType(const char* type);
VectorIndexStats VectorFieldStats_Deserialize(const MRReply* reply);
void FieldSpecStats_OpPlusEquals(FieldSpecStats *dst, const FieldSpecStats *src);

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
// Sets the identifier of the field spec.
void FieldSpecInfo_SetIdentifier(FieldSpecInfo *info, const char *identifier) {
    info->identifier = identifier;
}

// Sets the attribute of the field spec.
void FieldSpecInfo_SetAttribute(FieldSpecInfo *info, const char *attribute) {
    info->attribute = attribute;
}

// Sets the index error of the field spec.
void FieldSpecInfo_SetIndexError(FieldSpecInfo *info, IndexError error) {
    info->error = error;
}

// IO and cluster traits


void FieldSpecStats_Reply(const FieldSpecStats* stats, RedisModule_Reply *reply){
    if (!stats) {
        return;
    }
    switch (stats->type) {
    case INDEXFLD_T_VECTOR_ITZIK:
        REPLY_KVINT("memory", stats->vecStats.memory);
        REPLY_KVINT("marked_deleted", stats->vecStats.marked_deleted);
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
    FieldSpecStats_Reply(&info->stats, reply);
    // Set the error as a new object.
    RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
    IndexError_Reply(&info->error, reply, with_timestamp);

    RedisModule_Reply_MapEnd(reply);
}


// Adds the index error of the other FieldSpecInfo to the FieldSpecInfo.
void FieldSpecInfo_OpPlusEquals(FieldSpecInfo *info, const FieldSpecInfo *other) {
    RedisModule_Assert(info);
    RedisModule_Assert(other);
    if(!info->identifier) {
        info->identifier = other->identifier;
    }
    if(!info->attribute) {
        info->attribute = other->attribute;
    }
    IndexError_OpPlusEquals(&info->error, &other->error);
    FieldSpecStats_OpPlusEquals(&info->stats, &other->stats);
}

// Deserializes a FieldSpecInfo from a MRReply.
FieldSpecInfo FieldSpecInfo_Deserialize(const MRReply *reply) {
    FieldSpecInfo info = {0};
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
    info.stats = FieldStats_Deserialize(info.attribute, reply);

    return info;
}

FieldSpecStats FieldStats_Deserialize(const char* type,const MRReply* reply){
    FieldSpecStats stats = {0};
    FieldTypeItzik fieldType = getFieldType(type);
    switch (fieldType) {
        case INDEXFLD_T_VECTOR_ITZIK:
            stats.vecStats = VectorFieldStats_Deserialize(reply);
            stats.type = INDEXFLD_T_VECTOR_ITZIK;
        default:
            break;
    }
    return stats;
}

FieldTypeItzik getFieldType(const char* type){
    if(strcmp(type, "vector") == 0){
        return INDEXFLD_T_VECTOR_ITZIK;
    }
    return 0;
}

VectorIndexStats VectorFieldStats_Deserialize(const MRReply* reply){
    VectorIndexStats vecStats;
    vecStats.memory = MRReply_Integer(MRReply_MapElement(reply, "memory"));
    vecStats.marked_deleted = MRReply_Integer(MRReply_MapElement(reply, "marked_deleted"));
    return vecStats;
}


void FieldSpecStats_OpPlusEquals(FieldSpecStats *first, const FieldSpecStats *second) {
    if (!first || !second) {
        return;
    }
    if (!first->type){
        *first = *second;
        return;
    }
    switch (first->type) {
    case INDEXFLD_T_VECTOR_ITZIK:
        first->vecStats.memory += second->vecStats.memory;
        first->vecStats.marked_deleted += second->vecStats.marked_deleted;
        break;
    default:
        break;
    }
}

