/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "field_spec_info.h"
#include "reply_macros.h"

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

// IO and cluser traits

// Reply a Field spec info.
void FieldSpecInfo_Reply(const FieldSpecInfo *info, RedisModule_Reply *reply) {
    RedisModule_Reply_Map(reply);
    
    REPLY_KVSTR("identifier", info->identifier);
    REPLY_KVSTR("attribute", info->attribute);
    // Set the error as a new object.
    RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
    IndexError_Reply(&info->error, reply);

    RedisModule_Reply_MapEnd(reply);
}

#ifdef RS_COORDINATOR

#include "coord/src/rmr/reply.h"

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

    return info;
}

#endif