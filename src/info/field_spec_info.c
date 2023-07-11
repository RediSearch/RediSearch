/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "field_spec_info.h"
#include "reply_macros.h"

// Ctor/Dtor
// Initializes a FieldSpecInfo.
FieldSpecInfo *FieldSpecInfo_New() {
    FieldSpecInfo *info = malloc(sizeof(*info));
    info->identifier = NULL;
    info->attribute = NULL;
    info->error = IndexError_init();
    return info;}

// Frees a FieldSpecInfo.
void FieldSpecInfo_Free(FieldSpecInfo *info) {
    if(info) {
        // Free the identifier. Everything else is borrowed. 
        rm_free(info);
    }
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
    REPLY_KVSTR("identifier", info->identifier);
    REPLY_KVSTR("attribute", info->attribute);
    IndexError_Reply(&info->error, reply);
}

#ifndef RS_COORDINATOR

#include "coord/src/rmr/reply.h"

// Adds the index error of the other FieldSpecInfo to the FieldSpecInfo.
void FieldSpecInfo_OpPlusEquals(FieldSpecInfo *info, const FieldSpecInfo *other) {
    IndexError_OpPlusEquals(&info->error, &other->error);
}

// Deserializes a FieldSpecInfo from a MRReply.
FieldSpecInfo FieldSpecInfo_Deserialize(const MRReply *reply) {
    
}

#endif