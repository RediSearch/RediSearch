/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "resp3.h"

#include "redismodule.h"

#include <stdlib.h>

#define MR_REPLY_STRING 1
#define MR_REPLY_ARRAY 2
#define MR_REPLY_INTEGER 3
#define MR_REPLY_NIL 4
#define MR_REPLY_STATUS 5
#define MR_REPLY_ERROR 6

#define MR_REPLY_DOUBLE 7
#define MR_REPLY_BOOL 8
#define MR_REPLY_MAP 9
#define MR_REPLY_SET 10

#define MR_REPLY_ATTR 11
#define MR_REPLY_PUSH 12
#define MR_REPLY_BIGNUM 13
#define MR_REPLY_VERB 14

struct redisReply;
typedef struct redisReply MRReply;

void MRReply_Free(MRReply *reply);

int MRReply_Type(const MRReply *reply);

long long MRReply_Integer(const MRReply *reply);

double MRReply_Double(const MRReply *reply);

size_t MRReply_Length(const MRReply *reply);

/* Compare a string reply with a string, optionally case sensitive */
int MRReply_StringEquals(MRReply *r, const char *s, int caseSensitive);

const char *MRReply_String(const MRReply *reply, size_t *len);

MRReply *MRReply_ArrayElement(const MRReply *reply, size_t idx);
// Same as `MRReply_ArrayElement`, but takes ownership of the element.
MRReply *MRReply_TakeArrayElement(const MRReply *reply, size_t idx);

MRReply *MRReply_MapElement(const MRReply *reply, const char *key);
// Same as `MRReply_MapElement`, but takes ownership of the element.
MRReply *MRReply_TakeMapElement(const MRReply *reply, const char *key);

// Converts an array reply to a map reply type. The array must be of the form
// [key1, value1, key2, value2, ...] and the resulting map will be of the form
// {key1: value1, key2: value2, ...}
// Use this if you are sure the reply is an array and you want to convert it to
// a map.
void MRReply_ArrayToMap(MRReply *reply);

int MRReply_ToInteger(MRReply *reply, long long *i);
int MRReply_ToDouble(MRReply *reply, double *d);

int MR_ReplyWithMRReply(RedisModule_Reply *reply, MRReply *rep);
int RedisModule_ReplyKV_MRReply(RedisModule_Reply *reply, const char *key, MRReply *rep);

// Clone MRReply from another MRReply
// Currently implements a partial clone, only for the type and string types.
// Support types - MR_REPLY_STRING, MR_REPLY_ERROR
MRReply *MRReply_Clone(MRReply *src);
