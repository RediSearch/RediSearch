/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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

MRReply *MRReply_MapElement(const MRReply *reply, const char *key);

void MRReply_Print(FILE *fp, MRReply *r);
int MRReply_ToInteger(MRReply *reply, long long *i);
int MRReply_ToDouble(MRReply *reply, double *d);

int MR_ReplyWithMRReply(RedisModule_Reply *reply, MRReply *rep);
int RedisModule_ReplyKV_MRReply(RedisModule_Reply *reply, const char *key, MRReply *rep);

void print_mr_reply(MRReply *r);
