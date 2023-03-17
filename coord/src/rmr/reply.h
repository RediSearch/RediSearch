/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <stdlib.h>
#include "redismodule.h"
#include "hiredis/hiredis.h"

#define MR_REPLY_STRING 1
#define MR_REPLY_ARRAY 2
#define MR_REPLY_INTEGER 3
#define MR_REPLY_NIL 4
#define MR_REPLY_STATUS 5
#define MR_REPLY_ERROR 6

typedef struct redisReply MRReply;

static inline void MRReply_Free(MRReply *reply) {
  freeReplyObject(reply);
}

static inline int MRReply_Type(MRReply *reply) {
  return reply->type;
}

static inline long long MRReply_Integer(MRReply *reply) {
  return reply->integer;
}

static inline size_t MRReply_Length(MRReply *reply) {
  return reply->elements;
}

/* Compare a string reply with a string, optionally case sensitive */
int MRReply_StringEquals(MRReply *r, const char *s, int caseSensitive);

static inline char *MRReply_String(MRReply *reply, size_t *len) {
  if (len) {
    *len = reply->len;
  }
  return reply->str;
}

static inline MRReply *MRReply_ArrayElement(MRReply *reply, size_t idx) {
  return reply->element[idx];
}

void MRReply_Print(FILE *fp, MRReply *r);
int MRReply_ToInteger(MRReply *reply, long long *i);
int MRReply_ToDouble(MRReply *reply, double *d);
int MR_ReplyWithMRReply(RedisModuleCtx *ctx, MRReply *rep);
