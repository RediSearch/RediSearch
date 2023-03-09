/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __QUERY_PARSER_PARSE_H__
#define __QUERY_PARSER_PARSE_H__

#include "../cluster.h"
#include "parse.h"

typedef struct {
  MRClusterTopology *topology;
  char *my_id;
  char *shardFunc;
  int numSlots;
  int replication;
  int ok;
  char *errorMsg;
} parseCtx;

#endif  // !__QUERY_PARSER_PARSE_H__