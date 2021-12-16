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