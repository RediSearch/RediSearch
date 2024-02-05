/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "rmalloc.h"
#include "util/dict.h"
#include "rmr/common.h"
#include "rmr/endpoint.h"
#include "node.h"

typedef struct MRNodeMap MRNodeMap;

typedef struct MRNodeMapIterator {
  dictIterator *iter;
  MRNodeMap *m;
  MRClusterNode *excluded;
  MRClusterNode *(*Next)(struct MRNodeMapIterator *it);
  const char *host;
} MRNodeMapIterator;

MRNodeMapIterator MRNodeMap_IterateAll(MRNodeMap *m);
void MRNodeMapIterator_Free(MRNodeMapIterator *it);

MRNodeMap *MR_NewNodeMap();
void MRNodeMap_Free(MRNodeMap *m);
void MRNodeMap_Add(MRNodeMap *m, MRClusterNode *n);
size_t MRNodeMap_NumHosts(MRNodeMap *m);
size_t MRNodeMap_NumNodes(MRNodeMap *m);
