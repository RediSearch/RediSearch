/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "common.h"
#include "endpoint.h"
#include "triemap/triemap.h"

typedef enum { MRNode_Master = 0x1, MRNode_Self = 0x2, MRNode_Coordinator = 0x4 } MRNodeFlags;

typedef struct {
  MREndpoint endpoint;
  const char *id;
  MRNodeFlags flags;
} MRClusterNode;

/* Free an MRendpoint object */
void MRNode_Free(MRClusterNode *n);

/* Return 1 both nodes have the same host */
int MRNode_IsSameHost(MRClusterNode *n, MRClusterNode *other);

typedef struct MRNodeMap {
  TrieMap *nodes;
  TrieMap *hosts;
} MRNodeMap;

typedef struct MRNodeMapIterator {
  TrieMapIterator *iter;
  MRNodeMap *m;
  MRClusterNode *excluded;
  MRClusterNode *(*Next)(struct MRNodeMapIterator *it);
} MRNodeMapIterator;

MRNodeMapIterator MRNodeMap_IterateAll(MRNodeMap *m);
MRNodeMapIterator MRNodeMap_IterateRandomNodePerhost(MRNodeMap *m, MRClusterNode *excluded);
MRNodeMapIterator MRNodeMap_IterateHost(MRNodeMap *m, const char *host);
void MRNodeMapIterator_Free(MRNodeMapIterator *it);

MRNodeMap *MR_NewNodeMap();
void MRNodeMap_Free(MRNodeMap *m);
void MRNodeMap_Add(MRNodeMap *m, MRClusterNode *n);
size_t MRNodeMap_NumHosts(MRNodeMap *m);
size_t MRNodeMap_NumNodes(MRNodeMap *m);
