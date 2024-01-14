/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdlib.h>
#include <stdio.h>

#include "node_map.h"

#define ADDRESS_LENGTH 256

struct MRNodeMap {
  dict *nodes;
  dict *hosts;
};

void MRNodeMapIterator_Free(MRNodeMapIterator *it) {
  dictReleaseIterator(it->iter);
}

static MRClusterNode *_nmi_allNext(MRNodeMapIterator *it) {
  dictEntry *de = dictNext(it->iter);
  return de ? dictGetVal(de) : NULL;
}

MRNodeMapIterator MRNodeMap_IterateAll(MRNodeMap *m) {
  return (MRNodeMapIterator){.Next = _nmi_allNext,
                             .m = m,
                             .excluded = NULL,
                             .iter = dictGetIterator(m->nodes),
                             .host = NULL};
}

/* Return 1 if this is the host of the node's endpoint */
static int IsNodeHost(const MRClusterNode *node, const char *host) {
  return !strcasecmp(node->endpoint.host, host);
}

static MRClusterNode *_nmi_hostNext(MRNodeMapIterator *it) {
  dictEntry *de;
  while ((de = dictNext(it->iter)) && !IsNodeHost(dictGetVal(de), it->host)) {
  }
  return de ? dictGetVal(de) : NULL;
}

MRNodeMapIterator MRNodeMap_IterateHost(MRNodeMap *m, const char *host) {
  return (MRNodeMapIterator){.Next = _nmi_hostNext,
                             .m = m,
                             .excluded = NULL,
                             .iter = dictGetIterator(m->nodes),
                             .host = host};
}

void MRNodeMap_Free(MRNodeMap *m) {
  dictRelease(m->hosts);
  dictRelease(m->nodes);
  rm_free(m);
}

size_t MRNodeMap_NumHosts(MRNodeMap *m) {
  return dictSize(m->hosts);
}

size_t MRNodeMap_NumNodes(MRNodeMap *m) {
  return dictSize(m->nodes);
}

MRNodeMap *MR_NewNodeMap() {
  MRNodeMap *m = rm_malloc(sizeof(*m));
  m->hosts = dictCreate(&dictTypeHeapStrings, NULL);
  m->nodes = dictCreate(&dictTypeHeapStrings, NULL);
  return m;
}

void MRNodeMap_Add(MRNodeMap *m, MRClusterNode *n) {

  dictAdd(m->hosts, n->endpoint.host, NULL);

  char addr[ADDRESS_LENGTH];
  snprintf(addr, ADDRESS_LENGTH, "%s:%d", n->endpoint.host, n->endpoint.port);
  dictReplace(m->nodes, addr, n);
}
