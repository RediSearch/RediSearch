/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "node.h"

#include <stdlib.h>
#include <strings.h>
#include "rmalloc.h"

void MRNodeMapIterator_Free(MRNodeMapIterator *it) {
  RS_SubTrieIterator_Free(it->iter);
}

MRClusterNode *_nmi_allNext(MRNodeMapIterator *it) {
  char *str;
  size_t len;
  void *p;
  if (!RS_SubTrieIterator_Next(it->iter, &str, &len, &p)) {
    return NULL;
  }
  return p;
}

MRClusterNode *_nmi_randomNext(MRNodeMapIterator *it) {
  char *host;
  size_t len;
  void *p;
  if (!RS_SubTrieIterator_Next(it->iter, &host, &len, &p)) {
    return NULL;
  }

  // return the first, this code is not even been used
  // todo: remove this code
  char *node;
  size_t node_len;
  MRClusterNode *n = NULL;
  RS_SubTrieIterator *iter = RS_TrieMap_Find(it->m->nodes, host, len);
  RS_SubTrieIterator_Next(iter, &node, &node_len, (void**)&n);
  RS_SubTrieIterator_Free(iter);
  return n;
}
MRNodeMapIterator MRNodeMap_IterateAll(MRNodeMap *m) {
  return (MRNodeMapIterator){
      .Next = _nmi_allNext, .m = m, .excluded = NULL, .iter = RS_TrieMap_Find(m->nodes, "", 0)};
}

MRNodeMapIterator MRNodeMap_IterateHost(MRNodeMap *m, const char *host) {
  return (MRNodeMapIterator){.Next = _nmi_allNext,
                             .m = m,
                             .excluded = NULL,
                             .iter = RS_TrieMap_Find(m->nodes, host, strlen(host))};
}
MRNodeMapIterator MRNodeMap_IterateRandomNodePerhost(MRNodeMap *m, MRClusterNode *excludeNode) {
  return (MRNodeMapIterator){.Next = _nmi_randomNext,
                             .m = m,
                             .excluded = excludeNode,
                             .iter = RS_TrieMap_Find(m->hosts, "", 0)};
}

void *_node_replace(void *oldval, void *newval) {
  return newval;
}

void MRNodeMap_Free(MRNodeMap *m) {
  RS_TrieMap_Free(m->hosts, NULL);
  RS_TrieMap_Free(m->nodes, NULL);
  rm_free(m);
}

/* Return 1 both nodes have the same host */
int MRNode_IsSameHost(MRClusterNode *n, MRClusterNode *other) {
  if (!n || !other) return 0;
  return strcasecmp(n->endpoint.host, other->endpoint.host) == 0;
}

size_t MRNodeMap_NumHosts(MRNodeMap *m) {
  return RS_TrieMap_Size(m->hosts);
}

size_t MRNodeMap_NumNodes(MRNodeMap *m) {
  return RS_TrieMap_Size(m->nodes);
}

MRNodeMap *MR_NewNodeMap() {
  MRNodeMap *m = rm_malloc(sizeof(*m));
  m->hosts = RS_NewTrieMap();
  m->nodes = RS_NewTrieMap();
  return m;
}

void MRNodeMap_Add(MRNodeMap *m, MRClusterNode *n) {

  RS_TrieMap_Add(m->hosts, n->endpoint.host, strlen(n->endpoint.host), (void*)1);

  char *addr;
  __ignore__(rm_asprintf(&addr, "%s:%d", n->endpoint.host, n->endpoint.port));
  RS_TrieMap_Add(m->nodes, addr, strlen(addr), n);
  rm_free(addr);
}
