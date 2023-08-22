/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "node.h"
#include "triemap/triemap.h"
#include "triemap/triemap.h"

#include <stdlib.h>

void MRNodeMapIterator_Free(MRNodeMapIterator *it) {
  TrieMapIterator_Free(it->iter);
}

MRClusterNode *_nmi_allNext(MRNodeMapIterator *it) {
  char *str;
  tm_len_t len;
  void *p;
  if (!TrieMapIterator_Next(it->iter, &str, &len, &p)) {
    return NULL;
  }
  return p;
}

MRClusterNode *_nmi_randomNext(MRNodeMapIterator *it) {
  char *host;
  tm_len_t len;
  void *p;
  if (!TrieMapIterator_Next(it->iter, &host, &len, &p)) {
    return NULL;
  }
  int retries = 0;
  MRClusterNode *n = NULL;
  while (retries < 3) {
    n = TrieMap_RandomValueByPrefix(it->m->nodes, host, len);
    if (!n) break;
    // do not select the same node as excluded
    if (n->flags & MRNode_Self) {
      retries++;
      continue;
    }
    break;
  }
  return n;
}
MRNodeMapIterator MRNodeMap_IterateAll(MRNodeMap *m) {
  return (MRNodeMapIterator){
      .Next = _nmi_allNext, .m = m, .excluded = NULL, .iter = TrieMap_Iterate(m->nodes, "", 0)};
}

MRNodeMapIterator MRNodeMap_IterateHost(MRNodeMap *m, const char *host) {
  return (MRNodeMapIterator){.Next = _nmi_allNext,
                             .m = m,
                             .excluded = NULL,
                             .iter = TrieMap_Iterate(m->nodes, host, strlen(host))};
}
MRNodeMapIterator MRNodeMap_IterateRandomNodePerhost(MRNodeMap *m, MRClusterNode *excludeNode) {
  return (MRNodeMapIterator){.Next = _nmi_randomNext,
                             .m = m,
                             .excluded = excludeNode,
                             .iter = TrieMap_Iterate(m->hosts, "", 0)};
}

void *_node_replace(void *oldval, void *newval) {
  return newval;
}

void _nodemap_free(void *ptr) {
  // do not delete anything - the object is allocated elsewhere
}

void MRNodeMap_Free(MRNodeMap *m) {
  TrieMap_Free(m->hosts, NULL);
  TrieMap_Free(m->nodes, _nodemap_free);
  rm_free(m);
}

/* Return 1 both nodes have the same host */
int MRNode_IsSameHost(MRClusterNode *n, MRClusterNode *other) {
  if (!n || !other) return 0;
  return strcasecmp(n->endpoint.host, other->endpoint.host) == 0;
}

size_t MRNodeMap_NumHosts(MRNodeMap *m) {
  return m->hosts->cardinality;
}

size_t MRNodeMap_NumNodes(MRNodeMap *m) {
  return m->nodes->cardinality;
}

MRNodeMap *MR_NewNodeMap() {
  MRNodeMap *m = rm_malloc(NULL, sizeof(*m));
  m->hosts = NewTrieMap(NULL);
  m->nodes = NewTrieMap(NULL);
  return m;
}

void MRNodeMap_Add(MRNodeMap *m, MRClusterNode *n) {

  TrieMap_Add(m->hosts, n->endpoint.host, strlen(n->endpoint.host), NULL, NULL);

  char *addr;
  __ignore__(rm_asprintf(&addr, "%s:%d", n->endpoint.host, n->endpoint.port));
  TrieMap_Add(m->nodes, addr, strlen(addr), n, _node_replace);
  rm_free(addr);
}
