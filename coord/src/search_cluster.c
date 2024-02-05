/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "search_cluster.h"
#include "partition.h"
#include "alias.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

SearchCluster NewSearchCluster(size_t size, const char **table, size_t tableSize) {
  SearchCluster ret = (SearchCluster){.size = size, .shardsStartSlots=NULL,};
  PartitionCtx_Init(&ret.part, size, table, tableSize);
  if(size){
    // assume slots are equally distributed
    ret.shardsStartSlots = rm_malloc(size * sizeof *ret.shardsStartSlots);
    for(size_t j = 0, i = 0; j < size; j++, i+=((tableSize+size-1)/size)){
      ret.shardsStartSlots[j] = i;
    }
  }
  return ret;
}

SearchCluster __searchCluster;

SearchCluster *GetSearchCluster() {
  return &__searchCluster;
}

void InitGlobalSearchCluster(size_t size, const char **table, size_t tableSize) {
  __searchCluster = NewSearchCluster(size, table, tableSize);
}

void SearchCluster_Release(SearchCluster *sc) {
  if (!sc->shardsStartSlots) return;
  rm_free(sc->shardsStartSlots);
  sc->shardsStartSlots = NULL;
}

inline int SearchCluster_Ready(SearchCluster *sc) {
  return sc->size != 0;
}

inline size_t SearchCluster_Size(SearchCluster *sc) {
  return sc->size;
}

inline int SearchCluster_GetSlotByPartition(SearchCluster *sc, size_t partition) {
  return sc->shardsStartSlots[partition];
}

char* getConfigValue(RedisModuleCtx *ctx, const char* confName){
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "config", "cc", "get", confName);
  RedisModule_Assert(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY);
  if (RedisModule_CallReplyLength(rep) == 0){
    RedisModule_FreeCallReply(rep);
    return NULL;
  }
  RedisModule_Assert(RedisModule_CallReplyLength(rep) == 2);
  RedisModuleCallReply *valueRep = RedisModule_CallReplyArrayElement(rep, 1);
  RedisModule_Assert(RedisModule_CallReplyType(valueRep) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char* valueRepCStr = RedisModule_CallReplyStringPtr(valueRep, &len);

  char* res = rm_calloc(1, len + 1);
  memcpy(res, valueRepCStr, len);

  RedisModule_FreeCallReply(rep);

  return res;
}

int checkTLS(char** client_key, char** client_cert, char** ca_cert, char** key_pass){
  int ret = 1;
  RedisModuleCtx *ctx = RSDummyContext;
  RedisModule_ThreadSafeContextLock(ctx);
  char* clusterTls = NULL;
  char* tlsPort = NULL;

  clusterTls = getConfigValue(ctx, "tls-cluster");
  if (!clusterTls || strcmp(clusterTls, "yes")) {
    tlsPort = getConfigValue(ctx, "tls-port");
    if (!tlsPort || !strcmp(tlsPort, "0")) {
      ret = 0;
      goto done;
    }
  }

  *client_key = getConfigValue(ctx, "tls-key-file");
  *client_cert = getConfigValue(ctx, "tls-cert-file");
  *ca_cert = getConfigValue(ctx, "tls-ca-cert-file");
  *key_pass = getConfigValue(ctx, "tls-key-file-pass");

  if (!*client_key || !*client_cert || !*ca_cert){
    ret = 0;
    if(*client_key){
      rm_free(*client_key);
    }
    if(*client_cert){
      rm_free(*client_cert);
    }
    if(*ca_cert){
      rm_free(*client_cert);
    }
  }

done:
  if (clusterTls) {
    rm_free(clusterTls);
  }
  if (tlsPort) {
    rm_free(tlsPort);
  }
  RedisModule_ThreadSafeContextUnlock(ctx);
  return ret;
}

/* Make sure that the cluster either has a size or updates its size from the topology when updated.
 * If the user did not define the number of partitions, we just take the number of shards in the
 * first topology update and get a fix on that */
void SearchCluster_EnsureSize(RedisModuleCtx *ctx, SearchCluster *c, MRClusterTopology *topo) {
  // If the cluster doesn't have a size yet - set the partition number aligned to the shard number
  if (MRClusterTopology_IsValid(topo)) {
    RedisModule_Log(ctx, "debug", "Setting number of partitions to %ld", topo->numShards);
    c->size = topo->numShards;
    c->shardsStartSlots = rm_realloc(c->shardsStartSlots, c->size * sizeof *c->shardsStartSlots);
    for(size_t i = 0 ; i < c->size ; ++i){
      c->shardsStartSlots[i] = topo->shards[i].startSlot;
    }
    PartitionCtx_SetSize(&c->part, topo->numShards);
  }
}
