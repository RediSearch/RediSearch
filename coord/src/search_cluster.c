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
  return sc->size;
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

static const char *getUntaggedId(const char *id, size_t *outlen) {
  const char *openBrace = rindex(id, '{');
  if (openBrace) {
    *outlen = openBrace - id;
  } else {
    *outlen = strlen(id);
  }
  return id;
}

static const char *lookupAlias(const char *orig, size_t *len) {
  // borrowing the global reference to the spec
  StrongRef global = IndexAlias_Get(orig);
  IndexSpec *sp = StrongRef_Get(global);
  if (!sp) {
    *len = strlen(orig);
    return orig;
  }
  return getUntaggedId(sp->name, len);
}

/* Get the next multiplexed command for spellcheck command. Return 1 if we are not done, else 0 */
int SpellCheckMuxIterator_Next(void *ctx, MRCommand *cmd) {
  SCCommandMuxIterator *it = ctx;
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(it->cluster)) return 0;

  /* at end */
  if (it->offset >= it->cluster->size) {
    return 0;
  }

  *cmd = MRCommand_Copy(it->cmd);

  cmd->targetSlot = GetSlotByPartition(&it->cluster->part, it->offset++);

  MRCommand_AppendArgsAtPos(cmd, 3, 1, "FULLSCOREINFO");

  return 1;
}

int NoPartitionCommandMuxIterator_Next(void *ctx, MRCommand *cmd) {
  SCCommandMuxIterator *it = ctx;
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(it->cluster)) return 0;

  /* at end */
  if (it->offset >= it->cluster->size) {
    return 0;
  }

  *cmd = MRCommand_Copy(it->cmd);
  if (it->keyOffset >= 0 && it->keyOffset < it->cmd->num) {
    if (it->keyAlias) {
      MRCommand_ReplaceArg(cmd, it->keyOffset, it->keyAlias, strlen(it->keyAlias));
    }
  }

  cmd->targetSlot = it->cluster->shardsStartSlots[it->offset++];

  return 1;
}

/* Return the size of the command generator */
size_t SCCommandMuxIterator_Len(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  return it->cluster->size;
}

size_t NoPartitionCommandMuxIterator_Len(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  return it->cluster->size;
}

void SCCommandMuxIterator_Free(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  if (it->cmd) MRCommand_Free(it->cmd);
  it->cmd = NULL;
  rm_free(it->keyAlias);
  rm_free(it);
}

void NoPartitionCommandMuxIterator_Free(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  if (it->cmd) MRCommand_Free(it->cmd);
  it->cmd = NULL;
  rm_free(it->keyAlias);
  rm_free(it);
}

MRCommandGenerator noPartitionCommandGenerator = {.Next = NoPartitionCommandMuxIterator_Next,
                                              .Free = NoPartitionCommandMuxIterator_Free,
                                              .Len = NoPartitionCommandMuxIterator_Len,
                                              .ctx = NULL};

MRCommandGenerator spellCheckCommandGenerator = {.Next = SpellCheckMuxIterator_Next,
                                                 .Free = SCCommandMuxIterator_Free,
                                                 .Len = SCCommandMuxIterator_Len,
                                                 .ctx = NULL};

MRCommandGenerator SearchCluster_GetCommandGenerator(SCCommandMuxIterator *mux, MRCommand *cmd) {
  MRCommandGenerator *ptr = MRCommand_GetCommandGenerator(cmd);
  MRCommandGenerator ret;
  if (ptr) {
    ret = *ptr;
  } else {
    ret = noPartitionCommandGenerator;
  }
  ret.ctx = mux;
  return ret;
}

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
MRCommandGenerator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd) {

  SCCommandMuxIterator *mux = rm_malloc(sizeof(SCCommandMuxIterator));
  *mux = (SCCommandMuxIterator){
      .cluster = c, .cmd = cmd, .keyOffset = MRCommand_GetShardingKey(cmd),
      .offset = 0, .keyAlias = NULL};
  if (MRCommand_GetFlags(cmd) & MRCommand_Aliased) {
    if (mux->keyOffset > 0 && mux->keyOffset < cmd->num) {
      size_t newlen = 0;
      const char *target = lookupAlias(cmd->strs[mux->keyOffset], &newlen);
      if (strcmp(cmd->strs[mux->keyOffset], target) != 0) {
        mux->keyAlias = rm_strndup(target, newlen);
      }
    }
  }
  return SearchCluster_GetCommandGenerator(mux, cmd);
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

void SetMyPartition(MRClusterTopology *ct, MRClusterShard *myShard) {
  SearchCluster *c = GetSearchCluster();
  for (size_t i = 0; i < c->size; ++i) {
    int slot = GetSlotByPartition(&c->part, i);
    if (myShard->startSlot <= slot && myShard->endSlot >= slot) {
      c->myPartition = i;
      return;
    }
  }
}
