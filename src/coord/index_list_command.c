/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "index_list_command.h"

#include <stdbool.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#include "hiredis/sds.h"
#include "indexes.h"
#include "module.h"
#include "query_error_ffi.h"
#include "reply.h"
#include "rmalloc.h"
#include "rmr/reply.h"
#include "rmr/rmr.h"
#include "util/dict/dict.h"
#include "util/arr/arr.h"

#define FT_LIST_CS_KEY_INDEX "index"
#define FT_LIST_CS_KEY_STATUS "status"
#define FT_LIST_CS_KEY_WARNING "warning"
#define FT_LIST_CS_KEY_MISSING "missing_from_shards"
#define FT_LIST_CS_KEY_UNREACHABLE "unreachable_shards"
#define FT_LIST_CS_KEY_SCHEMA_GROUPS "schema_groups"
#define FT_LIST_CS_STATUS_OK "ok"

typedef struct {
  arrayof(char *) shardNodeIds;
} IndexListRequest;

static void clearShardNodeIds(IndexListRequest *request) {
  array_free_ex(request->shardNodeIds, rm_free(*(char **)ptr));
  request->shardNodeIds = NULL;
}

static void captureShardNodeIds(struct MRCtx *ctx, const MRClusterTopology *topology) {
  IndexListRequest *request = MRCtx_GetPrivData(ctx);
  clearShardNodeIds(request);
  if (!topology) {
    return;
  }
  request->shardNodeIds = array_new(char *, topology->numShards);
  for (uint32_t i = 0; i < topology->numShards; ++i) {
    array_append(request->shardNodeIds, rm_strdup(topology->shards[i].node.id));
  }
}

static void freeIndexListRequest(struct MRCtx *ctx) {
  IndexListRequest *request = MRCtx_GetPrivData(ctx);
  clearShardNodeIds(request);
  rm_free(request);
}

struct MRCtx *IndexList_CreateRequest(RedisModuleCtx *ctx, int replyCap) {
  IndexListRequest *request = rm_malloc(sizeof(*request));
  *request = (IndexListRequest){0};
  struct MRCtx *mc = MR_CreateCtx(ctx, NULL, request, replyCap);
  MRCtx_SetBeforeFanoutCB(mc, captureShardNodeIds);
  MRCtx_SetFreePrivDataCB(mc, freeIndexListRequest);
  return mc;
}

int IndexList_ReplyLocalPayload(RedisModuleCtx *ctx) {
  char *nodeId = MR_DuplicateLocalNodeId();
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  Indexes_ReplyWithClusterStatePayload(&reply, nodeId);
  rm_free(nodeId);
  return REDISMODULE_OK;
}

// A shard's comparability gates. Fingerprints are only comparable within one group.
typedef struct {
  long long recipe;
  long long encVer;
} ClusterStateGates;

// One reported fingerprint, tagged with the gate group that produced it.
typedef struct {
  uint32_t group;
  uint32_t slot;
  long long fp;
  bool valid;
} ClusterStateFingerprint;

// Per-index accumulator for IndexListClusterStateReducer.
typedef struct {
  arrayof(ClusterStateFingerprint) fps;
  size_t noFingerprint;
} ClusterStateIndexInfo;

static void ClusterStateIndexInfo_Free(void *unused, void *p) {
  ClusterStateIndexInfo *info = p;
  array_free(info->fps);
  rm_free(info);
}

static bool idInArray(const char *id, const char **ids, uint32_t n) {
  for (uint32_t i = 0; i < n; i++) {
    if (!strcmp(ids[i], id)) {
      return true;
    }
  }

  return false;
}

// Keys borrow the name reply, including its length, for the reducer's lifetime.
static uint64_t indexNameHash(const void *key) {
  size_t len;
  const char *name = MRReply_String(key, &len);
  RS_ASSERT(len <= INT_MAX);
  return dictGenHashFunction(name, len);
}

static int indexNameEqual(void *unused, const void *left, const void *right) {
  size_t leftLen, rightLen;
  const char *a = MRReply_String(left, &leftLen);
  const char *b = MRReply_String(right, &rightLen);
  return leftLen == rightLen && !memcmp(a, b, leftLen);
}

static dictType indexNames = {
    .hashFunction = indexNameHash,
    .keyCompare = indexNameEqual,
    .valDestructor = ClusterStateIndexInfo_Free,
};

// The shard payloads folded into one picture. Strings are borrowed from the
// replies, which outlive the reducer call.
typedef struct {
  dict *byName;  // index name -> ClusterStateIndexInfo
  // One slot per usable payload: shard's node id, or "" if none. Slots, not ids, are
  // the key, so an unnamed shard still counts.
  arrayof(const char *) shardIds;
  size_t nRejected;
  MRReply *firstError;
  // Distinct gate pairs seen; >1 means shards disagree on the recipe and some
  // fingerprints are incomparable. Grouped rather than compared against one reference,
  // so the result doesn't depend on reply order.
  arrayof(ClusterStateGates) gateGroups;
} ClusterStateReports;

static void ClusterStateReports_Clear(ClusterStateReports *reports) {
  dictRelease(reports->byName);
  array_free(reports->shardIds);
  array_free(reports->gateGroups);
}

// Peer data is a runtime boundary, not an assertion boundary. Validate the
// complete payload before counting this shard as reporting any indexes.
static bool validShardPayload(const MRReply *reply) {
  if (!reply || MRReply_Type(reply) != MR_REPLY_ARRAY || MRReply_Length(reply) != 4) {
    return false;
  }
  const MRReply *id = MRReply_ArrayElement(reply, 0);
  if (MRReply_Type(id) != MR_REPLY_STRING && MRReply_Type(id) != MR_REPLY_STATUS) {
    return false;
  }
  if (MRReply_Type(MRReply_ArrayElement(reply, 1)) != MR_REPLY_INTEGER ||
      MRReply_Type(MRReply_ArrayElement(reply, 2)) != MR_REPLY_INTEGER) {
    return false;
  }
  const MRReply *entries = MRReply_ArrayElement(reply, 3);
  if (MRReply_Type(entries) != MR_REPLY_ARRAY) {
    return false;
  }
  for (size_t i = 0; i < MRReply_Length(entries); ++i) {
    const MRReply *pair = MRReply_ArrayElement(entries, i);
    if (MRReply_Type(pair) != MR_REPLY_ARRAY || MRReply_Length(pair) != 2) {
      return false;
    }
    const MRReply *name = MRReply_ArrayElement(pair, 0);
    const MRReply *fp = MRReply_ArrayElement(pair, 1);
    size_t nameLen;
    MRReply_String(name, &nameLen);
    if (MRReply_Type(name) != MR_REPLY_STRING || nameLen > INT_MAX ||
        (MRReply_Type(fp) != MR_REPLY_INTEGER && MRReply_Type(fp) != MR_REPLY_NIL)) {
      return false;
    }
  }
  return true;
}

// Folds one shard payload into the picture. An error or malformed reply gets no
// slot, so it reads as non-reporting rather than divergence.
static void ClusterStateReports_AddShard(ClusterStateReports *reports, MRReply *r) {
  if (r && MRReply_Type(r) == MR_REPLY_ERROR) {
    reports->nRejected++;
    if (!reports->firstError) {
      reports->firstError = r;
    }
    return;
  }

  if (!validShardPayload(r)) {
    return;
  }
  const MRReply *recipe = MRReply_ArrayElement(r, 1);
  const MRReply *encVer = MRReply_ArrayElement(r, 2);
  const MRReply *entries = MRReply_ArrayElement(r, 3);
  const size_t nEntries = MRReply_Length(entries);

  size_t idLen = 0;
  const char *id = MRReply_String(MRReply_ArrayElement(r, 0), &idLen);
  array_append(reports->shardIds, id && idLen ? id : "");
  const uint32_t slot = array_len(reports->shardIds) - 1;

  const ClusterStateGates gates = {.recipe = MRReply_Integer(recipe),
                                   .encVer = MRReply_Integer(encVer)};
  uint32_t group = array_len(reports->gateGroups);
  for (uint32_t g = 0; g < array_len(reports->gateGroups); g++) {
    if (reports->gateGroups[g].recipe == gates.recipe &&
        reports->gateGroups[g].encVer == gates.encVer) {
      group = g;
      break;
    }
  }

  if (group == array_len(reports->gateGroups))
    reports->gateGroups = array_ensure_append_1(reports->gateGroups, gates);

  for (size_t j = 0; j < nEntries; j++) {
    const MRReply *pair = MRReply_ArrayElement(entries, j);
    const MRReply *name = MRReply_ArrayElement(pair, 0);
    ClusterStateIndexInfo *info = dictFetchValue(reports->byName, name);
    if (!info) {
      info = rm_malloc(sizeof(*info));
      *info = (ClusterStateIndexInfo){0};
      int added = dictAdd(reports->byName, (void *)name, info);
      RS_ASSERT(added == DICT_OK);
    }
    const MRReply *fp = MRReply_ArrayElement(pair, 1);
    const bool valid = MRReply_Type(fp) == MR_REPLY_INTEGER;
    const ClusterStateFingerprint reported = {
        .group = group, .slot = slot, .fp = valid ? MRReply_Integer(fp) : 0, .valid = valid};
    info->fps = array_ensure_append_1(info->fps, reported);
    info->noFingerprint += !valid;
  }
}

// Shards that reported but did not list this index. *count is how many; the
// returned array names only those with a node id, so it may be shorter.
static arrayof(const char *) shardsMissingIndex(const ClusterStateIndexInfo *info,
                                                const ClusterStateReports *reports, size_t *count) {
  const uint32_t nSlots = array_len(reports->shardIds);
  arrayof(const char *) missing = array_new(const char *, nSlots);
  *count = 0;
  for (uint32_t slot = 0; slot < nSlots; slot++) {
    bool present = false;
    for (uint32_t i = 0; i < array_len(info->fps); ++i) {
      present |= info->fps[i].slot == slot;
    }
    if (present) {
      continue;
    }

    (*count)++;
    if (reports->shardIds[slot][0]) {
      array_append(missing, reports->shardIds[slot]);
    }
  }
  return missing;
}

// Most distinct schemas held by any one gate-agreeing group for this index. Per-group
// counting keeps the result order-independent: incomparable groups never count against
// each other. No group is designated as the authoritative schema.
static uint32_t distinctSchemaCount(const ClusterStateIndexInfo *info, uint32_t nGroups) {
  const uint32_t n = array_len(info->fps);
  uint32_t worst = 0;
  for (uint32_t g = 0; g < nGroups; g++) {
    uint32_t distinct = 0;
    for (uint32_t i = 0; i < n; i++) {
      if (!info->fps[i].valid || info->fps[i].group != g) {
        continue;
      }

      bool seen = false;
      for (uint32_t j = 0; j < i; j++) {
        if (info->fps[j].valid && info->fps[j].group == g && info->fps[j].fp == info->fps[i].fp) {
          seen = true;
          break;
        }
      }
      distinct += !seen;
    }
    if (distinct > worst) {
      worst = distinct;
    }
  }
  return worst;
}

typedef struct {
  uint32_t gateGroup;
  long long fingerprint;
  arrayof(const char *) nodeIds;
} ClusterStateSchemaGroup;

// Fingerprints establish agreement only within the same comparison gates.
static arrayof(ClusterStateSchemaGroup)
    schemaGroups(const ClusterStateIndexInfo *info, const ClusterStateReports *reports) {
  arrayof(ClusterStateSchemaGroup) groups = NULL;
  for (uint32_t i = 0; i < array_len(info->fps); ++i) {
    const ClusterStateFingerprint *fp = &info->fps[i];
    const char *id = reports->shardIds[fp->slot];
    if (!fp->valid || !id[0]) {
      continue;
    }
    uint32_t group = 0;
    while (group < array_len(groups) &&
           (groups[group].gateGroup != fp->group || groups[group].fingerprint != fp->fp)) {
      ++group;
    }
    if (group == array_len(groups)) {
      const ClusterStateSchemaGroup newGroup = {.gateGroup = fp->group, .fingerprint = fp->fp};
      groups = array_ensure_append_1(groups, newGroup);
    }
    if (!idInArray(id, groups[group].nodeIds, array_len(groups[group].nodeIds))) {
      groups[group].nodeIds = array_ensure_append_1(groups[group].nodeIds, id);
    }
  }
  return groups;
}

static void replySchemaGroups(RedisModule_Reply *reply, arrayof(ClusterStateSchemaGroup) groups) {
  RedisModule_ReplyKV_Array(reply, FT_LIST_CS_KEY_SCHEMA_GROUPS);
  for (uint32_t i = 0; i < array_len(groups); ++i) {
    RedisModule_Reply_Array(reply);
    for (uint32_t j = 0; j < array_len(groups[i].nodeIds); ++j) {
      RedisModule_Reply_SimpleString(reply, groups[i].nodeIds[j]);
    }
    RedisModule_Reply_ArrayEnd(reply);
  }
  RedisModule_Reply_ArrayEnd(reply);
}

static void replyShardIds(RedisModule_Reply *reply, const char *key, arrayof(const char *) ids) {
  const uint32_t n = array_len(ids);
  if (n == 0) {
    return;
  }

  RedisModule_ReplyKV_Array(reply, key);
  for (uint32_t i = 0; i < n; i++) {
    RedisModule_Reply_SimpleString(reply, ids[i]);
  }
  RedisModule_Reply_ArrayEnd(reply);
}

// Inputs to one index's non-"ok" verdict message.
typedef struct {
  size_t nMissing;    // reporting shards that did not list the index
  uint32_t nSchemas;  // distinct schemas among the comparable reports
  size_t noFingerprint;
  size_t reportingShards;
  size_t nSilent;
  size_t nRejected;
  size_t expectedShards;  // shards the fanout asked; the denominator of the two above
  const char *errorText;
  bool versionSkew;
} ClusterStateVerdict;

// Renders the operator-facing warning. Both clauses are appended, not
// alternatives: a silent shard must not hide a skew between answering shards.
static void replyClusterStateWarning(RedisModule_Reply *reply, const ClusterStateVerdict *v) {
  sds msg = sdsnew(INCONSISTENT_INDEX_STATE);
  const size_t markerLen = sdslen(msg);

  if (v->nMissing > 0 || v->nSchemas > 1) {
    msg = sdscat(msg, ": ");
    if (v->nMissing > 0)
      msg = sdscatprintf(msg, "index is missing from %zu of %zu reporting shards", v->nMissing,
                         v->reportingShards);
    if (v->nMissing > 0 && v->nSchemas > 1) {
      msg = sdscat(msg, ", and ");
    }
    if (v->nSchemas > 1)
      msg = sdscatprintf(msg, "the shards that have it hold %u different schemas", v->nSchemas);
    msg = sdscat(msg, ". Drop the index and recreate it so that all shards agree.");
  }

  if (v->nSilent > 0 || v->nRejected > 0 || v->versionSkew || v->noFingerprint > 0) {
    msg = sdscat(msg, sdslen(msg) > markerLen ? " The rest of the picture cannot be determined: "
                                              : " cannot be determined: ");
    const char *sep = "";
    if (v->nSilent > 0) {
      msg = sdscatprintf(msg, "%s%zu of %zu shards did not reply", sep, v->nSilent,
                         v->expectedShards);
      sep = "; ";
    }
    if (v->nRejected > 0) {
      msg = sdscatprintf(msg, "%s%zu of %zu shards rejected the request", sep, v->nRejected,
                         v->expectedShards);
      if (v->errorText) {
        // Unescaped is safe: replied as a bulk string. The cap bounds size, not sanitizes.
        msg = sdscatprintf(msg, " (%.200s)", v->errorText);
      }
      sep = "; ";
    }
    if (v->versionSkew) {
      msg = sdscatprintf(msg, "%sshards are running incompatible versions or configurations", sep);
      sep = "; ";
    }
    if (v->noFingerprint > 0)
      msg =
          sdscatprintf(msg, "%s%zu of the reporting shards could not compute a schema fingerprint",
                       sep, v->noFingerprint);
    msg = sdscat(msg, ".");
  }

  RedisModule_ReplyKV_StringBuffer(reply, FT_LIST_CS_KEY_WARNING, msg, sdslen(msg));
  sdsfree(msg);
}

// One {index, status} entry of FT._LIST WITHCLUSTERSTATE, and the only producer of
// it - the single-shard path and the reducer can't drift. NULL verdict means consistent.
static void replyClusterStateEntry(RedisModule_Reply *reply, const char *name, size_t nameLen,
                                   const ClusterStateVerdict *verdict,
                                   arrayof(const char *) missing, arrayof(const char *) unreachable,
                                   arrayof(ClusterStateSchemaGroup) groups) {
  RedisModule_Reply_Map(reply);
  RedisModule_ReplyKV_StringBuffer(reply, FT_LIST_CS_KEY_INDEX, name, nameLen);
  if (!verdict) {
    RedisModule_ReplyKV_SimpleString(reply, FT_LIST_CS_KEY_STATUS, FT_LIST_CS_STATUS_OK);
  } else {
    RedisModule_ReplyKV_Map(reply, FT_LIST_CS_KEY_STATUS);
    replyClusterStateWarning(reply, verdict);
    if (verdict->nSchemas > 1) {
      replySchemaGroups(reply, groups);
    }
    replyShardIds(reply, FT_LIST_CS_KEY_MISSING, missing);
    replyShardIds(reply, FT_LIST_CS_KEY_UNREACHABLE, unreachable);
    RedisModule_Reply_MapEnd(reply);
  }
  RedisModule_Reply_MapEnd(reply);
}

static void replySpecStatusOk(IndexSpec *sp, void *ud) {
  RedisModule_Reply *reply = ud;
  size_t nameLen;
  const char *name = HiddenString_GetUnsafe(sp->specName, &nameLen);
  replyClusterStateEntry(reply, name, nameLen, NULL, NULL, NULL, NULL);
}

// Reducer for FT._LIST WITHCLUSTERSTATE: one map per index across the shards'
// lists. Divergence the replies prove is reported even when shards are silent.
int IndexListClusterStateReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  ClusterStateReports reports = {
      .byName = dictCreate(&indexNames, NULL),
      .shardIds = array_new(const char *, count),
  };
  for (int i = 0; i < count; i++) {
    ClusterStateReports_AddShard(&reports, replies[i]);
  }

  // Empty would read as "no indexes exist"; reply an error instead - the shards'
  // own if one arrived, else a fanout failure.
  const size_t nReporting = array_len(reports.shardIds);
  if (nReporting == 0) {
    const char *err = reports.firstError
                          ? MRReply_String(reports.firstError, NULL)
                          : QueryError_Strerror(QUERY_ERROR_CODE_CLUSTER_NO_RESPONSES);
    ClusterStateReports_Clear(&reports);
    return RedisModule_ReplyWithError(ctx, err);
  }

  // Shards the fanout asked, not current topology - one that joined since would
  // wrongly show as not having replied.
  const IndexListRequest *request = MRCtx_GetPrivData(mc);
  const size_t expectedCount = array_len(request->shardNodeIds);
  const char *const *expectedIds = (const char *const *)request->shardNodeIds;
  arrayof(const char *) unreachableIds = array_new(const char *, expectedCount);
  for (size_t i = 0; i < expectedCount; i++)
    if (!idInArray(expectedIds[i], reports.shardIds, array_len(reports.shardIds)))
      array_append(unreachableIds, expectedIds[i]);

  // Shards asked with no usable payload back, including rejections (their error took
  // no slot). Keeps nSilent below from underflowing: never fewer than the rejection tally.
  const size_t nNotReporting = array_len(unreachableIds);
  // Rejections are alive shards, so subtracted from silent rather than counted as silent.
  RS_ASSERT(nReporting + reports.nRejected <= expectedCount);
  RS_ASSERT(nNotReporting >= reports.nRejected);
  const size_t nSilent = nNotReporting - reports.nRejected;
  // >1 gate group means shards disagree on the recipe; some fingerprints aren't comparable.
  const bool versionSkew = array_len(reports.gateGroups) > 1;
  const bool uncertain = nNotReporting > 0 || versionSkew;

  // An empty union cannot carry a per-index warning about an unobserved shard.
  if (nNotReporting > 0 && dictSize(reports.byName) == 0) {
    RedisModule_ReplyWithError(ctx, INCONSISTENT_INDEX_STATE
                               " cannot be determined: incomplete shard reports; "
                               "the index list may be incomplete.");
    ClusterStateReports_Clear(&reports);
    array_free(unreachableIds);
    return REDISMODULE_OK;
  }

  // Error replies have no node identity. Naming all absent IDs would incorrectly
  // label reachable rejecting shards as unreachable, including in mixed failures.
  const bool canNameUnreachable = reports.nRejected == 0;

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  RedisModule_Reply_Array(reply);

  dictIterator *it = dictGetIterator(reports.byName);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    size_t nameLen;
    const char *name = MRReply_String(dictGetKey(entry), &nameLen);
    const ClusterStateIndexInfo *info = dictGetVal(entry);
    size_t nMissing = 0;
    arrayof(const char *) missing = shardsMissingIndex(info, &reports, &nMissing);
    const uint32_t nSchemas = distinctSchemaCount(info, array_len(reports.gateGroups));
    const bool inconsistent = nMissing > 0 || nSchemas > 1;

    if (inconsistent || uncertain || info->noFingerprint > 0) {
      const ClusterStateVerdict verdict = {
          .nMissing = nMissing,
          .nSchemas = nSchemas,
          .noFingerprint = info->noFingerprint,
          .reportingShards = nReporting,
          .nSilent = nSilent,
          .nRejected = reports.nRejected,
          .expectedShards = expectedCount,
          .errorText = reports.firstError ? MRReply_String(reports.firstError, NULL) : NULL,
          .versionSkew = versionSkew,
      };
      arrayof(ClusterStateSchemaGroup) groups = nSchemas > 1 ? schemaGroups(info, &reports) : NULL;
      replyClusterStateEntry(reply, name, nameLen, &verdict, missing,
                             canNameUnreachable ? unreachableIds : NULL, groups);
      array_free_ex(groups, array_free(((ClusterStateSchemaGroup *)ptr)->nodeIds));
    } else {
      replyClusterStateEntry(reply, name, nameLen, NULL, NULL, NULL, NULL);
    }

    array_free(missing);
  }
  dictReleaseIterator(it);

  RedisModule_Reply_ArrayEnd(reply);
  RedisModule_EndReply(reply);

  ClusterStateReports_Clear(&reports);
  array_free(unreachableIds);
  return REDISMODULE_OK;
}

int IndexList_ReplySingleShard(RedisModuleCtx *ctx) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_Array(&reply);
  Indexes_ForEachSpec(replySpecStatusOk, &reply);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_EndReply(&reply);
  return REDISMODULE_OK;
}
