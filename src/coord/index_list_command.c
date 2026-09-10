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
#include "rmutil/strings.h"
#include "triemap_ffi.h"
#include "util/arr/arr.h"

#define FT_LIST_CS_KEY_INDEX "index"
#define FT_LIST_CS_KEY_STATUS "status"
#define FT_LIST_CS_KEY_WARNING "warning"
#define FT_LIST_CS_KEY_MISSING "missing_from_shards"
#define FT_LIST_CS_KEY_UNREACHABLE "unreachable_shards"
#define FT_LIST_CS_STATUS_OK "ok"

// Reads rdbcompression into *enabled; false if unreadable. getRedisConfigBool() folds
// a failed read into its default, losing that distinction.
static bool readRdbCompression(RedisModuleCtx *ctx, bool *enabled) {
  int value = 0;
  if (RedisModule_ConfigGetBool(ctx, "rdbcompression", &value) != REDISMODULE_OK) {
    return false;
  }

  *enabled = value != 0;
  return true;
}

// Shard-side _FT._LIST; WITHCLUSTERSTATE replies the diagnostic payload the reducer consumes.
int IndexListInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc > 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc < 2) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx);
    Indexes_List(&_reply, false);
    return REDISMODULE_OK;
  }

  // argc == 2
  if (!RMUtil_StringEqualsCaseC(argv[1], "WITHCLUSTERSTATE"))
    return RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_ARG_UNRECOGNIZED));

  bool rdbCompression = false;  // An unreadable setting is reported as incomparable
  const bool comparable = readRdbCompression(ctx, &rdbCompression);
  const char *nodeId = MR_GetLocalNodeId();

  RedisModule_Reply _reply = RedisModule_NewReply(ctx);
  Indexes_ReplyWithClusterStatePayload(&_reply, nodeId, SchemaFingerprint_Recipe(rdbCompression),
                                       comparable);
  MR_ReleaseLocalNodeIdReadLock();
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
  long long fp;
} ClusterStateFingerprint;

// Per-index accumulator for IndexListClusterStateReducer.
typedef struct {
  arrayof(uint32_t) presentSlots;  // indices into ClusterStateReports.shardIds
  arrayof(ClusterStateFingerprint) fps;
  size_t noFingerprint;
} ClusterStateIndexInfo;

static void ClusterStateIndexInfo_Free(void *p) {
  ClusterStateIndexInfo *info = p;
  array_free(info->presentSlots);
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

static bool slotInArray(uint32_t slot, const uint32_t *slots, uint32_t n) {
  for (uint32_t i = 0; i < n; i++) {
    if (slots[i] == slot) {
      return true;
    }
  }

  return false;
}

// The shard payloads folded into one picture. Strings are borrowed from the
// replies, which outlive the reducer call.
typedef struct {
  TrieMap *byName;  // index name -> ClusterStateIndexInfo
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

  if (!r || MRReply_Type(r) != MR_REPLY_ARRAY || MRReply_Length(r) != 4) {
    return;
  }

  const MRReply *recipe = MRReply_ArrayElement(r, 1);
  const MRReply *encVer = MRReply_ArrayElement(r, 2);
  const MRReply *entries = MRReply_ArrayElement(r, 3);
  if (MRReply_Type(recipe) != MR_REPLY_INTEGER || MRReply_Type(encVer) != MR_REPLY_INTEGER ||
      MRReply_Type(entries) != MR_REPLY_ARRAY)
    return;

  // Validate every entry before taking a slot: skipping just the bad one would count
  // the shard as reporting while dropping an index it listed, reading as missing.
  const size_t nEntries = MRReply_Length(entries);
  for (size_t j = 0; j < nEntries; j++) {
    const MRReply *pair = MRReply_ArrayElement(entries, j);
    if (MRReply_Type(pair) != MR_REPLY_ARRAY || MRReply_Length(pair) != 2) {
      return;
    }
  }

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
    size_t nameLen = 0;
    const char *name = MRReply_String(MRReply_ArrayElement(pair, 0), &nameLen);
    // Name may already be NUL-truncated by the payload, and tm_len_t narrows again;
    // either can merge two distinct indexes into one entry.
    const tm_len_t keyLen = (tm_len_t)nameLen;
    if (!name || !keyLen) {
      continue;
    }

    ClusterStateIndexInfo *info = TrieMap_Find(reports->byName, name, keyLen);
    if (info == TRIEMAP_NOTFOUND) {
      info = rm_calloc(1, sizeof(*info));
      TrieMap_Add(reports->byName, (char *)name, keyLen, info, NULL);
    }
    info->presentSlots = array_ensure_append_1(info->presentSlots, slot);
    // MRReply_Integer skips the type check; hiredis callocs replies, so a non-integer
    // would read as 0 and compare equal to every other one.
    const MRReply *fp = MRReply_ArrayElement(pair, 1);
    if (MRReply_Type(fp) == MR_REPLY_INTEGER) {
      const ClusterStateFingerprint reported = {.group = group, .fp = MRReply_Integer(fp)};
      info->fps = array_ensure_append_1(info->fps, reported);
    } else {
      info->noFingerprint++;
    }
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
    if (slotInArray(slot, info->presentSlots, array_len(info->presentSlots))) {
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
// each other. Which shards are "wrong" isn't decidable, so this is a count only.
static uint32_t distinctSchemaCount(const ClusterStateIndexInfo *info, uint32_t nGroups) {
  const uint32_t n = array_len(info->fps);
  uint32_t worst = 0;
  for (uint32_t g = 0; g < nGroups; g++) {
    uint32_t distinct = 0;
    for (uint32_t i = 0; i < n; i++) {
      if (info->fps[i].group != g) {
        continue;
      }

      bool seen = false;
      for (uint32_t j = 0; j < i; j++) {
        if (info->fps[j].group == g && info->fps[j].fp == info->fps[i].fp) {
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
                                   arrayof(const char *) missing,
                                   arrayof(const char *) unreachable) {
  RedisModule_Reply_Map(reply);
  RedisModule_ReplyKV_StringBuffer(reply, FT_LIST_CS_KEY_INDEX, name, nameLen);
  if (!verdict) {
    RedisModule_ReplyKV_SimpleString(reply, FT_LIST_CS_KEY_STATUS, FT_LIST_CS_STATUS_OK);
  } else {
    RedisModule_ReplyKV_Map(reply, FT_LIST_CS_KEY_STATUS);
    replyClusterStateWarning(reply, verdict);
    replyShardIds(reply, FT_LIST_CS_KEY_MISSING, missing);
    replyShardIds(reply, FT_LIST_CS_KEY_UNREACHABLE, unreachable);
    RedisModule_Reply_MapEnd(reply);
  }
  RedisModule_Reply_MapEnd(reply);
}

// Uses the same lossy C-string name form as the shard payload, so single- and
// multi-shard replies render an index identically.
static void replySpecStatusOk(IndexSpec *sp, void *ud) {
  RedisModule_Reply *reply = ud;
  const char *name = IndexSpec_FormatName(sp, false);
  replyClusterStateEntry(reply, name, strlen(name), NULL, NULL, NULL);
}

// Reducer for FT._LIST WITHCLUSTERSTATE: one map per index across the shards'
// lists. Divergence the replies prove is reported even when shards are silent.
static int IndexListClusterStateReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  ClusterStateReports reports = {
      .byName = NewTrieMap(),
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
    TrieMap_Free(reports.byName, ClusterStateIndexInfo_Free);
    array_free(reports.shardIds);
    array_free(reports.gateGroups);
    return RedisModule_ReplyWithError(ctx, err);
  }

  // Shards the fanout asked, not current topology - one that joined since would
  // wrongly show as not having replied.
  size_t expectedCount = 0;
  const char **expectedIds = MRCtx_GetShardNodeIds(mc, &expectedCount);
  arrayof(const char *) unreachableIds = array_new(const char *, expectedCount);
  for (size_t i = 0; i < expectedCount; i++)
    if (!idInArray(expectedIds[i], reports.shardIds, array_len(reports.shardIds)))
      array_append(unreachableIds, expectedIds[i]);

  // Shards asked with no usable payload back, including rejections (their error took
  // no slot). Keeps nSilent below from underflowing: never fewer than the rejection tally.
  const size_t nNotReporting = array_len(unreachableIds);
  // Rejections are alive shards, so subtracted from silent rather than counted as silent.
  const size_t nSilent = nNotReporting - reports.nRejected;
  // >1 gate group means shards disagree on the recipe; some fingerprints aren't comparable.
  const bool versionSkew = array_len(reports.gateGroups) > 1;
  const bool uncertain = nNotReporting > 0 || versionSkew;

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  RedisModule_Reply_Array(reply);

  TrieMapIterator *it = TrieMap_Iterate(reports.byName);
  char *name;
  tm_len_t nameLen;
  void *ptr;
  while (TrieMapIterator_Next(it, &name, &nameLen, &ptr)) {
    const ClusterStateIndexInfo *info = ptr;
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
      replyClusterStateEntry(reply, name, nameLen, &verdict, missing, unreachableIds);
    } else {
      replyClusterStateEntry(reply, name, nameLen, NULL, NULL, NULL);
    }

    array_free(missing);
  }
  TrieMapIterator_Free(it);

  RedisModule_Reply_ArrayEnd(reply);
  RedisModule_EndReply(reply);

  TrieMap_Free(reports.byName, ClusterStateIndexInfo_Free);
  array_free(reports.shardIds);
  array_free(reports.gateGroups);
  array_free(unreachableIds);
  return REDISMODULE_OK;
}

// FT._LIST on the coordinator. The no-token form precedes every cluster and
// blocking check: it must keep working inside MULTI/Lua and when the cluster is down.
int IndexListCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc > 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc == 2 && !RMUtil_StringEqualsCaseC(argv[1], "WITHCLUSTERSTATE"))
    return RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_ARG_UNRECOGNIZED));

  if (argc == 1) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx);
    Indexes_List(&_reply, false);
    return REDISMODULE_OK;
  }

  if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }

  RS_AutoMemory(ctx);

  if (GetNumShards_UnSafe() == 1) {
    // Nothing to disagree with, so every local index is trivially consistent.
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    RedisModule_Reply_Array(reply);
    Indexes_ForEachSpec(replySpecStatusOk, reply);
    RedisModule_Reply_ArrayEnd(reply);
    RedisModule_EndReply(reply);
    return REDISMODULE_OK;
  }

  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  MRCommand_SetPrefix(&cmd, "_FT");
  struct MRCtx *mrctx = MR_CreateCtx(ctx, 0, NULL, GetNumShards_UnSafe());
  // The reducer names the shards that did not reply, so it needs the ones asked.
  MRCtx_CaptureShardNodeIds(mrctx);
  MR_Fanout(mrctx, IndexListClusterStateReducer, cmd, true);
  return REDISMODULE_OK;
}
